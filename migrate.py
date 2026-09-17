"""MarkLogic -> Snowflake migration driver, built one step at a time.

  python migrate.py stat     [--all]              documents / collections / formats per database
  python migrate.py fetch    --out docs.csv       every document URI + ID + created date -> CSV
  python migrate.py fetch    --no-ids             ... URIs only, without opening each document
  python migrate.py fetch    --exclude-migrated   ... leaving out what 'load' has marked done
  python migrate.py bench    --sample 500         measure read speed, project the full run
  python migrate.py sfcheck  [--write]            can we reach Snowflake with these settings?
  python migrate.py extract  --uri U              that document, its versions and its files
                                                  -> <stage>/<documentuuid>/ on local disk
  python migrate.py load     --uri U              mark it migrated in its properties (no write yet)

Nothing here knows any particular document schema. The ID and creation date are
found by name, from ML_ID_PATH / ML_CREATED_PATH in .env.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import time
import sys
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any

import requests

from config import ML, OUTPUT_DIR, SF, STAGE
from marklogic import MarkLogicClient, MarkLogicError, find_key
from snowflake_loader import check_connection, reach_check


def client() -> MarkLogicClient:
    kerberos = ML.AUTH == "kerberos"
    return MarkLogicClient(
        host=ML.HOST, port=ML.PORT,
        user="" if kerberos else ML.user(),
        password="" if kerberos else ML.password(),
        database=ML.DATABASE, auth=ML.AUTH, scheme=ML.SCHEME,
    )


def selector_from_args(args) -> dict:
    sel: dict = {}
    if getattr(args, "collection", None):
        sel["collection"] = args.collection
    if getattr(args, "directory", None):
        sel["directory"] = args.directory
    if getattr(args, "query", None):
        sel["q"] = args.query
    if ML.DATABASE:
        sel["database"] = ML.DATABASE
    return sel


# ---------- commands ----------

def database_stat(cli, name: str, sample: int, top: int) -> None:
    print(f"=== {name} ===")
    db = cli.for_database(name)
    try:
        total = db.count({})
    except MarkLogicError as exc:
        print(f"  (cannot read: {exc})")
        print()
        return
    print(f"  documents  : {total:,}")
    if not total:
        print()
        return

    try:
        colls = db.collections()
        print(f"  collections: {len(colls)}")
        counts = sorted(((c, db.count({"collection": c})) for c in colls),
                        key=lambda cn: -cn[1])
        for coll, n in counts[:top]:
            print(f"    {coll:<40} {n:>12,}")
        if len(counts) > top:
            print(f"    ... and {len(counts) - top} more")
    except MarkLogicError as exc:
        print(f"  collections: (cannot list: {exc})")

    shown = min(sample, total)
    kinds: dict[str, int] = {}
    for row in islice(db.iter_uri_rows({}, page_length=min(sample, 1000)), shown):
        kind = row.get("mimetype") or row.get("format") or "(unknown)"
        kinds[kind] = kinds.get(kind, 0) + 1
    print(f"  formats    : (sample of {shown:,} documents)")
    for kind, n in sorted(kinds.items(), key=lambda kn: -kn[1]):
        share = 100 * n / shown if shown else 0
        print(f"    {kind:<40} {n:>12,}  {share:5.1f}%")
    print()


def database_summary(cli, names: list[str]) -> None:
    """One line per database: totals only, no per-collection detail."""
    print(f"{'DATABASE':<30} {'DOCUMENTS':>14} {'COLLECTIONS':>12}")
    print("-" * 58)
    rows = []
    for name in names:
        db = cli.for_database(name)
        try:
            docs = db.count({})
        except MarkLogicError:
            rows.append((name, None, None))
            continue
        try:
            colls = len(db.collections()) if docs else 0
        except MarkLogicError:
            colls = None
        rows.append((name, docs, colls))

    for name, docs, colls in sorted(rows, key=lambda r: -(r[1] or 0)):
        if docs is None:
            print(f"{name:<30} {'(no access)':>14} {'':>12}")
        else:
            print(f"{name:<30} {docs:>14,} {('?' if colls is None else f'{colls:,}'):>12}")
    print()
    print("Details for one database:  python migrate.py stat --database <name>")


def cmd_stat(args) -> int:
    cli = client()
    print(f"MarkLogic at {cli.base}")
    print()
    if args.database:
        for name in args.database:
            database_stat(cli, name, args.sample, args.top)
        return 0
    if args.all:
        try:
            names = cli.databases()
        except MarkLogicError as exc:
            print(f"Cannot list databases ({exc}); showing {ML.DATABASE!r} instead.")
            database_stat(cli, ML.DATABASE, args.sample, args.top)
            return 0
        database_summary(cli, names)
        return 0
    database_stat(cli, ML.DATABASE, args.sample, args.top)
    return 0


def fetch_fields(args) -> dict[str, str]:
    return {"id": args.id_path or "", "created": args.created_path or ""}


def cmd_fetch(args) -> int:
    cli = client()
    # True = only migrated, False = only what is left, None = both.
    args.migrated = True if args.only_migrated else (False if args.exclude_migrated else None)
    if args.no_ids:
        args.id_path = args.created_path = None
    fields = fetch_fields(args)
    reading = [path for path in fields.values() if path]
    selector = selector_from_args(args)
    total = cli.count(selector, migrated=args.migrated)
    # Stamped, so a second run never overwrites the first listing. An explicit
    # --out is taken as given.
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = (Path(args.out) if args.out
           else OUTPUT_DIR / f"documents-{safe_name(ML.DATABASE)}-{stamp}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    limit = min(args.limit or total, total)
    sort_by_created = bool(args.created_path) and not args.no_sort

    print(f"  Database : {ML.DATABASE} at {cli.base}")
    print(f"  Matched  : {total:,} document{'' if total == 1 else 's'}"
          + (f", writing the first {limit:,}" if limit < total else ""))
    print(f"  Output   : {out}")
    print()

    header = ["uri", "migrated", "mimetype", "format", "database"]
    if reading:
        header[2:2] = ["document_id", "id_source", "created", "version", "version_of", "linked_record"]

    lines: list[list] = []
    rows = islice(cli.iter_uri_rows(selector, page_length=args.page_size,
                                    migrated=args.migrated,
                                    # a filter fixes the flag for every row, and
                                    # the field lookup already carries it otherwise.
                                    with_migrated=not reading and args.migrated is None), limit)
    fh = out.open("w", encoding="utf-8", newline="")
    writer = csv.writer(fh)
    writer.writerow(header)
    written = 0
    while True:
        page = list(islice(rows, args.page_size))
        if not page:
            break
        info = (cli.document_fields([r["uri"] for r in page], fields,
                                    link=args.link_path, workers=args.workers)
                if reading else {})
        for row in page:
            found = info.get(row["uri"], {}) if reading else row
            flag = args.migrated if args.migrated is not None else found.get("migrated")
            line = [row["uri"], "Yes" if flag else "No",
                    row.get("mimetype", ""), row.get("format", ""), ML.DATABASE]
            if reading:
                values, srcs = found.get("values", {}), found.get("sources", {})
                line[2:2] = [values.get("id") or "", srcs.get("id", ""),
                             values.get("created") or "", found.get("version", ""),
                             found.get("version_of", ""), found.get("record", "")]
            if sort_by_created:
                lines.append(line)
            else:
                writer.writerow(line)
        written += len(page)
        if limit > args.page_size:                # one page needs no progress
            print(f"  {written:,}/{limit:,}")

    if sort_by_created:
        # Oldest first; rows without a date go last, in URI order.
        created_col = header.index("created")
        lines.sort(key=lambda ln: (ln[created_col] == "", ln[created_col], ln[0]))
        writer.writerows(lines)
    fh.close()
    print(f"Wrote {written:,} row{'' if written == 1 else 's'}"
          + (", oldest first." if sort_by_created else " in URI order."))
    return 0


def human_time(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f} seconds"
    if seconds < 5400:
        return f"{seconds / 60:.1f} minutes"
    if seconds < 172800:
        return f"{seconds / 3600:.1f} hours"
    return f"{seconds / 86400:.1f} days"


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}"
        n /= 1024


def read_timing(cli, uris: list[str], workers: int, chunk: int):
    """Read the given URIs; return (seconds, documents, bytes, failures_by_reason)."""
    started = time.perf_counter()
    docs = byte_count = 0
    reasons: dict[str, int] = {}
    for src in cli.read_documents(uris, chunk_size=chunk, workers=workers):
        if src.error:
            reason = src.error.split(":")[0].strip()[:60]
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        docs += 1
        byte_count += src.size
    return time.perf_counter() - started, docs, byte_count, reasons


def cmd_bench(args) -> int:
    cli = client()
    selector = selector_from_args(args)
    total = cli.count(selector)
    if total == 0:
        print("No documents matched that selector.")
        return 1

    sample = min(args.sample, total)
    print(f"{total:,} documents match; timing a sample of {sample:,}...")
    listing_started = time.perf_counter()
    uris = [row["uri"] for row in islice(cli.iter_uri_rows(selector), sample)]
    listing = time.perf_counter() - listing_started
    print(f"Step 1 - listing {len(uris):,} URIs: {listing:.1f}s "
          f"({len(uris) / listing:,.0f} URIs/s)")
    print()
    print(f"Step 2 - downloading those {len(uris):,} documents (content + properties + hash):")

    worker_counts = [1, 4, 8, 16] if args.compare else [args.workers]
    best = None
    last_reasons: dict[str, int] = {}
    for workers in worker_counts:
        seconds, docs, byte_count, reasons = read_timing(cli, uris, workers, args.chunk)
        failures = sum(reasons.values())
        last_reasons = reasons or last_reasons
        if not docs:
            print(f"  workers {workers:>3}: every document failed ({failures})")
            continue
        rate = docs / seconds
        print(f"  workers {workers:>3}: {seconds:6.1f}s  {rate:8,.1f} docs/s  "
              f"{human_bytes(byte_count / seconds)}/s  avg {human_bytes(byte_count / docs)}"
              + (f"  ({failures} failed)" if failures else ""))
        if best is None or rate > best[0]:
            best = (rate, byte_count / docs, workers, docs, failures)

    if last_reasons:
        total_failed = sum(last_reasons.values())
        print()
        print(f"{total_failed:,} of {len(uris):,} sampled documents could not be read:")
        for reason, n in sorted(last_reasons.items(), key=lambda rn: -rn[1]):
            print(f"  {n:>6,}  {reason}")

    if best is None:
        return 1
    rate, avg_size, workers, read_ok, failures = best
    share = read_ok / (read_ok + failures) if (read_ok + failures) else 1.0
    projected = int(total * share)
    print()
    print(f"Best: {rate:,.1f} docs/s with {workers} workers "
          f"(chunk {args.chunk}, one process)")
    print(f"  10,000 documents : {human_time(10000 / rate)}")
    label = f"  {projected:,} documents : "
    print(f"{label}{human_time(projected / rate)}"
          f"  ({human_bytes(projected * avg_size)} to transfer)")
    if share < 1.0:
        print(f"  ... that is the {share * 100:.0f}% of {total:,} this reader handles today;")
        print("      the rest (see reasons above) still need their own path.")
    print()
    print("Reading only - loading into Snowflake is extra. Running several")
    print("processes in parallel scales this further if MarkLogic allows it.")
    return 0


def cmd_sfcheck(args) -> int:
    host = SF.hostname()
    if not host and not os.getenv("SF_ACCOUNT", "").strip():
        print("Set SF_HOST (or SF_ACCOUNT) in .env first.")
        return 1
    if not host:
        host = f"{SF.account()}.snowflakecomputing.com"

    print(f"Reaching {host}"
          + (f" through proxy {SF.PROXY_HOST}:{SF.PROXY_PORT or 8080}" if SF.PROXY_HOST else "")
          + ":")
    reachable = True
    for name, ok, detail in reach_check(host, int(SF.PORT or 443), SF.PROXY_HOST, SF.PROXY_PORT):
        print(f"  [{'ok ' if ok else 'FAIL'}] {name:<48} {detail}")
        reachable = reachable and ok
    print()
    if not reachable:
        print("Cannot reach Snowflake. Check the host name, the proxy settings, "
              "and whether you are on the company network or VPN.")
        return 1
    if args.reach_only:
        return 0

    try:
        settings = SF.conn_kwargs()
    except SystemExit as exc:
        print(f"Endpoint is reachable. Cannot log in yet: {exc}")
        return 0

    print(f"Connecting to Snowflake by {SF.auth_method()}:")
    for key, value in settings.items():
        if key == "password":
            value = f"(set, {len(value)} characters)"
        elif key == "private_key":
            value = f"(loaded, {len(value)} bytes)"
        print(f"  {key:<14}: {value}")
    print()

    try:
        info = check_connection(settings, write_test=args.write)
    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}")
        print()
        print(snowflake_hint(exc))
        return 1

    print("Connected. The session resolves to:")
    for key, value in info.items():
        print(f"  {key:<14}: {value or '(none)'}")
    missing = [k for k in ("warehouse", "database") if not info.get(k)]
    if missing:
        print()
        print(f"Warning: {', '.join(missing)} resolved to nothing - "
              "check the name, or that your role has access to it.")
        return 1
    return 0


def snowflake_hint(exc: Exception) -> str:
    text = str(exc).lower()
    if "incorrect username or password" in text or "250001" in text:
        return ("If your Snowflake login goes through SSO, a password will not work: "
                "set SF_AUTHENTICATOR=externalbrowser in .env (a browser window opens), "
                "or ask for a service account.")
    if "does not exist" in text and "warehouse" in text:
        return "SF_WAREHOUSE does not exist or your role cannot use it."
    if "object does not exist" in text or "does not exist or not authorized" in text:
        return "Check SF_DATABASE / SF_SCHEMA / SF_ROLE - the name may be wrong, or the role lacks access."
    if "could not connect" in text or "getaddrinfo" in text or "timed out" in text:
        return ("Network problem: check SF_ACCOUNT (e.g. abcd-xy12345), your VPN, "
                "and whether a proxy or firewall allows *.snowflakecomputing.com.")
    if "authenticator" in text or "saml" in text:
        return "Check SF_AUTHENTICATOR; for company SSO it is usually 'externalbrowser'."
    return "Check the SF_* settings in .env against what you use to log in to Snowflake."


def folder_name(uri: str, doc: dict | None) -> tuple[str, str]:
    """The folder a document belongs in: its own ID, else the UUID in its URI."""
    if doc is not None and ML.ID_PATH:
        found = find_key(doc, ML.ID_PATH.split(".")[-1])
        if found:
            return safe_name(str(found)), ML.ID_PATH
    return safe_name(Path(uri).stem), "URI"


# Reserved on Windows whatever the extension; harmless names on Linux.
WINDOWS_DEVICES = {"con", "prn", "aux", "nul",
                   *(f"com{n}" for n in range(1, 10)),
                   *(f"lpt{n}" for n in range(1, 10))}


def safe_name(name: str) -> str:
    """A document ID or URI turned into one path component.

    The same tree has to be writable on Linux and on Windows, so this strips
    what either one forbids: separators, control characters, the Windows
    reserved characters and device names, and trailing dots or spaces.
    """
    # Both separators and the characters Windows forbids become _; control
    # characters are dropped; Windows also rejects a trailing dot or space.
    cleaned = re.sub(r'[<>:"|?*/\\]+', "_", name)
    cleaned = "".join(ch for ch in cleaned if ch >= " ").strip(". ")
    if cleaned.split(".")[0].lower() in WINDOWS_DEVICES:
        cleaned = "_" + cleaned
    return cleaned[:150] or "unnamed"


def write_file(path: Path, payload: bytes | str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    path.write_bytes(payload)
    return len(payload)


def member_names(uris: list[str]) -> dict[str, str]:
    """MarkLogic URI -> the name it is stored under, exactly as MarkLogic has it.

    Each file keeps its own basename. Two documents in different MarkLogic
    directories can share one - the envelope /GDXUI/envelope/<uuid>.json and its
    DOM /DOM/<uuid>.json - and so can two names that differ only in case, which
    Windows would treat as one file. Those keep their MarkLogic directory as a
    subfolder, so every name stays exactly what it was.
    """
    names: dict[str, str] = {}
    taken: set[str] = set()
    for uri in uris:                          # master first, so it keeps the top
        base = safe_name(Path(uri).name)
        name = base
        if name.lower() in taken:
            parent = [safe_name(part) for part in Path(uri).parent.parts
                      if part not in ("/", "\\")]
            name = "/".join([*parent, base])
        names[uri] = name
        taken.add(name.lower())
    return names


def extract_one(cli, uri: str, root: Path) -> dict:
    """Write one document's whole family into <root>/<documentuuid>/.

    The document, every version of it, and whatever it points at - each under
    the name MarkLogic stores it by, with its properties XML beside it.
    """
    family = cli.family(uri, links=ML.LINK_PATH)
    master = family["master"]
    if not master.get("exists"):
        return {"uri": uri, "error": "document not found"}

    # The master first, then the versions oldest first, then what it points at.
    members: list[dict] = [master]
    members += family.get("versions") or []
    members += family.get("links") or []

    names = member_names([m["uri"] for m in members])
    content: dict[str, Any] = {}
    files: list[tuple[str, int]] = []
    problems: list[str] = []
    staged: dict[str, bytes | str] = {}

    for member in members:
        name = names[member["uri"]]
        if not member.get("exists"):
            problems.append(f"{member['uri']}: not found")
            continue
        binary = member["kind"] == "binary" and not member.get("binary_json")
        if binary:
            # Byte-for-byte, whatever it is - a PDF is not text.
            staged[name] = cli.get_bytes(member["uri"])
        else:
            src = next(iter(cli.read_documents([member["uri"]])))
            if src.error:
                problems.append(f"{member['uri']}: {src.error}")
                continue
            if not src.hash_ok:
                problems.append(f"{member['uri']}: hash mismatch, ML {src.ml_hash} "
                                f"!= local {src.local_hash}")
                continue
            staged[name] = src.text
            if member is master:
                content = src.content()
        # MarkLogic's own <prop:properties>, as it serializes it - namespaces and
        # all. Converting it to JSON would drop the prefixes that tell dls:version
        # apart from anyone else's version element. Named after the file it
        # describes, extension included, so the pairing is unambiguous.
        properties = member.get("properties_xml") or ""
        if properties:
            staged[f"{name}.properties.xml"] = properties

    name_from, id_source = folder_name(master["uri"], content or None)
    folder = root / name_from
    for name, payload in staged.items():
        files.append((name, write_file(folder / name, payload)))

    return {
        "uri": master["uri"],
        "folder": folder,
        "document_id": name_from,
        "id_source": id_source,
        "files": sorted(files),
        "versions": len(family.get("versions") or []),
        "links": len(family.get("links") or []),
        "problems": problems,
    }


def cmd_extract(args) -> int:
    """Write documents to local disk in the shape the Snowflake stage expects.

    One folder per document, named by the document's ID, holding the document,
    every version of it, whatever it points at, and a .properties.json beside
    each one. What lands here is what goes up.
    """
    cli = client()
    root = Path(args.out) if args.out else STAGE.DIR
    print(f"  Documents: {len(args.uri)}")
    print(f"  Output   : {root}")
    if SF.STAGE:
        try:
            print(f"  Destined : {STAGE.snowflake_path('<documentuuid>')}")
        except SystemExit as exc:            # say so, but still write the files
            print(f"  Destined : unresolved - {exc}")
    print()

    failed = 0
    for uri in args.uri:
        result = extract_one(cli, uri, root)
        if result.get("error"):
            failed += 1
            print(f"  FAILED  {uri}: {result['error']}")
            continue
        print(f"  {result['uri']}")
        print(f"    document id : {result['document_id']}  (from {result['id_source']})")
        print(f"    versions    : {result['versions']}"
              + (f",  linked documents: {result['links']}" if result["links"] else ""))
        print(f"    folder      : {result['folder']}{os.sep}")
        for name, size in result["files"]:
            print(f"      {name:<42} {size:>12,} bytes")
        if result["problems"]:
            failed += 1
            for problem in result["problems"]:
                print(f"      not written: {problem}")
    print()
    print(f"Extracted {len(args.uri) - failed} of {len(args.uri)} document(s).")
    return 1 if failed else 0


def cmd_load(args) -> int:
    """Read the given documents and mark them migrated in their properties.

    The Snowflake write itself is not built yet; what this does today is the
    bookkeeping around it, so a later run can skip what is already done.
    """
    cli = client()
    ok, unreadable = [], []
    for src in cli.read_documents(args.uri, workers=args.workers):
        if not src.error and not src.hash_ok:
            src.error = f"hash mismatch in transfer: ML {src.ml_hash} != local {src.local_hash}"
        if src.error:
            unreadable.append((src.uri, src.error))
        else:
            ok.append(src.uri)

    for uri, error in unreadable:
        print(f"  skipped  {uri}\n           {error}")
    if not ok:
        print(f"Nothing to mark: {len(unreadable)} document(s) could not be read.")
        return 1

    print(f"Loading into Snowflake is not built yet; marking {len(ok)} document(s) migrated.")
    failed = 0
    for result in cli.mark_migrated(ok, migrated=not args.unmark, workers=args.workers):
        if result.get("error"):
            failed += 1
            print(f"  FAILED   {result['uri']}\n           {result['error']}")
        else:
            print(f"  marked   {result['uri']}  "
                  f"migrated={result['migrated']} at {result['timestamp']}")
    print(f"Marked {len(ok) - failed} of {len(args.uri)} document(s); "
          f"{failed + len(unreadable)} not marked.")
    return 1 if (failed or unreadable) else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    st = sub.add_parser("stat", help="documents, collections and formats per database")
    st.add_argument("--database", action="append", help="database to inspect (repeatable)")
    st.add_argument("--all", action="store_true", help="every database on the server")
    st.add_argument("--sample", type=int, default=1000, help="documents to sample for formats")
    st.add_argument("--top", type=int, default=25, help="collections to show per database")
    st.set_defaults(fn=cmd_stat)

    fe = sub.add_parser("fetch", help="write every document URI to a CSV file")
    fe.add_argument("--out", help="CSV path (default output/documents-<database>.csv)")
    fe.add_argument("--collection")
    fe.add_argument("--directory", help="e.g. /gds/ (must end with /)")
    fe.add_argument("--query", help="MarkLogic string query")
    fe.add_argument("--limit", type=int, help="stop after N documents")
    fe.add_argument("--page-size", type=int, default=1000)
    fe.add_argument("--id-path", default=ML.ID_PATH or None,
                    help="field holding the document ID (default: ML_ID_PATH in .env)")
    fe.add_argument("--created-path", default=ML.CREATED_PATH or None,
                    help="field holding the creation date (default: ML_CREATED_PATH)")
    fe.add_argument("--link-path", default=ML.LINK_PATH or None,
                    help="fields in a record pointing at related documents, comma-separated (default: ML_LINK_PATH)")
    fe.add_argument("--no-ids", action="store_true",
                    help="URIs only - skip reading IDs and dates, much faster")
    migrated = fe.add_mutually_exclusive_group()
    migrated.add_argument("--exclude-migrated", action="store_true",
                          help="leave out documents 'load' has already marked migrated")
    migrated.add_argument("--only-migrated", action="store_true",
                          help="list only the documents already marked migrated")
    fe.add_argument("--no-sort", action="store_true",
                    help="keep URI order instead of sorting oldest first")
    fe.add_argument("--workers", type=int, default=8)
    fe.set_defaults(fn=cmd_fetch)

    bm = sub.add_parser("bench", help="measure MarkLogic read speed and project the full run")
    bm.add_argument("--collection")
    bm.add_argument("--directory", help="e.g. /gds/ (must end with /)")
    bm.add_argument("--query", help="MarkLogic string query")
    bm.add_argument("--sample", type=int, default=200, help="documents to read (default 200)")
    bm.add_argument("--workers", type=int, default=8)
    bm.add_argument("--chunk", type=int, default=100, help="documents per server call")
    bm.add_argument("--compare", action="store_true", help="try 1, 4, 8 and 16 workers")
    bm.set_defaults(fn=cmd_bench)

    sc = sub.add_parser("sfcheck", help="test the Snowflake connection")
    sc.add_argument("--reach-only", action="store_true",
                    help="only test network reachability, do not try to log in")
    sc.add_argument("--write", action="store_true",
                    help="also create, insert into and drop a temporary table")
    sc.set_defaults(fn=cmd_sfcheck)

    ex = sub.add_parser("extract",
                        help="a document, its versions and its files -> one folder per document")
    ex.add_argument("--uri", action="append", required=True,
                    help="document URI to extract (repeatable)")
    ex.add_argument("--out", help=f"staging root (default: STAGE_DIR, now {STAGE.DIR})")
    ex.set_defaults(fn=cmd_extract)

    ld = sub.add_parser("load", help="mark documents migrated (the Snowflake write comes later)")
    ld.add_argument("--uri", action="append", required=True,
                    help="document URI to load (repeatable)")
    ld.add_argument("--unmark", action="store_true",
                    help="write migrated=false instead, to redo a document")
    ld.add_argument("--workers", type=int, default=8)
    ld.set_defaults(fn=cmd_load)

    args = p.parse_args()
    try:
        return args.fn(args)
    except MarkLogicError as exc:
        print(f"MarkLogic error: {exc}")
        return 1
    except requests.exceptions.ConnectionError:
        print(f"Cannot reach MarkLogic at {ML.SCHEME}://{ML.HOST}:{ML.PORT}.")
        print("Check that the server is running and that ML_HOST / ML_PORT in .env are right.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
