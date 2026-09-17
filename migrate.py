"""MarkLogic -> Snowflake migration driver, built one step at a time.

  python migrate.py stat     [--all]              documents / collections / formats per database
  python migrate.py fetch    --out docs.csv       every document URI + ID + created date -> CSV
  python migrate.py fetch    --no-ids             ... URIs only, without opening each document
  python migrate.py fetch    --exclude-migrated   ... leaving out what 'load' has marked done
  python migrate.py bench    --sample 500         measure read speed, project the full run
  python migrate.py sfcheck  [--write]            can we reach Snowflake with these settings?
  python migrate.py family   --uri U | --id I     its versions, linked files and timestamps (read-only)
  python migrate.py extract  --uri U | --id I     that document, its versions and its files
                                                  -> <stage>/<documentuuid>/ on local disk
  python migrate.py load     --uri U | --id I     extract it, upload to @stage/<documentuuid>/,
                                                  then mark it migrated in its properties

Nothing here knows any particular document schema. The ID and creation date are
found by name, from ML_ID_PATH / ML_CREATED_PATH in .env.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
import sys
import tempfile
from datetime import datetime
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any

import requests

from config import ML, OUTPUT_DIR, SF, STAGE
from marklogic import MIGRATION_NS, MarkLogicClient, MarkLogicError, find_key, sha256_hex
from snowflake_loader import (CONTROL_WIDTHS, HASH_FUNCTION_FILE, check_connection,
                              check_control_table, connect, control_rows_sql,
                              hash_function_exists, hash_function_sql, merge_control_rows,
                              reach_check, stage_sha256, upload_folder)


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
        info = check_connection(settings, stage=SF.stage() if args.write else "")
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


# Written into every document folder: each file's name, source URI, size and
# SHA-256. The leading _ keeps it apart from anything MarkLogic stores.
MANIFEST = "_manifest.json"


def parse_time(value: str) -> datetime | None:
    """A MarkLogic dateTime ('2026-09-23T18:06:05.467708Z', any number of
    fraction digits, Z or an offset) as an aware datetime; None if unreadable."""
    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)?",
                     (value or "").strip())
    if not m:
        return None
    fraction = (m.group(2) or "").ljust(6, "0")[:6]
    zone = m.group(3) or "Z"
    return datetime.fromisoformat(f"{m.group(1)}.{fraction}"
                                  f"{'+00:00' if zone == 'Z' else zone}")


def matched_refs(envelope: dict, is_master: bool, links: list[dict]) -> dict:
    """Which version of each linked file belongs with this envelope version.

    The current envelope goes with the current file. A version copy of the
    envelope goes with the version of the file that was current when that
    envelope version was created: the one whose Library Services created..replaced
    window holds the envelope's created time (no replaced = still current).
    Returns {"refs": field -> URI, "ref_notes": [...]} for the control table.
    """
    by_uri = {link["uri"]: link for link in links}
    refs, notes = {}, []
    when = parse_time(envelope.get("created", ""))
    for field, uri in (envelope.get("refs") or {}).items():
        link = by_uri.get(uri)
        copies = (link or {}).get("versions") or []
        if is_master or not copies:
            refs[field] = uri
            continue
        match = None
        if when is not None:
            for copy in copies:
                start, end = parse_time(copy.get("created", "")), parse_time(copy.get("replaced", ""))
                if start is not None and start <= when and (end is None or when < end):
                    match = copy
        if match:
            refs[field] = match["uri"]
        else:
            refs[field] = uri
            notes.append(f"no version of {field} matches this envelope's created time "
                         f"{envelope.get('created') or '(none)'}; recorded {uri} as named")
    return {"refs": refs, "ref_notes": notes}


def with_marklogic_hash(properties_xml: str, sha256: str) -> str:
    """The properties XML with the SHA-256 MarkLogic computed for the file it
    describes added as the last child, in ml2sf's own namespace so it cannot
    be mistaken for a property of the source system."""
    if not sha256:
        return properties_xml
    end = properties_xml.rstrip().rfind("</")
    element = f'<marklogic-sha256 xmlns="{MIGRATION_NS}">{sha256}</marklogic-sha256>'
    return properties_xml[:end] + element + properties_xml[end:]


def extract_one(cli, uri: str, root: Path) -> dict:
    """Write one document's whole family into <root>/<documentuuid>/.

    The document, every version of it, and whatever it points at - each under
    the name MarkLogic stores it by, with its properties XML beside it.
    """
    family = cli.family(uri, links=ML.link_fields())
    master = family["master"]
    if not master.get("exists"):
        return {"uri": uri, "error": "document not found"}

    # The master first, then the versions oldest first, then what it points at.
    members: list[dict] = [master]
    members += family.get("versions") or []
    for link in family.get("links") or []:
        members += [link, *(link.get("versions") or [])]

    names = member_names([m["uri"] for m in members])
    content: dict[str, Any] = {}
    files: list[dict] = []
    problems: list[str] = []
    staged: dict[str, bytes | str] = {}
    expected: dict[str, str] = {}          # name -> the SHA-256 MarkLogic computed
    checked: dict[str, str] = {}           # name -> MarkLogic's SHA-256, matched before writing
    source: dict[str, str] = {}            # name -> the MarkLogic URI it came from

    for member in members:
        name = names[member["uri"]]
        if not member.get("exists"):
            problems.append(f"{member['uri']}: not found")
            continue
        binary = member["kind"] == "binary" and not member.get("binary_json")
        if binary:
            # Byte-for-byte, whatever it is - a PDF is not text.
            staged[name] = cli.get_bytes(member["uri"])
            expected[name] = member.get("binary_hash") or ""
            source[name] = member["uri"]
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
            expected[name] = src.ml_hash or ""
            source[name] = member["uri"]
            if member is master:
                content = src.content()
        # MarkLogic's own <prop:properties>, as it serializes it - namespaces and
        # all. Converting it to JSON would drop the prefixes that tell dls:version
        # apart from anyone else's version element. Named after the file it
        # describes, extension included, so the pairing is unambiguous.
        properties = member.get("properties_xml") or ""
        if properties:
            props_name = f"{name}.properties.xml"
            want = member.get("properties_hash") or ""
            # Check what MarkLogic sent before adding to it: the staged file
            # carries one more element, so it no longer hashes the same.
            if want and sha256_hex(properties) != want:
                problems.append(f"{props_name}: hash mismatch, MarkLogic {want} "
                                f"!= received {sha256_hex(properties)}")
                continue
            staged[props_name] = with_marklogic_hash(properties, expected.get(name, ""))
            checked[props_name] = want
            source[props_name] = member["uri"]

    name_from, id_source = folder_name(master["uri"], content or None)
    folder = root / name_from
    for name, payload in staged.items():
        size = write_file(folder / name, payload)
        # Hash what actually landed on disk, not what was held in memory: this
        # covers the write and the encoding as well as the transfer.
        on_disk = (folder / name).read_bytes()
        sha = hashlib.sha256(on_disk).hexdigest()
        want = expected.get(name) or ""
        if want and want != sha:
            problems.append(f"{name}: hash mismatch, MarkLogic {want} != on disk {sha}")
        # MD5 too: it is the hash Snowflake's LIST reports for a staged file.
        files.append({"name": name, "source": source.get(name, ""), "size": size,
                      "marklogic_sha256": want or checked.get(name, ""), "sha256": sha,
                      "md5": hashlib.md5(on_disk).hexdigest(),
                      "verified": want == sha if want else bool(checked.get(name))})
    files.sort(key=lambda f: f["name"])

    # What the folder should hold, uploaded with it, so the stage copy can be
    # checked again later without going back to MarkLogic.
    manifest = {
        "document_uri": master["uri"],
        "document_id": name_from,
        "id_source": id_source,
        "extracted_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "files": files,
    }
    manifest_text = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    manifest_size = write_file(folder / MANIFEST, manifest_text)
    manifest_bytes = (folder / MANIFEST).read_bytes()

    # The document and each version of it, with what the control table
    # records for it: its own file, its properties and the files it names.
    by_name = {f["name"]: f for f in files}
    envelopes = []
    for member in [master, *(family.get("versions") or [])]:
        name = names[member["uri"]]
        if not member.get("exists") or name not in by_name:
            continue
        envelopes.append({
            "uri": member["uri"],
            "version": member.get("version") or "",
            "created": member.get("created") or "",
            "replaced": member.get("replaced") or "",
            "is_master": member is master,
            "file": by_name[name],
            "properties": by_name.get(f"{name}.properties.xml"),
            **matched_refs(member, member is master, family.get("links") or []),
        })

    return {
        "uri": master["uri"],
        "folder": folder,
        "document_id": name_from,
        "id_source": id_source,
        "files": files,
        # Written here, not read from MarkLogic, so it has no MarkLogic hash.
        "manifest": {"name": MANIFEST, "source": "", "size": manifest_size,
                     "marklogic_sha256": "",
                     "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                     "md5": hashlib.md5(manifest_bytes).hexdigest(), "verified": False},
        # Every URI in the family that exists, so all of them get marked.
        "members": [m["uri"] for m in members if m.get("exists")],
        "envelopes": envelopes,
        "versions": len(family.get("versions") or []),
        "links": len(family.get("links") or []),
        "problems": problems,
    }


def resolve_uris(cli, args) -> bool:
    """Turn every --id into the URI of its current envelope, added to args.uri.

    MarkLogic is asked for the documents whose ID field (ML_ID_PATH) holds the
    ID - no path needed. Returns False, having said why, if an ID matches no
    envelope or more than one.
    """
    uris = list(args.uri or [])
    ok = True
    for doc_id in args.id or []:
        found = cli.find_by_id(doc_id, ML.ID_PATH or "documentId", ML.link_fields())
        envelopes = found.get("envelopes") or []
        how = (f"its {ML.ID_PATH or 'documentId'} field" if found.get("matched_by") == "field"
               else "a URI containing it")
        if len(envelopes) == 1:
            print(f"  id {doc_id} -> {envelopes[0]}  (by {how})")
            uris.append(envelopes[0])
        elif not envelopes:
            ok = False
            print(f"  id {doc_id}: no document found (searched {ML.ID_PATH or 'documentId'} "
                  "and URIs containing it)")
        else:
            ok = False
            print(f"  id {doc_id}: {len(envelopes)} documents match (by {how}); "
                  "pass the one you mean with --uri:")
            for uri in envelopes:
                print(f"      {uri}")
    if not uris and ok:
        print("Give --uri or --id.")
        ok = False
    args.uri = uris
    return ok


def family_view(envelopes: list[dict], brief: bool = False) -> list[str]:
    """The lines `family` prints for one document: who is who, then one row per
    envelope version with the PDF/DOM version load pairs it with, then each
    linked file's own versions. Versions are shown as v1, v2 - not file names."""
    if not envelopes or not envelopes[0].get("exists"):
        return [f"  NOT FOUND: {envelopes[0]['uri'] if envelopes else '?'}"]
    names = {ML.BINARY_FIELD: "PDF", ML.DOM_FIELD: "DOM",
             ML.CONVERTED_PDF_FIELD: "CONVERTED", ML.TRANSLATED_PDF_FIELD: "TRANSLATED"}

    def label_of(field: str) -> str:
        leaf = field.split(".")[-1]
        return names.get(leaf) or leaf

    def when(value: str, same_day_as: str = "") -> str:
        text = (value or "").replace("T", " ").rstrip("Z")[:23]
        if text and same_day_as and text[:10] == same_day_as.replace("T", " ")[:10]:
            return text[11:]                   # same day: the time is enough
        return text or "-"

    master = envelopes[0]
    copies = sorted(envelopes[1:], key=lambda e: int(e["version_id"] or 0))
    # The current envelope is normally just the latest copy: same created time,
    # never replaced. Then it gets no row of its own (as in the control table).
    current = next((c for c in copies if not c["dls_replaced"]
                    and parse_time(c["dls_created"]) is not None
                    and parse_time(c["dls_created"]) == parse_time(master["dls_created"])), None)

    # Every linked file, once, under a short name: PDF, DOM, ...
    links: dict[str, dict] = {}
    for env in envelopes:
        for ref in env["refs"]:
            links.setdefault(ref["uri"], {**ref, "label": label_of(ref["field"])})
    columns = list(dict.fromkeys(link["label"] for link in links.values()))

    def version_label(uri: str) -> str:
        for link in links.values():
            if uri == link["uri"]:
                return "cur"
            for version in link["versions"]:
                if uri == version["uri"]:
                    return f"v{version['version_id']}"
        return PurePosixPath(uri).name

    rows, notes = [], []
    for env in ([] if current else [master]) + copies:
        is_master = env is master
        label = "cur" if is_master else f"v{env['version_id'] or '?'}"
        refs = {r["field"].split(".")[-1]: r["uri"] for r in env["refs"]}
        pick = matched_refs(
            {"created": env["dls_created"], "refs": refs}, is_master,
            [{"uri": r["uri"], "created": r["dls_created"],
              "versions": [{"uri": v["uri"], "created": v["dls_created"],
                            "replaced": v["dls_replaced"]} for v in r["versions"]]}
             for r in env["refs"]])
        paired = {label_of(field): version_label(uri) for field, uri in pick["refs"].items()}
        for field in refs:
            if any(field in n for n in pick["ref_notes"]):
                paired[label_of(field)] += "?"
                notes.append(f"{label}: no {label_of(field)} version was current when it was "
                             f"created ({when(env['dls_created'])}) - recorded as named")
        rows.append([label, when(env["dls_created"]),
                     when(env["dls_replaced"], env["dls_created"]),
                     *(paired.get(c, "-") for c in columns),
                     "<- current" if env is current else ""])

    count = (f"current + {len(copies)} version(s)" if copies else "no versions")
    out = [f"DOCUMENT  {master['uri']}",
           f"  {len(envelopes)} envelope(s): {count}"
           + (f"; the current one is v{current['version_id']}" if current else "")]
    if not brief:
        for link in links.values():
            n = len(link["versions"])
            out.append(f"  {link['label']:<10} {link['uri']}   "
                       f"{n} version{'' if n == 1 else 's'}"
                       + ("" if link["exists"] else "   NOT FOUND"))

    header = ["VERSION", "CREATED", "REPLACED", *columns, ""]
    widths = [max(len(str(r[i])) for r in [header, *rows]) for i in range(len(header))]
    out.append("")
    for row in [header, *rows]:
        out.append("  " + "  ".join(str(v).ljust(w) for v, w in zip(row, widths)).rstrip())

    if not brief and links:
        out.append("")
        for link in links.values():
            for i, version in enumerate(link["versions"]):
                head = f"{link['label']} versions" if i == 0 else ""
                end_at = version["dls_replaced"]
                out.append(f"  {head:<20}v{version['version_id']:<4}"
                           f"{when(version['dls_created'])}  ->  "
                           f"{when(end_at, version['dls_created']) if end_at else 'current'}")
    out.append("")
    if notes:
        out += [f"  ! {n}" for n in notes]
    elif rows and links:
        out.append("  Every version is paired with a file version by time.")
    return out


def cmd_family(args) -> int:
    """Show how one document's pieces hang together, read-only: its envelope
    versions, the PDF/DOM version load pairs each with (by time), and those
    files' own versions. --brief shows only the pairing table; --xml adds each
    envelope's properties as MarkLogic holds them.
    """
    cli = client()
    if not resolve_uris(cli, args):
        return 1
    for uri in args.uri:
        envelopes = cli.discover(uri, ML.CREATED_PATH)
        print("\n".join(family_view(envelopes, brief=args.brief)))
        if args.xml:
            for env in envelopes:
                label = "current" if not env["version_of"] else f"v{env['version_id']}"
                print(f"PROPERTIES of {label}  {env['uri']}")
                for text in (env.get("properties_xml") or "(none)").splitlines():
                    print(f"    {text}")
                print()
        print()
    return 0


def cmd_extract(args) -> int:
    """Write documents to local disk in the shape the Snowflake stage expects.

    One folder per document, named by the document's ID, holding the document,
    every version of it, whatever it points at, and a .properties.json beside
    each one. What lands here is what goes up.
    """
    cli = client()
    if not resolve_uris(cli, args):
        return 1
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
        for item in result["files"]:
            print(f"      {item['name']:<44} {item['size']:>11,} bytes")
            mark = "verified against MarkLogic" if item["verified"] else "local only"
            print(f"        sha256 {item['sha256']}  ({mark})")
        print(f"      {MANIFEST:<44} (name, source URI, size and sha256 of each file above)")
        if result["problems"]:
            failed += 1
            for problem in result["problems"]:
                print(f"      not written: {problem}")
    print()
    print(f"Extracted {len(args.uri) - failed} of {len(args.uri)} document(s).")
    return 1 if failed else 0


def mark(cli, uris: list[str], migrated: bool, workers: int) -> int:
    """Set the migrated flag in each document's properties; return failures."""
    failed = 0
    for result in cli.mark_migrated(uris, migrated=migrated, workers=workers):
        if result.get("error"):
            failed += 1
            print(f"  FAILED   {result['uri']}\n           {result['error']}")
        else:
            print(f"  marked   {result['uri']}  "
                  f"migrated={result['migrated']} at {result['timestamp']}")
    return failed


def family_uris(cli, uri: str) -> list[str]:
    """The document, every version of it and what it links to - the URIs
    'load' uploads together and so marks together."""
    family = cli.family(uri, links=ML.link_fields())
    members = [family["master"], *(family.get("versions") or []), *(family.get("links") or [])]
    return [m["uri"] for m in members if m.get("exists")]


REPORT_FIELDS = ["document_uri", "document_id", "file", "source_uri", "size",
                 "marklogic_sha256", "local_sha256", "local_md5",
                 "put_source_size", "put_target_size", "put_compression",
                 "stage_size", "stage_md5", "stage_sha256", "result"]


def check_stage(result: dict, listed: dict[str, dict], hashes: dict[str, str] | None,
                put: dict[str, dict] | None = None) -> list[dict]:
    """One report row per file: does the staged copy match MarkLogic?

    MarkLogic's SHA-256 was already matched against the file on disk. Here the
    SHA-256 Snowflake computed of the staged file (`hashes`, from the hash UDF,
    over the decrypted content) must equal it too - the same algorithm end to
    end, whatever the stage's encryption and however large the file. LIST only
    has to show the file is there; its size and MD5 are reported, not compared:
    on a client-side encrypted stage they describe the encrypted copy.

    hashes=None means the check is off (SF_HASH_CHECK=off): a file then passes
    on being in the stage, and its row says it was not hash-checked.
    """
    rows = []
    for f in [*result["files"], result["manifest"]]:
        staged = listed.get(f["name"])
        row = {
            "document_uri": result["uri"], "document_id": result["document_id"],
            "file": f["name"], "source_uri": f["source"], "size": f["size"],
            "marklogic_sha256": f["marklogic_sha256"], "local_sha256": f["sha256"],
            "local_md5": f["md5"],
            "stage_size": staged["size"] if staged else "",
            "stage_md5": staged["md5"] if staged else "",
            "stage_sha256": (hashes or {}).get(f["name"], ""),
            **{f"put_{k}": v for k, v in ((put or {}).get(f["name"]) or {}).items()},
        }
        if staged is None:
            row["result"] = "MISSING from stage"
        elif hashes is None:
            row["result"] = "OK - in stage, NOT hash-checked (SF_HASH_CHECK=off)"
        elif not row["stage_sha256"]:
            row["result"] = "NOT HASHED in Snowflake"
        elif row["stage_sha256"] != f["sha256"]:
            row["result"] = "SHA-256 MISMATCH"
        else:
            row["result"] = "OK"
        if row["result"] == "OK" and f["name"] != MANIFEST and not f["verified"]:
            row["result"] += " - no MarkLogic hash to compare"
        rows.append(row)
    return rows


# Control-table file columns and the envelope field that names each file.
LINK_COLUMNS = [("BINARY", ML.BINARY_FIELD), ("DOM", ML.DOM_FIELD),
                ("CONVERTED_PDF", ML.CONVERTED_PDF_FIELD),
                ("TRANSLATED_PDF", ML.TRANSLATED_PDF_FIELD)]


def control_rows(result: dict) -> tuple[list[dict], list[str]]:
    """One control-table row per version of the envelope, and the reasons any
    of them cannot be written as they stand. The current document gets a row
    of its own (VERSION_ID 0) only when it is not simply the latest version.

    File columns hold the MarkLogic name and URI. The properties file has no URI
    of its own in MarkLogic; it is reached through its document's URI, so that
    is what PROPERTY_FILE_URI holds. DOC_ML_HASH_SHA256 is MarkLogic's SHA-256
    of the envelope; DOC_SF_HASH_SHA256 is the SHA-256 Snowflake computes of
    the staged envelope, filled in once the upload is checked.
    """
    rows: list[dict] = []
    errors: list[str] = []
    by_version: dict[int, dict] = {}

    # Under Library Services the current document carries no version-id of its
    # own: it IS the latest version copy - same created time, never replaced.
    # That version's row stands for it, so it does not get a second row.
    envelopes = result["envelopes"]
    current_is = {}                           # version copy URI -> current document URI
    master = next((e for e in envelopes if e["is_master"]), None)
    if master and not str(master["version"]).strip().isdigit():
        when = parse_time(master.get("created", ""))
        for env in envelopes:
            if (not env["is_master"] and when is not None and not env.get("replaced")
                    and parse_time(env.get("created", "")) == when):
                current_is[env["uri"]] = master["uri"]
        if current_is:
            envelopes = [e for e in envelopes if e is not master]

    for env in envelopes:
        notes = list(env.get("ref_notes") or [])
        if env["uri"] in current_is:
            notes.append(f"current version; the current document is {current_is[env['uri']]}")
        version = str(env["version"]).strip()
        if version.isdigit():
            version_id = int(version)
        elif env["is_master"]:
            # No DLS version-id: the document is not under Library Services.
            version_id = 0
            notes.append("current document; no DLS version-id, recorded as VERSION_ID 0")
        else:
            errors.append(f"{env['uri']}: version copy without a version-id")
            continue
        if version_id in by_version:
            # Snowflake does not enforce the key, so say so rather than write two.
            by_version[version_id]["_notes"].append(
                f"{env['uri']} has the same version-id {version_id}")
            continue

        row = {
            "DOCUMENT_ID": result["document_id"],
            "VERSION_ID": version_id,
            "ENVELOPE_FILE_NAME": PurePosixPath(env["uri"]).name,
            "ENVELOPE_FILE_URI": env["uri"],
            "PROPERTY_FILE_NAME": (PurePosixPath(env["properties"]["name"]).name
                                   if env["properties"] else None),
            "PROPERTY_FILE_URI": env["uri"] if env["properties"] else None,
            "DOC_ML_HASH_SHA256": env["file"]["marklogic_sha256"] or None,
            "DOC_SF_HASH_SHA256": None,
            "_file": env["file"]["name"],
            "_notes": notes,
        }
        for column, field in LINK_COLUMNS:
            uri = env["refs"].get(field) if field else None
            row[f"{column}_FILE_NAME"] = PurePosixPath(uri).name if uri else None
            row[f"{column}_FILE_URI"] = uri or None
        if not row["BINARY_FILE_URI"]:
            # NOT NULL in the table; empty says "none", the comment says why.
            row["BINARY_FILE_NAME"] = row["BINARY_FILE_URI"] = ""
            notes.append(f"no {ML.BINARY_FIELD} in this envelope")
        by_version[version_id] = row
        rows.append(row)

    for row in rows:
        for column, width in CONTROL_WIDTHS.items():
            value = row.get(column)
            if column != "COMMENT" and value is not None and len(str(value)) > width:
                errors.append(f"version {row['VERSION_ID']}: {column} is {len(str(value))} "
                              f"characters, the column holds {width}: {value}")
    return rows, errors


def cmd_load(args) -> int:
    """Upload each document's folder to the Snowflake stage, then mark it migrated.

    Per document: extract its whole family (as 'extract' does, SHA-256 checked
    against MarkLogic), replace @stage/<documentuuid>/ with those files, list
    the stage and check every file's size and MD5 against the local copy, and
    only then set migrated=true in the properties of every member of the
    family - the document, its versions and what it links to. A document that
    fails any step is left unmarked, so the next run picks it up again.

    Each version also gets a row in the control table (SF_CONTROL_TABLE):
    CREATED before the upload, PROCESSED once verified and marked, EXCEPTION
    with the reason in COMMENT if a step fails.

    Every file checked goes into a CSV report under output/.
    """
    cli = client()
    if not resolve_uris(cli, args):
        return 1
    if args.unmark:
        failed = 0
        for uri in args.uri:
            failed += 1 if mark(cli, family_uris(cli, uri), migrated=False,
                                workers=args.workers) else 0
        print(f"Unmarked {len(args.uri) - failed} of {len(args.uri)} document(s), "
              "with their versions and linked documents.")
        return 1 if failed else 0

    stage = SF.stage()
    table = SF.control_table()
    function = SF.hash_function()
    settings = SF.conn_kwargs()
    report = OUTPUT_DIR / f"load-report-{datetime.now():%Y%m%d-%H%M%S}.csv"
    print(f"  Documents: {len(args.uri)}")
    print(f"  Stage    : {stage}")
    print(f"  Table    : {table}"
          + (f"  - NOT written; statements go to {args.control_sql}" if args.control_sql else ""))
    print(f"  Verify   : SHA-256 of each staged file, computed in Snowflake by {function}"
          if SF.HASH_CHECK else
          "  Verify   : OFF (SF_HASH_CHECK=off) - staged files are NOT hash-checked; "
          "MarkLogic vs disk still is")
    print(f"  Local    : {STAGE.DIR if args.keep_local else '(temporary, removed afterwards)'}")
    print(f"  Report   : {report}")
    print()

    try:
        conn = connect(settings)
    except Exception as exc:
        print(f"Cannot log in to Snowflake: {type(exc).__name__}: {exc}")
        print(snowflake_hint(exc))
        return 1

    loaded = 0
    with conn, tempfile.TemporaryDirectory(prefix="ml2sf-") as tmp, \
            open(report, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        root = STAGE.DIR if args.keep_local else Path(tmp)
        cur = conn.cursor()
        # Stopgap while this user cannot write the table: the statements are
        # saved, in order, for someone who can to run.
        sql_file = open(args.control_sql, "w", encoding="utf-8") if args.control_sql else None
        if sql_file:
            sql_file.write(f"-- ml2sf load {datetime.now():%Y-%m-%d %H:%M:%S}: control-table "
                           f"statements for {table}.\n-- Run in order; each is a MERGE, "
                           "so running the file twice does no harm.\n")
        # Stop before extracting anything if the check could not run anyway.
        present = not SF.HASH_CHECK
        try:
            present = present or hash_function_exists(cur, function)
        except Exception as exc:
            print(f"Cannot look up {function}: {type(exc).__name__}: {exc}")
        if not present:
            print(f"The hash function {function} does not exist, or this role cannot use it.")
            print("load checks every staged file by its SHA-256, which only a function")
            print(f"inside Snowflake can compute on this stage. Have an admin run")
            print(f"{HASH_FUNCTION_FILE} once (shown below; put the")
            print(f"role in SF_ROLE, {SF.ROLE or 'this user default role'}, in the GRANT):")
            print()
            print(hash_function_sql(function))
            print("Or set SF_HASH_FUNCTION in .env if it lives somewhere else, or")
            print("SF_HASH_CHECK=off to upload without this check until it exists.")
            return 1
        try:
            if not sql_file:
                check_control_table(cur, table)
        except Exception as exc:
            print(f"Cannot read the control table {table}: {type(exc).__name__}: {exc}")
            try:
                role, secondary = cur.execute(
                    "SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES()").fetchone()
                print(f"This session's role is {role}, secondary roles {secondary}.")
            except Exception:
                pass
            print("Check SF_CONTROL_TABLE, and that SF_ROLE has SELECT, INSERT and UPDATE on it.")
            print("The web console may see it through another role, or through secondary")
            print("roles, which a Python session does not use unless told to.")
            return 1

        def failed(uri: str, reason: str) -> None:
            """A document that never got as far as the per-file check."""
            writer.writerow({"document_uri": uri, "result": f"FAILED - {reason}"})
            out.flush()

        def record(uri: str, rows: list[dict], status: str, note: str = "") -> bool:
            """Write the document's control rows with this status; say if it failed."""
            for row in rows:
                row["MIG_STATUS"] = status
                comment = "; ".join([*row["_notes"], *([note] if note else [])])
                row["COMMENT"] = (comment[:997] + "...") if len(comment) > 1000 else comment or None
            try:
                if sql_file:
                    sql_file.write(control_rows_sql(table, rows))
                    sql_file.flush()
                else:
                    merge_control_rows(cur, table, rows)
                return True
            except Exception as exc:
                print(f"  FAILED   {uri}\n           control table ({status}): "
                      f"{type(exc).__name__}: {exc}")
                failed(uri, f"control table ({status}): {type(exc).__name__}: {exc}")
                return False

        for uri in args.uri:
            result = extract_one(cli, uri, root)
            if result.get("error"):
                print(f"  FAILED   {uri}\n           {result['error']}")
                failed(uri, result["error"])
                continue
            control, too_long = control_rows(result)
            if too_long:
                print(f"  FAILED   {uri}  (not uploaded - does not fit the control table)")
                for problem in too_long:
                    print(f"           {problem}")
                failed(uri, "does not fit the control table: " + "; ".join(too_long))
                continue
            if result["problems"]:
                print(f"  FAILED   {uri}  (not uploaded)")
                for problem in result["problems"]:
                    print(f"           {problem}")
                failed(uri, "not uploaded: " + "; ".join(result["problems"]))
                record(uri, control, "EXCEPTION", "not uploaded: " + "; ".join(result["problems"]))
                continue
            if not record(uri, control, "CREATED"):
                continue                      # not uploaded: the table would not say so

            target = STAGE.snowflake_path(result["document_id"])
            try:
                up = upload_folder(cur, result["folder"], target)
            except Exception as exc:
                print(f"  FAILED   {uri}\n           upload: {type(exc).__name__}: {exc}")
                failed(uri, f"upload: {type(exc).__name__}: {exc}")
                record(uri, control, "EXCEPTION", f"upload: {type(exc).__name__}: {exc}")
                continue
            for line in up["failed"]:
                print(f"           not uploaded: {line}")

            try:
                hashes = (stage_sha256(cur, function, target, sorted(up["listed"]))
                          if SF.HASH_CHECK else None)
            except Exception as exc:
                print(f"  FAILED   {uri}  (not marked)\n"
                      f"           hashing in Snowflake: {type(exc).__name__}: {exc}")
                failed(uri, f"hashing in Snowflake: {type(exc).__name__}: {exc}")
                record(uri, control, "EXCEPTION", f"hashing in Snowflake: {type(exc).__name__}: {exc}")
                continue
            rows = check_stage(result, up["listed"], hashes, up["put"])
            writer.writerows(rows)
            out.flush()
            bad = [r for r in rows if not r["result"].startswith("OK")]
            if up["failed"] or bad:
                print(f"  FAILED   {uri}  (stage copy does not match - not marked)")
                for r in bad:
                    print(f"           {r['result']}: {r['file']}")
                record(uri, control, "EXCEPTION", "stage copy does not match: " + "; ".join(
                    [*up["failed"], *(f"{r['result']}: {r['file']}" for r in bad)]))
                continue

            print(f"  uploaded {result['uri']}")
            print(f"           {up['files']} file(s) -> {target}/"
                  f"  ({result['versions']} version(s), {result['links']} linked)")
            if hashes is None:
                print(f"  NOT hash-checked in Snowflake (SF_HASH_CHECK=off); "
                      f"on disk = MarkLogic, all {len(rows)} file(s) in the stage")
            else:
                print(f"  verified {len(rows)} file(s): SHA-256 in Snowflake = on disk = MarkLogic")
                for row in control:
                    row["DOC_SF_HASH_SHA256"] = hashes.get(row["_file"])

            # The whole family went up, so the whole family is marked.
            if mark(cli, result["members"], migrated=True, workers=args.workers):
                print(f"  FAILED   {uri}  (uploaded, but not every member could be marked)")
                record(uri, control, "EXCEPTION",
                       "uploaded and verified, but not marked migrated in MarkLogic")
                continue
            if not record(uri, control, "PROCESSED",
                          f"verified {len(rows)} file(s) in the stage by SHA-256" if hashes is not None
                          else f"uploaded {len(rows)} file(s); stage copy NOT hash-checked "
                               "(SF_HASH_CHECK=off) - re-run with the check on"):
                print("           (it IS uploaded, verified and marked in MarkLogic)")
                continue
            print(f"  recorded {len(control)} version(s) as PROCESSED"
                  + (f" in {args.control_sql} (not yet in the table)" if sql_file else f" in {table}"))
            loaded += 1

    print()
    print(f"Loaded, verified, marked and recorded {loaded} of {len(args.uri)} document(s).")
    print(f"Report: {report}")
    if sql_file:
        sql_file.close()
    if args.control_sql:
        print(f"Control table NOT updated. Run {args.control_sql} in Snowflake as a user")
        print(f"that can write {table}, to record these documents there.")
    return 0 if loaded == len(args.uri) else 1


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
                    help="also upload a small test file to SF_STAGE, then remove it")
    sc.set_defaults(fn=cmd_sfcheck)

    fa = sub.add_parser("family",
                        help="show a document's versions, linked files and their timestamps")
    fa.add_argument("--uri", action="append",
                    help="document URI to show (repeatable)")
    fa.add_argument("--id", action="append",
                    help="document ID to show, found by ML_ID_PATH - no URI needed (repeatable)")
    fa.add_argument("--xml", action="store_true",
                    help="also print each document's properties XML as MarkLogic holds it")
    fa.add_argument("--brief", action="store_true",
                    help="only the table: each version and the PDF/DOM version load pairs it with")
    fa.set_defaults(fn=cmd_family)

    ex = sub.add_parser("extract",
                        help="a document, its versions and its files -> one folder per document")
    ex.add_argument("--uri", action="append",
                    help="document URI to extract (repeatable)")
    ex.add_argument("--id", action="append",
                    help="document ID to extract, found by ML_ID_PATH - no URI needed (repeatable)")
    ex.add_argument("--out", help=f"staging root (default: STAGE_DIR, now {STAGE.DIR})")
    ex.set_defaults(fn=cmd_extract)

    ld = sub.add_parser("load", help="upload documents to the Snowflake stage and mark them migrated")
    ld.add_argument("--uri", action="append",
                    help="document URI to load (repeatable)")
    ld.add_argument("--id", action="append",
                    help="document ID to load, found by ML_ID_PATH - no URI needed (repeatable)")
    ld.add_argument("--keep-local", action="store_true",
                    help=f"keep the extracted files in STAGE_DIR ({STAGE.DIR}) "
                         "instead of a temporary folder")
    ld.add_argument("--unmark", action="store_true",
                    help="only write migrated=false on the document, its versions and linked "
                         "documents, to redo it (nothing is uploaded)")
    ld.add_argument("--control-sql", metavar="FILE",
                    help="do not write the control table; save its MERGE statements to FILE "
                         "to run by hand (while this Snowflake user cannot write the table)")
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
