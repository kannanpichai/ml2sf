"""Generic MarkLogic -> Snowflake migration driver.

  python migrate.py stat     [--all]              documents / collections / formats per database
  python migrate.py fetch    --out docs.csv       every document URI + format -> CSV (--limit N)
  python migrate.py bench    --sample 500          measure read speed, project the full run
  python migrate.py sfcheck  [--write]            can we reach Snowflake with these settings?
  python migrate.py profile  --collection X       inspect documents, write mappings/X.yml
  python migrate.py extract  --mapping X          MarkLogic -> output/X.jsonl
  python migrate.py extract  --uri U              just that document (repeat --uri for more)
  python migrate.py load     --mapping X          output/X.jsonl -> Snowflake
  python migrate.py run      --mapping X          extract + load
  python migrate.py sql      --mapping X          print the SQL, connect to nothing

Nothing here knows any particular document schema; that lives in mappings/*.yml.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
import sys
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

import profiler
from config import MAPPINGS_DIR, ML, OUTPUT_DIR, SF, Mapping
from marklogic import MarkLogicClient, MarkLogicError, SourceDocument
from snowflake_loader import (build_frame, check_connection, ddl, insert_sql,
                              load_to_snowflake, merge_sql, reach_check)


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


def jsonl_path(name: str) -> Path:
    return OUTPUT_DIR / f"{name}.jsonl"


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


def cmd_fetch(args) -> int:
    cli = client()
    selector = selector_from_args(args)
    total = cli.count(selector)
    out = Path(args.out) if args.out else OUTPUT_DIR / f"documents-{ML.DATABASE}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    limit = args.limit or total
    print(f"{total:,} documents match; writing {min(limit, total):,} rows -> {out}")

    written = 0
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["uri", "mimetype", "format", "database"])
        for row in islice(cli.iter_uri_rows(selector, page_length=args.page_size), limit):
            writer.writerow([row["uri"], row.get("mimetype", ""),
                             row.get("format", ""), ML.DATABASE])
            written += 1
            if written % 10000 == 0:
                print(f"  {written:,}/{min(limit, total):,}")
    print(f"Wrote {written:,} rows to {out}")
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


def cmd_profile(args) -> int:
    cli = client()
    selector = selector_from_args(args)
    total = cli.count(selector)
    if total == 0:
        print("No documents matched that selector.")
        return 1

    sample_n = min(args.sample, total)
    print(f"{total} documents match; profiling a sample of {sample_n}...\n")
    uris = list(islice(cli.iter_uris(selector), sample_n))
    docs = [doc for _, doc in cli.get_documents(uris)]

    stats, n = profiler.profile(docs)
    print(profiler.report(stats, n))

    columns = profiler.suggest_columns(stats, n, min_fill=args.min_fill)
    name = args.name or args.collection or "migration"
    name = name.strip("/").replace("/", "_") or "migration"
    table = (args.table or name).upper().replace("-", "_")

    mapping = Mapping.default(name, selector, columns, table)
    path = mapping.save(MAPPINGS_DIR / f"{name}.yml")
    print(f"\n{len(columns)} candidate columns -> {path}")
    print("Review/trim that file, then:  python migrate.py run --mapping " + name)
    return 0


def load_mapping(args) -> Mapping:
    if args.mapping:
        return Mapping.load(args.mapping)
    if getattr(args, "uri", None) and args.cmd == "extract":
        # Reading specific documents needs no schema: keep only the raw document.
        return Mapping.default("adhoc", {}, {}, "ADHOC")
    raise SystemExit(f"'{args.cmd}' needs --mapping (see: python migrate.py profile --help).")


def cmd_extract(args) -> int:
    mapping = load_mapping(args)
    cli = client()
    if args.uri:
        uris = args.uri
    else:
        total = cli.count(mapping.source)
        if total == 0:
            print("No documents matched the mapping's source selector.")
            return 1
        print(f"Listing {total} document URIs...")
        uris = list(cli.iter_uris(mapping.source, page_length=args.page_size))

    out = jsonl_path(mapping.name)
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    print(f"Extracting {len(uris)} documents -> {out}")

    written = failed = 0
    with out.open("w", encoding="utf-8") as fh:
        for src in cli.read_documents(uris, workers=args.workers):
            if not src.error and not src.hash_ok:
                src.error = f"hash mismatch in transfer: ML {src.ml_hash} != local {src.local_hash}"
            if args.uri:
                show_document(src)
            if src.error:
                failed += 1
                fh.write(json.dumps({"uri": src.uri, "error": src.error}, ensure_ascii=False) + "\n")
                continue
            record = {
                "uri": src.uri,
                "format": src.format,
                "size": src.size,
                "ml_hash": src.ml_hash,
                "properties": src.properties,
                "row": mapping.row_for(src.uri, src.content(), extracted_at),
            }
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1
            if written % 500 == 0:
                print(f"  {written}/{len(uris)}")
    print(f"Wrote {written} documents, {failed} failed.")
    return 1 if failed else 0


def show_document(src: SourceDocument) -> None:
    print(f"\n{src.uri}")
    if src.format:
        print(f"  format     : {src.format}")
    if src.text is not None:
        print(f"  size       : {src.size:,} bytes")
        print(f"  ML hash    : {src.ml_hash}")
        print(f"  local hash : {src.local_hash}  ({'match' if src.hash_ok else 'MISMATCH'})")
    if src.properties:
        print("  properties :")
        for name, value in src.properties.items():
            print(f"    {name}: {json.dumps(value, ensure_ascii=False)}")
    elif not src.error or src.format:
        print("  properties : (none)")
    if src.error:
        print(f"  ERROR      : {src.error}")


def _rows(mapping: Mapping):
    path = jsonl_path(mapping.name)
    if not path.exists():
        raise SystemExit(f"{path} not found. Run: python migrate.py extract --mapping {mapping.name}")
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            record = json.loads(line) if line else {}
            if "row" in record:
                yield record["row"]


def cmd_load(args) -> int:
    mapping = load_mapping(args)
    df = build_frame(mapping, _rows(mapping))
    print(f"Loading {len(df)} rows x {len(df.columns)} columns into {mapping.table} (mode={mapping.mode})")
    load_to_snowflake(mapping, df, SF.conn_kwargs())
    return 0


def cmd_run(args) -> int:
    rc = cmd_extract(args)
    return rc or cmd_load(args)


def cmd_sql(args) -> int:
    mapping = load_mapping(args)
    target, stage = ddl(mapping)
    print(target + ";\n")
    print(stage + ";\n")
    print((merge_sql(mapping) if mapping.mode in ("merge", "upsert") else insert_sql(mapping)) + ";")
    return 0


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

    pr = sub.add_parser("profile", help="infer schema and generate a mapping file")
    pr.add_argument("--collection")
    pr.add_argument("--directory")
    pr.add_argument("--query")
    pr.add_argument("--sample", type=int, default=200, help="documents to inspect (default 200)")
    pr.add_argument("--min-fill", type=float, default=0.0, help="drop paths present in < this fraction (0-1)")
    pr.add_argument("--name", help="mapping name (default: collection name)")
    pr.add_argument("--table", help="Snowflake table name (default: mapping name)")
    pr.set_defaults(fn=cmd_profile)

    for cmd, fn, helptext in [
        ("extract", cmd_extract, "MarkLogic -> local jsonl"),
        ("load", cmd_load, "local jsonl -> Snowflake"),
        ("run", cmd_run, "extract then load"),
        ("sql", cmd_sql, "print generated SQL without connecting"),
    ]:
        sp = sub.add_parser(cmd, help=helptext)
        sp.add_argument("--mapping", help="mappings/<name>.yml (optional for extract --uri)")
        sp.add_argument("--workers", type=int, default=8)
        sp.add_argument("--page-size", type=int, default=500)
        sp.add_argument("--uri", action="append", help="migrate only this document URI (repeatable)")
        sp.set_defaults(fn=fn)

    args = p.parse_args()
    try:
        return args.fn(args)
    except MarkLogicError as exc:
        print(f"MarkLogic error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
