"""Generic MarkLogic -> Snowflake migration driver.

  python migrate.py discover                      what databases / collections exist
  python migrate.py profile  --collection X       inspect documents, write mappings/X.yml
  python migrate.py extract  --mapping X          MarkLogic -> output/X.jsonl
  python migrate.py load     --mapping X          output/X.jsonl -> Snowflake
  python migrate.py run      --mapping X          extract + load
  python migrate.py sql      --mapping X          print the SQL, connect to nothing

Nothing here knows any particular document schema; that lives in mappings/*.yml.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

import profiler
from config import MAPPINGS_DIR, ML, OUTPUT_DIR, SF, Mapping
from marklogic import MarkLogicClient, MarkLogicError
from snowflake_loader import build_frame, ddl, insert_sql, load_to_snowflake, merge_sql


def client() -> MarkLogicClient:
    return MarkLogicClient(
        host=ML.HOST, port=ML.PORT, user=ML.user(), password=ML.password(),
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

def cmd_discover(args) -> int:
    cli = client()
    print(f"MarkLogic at {cli.base}\n")
    try:
        print("Databases:")
        for db in cli.databases():
            print(f"  {db}")
    except MarkLogicError as exc:
        print(f"  (could not list databases: {exc})")
    print(f"\nCollections in {ML.DATABASE!r}:")
    try:
        colls = cli.collections()
        if not colls:
            print("  (none - documents may be stored without collections)")
        for c in colls:
            print(f"  {c}  ({cli.count({'collection': c})} docs)")
    except MarkLogicError as exc:
        print(f"  (could not list collections: {exc})")
    print(f"\nTotal documents in {ML.DATABASE!r}: {cli.count({})}")
    return 0


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


def cmd_extract(args) -> int:
    mapping = Mapping.load(args.mapping)
    cli = client()
    total = cli.count(mapping.source)
    if total == 0:
        print("No documents matched the mapping's source selector.")
        return 1

    out = jsonl_path(mapping.name)
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    print(f"Extracting {total} documents -> {out}")

    written = 0
    with out.open("w", encoding="utf-8") as fh:
        uris = list(cli.iter_uris(mapping.source, page_length=args.page_size))
        for uri, doc in cli.get_documents(uris, workers=args.workers):
            row = mapping.row_for(uri, doc, extracted_at)
            fh.write(json.dumps({"uri": uri, "doc": doc, "row": row}, ensure_ascii=False) + "\n")
            written += 1
            if written % 500 == 0:
                print(f"  {written}/{total}")
    print(f"Wrote {written} documents.")
    return 0


def _rows(mapping: Mapping):
    path = jsonl_path(mapping.name)
    if not path.exists():
        raise SystemExit(f"{path} not found. Run: python migrate.py extract --mapping {mapping.name}")
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)["row"]


def cmd_load(args) -> int:
    mapping = Mapping.load(args.mapping)
    df = build_frame(mapping, _rows(mapping))
    print(f"Loading {len(df)} rows x {len(df.columns)} columns into {mapping.table} (mode={mapping.mode})")
    load_to_snowflake(mapping, df, SF.conn_kwargs())
    return 0


def cmd_run(args) -> int:
    rc = cmd_extract(args)
    return rc or cmd_load(args)


def cmd_sql(args) -> int:
    mapping = Mapping.load(args.mapping)
    target, stage = ddl(mapping)
    print(target + ";\n")
    print(stage + ";\n")
    print((merge_sql(mapping) if mapping.mode in ("merge", "upsert") else insert_sql(mapping)) + ";")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("discover", help="list databases and collections").set_defaults(fn=cmd_discover)

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
        sp.add_argument("--mapping", required=True)
        sp.add_argument("--workers", type=int, default=8)
        sp.add_argument("--page-size", type=int, default=500)
        sp.set_defaults(fn=fn)

    args = p.parse_args()
    try:
        return args.fn(args)
    except MarkLogicError as exc:
        print(f"MarkLogic error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
