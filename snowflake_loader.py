"""Everything on the Snowflake side: DDL / MERGE generation and the load.

The Snowflake path stages every column as text and casts during the MERGE,
because write_pandas cannot write VARIANT directly.
"""
from __future__ import annotations

import json
from typing import Iterable

import pandas as pd

from config import Mapping

CASTS = {
    "VARIANT": "TRY_PARSE_JSON({c})",
    "DATE": "TRY_TO_DATE({c})",
    "TIMESTAMP_NTZ": "TRY_TO_TIMESTAMP_NTZ({c})",
    "BOOLEAN": "TRY_TO_BOOLEAN({c})",
    "NUMBER": "TRY_TO_NUMBER({c})",
    "FLOAT": "TRY_TO_DOUBLE({c})",
}


def to_text(value):
    """Stringify for an all-VARCHAR staging table, preserving real NULLs."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def build_frame(mapping: Mapping, rows: Iterable[dict]) -> pd.DataFrame:
    cols = [c for c, _ in mapping.sf_columns()]
    df = pd.DataFrame(list(rows), columns=cols)
    return df.map(to_text)


def cast(col: str, sf_type: str) -> str:
    return CASTS.get(sf_type, "{c}").format(c=f"s.{col}")


def ddl(mapping: Mapping) -> tuple[str, str]:
    cols = mapping.sf_columns()
    target = ",\n  ".join(f"{c} {t}" for c, t in cols)
    stage = ",\n  ".join(f"{c} STRING" for c, _ in cols)
    return (
        f"CREATE TABLE IF NOT EXISTS {mapping.table} (\n  {target}\n)",
        f"CREATE OR REPLACE TRANSIENT TABLE {mapping.table}_STG (\n  {stage}\n)",
    )


def merge_sql(mapping: Mapping) -> str:
    cols = mapping.sf_columns()
    key = mapping.key_column
    non_key = [(c, t) for c, t in cols if c != key]
    sets = ",\n    ".join(f"t.{c} = {cast(c, t)}" for c, t in non_key)
    names = ", ".join(c for c, _ in cols)
    vals = ", ".join(cast(c, t) for c, t in cols)
    return f"""MERGE INTO {mapping.table} t
USING {mapping.table}_STG s ON t.{key} = s.{key}
WHEN MATCHED THEN UPDATE SET
    {sets}
WHEN NOT MATCHED THEN INSERT ({names})
  VALUES ({vals})"""


def insert_sql(mapping: Mapping) -> str:
    cols = mapping.sf_columns()
    names = ", ".join(c for c, _ in cols)
    vals = ", ".join(cast(c, t) for c, t in cols)
    return f"INSERT INTO {mapping.table} ({names})\nSELECT {vals} FROM {mapping.table}_STG s"


def load_to_snowflake(mapping: Mapping, df: pd.DataFrame, conn_kwargs: dict) -> None:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    target_ddl, stage_ddl = ddl(mapping)
    with snowflake.connector.connect(**conn_kwargs) as conn:
        cur = conn.cursor()
        if mapping.mode == "replace":
            cur.execute(f"DROP TABLE IF EXISTS {mapping.table}")
        cur.execute(target_ddl)
        cur.execute(stage_ddl)

        ok, chunks, nrows, _ = write_pandas(
            conn, df, f"{mapping.table}_STG", quote_identifiers=False
        )
        if not ok:
            raise RuntimeError("write_pandas failed")
        print(f"  staged {nrows} rows ({chunks} chunk(s))")

        if mapping.mode in ("merge", "upsert"):
            cur.execute(merge_sql(mapping))
            result = cur.fetchone() or (0, 0)
            print(f"  merged: {result[0]} inserted, {result[1] if len(result) > 1 else 0} updated")
        else:
            cur.execute(insert_sql(mapping))
            print(f"  appended {cur.rowcount} rows")

        cur.execute(f"DROP TABLE IF EXISTS {mapping.table}_STG")
        cur.execute(f"SELECT COUNT(*) FROM {mapping.table}")
        print(f"  {mapping.table} now holds {cur.fetchone()[0]} rows")
