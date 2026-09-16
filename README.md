# MarkLogic -> Snowflake migration

Migrates documents of *any* shape from MarkLogic to Snowflake. Nothing in the code
knows a schema; each migration is described by a YAML file that the tool can
generate for you by inspecting the documents.

## Why it is built this way

At a new client you do not know what the documents look like. Three things follow:

1. **Nothing is ever lost.** Every document lands whole in a `RAW_DOC VARIANT`
   column, so fields nobody thought to map are still queryable in Snowflake.
2. **Profile, don't guess.** `migrate.py profile` samples the collection, walks
   every path, infers types and fill rates, and writes a candidate mapping.
3. **The mapping is data, not code.** A new client is a new `mappings/*.yml`,
   never a new script.

## Usage

```powershell
python migrate.py discover                       # what databases/collections exist
python migrate.py profile --collection trades    # inspect docs -> mappings/trades.yml
#   ... review and trim mappings/trades.yml ...
python migrate.py sql     --mapping trades       # see the SQL, connect to nothing
python migrate.py run     --mapping trades       # extract + load to Snowflake
```

`extract` and `load` can also be run separately; `extract` writes
`output/<name>.jsonl` so you can inspect exactly what will be loaded.

### Selecting documents

`profile` accepts `--collection`, `--directory`, or `--query` (a MarkLogic string
query). For anything more complex, put a raw MarkLogic structured query in the
mapping's `source.structured_query` and it is POSTed to `/v1/search` as-is.

## The mapping file

```yaml
name: trades
source:
  collection: trades          # or directory: / q: / structured_query:
  database: Documents
target:
  table: TRADES
  key: DOC_URI                # MERGE key
  mode: merge                 # merge | append | replace
raw:
  include: true               # keep the whole document as VARIANT
  column: RAW_DOC
columns:
  TRADE_ID:
    path: trade.@id           # XML attributes are @name
    type: STRING
  TRADE_COUNTERPARTIES_CP_TEXT:
    path: trade.counterparties.cp[].#text   # [] collects across repeats
    type: VARIANT
```

Path syntax: `a.b.c` for nesting, `@attr` for XML attributes, `#text` for element
text, and `[]` to collect every value across a repeating element into an array.
Add `default:` to any column to substitute a value when the path is absent.

## What it handles

| Concern | How |
|---|---|
| JSON documents | Parsed directly |
| XML documents | Converted to dicts; attributes -> `@name`, text -> `#text` |
| Repeating elements | Collected into a JSON array, typed `VARIANT` |
| Cardinality drift | One `<cp>` in one doc and two in another map to the *same* column |
| Unknown types | Inferred from values: DATE, TIMESTAMP_NTZ, NUMBER, FLOAT, BOOLEAN, STRING |
| Mixed types on one path | Widened to the safest common type |
| Re-runs | `MERGE` on the key column, so loads are idempotent |
| Unmapped fields | Still present in `RAW_DOC` |

## Querying in Snowflake

```sql
SELECT TRADE_ID,
       RAW_DOC:trade.instrument.isin::STRING     AS isin,
       TRADE_COUNTERPARTIES_CP_TEXT[0]::STRING   AS first_counterparty
FROM TRADES;
```

## Layout

| Path | Role |
|---|---|
| `migrate.py` | CLI entry point: discover / profile / extract / load / run / sql |
| `config.py` | `.env` connection settings and the YAML mapping model |
| `marklogic.py` | MarkLogic REST client, XML->dict, path walking |
| `profiler.py` | Schema inference from a document sample, column naming |
| `snowflake_loader.py` | DDL / MERGE generation and the Snowflake load |
| `mappings/*.yml` | One file per migration |

## Notes

- Use the App-Services port (**8000**), not Admin (8001). Admin serves no `/v1` API.
- `write_pandas` cannot write `VARIANT`, so every column is staged as text and
  cast during the `MERGE` with `TRY_PARSE_JSON` / `TRY_TO_DATE` / etc.
- `TRY_*` casts mean a bad value becomes NULL rather than failing the whole load.
