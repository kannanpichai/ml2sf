# MarkLogic -> Snowflake migration

A one-time migration of documents of *any* shape out of MarkLogic. Nothing in the
code knows a schema. Built one step at a time: today it surveys the server and
reads documents; the Snowflake load is not written yet.

## Setup

Linux:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env          # then fill it in
kinit                         # only for ML_AUTH=kerberos
venv/bin/python migrate.py stat
```

Windows (PowerShell):

```powershell
python -m venv venv
venv\Scripts\pip install -r requirements.txt
Copy-Item .env.example .env   # then fill it in
venv\Scripts\python migrate.py stat
```

`requirements.txt` picks the Kerberos library per platform: `requests-negotiate-sspi`
on Windows (the logged-in account), `requests-kerberos` elsewhere (the `kinit`
ticket, or the service account you are running as).

## Usage

The examples below are PowerShell; on Linux they are the same with `python3`.

```powershell
python migrate.py stat --all                     # docs / collections / formats per database
python migrate.py fetch                          # URI + ID + created date -> timestamped CSV
python migrate.py fetch --out docs.csv           # ... or to a path you choose
python migrate.py fetch --no-ids                 # ... URIs only, without opening each document
python migrate.py fetch --exclude-migrated       # ... leaving out what load has marked done
python migrate.py bench --sample 500 --compare   # read speed, and how long the full run would take
python migrate.py sfcheck                        # can we reach Snowflake? (--write to test writing)
python migrate.py extract --uri /path/doc.json   # that document + its versions -> one folder
python migrate.py load --uri /path/doc.json      # mark it migrated in its properties
python migrate.py load --id <document id>        # same, found by its ID (ML_ID_PATH) - no URI needed
python migrate.py family --id <document id> --brief   # its versions and the PDF/DOM version each gets
```

## What extract writes

`extract` writes to local disk in the shape the Snowflake stage expects, so what
lands here is what goes up. One folder per document, named by the document's own
ID (`ML_ID_PATH`), falling back to the UUID its URI is named after:

```
<STAGE_DIR>/6ab9a482-9886-4d80-b477-3525f856a003/
  6ab9a482-....json                    the document, under its MarkLogic name
  6ab9a482-....json.properties.xml     its <prop:properties>, as MarkLogic serializes it
  1-6ab9a482.json                      each version, under its own MarkLogic name
  1-6ab9a482.json.properties.xml
  2-6ab9a482.json  ...
  6ab9a482-....pdf                     what binaryUri points at, byte for byte
  DOM/6ab9a482-....json                what domURI points at
```

Every file keeps the name MarkLogic stores it by. Two documents in different
MarkLogic directories can share a basename - the envelope `/GDXUI/envelope/<uuid>.json`
and its DOM `/DOM/<uuid>.json` - and so can two names differing only in case,
which Windows treats as one file. The document itself keeps the plain name; a
later clash keeps its MarkLogic directory as a subfolder, so no name is invented
and nothing is overwritten.

Properties are kept as XML, not converted: the namespace prefixes are what tell
`dls:version` apart from any other `version` element. Each is named after the
file it describes, extension included, so the pairing is unambiguous.

Each staged properties file gets one element added as its last child: the
SHA-256 MarkLogic computed for the file it describes, in ml2sf's own namespace.

```xml
<marklogic-sha256 xmlns="http://ml2sf/snowflake-migration">9f86d0...</marklogic-sha256>
```

The properties XML as MarkLogic sent it is checked against MarkLogic's own hash
of it *before* the element is added. Only the staged copy changes, not MarkLogic.

Versions are found by asking MarkLogic for the documents whose Library Services
properties point at the master URI, so passing any version's URI extracts the
whole family. Which fields are followed to related documents is `ML_LINK_PATH`.

Every text document is hash-verified in transit (see below); binaries are copied
byte for byte. A document that fails its check is reported and not written.

Where the tree goes is configuration, not code:

```
STAGE_DIR=                        # local tree (default output/stage), or --out per run
SF_DATABASE=GDX_DOCUMENTS_DB      # the stage lives in the database and schema
SF_SCHEMA=GDX_DOCUMENTS           #   already configured for Snowflake
SF_STAGE=GDX_MARKLOGIC_DOCUMENTS  # -> @GDX_DOCUMENTS_DB.GDX_DOCUMENTS.GDX_MARKLOGIC_DOCUMENTS/<uuid>/
```

Give `SF_STAGE` a full `DB.SCHEMA.STAGE` name to put the stage somewhere other
than the database and schema the connection uses. Uploading the tree into that
stage is not built yet - `extract` only writes the tree locally.

## Checking the stage copy

Every document folder also gets `_manifest.json`: each file's name, source URI,
size, SHA-256 (the same SHA-256 MarkLogic computed) and MD5. It goes up with the files.

`load` always checks that the stage holds MarkLogic's bytes, by SHA-256 at both ends:

1. MarkLogic's SHA-256 = the SHA-256 of the file written locally (before upload).
2. The SHA-256 Snowflake computes of the staged file = that same SHA-256 (after
   upload). `LIST` only has to show every file is there.

Step 2 needs a small Python function in Snowflake, `STAGE_SHA256`, that reads a
staged file and returns its SHA-256. It is needed because the stage encrypts
client-side (`SHOW STAGES` type `INTERNAL`): `LIST` then reports the size and MD5
of the *encrypted* copy, which never match the file. The function reads the file
after Snowflake decrypts it, so it works on any stage and for files of any size
(`LIST` gives no whole-file MD5 for large files uploaded in parts). Nothing is
downloaded.

Its SQL is in [`sql/stage_sha256.sql`](sql/stage_sha256.sql): the `CREATE
FUNCTION` and the `GRANT USAGE` for the migration role, for an admin to run once
as a role that can create functions in that schema and READ the stage.

The function is `SF_HASH_FUNCTION`, by default `STAGE_SHA256` in the stage's own
database and schema. If it is missing, `load` stops before doing anything and
prints that file, under the configured name.

**Until the function exists:** `SF_HASH_CHECK=off` in `.env` uploads without the
Snowflake-side check. MarkLogic vs disk is still checked and every file must still
be in the stage. Each such file's report row says `NOT hash-checked`, and the
control-table `COMMENT` says `NOT hash-checked (SF_HASH_CHECK=off)`, with
`DOC_SF_HASH_SHA256` empty - so those documents can be found and re-run once the
check is on. Remove the setting (it defaults to `on`) as soon as the function is
created.

A document with any file missing or different is not marked migrated. Every file
checked is a row in `output/load-report-<timestamp>.csv`: every hash and size at
every step (MarkLogic, disk, what `PUT` read and sent, what `LIST` shows, what
Snowflake hashed), and the result.

## Migration control table

`load` writes one row per envelope - the document and each version of it - into
`SF_CONTROL_TABLE` (e.g. `GDX_DB.GDX.MARKLOGIC_SNOWFLAKE_MIG_CONTROL`). The table
must already exist; `SF_ROLE` needs SELECT, INSERT and UPDATE on it.

| Column | Value |
|---|---|
| `DOCUMENT_ID` | the document folder name (its ID, else the UUID in its URI) |
| `VERSION_ID` | the DLS `version-id` of each version copy. The current document has none of its own: it is the latest copy (same `created`, never `replaced`), so that copy's row stands for it and `COMMENT` names it. Only a document outside Library Services gets `0` |
| `ENVELOPE_FILE_NAME` / `_URI` | that version's own name and MarkLogic URI |
| `PROPERTY_FILE_NAME` / `_URI` | its `.properties.xml`; the URI is the document's, which is how MarkLogic reaches its properties |
| `BINARY_`, `DOM_`, `CONVERTED_PDF_`, `TRANSLATED_PDF_FILE_NAME` / `_URI` | what that version's `ML_BINARY_FIELD`, `ML_DOM_FIELD`, `ML_CONVERTED_PDF_FIELD`, `ML_TRANSLATED_PDF_FIELD` point at |
| `DOC_ML_HASH_SHA256` | MarkLogic's SHA-256 of the envelope |
| `DOC_SF_HASH_SHA256` | SHA-256 Snowflake computed of the staged envelope (filled in once verified) |
| `MIG_STATUS` | `CREATED` before upload; `PROCESSED` once verified and marked; `EXCEPTION` if a step failed |
| `COMMENT` | why, for `EXCEPTION`; notes such as "no binaryUri in this envelope" |

Rows are written with `MERGE` on `(DOCUMENT_ID, VERSION_ID)`, so a re-run updates
them; Snowflake does not enforce the primary key itself. A value longer than its
column (file names are `VARCHAR(100)`) is never cut: that document is not
uploaded and the reason is printed and put in the report.

**Stopgap while the Snowflake user cannot write the table:** `load --control-sql
control.sql` does everything else as usual but saves the `MERGE` statements to
that file instead of running them. Run the file in Snowflake as a user that can
write the table; running it twice does no harm. Until then the table lags behind
what is migrated. Drop the option once the grant is in place.

Every file those fields name is extracted and uploaded with the document - and,
since the PDF and DOM are versioned by Library Services like the envelope, every
version copy of them too (`<name>_pdf_versions/1-<uuid>.pdf`, ...).

Which version of a linked file goes on which row is decided by time. The current
envelope gets the current file. A version copy of the envelope gets the file
version whose DLS `created`..`replaced` window holds the envelope version's own
DLS `created` time (no `replaced` = still current). If none does, the row keeps
the URI the envelope names and `COMMENT` says so.

`python migrate.py family --uri U [--xml]` shows every version, link and
timestamp of a document, read-only.

## Tracking what has been migrated

`load` writes a section into the MarkLogic properties of the document, every
version of it and every document it links to - everything it uploaded:

```xml
<snowflake-migration xmlns="http://ml2sf/snowflake-migration">
  <migrated>true</migrated>
  <migrated-timestamp>2026-09-23T06:58:30.87+05:30</migrated-timestamp>
</snowflake-migration>
```

Its own namespace, so it cannot collide with a property the source system uses.
Re-running `load` replaces the section rather than stacking copies; `--unmark`
writes `migrated=false` on the same family to put a document back in the queue.

Every `fetch` CSV carries a `migrated` column (`Yes` / `No`) read from that
section. `fetch --exclude-migrated` lists only what is still to do, which is how
a run resumes after a stop; `fetch --only-migrated` lists what is already done.

The filtering happens inside MarkLogic, as a query on the properties fragment,
so documents on the other side are never fetched. It needs the URI lexicon (on
by default) and is refused with `--query`.

**`load` writes to MarkLogic** - the account needs update permission on the
documents. Nothing else in this tool writes to the source.

Each document is read together with its MarkLogic properties and a SHA-256
computed inside MarkLogic over the exact text returned; the text is hashed again
locally and must match, or the document is reported as failed.

The document ID is looked up by field name (`ML_ID_PATH` in `.env`) anywhere in
the document, so `documentId` also matches `systemAttributes.documentId`.

### Selecting documents

`fetch` and `bench` accept `--collection`, `--directory`, or `--query` (a
MarkLogic string query).

## What it handles

| Concern | How |
|---|---|
| JSON documents | Parsed directly |
| XML documents | Converted to dicts; attributes -> `@name`, text -> `#text` |
| JSON stored as a binary | Decoded and parsed (uploads through Library Services arrive this way) |
| Other binaries (PDF) | Reported as unsupported - not built yet |
| Versions | Each version copy is its own URI; `fetch` reports `version` / `version_of` |

## Layout

| Path | Role |
|---|---|
| `migrate.py` | CLI entry point: stat / fetch / bench / sfcheck / family / extract / load |
| `config.py` | `.env` connection settings |
| `marklogic.py` | MarkLogic REST client, XML->dict, path walking |
| `snowflake_loader.py` | Snowflake login check, stage upload, SHA-256 check, control table |
| `sql/stage_sha256.sql` | the `STAGE_SHA256` function, for an admin to create once |

## Notes

- Use the App-Services port (**8000**), not Admin (8001). Admin serves no `/v1` API.
- Runs on Linux and Windows from the same tree. Folder and file names are built
  to be legal on both: separators and the characters Windows forbids become `_`,
  reserved device names are prefixed, and two names that differ only in case are
  renamed - Linux would keep them apart, Windows would overwrite one.
- In Git Bash on Windows, quote MarkLogic URIs (`--uri "/gds-docs/x.json"`) or
  set `MSYS_NO_PATHCONV=1`; otherwise `/gds-docs/...` is rewritten as a Windows
  path before Python sees it. PowerShell and Linux shells are unaffected.
- `ML_AUTH=kerberos` signs in as the logged-in Windows account; `ML_USER` /
  `ML_PASSWORD` are then unused.
