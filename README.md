# MarkLogic -> Snowflake migration

A one-time migration of documents of *any* shape out of MarkLogic into a Snowflake
stage: every document with all its versions and the files it names, verified by
SHA-256, recorded in a control table, and marked migrated in MarkLogic so a stopped
run resumes where it stopped. Nothing in the code knows a schema or a path layout.

## Setup

Linux:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env          # then fill it in
kinit                         # only for ML_AUTH=kerberos
venv/bin/python main.py status
```

Windows (PowerShell):

```powershell
python -m venv venv
venv\Scripts\pip install -r requirements.txt
Copy-Item .env.example .env   # then fill it in
venv\Scripts\python main.py status
```

`requirements.txt` picks the Kerberos library per platform: `requests-negotiate-sspi`
on Windows (the logged-in account), `requests-kerberos` elsewhere (the `kinit`
ticket, or the service account you are running as).

## Usage

The examples below are PowerShell; on Linux they are the same with `python3`.

```powershell
python main.py status                            # documents migrated / to go
python main.py status --detail                   # ... plus what the database holds: formats, versions, collections
python main.py plan                              # the documents still to do, oldest first -> CSV
python main.py migrate                           # everything not yet migrated, oldest first (resumable)
python main.py migrate --workers 4 --limit 1000  # 4 at a time, at most 1,000 this run
python main.py migrate --id <document id>        # just that document - no URI needed
python main.py migrate --uri /path/doc.json      # ... or by its URI
python main.py migrate --from-csv ids.csv        # the documents listed in a CSV (ids or uris)
python main.py family --id <document id>         # its versions and the PDF/DOM version each gets
python main.py get --id <document id>            # write it to local disk only (dry run)
python main.py sfcheck                           # can we reach Snowflake? (--write to test writing)
```

## What a document folder holds

`get` writes to local disk in the shape the Snowflake stage expects, so what
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
than the database and schema the connection uses. `get` only writes the tree
locally; `migrate` builds the same tree and uploads it.

## Checking the stage copy

Every document folder also gets `_manifest.json`: each file's name, source URI,
size, SHA-256 (the same SHA-256 MarkLogic computed) and MD5. It goes up with the files.

`migrate` always checks that the stage holds MarkLogic's bytes, by SHA-256 at both ends:

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
database and schema. If it is missing, `migrate` stops before doing anything and
prints that file, under the configured name.

**Until the function exists:** `SF_HASH_CHECK=off` in `.env` uploads without the
Snowflake-side check. MarkLogic vs disk is still checked and every file must still
be in the stage. Each such file's report row says `NOT hash-checked`, and the
control-table `COMMENT` says `NOT hash-checked (SF_HASH_CHECK=off)`, with
`DOC_SF_HASH_SHA256` empty - so those documents can be found and re-run once the
check is on. Remove the setting (it defaults to `on`) as soon as the function is
created.

A document with any file missing or different is not marked migrated. Every file
checked is a row in `output/migrate-report-<timestamp>.csv`: every hash and size at
every step (MarkLogic, disk, what `PUT` read and sent, what `LIST` shows, what
Snowflake hashed), and the result.

## Migration control table

`migrate` writes one row per envelope - the document and each version of it - into
`SF_CONTROL_TABLE` (e.g. `GDX_DB.GDX.MARKLOGIC_SNOWFLAKE_MIG_CONTROL`). The table
must already exist; `SF_ROLE` needs SELECT, INSERT and UPDATE on it.

The table is the hand-off to the team downstream, so `migrate` only ever writes
`MIG_STATUS = 'CREATED'`, and only for a document that is fully in Snowflake -
uploaded and verified. Moving it on (to `PROCESSED`, `EXCEPTION`, ...) is theirs.
A document that fails gets no row; the run's report says why, and the next run
tries it again. The rows are written *before* the document is marked migrated in
MarkLogic, so a document is never marked without its rows.

| Column | Value |
|---|---|
| `DOCUMENT_ID` | the document folder name (its ID, else the UUID in its URI) |
| `VERSION_ID` | the DLS `version-id` of each version copy. The current document has none of its own: it is the latest copy (same `created`, never `replaced`), so that copy's row stands for it and `COMMENT` names it. Only a document outside Library Services gets `0` |
| `ENVELOPE_FILE_NAME` / `_URI` | that version's own name and MarkLogic URI |
| `PROPERTY_FILE_NAME` / `_URI` | its `.properties.xml`; the URI is the document's, which is how MarkLogic reaches its properties |
| `BINARY_`, `DOM_`, `CONVERTED_PDF_`, `TRANSLATED_PDF_FILE_NAME` / `_URI` | what that version's `ML_BINARY_FIELD`, `ML_DOM_FIELD`, `ML_CONVERTED_PDF_FIELD`, `ML_TRANSLATED_PDF_FIELD` point at |
| `DOC_ML_HASH_SHA256` | MarkLogic's SHA-256 of the envelope |
| `DOC_SF_HASH_SHA256` | SHA-256 Snowflake computed of the staged envelope (filled in once verified) |
| `MIG_STATUS` | always `CREATED`: in Snowflake and ready for the team downstream |
| `COMMENT` | notes, e.g. "no binaryUri in this envelope", "verified 9 file(s) in the stage by SHA-256", or "NOT hash-checked (SF_HASH_CHECK=off)" |

Rows are written with `MERGE` on `(DOCUMENT_ID, VERSION_ID)`, so a re-run updates
them; Snowflake does not enforce the primary key itself. Migrating a document again
(after `reset`, or with `--id`) sets its rows back to `CREATED` - its files in the
stage were just replaced, so the team downstream should pick it up again. A value longer than its
column (file names are `VARCHAR(100)`) is never cut: that document is not
uploaded and the reason is printed and put in the report.

**Stopgap while the Snowflake user cannot write the table:** `migrate --control-sql
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

`python main.py family --uri U [--xml]` shows every version, link and
timestamp of a document, read-only.

## Migrating everything

`migrate` with no `--uri` / `--id` / `--from-csv` migrates every document still to do,
oldest first:

```
python main.py status                      # where things stand - changes nothing
python main.py migrate --workers 4 --limit 1000   # the next 1,000, 4 at a time
python main.py migrate --workers 4            # all the rest
```

- **What a document is - no path involved:** a record that holds a document ID
  (`ML_ID_PATH`, e.g. `documentId`, at any depth in the JSON) and is an *original*,
  i.e. not a Library Services version copy (MarkLogic marks those with the URI of
  their original). Where it is stored does not matter.
- **What goes with it:** all its versions and the files its link fields name
  (`ML_LINK_PATH`: the PDF, the DOM) with all *their* versions, into
  `@stage/<document id>/` - exactly as `migrate --id` does.
- **Records sharing an ID** are one document: the plan keeps the one the others are
  linked from (a DOM holding the same ID is reached from its envelope). If two hold
  the same ID and neither links to the other, the oldest is taken and the plan's
  `note` column names the other, for you to check.
- **Records with no ID** that no document points at are not migrated; the plan
  lists them in `output/plan-<database>-no-id.csv`, so nothing is left out unseen.
- **Order:** by `ML_CREATED_PATH` (`createdDate`), else the Library Services
  created time; oldest first, undated last.
- **The plan:** the first run reads every remaining record's ID, date and links
  once, inside MarkLogic, and saves the sorted list to
  `output/plan-<database>.csv`; later runs reuse it. `--replan` rebuilds it (e.g.
  for documents added since). A plan file can also be split and fed back with
  `--from-csv`.
- **Skipping what is done:** the migrated flag is the only record that counts.
  Each run checks the plan against it up front (to know exactly how many are left)
  and each document again just before it is processed.
- **Parallel:** `--workers N` migrates N documents at a time, each with its own
  Snowflake connection; each is verified and marked on its own, so this is as safe
  as one at a time. Start small and watch docs/min.
- **Stopping and resuming:** Ctrl+C stops taking new documents and lets those in
  hand finish (a second Ctrl+C abandons them - they are not marked, so they are
  redone). A closed session or a crash is the same: run the command again. A
  document that failed is left unmarked and tried again next run.
- **Disk:** each document's local folder is removed once it is uploaded
  (`--keep-local` keeps them).
- **Directory table:** at the end of a run that uploaded anything, `migrate` runs
  `ALTER STAGE ... REFRESH`, so the Snowflake UI's stage page and `DIRECTORY(@stage)`
  show the new files (internal stages never refresh by themselves). If the role may
  not refresh the stage, the run says so and prints the statement to run by hand;
  the files are in the stage either way. `--no-refresh` skips it.
- **Statistics:** `status` prints documents, migrated (%) and to go, counted in
  MarkLogic. A run prints the same before and after, and a line after each document:

```
[12/1,000]  ok 11  failed 1  |  9.6 docs/min, run ETA 1.7 hours, all ETA 2.4 days  |  overall 1,245/12,345 migrated (10.1%)
```

## Tracking what has been migrated

`migrate` writes a section into the MarkLogic properties of the document, every
version of it and every document it links to - everything it uploaded:

```xml
<snowflake-migration xmlns="http://ml2sf/snowflake-migration">
  <migrated>true</migrated>
  <migrated-timestamp>2026-09-23T06:58:30.87+05:30</migrated-timestamp>
</snowflake-migration>
```

Its own namespace, so it cannot collide with a property the source system uses.
Re-running `migrate` replaces the section rather than stacking copies.

`reset` takes the section off again - removed, not set to false - so `migrate`
treats the document as never migrated:

```
python main.py reset --id <document id>         # that document, its versions, PDF and DOM
python main.py reset --from-csv ids.csv         # the documents listed in a CSV
python main.py reset --all                      # every record in the database (asks first)
```

Only the mark changes: the files in the stage and the control-table rows stay
until the next `migrate` of that document replaces them. A bulk `migrate` works
from its plan, so rebuild it (`plan` or `migrate --replan`) to include documents
reset after it was built.

All selection by this flag happens inside MarkLogic, as a query on the properties
fragment, so nothing is read to find out what is left. It needs the URI lexicon
(on by default).

**`migrate` writes to MarkLogic** - the account needs update permission on the
documents. Nothing else in this tool writes to the source.

Each document is read together with its MarkLogic properties and a SHA-256
computed inside MarkLogic over the exact text returned; the text is hashed again
locally and must match, or the document is reported as failed.

The document ID is looked up by field name (`ML_ID_PATH` in `.env`) anywhere in
the document, so `documentId` also matches `systemAttributes.documentId`.

## What it handles

| Concern | How |
|---|---|
| JSON documents | Parsed directly |
| XML documents | Converted to dicts; attributes -> `@name`, text -> `#text` |
| JSON stored as a binary | Decoded and parsed (uploads through Library Services arrive this way) |
| Other binaries (PDF) | Copied byte for byte, checked against MarkLogic's SHA-256 |
| Versions | Every Library Services version copy goes with its original |

## Layout

| Path | Role |
|---|---|
| `main.py` | CLI entry point: status / plan / migrate / family / get / sfcheck |
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
