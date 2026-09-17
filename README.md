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

## Tracking what has been migrated

`load` writes a section into the document's own MarkLogic properties:

```xml
<snowflake-migration xmlns="http://ml2sf/snowflake-migration">
  <migrated>true</migrated>
  <migrated-timestamp>2026-09-23T06:58:30.87+05:30</migrated-timestamp>
</snowflake-migration>
```

Its own namespace, so it cannot collide with a property the source system uses.
Re-running `load` replaces the section rather than stacking copies; `--unmark`
writes `migrated=false` to put a document back in the queue.

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
| `migrate.py` | CLI entry point: stat / fetch / bench / sfcheck / extract / load |
| `config.py` | `.env` connection settings |
| `marklogic.py` | MarkLogic REST client, XML->dict, path walking |
| `snowflake_loader.py` | Snowflake reachability and login check |

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
