"""MarkLogic -> Snowflake migration driver.

  python main.py status                        documents migrated / to go (counted in MarkLogic)
  python main.py plan                          list the documents still to do, oldest first,
                                               in output/plan-<database>.csv - migrates nothing
  python main.py migrate  [--workers N]        every document not yet migrated, oldest first;
                          [--limit N]          stop any time (Ctrl+C), run again to resume
  python main.py migrate  --id I | --uri U     just these documents
  python main.py migrate  --from-csv F         the documents listed in a CSV (e.g. part of a plan)
  python main.py reset    --id I | --uri U     take the migrated mark off a document (its
                                               versions and files too), so migrate redoes it
  python main.py reset    --all                ... off every record in the database
  python main.py family   --id I | --uri U     a document's versions, PDF/DOM and timestamps
  python main.py get      --id I | --uri U     write a document to local disk only (dry run)
  python main.py sfcheck  [--write]            can we reach Snowflake with these settings?

A document is a record holding a document ID (ML_ID_PATH) that is not a
Library Services version copy - wherever it is stored. It is migrated with all
its versions and the files it names (ML_LINK_PATH) into @stage/<document id>/.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

import requests

from config import ML, OUTPUT_DIR, SF, STAGE
from marklogic import MIGRATION_NS, MarkLogicClient, MarkLogicError, find_key, sha256_hex
from snowflake_loader import (CONTROL_WIDTHS, HASH_FUNCTION_FILE, check_connection,
                              check_control_table, connect, control_rows_sql,
                              hash_function_exists, hash_function_sql, merge_control_rows,
                              reach_check, refresh_directory, stage_sha256, upload_folder)


def client() -> MarkLogicClient:
    kerberos = ML.AUTH == "kerberos"
    return MarkLogicClient(
        host=ML.HOST, port=ML.PORT,
        user="" if kerberos else ML.user(),
        password="" if kerberos else ML.password(),
        database=ML.DATABASE, auth=ML.AUTH, scheme=ML.SCHEME,
    )


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}"
        n /= 1024


def human_time(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f} seconds"
    if seconds < 5400:
        return f"{seconds / 60:.1f} minutes"
    if seconds < 172800:
        return f"{seconds / 3600:.1f} hours"
    return f"{seconds / 86400:.1f} days"


# ---------- commands ----------

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
    envelope version with the PDF/DOM version `migrate` pairs it with, then each
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
    versions, the PDF/DOM version `migrate` pairs each with (by time), and those
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


def cmd_get(args) -> int:
    """Write documents to local disk in the shape the Snowflake stage expects.

    One folder per document, named by the document's ID, holding the document,
    every version of it, whatever it points at, and a .properties.xml beside
    each one. What lands here is what `migrate` uploads; nothing is uploaded here.
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
    `migrate` uploads together and so marks together."""
    family = cli.family(uri, links=ML.link_fields())
    members = [family["master"], *(family.get("versions") or [])]
    for link in family.get("links") or []:
        members += [link, *(link.get("versions") or [])]
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


# ---------- which documents, in what order ----------
#
# "A document" is a record that holds a document ID (ML_ID_PATH) and is an
# original - not a Library Services version copy. Nothing depends on where it
# is stored: the ID decides its stage folder, and everything that belongs to
# it - its versions, the PDF and DOM it names and theirs - goes in with it.

def id_field() -> str:
    return ML.ID_PATH or "documentId"


def migration_status(cli) -> dict:
    """Documents migrated and still to go, counted in MarkLogic from the
    migrated flag - right however earlier runs ended."""
    migrated = cli.count_documents(id_field(), migrated=True)
    remaining = cli.count_documents(id_field(), migrated=False)
    return {"total": migrated + remaining, "migrated": migrated, "remaining": remaining}


def print_status(stats: dict, title: str = "MIGRATION STATUS") -> None:
    total = stats["total"] or 1
    print(f"{title}   {ML.DATABASE}")
    print(f"  Documents        {stats['total']:>12,}   (records holding a {id_field()}; "
          "version copies and the files they name are not counted)")
    print(f"  Migrated         {stats['migrated']:>12,}   ({stats['migrated'] / total:.1%})")
    print(f"  To go            {stats['remaining']:>12,}")


def plan_path() -> Path:
    return OUTPUT_DIR / f"plan-{safe_name(ML.DATABASE)}.csv"


PLAN_FIELDS = ["document_id", "uri", "created", "records", "note"]


def created_key(value: str) -> tuple:
    """Sort key for a creation time: oldest first, missing or unreadable last."""
    text = (value or "").strip()
    when = parse_time(text)
    if when is None and re.fullmatch(r"\d{4}-\d\d-\d\d", text):
        when = parse_time(text + "T00:00:00Z")
    return (when is None, when.timestamp() if when else 0.0, text)


def build_plan(cli, path: Path, page: int = 1000) -> list[dict]:
    """Every document still to migrate, one entry per document ID, oldest first,
    saved to `path` so later runs resume from it.

    Records sharing an ID are one document: the entry is the record none of the
    others links to (a DOM holding the ID is reached from its envelope). Also
    lists, in a file of its own, originals with no ID that no document points
    at - so nothing is left behind unnoticed.
    """
    links = ML.link_fields()
    print(f"Planning: documents not yet migrated (records holding a {id_field()}, "
          f"not version copies), by {ML.CREATED_PATH or 'Library Services created time'} ...")
    rows: list[dict] = []
    after, started = "", time.monotonic()
    while True:
        result = cli.plan_page(after, page, id_field(), ML.CREATED_PATH, links)
        got = result.get("rows") or []
        rows += got
        if len(rows) // 10_000 > (len(rows) - len(got)) // 10_000:
            print(f"  {len(rows):,} read ({human_time(time.monotonic() - started)})")
        if not result.get("last"):
            break
        after = result["last"]

    linked = {link for row in rows for link in row.get("links") or []}
    groups: dict[str, list[dict]] = {}
    for row in rows:
        groups.setdefault(row["id"] or f"(no id) {row['uri']}", []).append(row)

    entries = []
    for doc_id, members in groups.items():
        members.sort(key=lambda r: created_key(r["created"]))
        roots = [m for m in members if m["uri"] not in linked]
        note = ""
        if len(roots) == 1:
            root = roots[0]
        elif not roots:
            root = members[0]
            note = "every record with this ID is linked from another; took the oldest"
        else:
            root = roots[0]
            note = (f"{len(roots)} records hold this ID and none links to the others; took the "
                    f"oldest, check: " + " ".join(r["uri"] for r in roots[1:]))
        entries.append({"document_id": doc_id, "uri": root["uri"], "created": members[0]["created"],
                        "records": len(members), "note": note})
    entries.sort(key=lambda e: (created_key(e["created"]), e["document_id"]))

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=PLAN_FIELDS)
        writer.writeheader()
        writer.writerows(entries)
    undated = sum(1 for e in entries if created_key(e["created"])[0])
    flagged = sum(1 for e in entries if e["note"])
    print(f"  {len(rows):,} records -> {len(entries):,} documents, oldest first -> {path}")
    if undated:
        print(f"  {undated:,} without a readable creation time go last")
    if flagged:
        print(f"  {flagged:,} have a note in the plan (the 'note' column) - worth a look")

    # Originals with no ID that no document points at: not part of any document.
    loose, after = [], ""
    while True:
        uris = cli.no_id_page(after, page, id_field())
        if not uris:
            break
        loose += [u for u in uris if u not in linked]
        after = uris[-1]
    if loose:
        loose_path = path.with_name(path.stem + "-no-id.csv")
        with loose_path.open("w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerows([["uri"], *([u] for u in loose)])
        print(f"  {len(loose):,} originals hold no {id_field()} and no document points at them: "
              f"NOT migrated, listed in {loose_path}")
    return entries


def read_plan(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return [row for row in csv.DictReader(fh) if row.get("uri")]


def pending(cli, entries: list[dict], limit: int, batch: int, workers: int = 8) -> list[str]:
    """The plan's documents not yet migrated, in plan order, at most `limit` -
    checked against the migrated flag up front (several batches at once), so
    the run knows exactly how many it has to do."""
    parts = [entries[i:i + batch] for i in range(0, len(entries), batch)]
    todo: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for part, done in zip(parts, pool.map(
                lambda part: cli.migrated_among([e["uri"] for e in part]), parts)):
            todo += [e["uri"] for e in part if e["uri"] not in done]
            if len(todo) >= limit:
                break
    return todo[:limit]


def uris_from_csv(cli, path: Path) -> list[str] | None:
    """Documents listed in a CSV: a 'uri' column, or a 'document_id' column
    (each looked up by ID), or else the first column as URIs. A plan file works."""
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    if not rows:
        return []
    head = [h.strip().lower() for h in rows[0]]
    if "uri" in head:
        return [r[head.index("uri")].strip() for r in rows[1:] if len(r) > head.index("uri")
                and r[head.index("uri")].strip()]
    if "document_id" in head:
        ids = [r[head.index("document_id")].strip() for r in rows[1:]
               if len(r) > head.index("document_id") and r[head.index("document_id")].strip()]
        args = argparse.Namespace(uri=[], id=ids)
        return args.uri if resolve_uris(cli, args) else None
    return [r[0].strip() for r in rows if r and r[0].strip()]


def cmd_status(args) -> int:
    """How far the migration has got: counted in MarkLogic, nothing changed."""
    cli = client()
    print_status(migration_status(cli))
    path = plan_path()
    if path.exists():
        built = datetime.fromtimestamp(path.stat().st_mtime)
        print(f"  Plan             {path}  ({len(read_plan(path)):,} documents when "
              f"built {built:%Y-%m-%d %H:%M})")
    else:
        print("  Plan             not built yet - `plan`, or the first `migrate` without "
              "--uri/--id/--from-csv, builds it")
    if args.detail:
        print()
        print_inventory(cli.inventory(top=args.top))
    return 0


def print_inventory(inv: dict) -> None:
    """Everything in the database, not just the documents being migrated:
    records by format, version copies, collections - all from the indexes."""
    records = inv["records"] or 1
    print(f"DATABASE CONTENTS   {ML.DATABASE}  (from the indexes - nothing read)")
    print(f"  Records (all)    {inv['records']:>12,}   documents, versions, PDFs, DOMs - everything")
    if inv.get("formats_error"):
        print(f"    by format: not available - {inv['formats_error']}")
    for name, label in [("json", "JSON"), ("xml", "XML"), ("text", "Text"),
                        ("binary", "Binary (PDF...)")]:
        n = inv["formats"].get(name, 0)
        if n:
            print(f"    {label:<15}{n:>12,}   ({n / records:.1%})")
    print(f"  Version copies   {inv['version_copies']:>12,}   (Library Services)")
    print(f"  Marked migrated  {inv['migrated_records']:>12,}   records of any kind "
          "(a document's versions, PDF and DOM are marked with it)")
    if inv.get("collections_error"):
        print(f"  Collections      not available: {inv['collections_error']}")
    elif inv.get("collections") is not None:
        shown, total = inv["collections"], inv.get("collections_total", 0)
        print(f"  Collections      {total:>12,}" + (f"   (largest {len(shown)})" if total > len(shown) else ""))
        width = min(max([len(c["name"]) for c in shown] + [10]), 48)
        for c in shown:
            name = c["name"] if len(c["name"]) <= width else c["name"][:width - 3] + "..."
            print(f"    {name:<{width}}  {c['count']:>11,}")


def cmd_plan(args) -> int:
    """Build the plan - every document still to migrate, oldest first - and stop.

    Migrates nothing. The CSV can be read, split, and parts of it handed to
    `migrate --from-csv`; a `migrate` without --uri/--id/--from-csv resumes
    from it as well. Replaces any earlier plan.
    """
    cli = client()
    print_status(migration_status(cli))
    print()
    entries = build_plan(cli, plan_path())
    print()
    print(f"Plan ready: {len(entries):,} document(s). Migrate them all with "
          "`python main.py migrate`, or a part with `--from-csv <part of the plan>`.")
    return 0


def cmd_reset(args) -> int:
    """Take the migrated mark off documents again: the migration section is
    removed from their properties (not set to false), so `migrate` treats them
    as never migrated. Only that section changes; the stage and the control
    table are left as they are - the next migrate replaces both.
    """
    cli = client()
    if args.all:
        return reset_everything(cli, args.yes)
    if args.from_csv:
        listed = uris_from_csv(cli, Path(args.from_csv))
        if listed is None:
            return 1
        args.uri = (args.uri or []) + listed
    if not (args.uri or args.id):
        print("Say which documents: --id, --uri or --from-csv - or --all for every record.")
        return 1
    if args.id and not resolve_uris(cli, args):
        return 1
    failed = 0
    for uri in dict.fromkeys(args.uri):
        # The same records migrate marks: the document, its versions, the
        # files it names and their versions.
        results = cli.demark(family_uris(cli, uri))
        errors = [f"{r['uri']}: {r['error']}" for r in results if r.get("error")]
        removed = sum(1 for r in results if r.get("removed"))
        print(f"  {'FAILED' if errors else 'reset ':<6} {uri}  "
              f"({removed} of {len(results)} record(s) had the mark)"
              + "".join(f"\n             {e}" for e in errors))
        failed += 1 if errors else 0
    total = len(dict.fromkeys(args.uri))
    print(f"\nReset {total - failed} of {total} document(s). `migrate --id`/`--uri` "
          "migrates them again; a bulk `migrate` needs a new plan to include them "
          "(`plan`, or `migrate --replan`).")
    return 1 if failed else 0


def reset_everything(cli, confirmed: bool, page: int = 1000) -> int:
    """Remove the migration section from every record in the database."""
    uris, after = [], ""
    while True:
        batch = cli.marked_page(after, page)
        if not batch:
            break
        uris += batch
        after = batch[-1]
    if not uris:
        print(f"No record in {ML.DATABASE} carries the migration mark.")
        return 0
    print(f"{len(uris):,} record(s) in {ML.DATABASE} carry the migration mark. Removing it "
          "makes migrate treat all of them as never migrated.")
    if not confirmed:
        try:
            answer = input("Type yes to remove it from all of them: ")
        except EOFError:
            answer = ""
        if answer.strip().lower() != "yes":
            print("Nothing changed.")
            return 1
    removed, errors, started = 0, [], time.monotonic()
    for i in range(0, len(uris), page):
        for r in cli.demark(uris[i:i + page]):
            if r.get("error"):
                errors.append(f"{r['uri']}: {r['error']}")
            elif r.get("removed"):
                removed += 1
        print(f"  {min(i + page, len(uris)):,}/{len(uris):,} "
              f"({human_time(time.monotonic() - started)})", flush=True)
    print(f"\nRemoved the mark from {removed:,} record(s)"
          + (f"; {len(errors):,} failed:" if errors else "."))
    for e in errors[:20]:
        print(f"  {e}")
    if len(errors) > 20:
        print(f"  ... {len(errors) - 20:,} more")
    if plan_path().exists():
        print(f"\nThe plan {plan_path()} predates this: rebuild it with `plan` "
              "(or `migrate --replan`) before a bulk migrate.")
    return 1 if errors else 0


# ---------- migrating documents ----------

def mark_quietly(cli, uris: list[str], migrated: bool) -> list[str]:
    """Set the migrated flag on these URIs; return the errors, if any."""
    return [f"{r['uri']}: {r['error']}"
            for r in cli.mark_migrated(uris, migrated=migrated, workers=4) if r.get("error")]


class Loader:
    """Migrates one document at a time - safe to use from several threads.

    Each thread gets its own Snowflake connection and its own local work
    folder. Shared things (the report, the control-table SQL file, the counts)
    are written under a lock, and one document's stage folder is only ever
    worked on by one thread at a time, so the same document named twice cannot
    collide. Nothing is printed here: each document's lines are returned, for
    the caller to print whole.
    """

    def __init__(self, cli, settings: dict, table: str, function: str, work_root: Path,
                 keep_dir: Path | None, report, sql_file=None, control_sql: str = "",
                 skip_migrated: bool = False):
        self.cli, self.settings, self.table, self.function = cli, settings, table, function
        self.skip_migrated = skip_migrated
        self.work_root, self.keep_dir = work_root, keep_dir
        self.target_locks: dict[str, threading.Lock] = {}
        self.report_fh, self.sql_file, self.control_sql = report, sql_file, control_sql
        self.writer = csv.DictWriter(report, fieldnames=REPORT_FIELDS)
        self.writer.writeheader()
        self.lock = threading.Lock()
        self.local = threading.local()
        self.connections = []
        # What went up, for the performance summary at the end of a run.
        self.files_sent, self.bytes_sent = 0, 0

    def target_lock(self, target: str) -> threading.Lock:
        """The lock for one stage folder: one thread at a time works on it."""
        with self.lock:
            return self.target_locks.setdefault(target, threading.Lock())

    def cursor(self):
        if not hasattr(self.local, "cursor"):
            conn = connect(self.settings)
            with self.lock:
                self.connections.append(conn)
            self.local.cursor = conn.cursor()
        return self.local.cursor

    def close(self) -> None:
        for conn in self.connections:
            try:
                conn.close()
            except Exception:
                pass

    def report(self, rows: list[dict]) -> None:
        with self.lock:
            self.writer.writerows(rows)
            self.report_fh.flush()

    def record(self, rows: list[dict], status: str, note: str = "") -> str:
        """Write the document's control rows with this status; '' or the error."""
        for row in rows:
            row["MIG_STATUS"] = status
            comment = "; ".join([*row["_notes"], *([note] if note else [])])
            row["COMMENT"] = (comment[:997] + "...") if len(comment) > 1000 else comment or None
        try:
            if self.sql_file:
                with self.lock:
                    self.sql_file.write(control_rows_sql(self.table, rows))
                    self.sql_file.flush()
            else:
                merge_control_rows(self.cursor(), self.table, rows)
            return ""
        except Exception as exc:
            return f"control table ({status}): {type(exc).__name__}: {exc}"

    def migrate(self, uri: str) -> tuple[bool, list[str]]:
        """Extract, upload, verify, mark and record one document. Returns
        (ok, lines to print). A failure leaves it unmarked for the next run."""
        try:
            return self._migrate(uri)
        except Exception as exc:                    # anything unforeseen: this one fails
            self.report([{"document_uri": uri, "result": f"FAILED - {type(exc).__name__}: {exc}"}])
            return False, [f"  FAILED   {uri}", f"           {type(exc).__name__}: {exc}"]
        finally:
            # Free the disk as we go: a TB-sized run cannot keep every folder
            # (--keep-local has copied it to STAGE_DIR already).
            folder = getattr(self.local, "folder", None)
            if folder:
                shutil.rmtree(folder, ignore_errors=True)
            self.local.folder = None

    def _migrate(self, uri: str) -> tuple[bool, list[str]]:
        lines: list[str] = []
        if self.skip_migrated and self.cli.migrated_among([uri]):
            # Done by another run since this one was planned: nothing to do.
            return True, [f"  SKIPPED  {uri}  (already migrated)"]

        def fail(reason: str, *details: str) -> tuple[bool, list[str]]:
            # A failure gets no control-table row - the table is for documents
            # that are fully in Snowflake. The report says why.
            lines[:0] = [f"  FAILED   {uri}", f"           {reason}"]
            lines.extend(f"           {d}" for d in details)
            self.report([{"document_uri": uri, "result": "FAILED - " + "; ".join([reason, *details])}])
            return False, lines

        # This thread's own work folder: two threads never write the same files.
        result = extract_one(self.cli, uri, self.work_root / f"w{threading.get_ident()}")
        if result.get("error"):
            return fail(result["error"])
        self.local.folder = result["folder"]
        control, too_long = control_rows(result)
        if too_long:
            return fail("not uploaded - does not fit the control table", *too_long)
        if result["problems"]:
            return fail("not uploaded", *result["problems"])

        target = STAGE.snowflake_path(result["document_id"])
        with self.target_lock(target):
            cur = self.cursor()
            try:
                up = upload_folder(cur, result["folder"], target)
            except Exception as exc:
                return fail(f"upload: {type(exc).__name__}: {exc}")
            try:
                hashes = (stage_sha256(cur, self.function, target, sorted(up["listed"]))
                          if SF.HASH_CHECK else None)
            except Exception as exc:
                return fail(f"hashing in Snowflake: {type(exc).__name__}: {exc}")
            rows = check_stage(result, up["listed"], hashes, up["put"])
            self.report(rows)
            bad = [r for r in rows if not r["result"].startswith("OK")]
            if up["failed"] or bad:
                return fail("stage copy does not match - not marked",
                            *(f"not uploaded: {line}" for line in up["failed"]),
                            *(f"{r['result']}: {r['file']}" for r in bad))
            if hashes is not None:
                for row in control:
                    row["DOC_SF_HASH_SHA256"] = hashes.get(row["_file"])
            with self.lock:
                self.files_sent += len(rows)
                self.bytes_sent += sum(int(r["size"] or 0) for r in rows)

            # Hand it to the team downstream: CREATED is the only status written
            # here. Before marking, so a document never ends up marked migrated
            # without its rows - if this fails it stays unmarked and is redone.
            verified = (f"verified {len(rows)} file(s) in the stage by SHA-256" if hashes is not None
                        else f"uploaded {len(rows)} file(s); stage copy NOT hash-checked "
                             "(SF_HASH_CHECK=off) - re-run with the check on")
            error = self.record(control, "CREATED", verified)
            if error:
                return fail(f"uploaded and verified, but {error} - not marked, redone next run")

            # The whole family went up, so the whole family is marked.
            errors = mark_quietly(self.cli, result["members"], migrated=True)
            if errors:
                return fail("uploaded, verified and recorded CREATED, but not marked migrated in "
                            "MarkLogic - redone next run (the rows are simply written again)", *errors)
            lines.append(f"  OK       {result['document_id']}  {result['uri']}")
            lines.append(f"           {up['files']} file(s), {result['versions']} version(s), "
                         f"{result['links']} linked -> {target}/; "
                         + ("verified by SHA-256" if hashes is not None else "NOT hash-checked")
                         + "; " + (f"control rows in {self.control_sql}" if self.sql_file
                                   else f"{len(control)} control row(s) CREATED"))
            if self.keep_dir is not None:
                # --keep-local: the uploaded tree, kept where STAGE_DIR says.
                kept = self.keep_dir / result["document_id"]
                shutil.rmtree(kept, ignore_errors=True)
                shutil.copytree(result["folder"], kept)
            return True, lines


# Each worker makes up to 4 MarkLogic requests at once (marking runs 4 in
# parallel); 16 workers is already 64 - more than a MarkLogic app server
# usually serves at once (its default is 32 threads).
MAX_WORKERS = 16


def print_progress(done: int, ok: int, planned: int, started: float, stats: dict | None) -> None:
    """After each document: where this run is, its speed, and - for a bulk
    run - where the whole migration stands."""
    elapsed = time.monotonic() - started
    rate = done / elapsed * 60 if done and elapsed else 0.0
    line = f"[{done:,}/{planned:,}]  ok {ok:,}  failed {done - ok:,}  |  {rate:.1f} docs/min"
    if rate:
        line += f", run ETA {human_time((planned - done) / rate * 60)}"
    if stats:
        migrated = stats["migrated"] + ok
        if rate:
            line += f", all ETA {human_time(max(stats['remaining'] - ok, 0) / rate * 60)}"
        line += (f"  |  overall {migrated:,}/{stats['total']:,} migrated "
                 f"({migrated / (stats['total'] or 1):.1%})")
    print(line, flush=True)


def run_documents(loader: Loader, uris, planned: int, workers: int,
                  stats: dict | None) -> tuple[int, int, bool]:
    """Migrate `uris` with `workers` at a time. Returns (done, ok, stopped).

    Ctrl+C stops taking new documents and lets the ones in hand finish, so
    none is left half done; a second Ctrl+C abandons those too (they are not
    marked, so the next run redoes them).
    """
    started, done, ok, stopped = time.monotonic(), 0, 0, False
    pool = ThreadPoolExecutor(max_workers=workers)
    in_hand: set = set()

    def finish(futures) -> None:
        nonlocal done, ok
        for future in futures:
            in_hand.discard(future)
            success, lines = future.result()
            done += 1
            ok += 1 if success else 0
            print("\n".join(lines))
            print_progress(done, ok, planned, started, stats)

    try:
        for uri in uris:
            while len(in_hand) >= workers * 2:
                finished, _ = wait(in_hand, return_when=FIRST_COMPLETED)
                finish(finished)
            in_hand.add(pool.submit(loader.migrate, uri))
        while in_hand:
            finished, _ = wait(in_hand, return_when=FIRST_COMPLETED)
            finish(finished)
    except KeyboardInterrupt:
        stopped = True
        for future in list(in_hand):
            if future.cancel():
                in_hand.discard(future)
        print(f"\nStopping: finishing the {len(in_hand)} document(s) in hand "
              "(Ctrl+C again to abandon them) ...", flush=True)
        try:
            while in_hand:
                finished, _ = wait(in_hand, return_when=FIRST_COMPLETED)
                finish(finished)
        except KeyboardInterrupt:
            print(f"Abandoned {len(in_hand)} document(s) part way: not marked, "
                  "so the next run does them again.")
    pool.shutdown(wait=not in_hand, cancel_futures=True)
    return done, ok, stopped


def cmd_migrate(args) -> int:
    """Migrate documents to the Snowflake stage, verify them, record them in the
    control table and mark them migrated in MarkLogic.

    Which documents: --uri / --id / --from-csv, or - with none of those - every
    document not yet migrated, oldest first, from a plan built once and reused,
    so a stopped run carries on where it stopped. --workers documents at a time.

    Per document: extract its whole family (SHA-256 checked against MarkLogic)
    into <document id>/, replace that stage folder with it, check every staged
    file (SHA-256 in Snowflake), record each version in the control table as
    CREATED - the only status written here; a team downstream takes it from
    there - and then mark every member of the family migrated. A document that
    fails gets no row and is left unmarked, so the next run tries it again.
    """
    if not 1 <= args.workers <= MAX_WORKERS:
        print(f"--workers must be between 1 and {MAX_WORKERS}: each worker makes up to 4 "
              "MarkLogic requests at once, and MarkLogic serves a limited number in parallel.")
        return 1
    cli = client()
    bulk = not (args.uri or args.id or args.from_csv)
    stats = None
    if bulk:
        stats = migration_status(cli)
        print_status(stats, "BEFORE THIS RUN")
        print()
        if not stats["remaining"]:
            print("Nothing left to migrate.")
            return 0
        path = plan_path()
        resuming = path.exists() and not args.replan
        entries = read_plan(path) if resuming else build_plan(cli, path)
        if resuming:
            print(f"Resuming from the plan {path} ({len(entries):,} documents; "
                  "--replan rebuilds it)")
        print("Checking the plan against the migrated flag ...")
        uris = pending(cli, entries, args.limit or len(entries), args.batch_size)
        planned = len(uris)
        print(f"  {planned:,} document(s) to do this run"
              + (f" (--limit {args.limit:,})" if args.limit else ""))
        print()
        if not uris:
            print("Every document in the plan is migrated.")
            explain_leftovers(stats["remaining"], 0)
            return 0
    else:
        if args.from_csv:
            listed = uris_from_csv(cli, Path(args.from_csv))
            if listed is None:
                return 1
            args.uri = (args.uri or []) + listed
        if args.id and not resolve_uris(cli, args):
            return 1
        uris = list(dict.fromkeys(args.uri))[:args.limit or None]
        planned = len(uris)

    stage, table, function = SF.stage(), SF.control_table(), SF.hash_function()
    report_path = OUTPUT_DIR / f"migrate-report-{datetime.now():%Y%m%d-%H%M%S}.csv"
    print(f"  Documents: {planned:,}" + ("  (this run; oldest first, migrated ones skipped)"
                                        if bulk else ""))
    print(f"  Workers  : {args.workers} document(s) at a time")
    print(f"  Stage    : {stage}")
    print(f"  Table    : {table}"
          + (f"  - NOT written; statements go to {args.control_sql}" if args.control_sql else ""))
    print(f"  Verify   : SHA-256 of each staged file, computed in Snowflake by {function}"
          if SF.HASH_CHECK else
          "  Verify   : OFF (SF_HASH_CHECK=off) - staged files are NOT hash-checked; "
          "MarkLogic vs disk still is")
    print(f"  Local    : {STAGE.DIR if args.keep_local else '(temporary, each folder removed when done)'}")
    print(f"  Report   : {report_path}")
    print()

    sql_file = open(args.control_sql, "w", encoding="utf-8") if args.control_sql else None
    if sql_file:
        sql_file.write(f"-- ml2sf migrate {datetime.now():%Y-%m-%d %H:%M:%S}: control-table "
                       f"statements for {table}.\n-- Run in order; each is a MERGE, "
                       "so running the file twice does no harm.\n")
    with tempfile.TemporaryDirectory(prefix="ml2sf-") as tmp, \
            open(report_path, "w", newline="", encoding="utf-8") as report:
        loader = Loader(cli, SF.conn_kwargs(), table, function, Path(tmp),
                        STAGE.DIR if args.keep_local else None,
                        report, sql_file, args.control_sql or "", skip_migrated=bulk)
        try:
            if not preflight(loader, function, table, bool(sql_file)):
                return 1
            started = time.monotonic()
            done, ok, stopped = run_documents(loader, uris, planned, args.workers, stats)
            elapsed = time.monotonic() - started
            if loader.files_sent and not args.no_refresh:
                refresh_stage_directory(loader, stage)
        finally:
            loader.close()
            if sql_file:
                sql_file.close()

    print()
    print(f"Migrated {ok:,} of {done:,} document(s)"
          + (f" ({done - ok:,} failed - left unmarked, tried again next run)" if done > ok else "")
          + ".")
    if done:
        # The performance numbers: per run, with this many workers.
        minutes = elapsed / 60 or 1e-9
        print(f"Took {human_time(elapsed)} with {args.workers} worker(s): "
              f"{done / minutes:.1f} docs/min ({elapsed / done:.1f} s per document), "
              f"{loader.files_sent:,} files, {human_bytes(loader.bytes_sent)} "
              f"({human_bytes(loader.bytes_sent / (elapsed or 1e-9))}/s)")
        if ok:
            print(f"At this rate 1,000 documents take about {human_time(1000 * elapsed / done)}.")
    if bulk:
        print()
        after = migration_status(cli)
        print_status(after, "AFTER THIS RUN")
        if stopped:
            print("\nStopped. Run the same command to carry on where it stopped.")
        elif not args.limit:
            explain_leftovers(after["remaining"], done - ok)
    print(f"Report: {report_path}")
    if args.control_sql:
        print(f"Control table NOT updated. Run {args.control_sql} in Snowflake as a user")
        print(f"that can write {table}, to record these documents there.")
    if stopped:
        return 130
    return 0 if ok == done else 1


def explain_leftovers(remaining: int, failed: int) -> None:
    """The plan is done, yet MarkLogic may still count records as not migrated:
    say what those can be rather than leave a puzzle."""
    other = remaining - failed
    if failed:
        print(f"\n{failed:,} document(s) failed this run: left unmarked, tried again next run "
              "(the report says why).")
    if other > 0:
        print(f"\n{other:,} record(s) holding a {id_field()} are not migrated and not on the "
              "plan's to-do list. They can be: documents added since the plan was built "
              "(--replan picks them up), or extra records sharing a document ID with another "
              "(the plan's 'note' column names them).")


def refresh_stage_directory(loader: Loader, stage: str) -> None:
    """After a run that uploaded files: refresh the stage's directory table, so
    the Snowflake UI and DIRECTORY(@stage) show them. The files are in the stage
    either way - a failure here is reported, never fatal."""
    print()
    try:
        changed = refresh_directory(loader.cursor(), stage)
        print(f"Refreshed the directory table of {stage} ({changed:,} change(s)): "
              "the Snowflake UI and DIRECTORY() now show the uploaded files.")
    except Exception as exc:
        print(f"Could not refresh the directory table of {stage}: {type(exc).__name__}: {exc}")
        print("The files ARE in the stage (LIST shows them); only the UI's stage page and "
              "DIRECTORY() lag behind. Someone allowed to (usually the stage owner) can run:")
        print(f"  ALTER STAGE {stage.lstrip('@')} REFRESH;")
        print("If it says the directory table is not enabled, first: "
              f"ALTER STAGE {stage.lstrip('@')} SET DIRECTORY = (ENABLE = TRUE);")


def preflight(loader: Loader, function: str, table: str, to_file: bool) -> bool:
    """Before any document: can we log in, hash staged files and write the table?"""
    try:
        cur = loader.cursor()
    except Exception as exc:
        print(f"Cannot log in to Snowflake: {type(exc).__name__}: {exc}")
        print(snowflake_hint(exc))
        return False
    present = not SF.HASH_CHECK
    try:
        present = present or hash_function_exists(cur, function)
    except Exception as exc:
        print(f"Cannot look up {function}: {type(exc).__name__}: {exc}")
    if not present:
        print(f"The hash function {function} does not exist, or this role cannot use it.")
        print("migrate checks every staged file by its SHA-256, which only a function")
        print("inside Snowflake can compute on this stage. Have an admin run")
        print(f"{HASH_FUNCTION_FILE} once (shown below; put the")
        print(f"role in SF_ROLE, {SF.ROLE or 'this user default role'}, in the GRANT):")
        print()
        print(hash_function_sql(function))
        print("Or set SF_HASH_FUNCTION in .env if it lives somewhere else, or")
        print("SF_HASH_CHECK=off to upload without this check until it exists.")
        return False
    if to_file:
        return True
    try:
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
        return False
    return True


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)




    ss = sub.add_parser("status", help="how many documents are migrated and how many to go")
    ss.add_argument("--detail", action="store_true",
                    help="also what the database holds: records by format, version copies, "
                         "collections")
    ss.add_argument("--top", type=int, default=20,
                    help="with --detail: largest collections to show (default 20)")
    ss.set_defaults(fn=cmd_status)

    pl = sub.add_parser("plan", help="list the documents still to migrate, oldest first, "
                                     "in a CSV - migrates nothing")
    pl.set_defaults(fn=cmd_plan)

    ld = sub.add_parser("migrate", help="migrate documents to Snowflake: all that are left, "
                                        "oldest first, or the ones given")
    ld.add_argument("--uri", action="append", help="document URI to migrate (repeatable)")
    ld.add_argument("--id", action="append",
                    help="document ID to migrate, found by ML_ID_PATH - no URI needed (repeatable)")
    ld.add_argument("--from-csv", metavar="FILE",
                    help="migrate the documents listed in FILE: a 'uri' or 'document_id' column "
                         "(a plan file works)")
    ld.add_argument("--workers", type=int, default=1,
                    help=f"documents migrated at the same time, 1-{MAX_WORKERS} (default 1)")
    ld.add_argument("--limit", type=int, help="migrate at most this many documents this run")
    ld.add_argument("--batch-size", type=int, default=1000,
                    help="without --uri/--id/--from-csv: plan entries checked against the "
                         "migrated flag at a time (default 1000)")
    ld.add_argument("--replan", action="store_true",
                    help="without --uri/--id/--from-csv: rebuild the oldest-first plan first")
    ld.add_argument("--no-refresh", action="store_true",
                    help="do not refresh the stage's directory table at the end of the run "
                         "(the UI and DIRECTORY() then lag behind until someone does)")
    ld.add_argument("--keep-local", action="store_true",
                    help=f"keep the extracted files in STAGE_DIR ({STAGE.DIR}) "
                         "instead of removing each one once uploaded")
    ld.add_argument("--control-sql", metavar="FILE",
                    help="do not write the control table; save its MERGE statements to FILE "
                         "to run by hand (while this Snowflake user cannot write the table)")
    ld.set_defaults(fn=cmd_migrate)

    dm = sub.add_parser("reset", help="take the migrated mark off documents, so migrate "
                                      "does them again")
    dm.add_argument("--uri", action="append", help="document URI (repeatable)")
    dm.add_argument("--id", action="append",
                    help="document ID, found by ML_ID_PATH - no URI needed (repeatable)")
    dm.add_argument("--from-csv", metavar="FILE",
                    help="the documents listed in FILE: a 'uri' or 'document_id' column")
    dm.add_argument("--all", action="store_true",
                    help="every record in the database that carries the mark")
    dm.add_argument("--yes", action="store_true", help="with --all: do not ask to confirm")
    dm.set_defaults(fn=cmd_reset)

    fa = sub.add_parser("family",
                        help="show a document's versions, linked files and their timestamps")
    fa.add_argument("--uri", action="append",
                    help="document URI to show (repeatable)")
    fa.add_argument("--id", action="append",
                    help="document ID to show, found by ML_ID_PATH - no URI needed (repeatable)")
    fa.add_argument("--xml", action="store_true",
                    help="also print each document's properties XML as MarkLogic holds it")
    fa.add_argument("--brief", action="store_true",
                    help="only the table: each version and the PDF/DOM version migrate pairs it with")
    fa.set_defaults(fn=cmd_family)

    ex = sub.add_parser("get", help="write a document, its versions and its files to local "
                                    "disk only (dry run) - one folder per document")
    ex.add_argument("--uri", action="append",
                    help="document URI to get (repeatable)")
    ex.add_argument("--id", action="append",
                    help="document ID to get, found by ML_ID_PATH - no URI needed (repeatable)")
    ex.add_argument("--out", help=f"staging root (default: STAGE_DIR, now {STAGE.DIR})")
    ex.set_defaults(fn=cmd_get)

    sc = sub.add_parser("sfcheck", help="test the Snowflake connection")
    sc.add_argument("--reach-only", action="store_true",
                    help="only test network reachability, do not try to log in")
    sc.add_argument("--write", action="store_true",
                    help="also upload a small test file to SF_STAGE, then remove it")
    sc.set_defaults(fn=cmd_sfcheck)

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
