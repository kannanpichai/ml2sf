"""Everything on the MarkLogic side: the REST client, and turning whatever it
returns (JSON, XML or text) into a plain dict that can be walked by path.

Makes no assumptions about document shape.
"""
from __future__ import annotations

import copy
import html
import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Iterator
from xml.etree import ElementTree as ET

import requests
from requests.auth import AuthBase, HTTPBasicAuth, HTTPDigestAuth


def _make_auth(auth: str, user: str, password: str) -> AuthBase:
    if auth == "digest":
        return HTTPDigestAuth(user, password)
    if auth == "basic":
        return HTTPBasicAuth(user, password)
    if auth == "kerberos":
        # Uses the logged-in Windows account (or the kinit ticket elsewhere);
        # ML_USER / ML_PASSWORD are not needed.
        try:
            from requests_negotiate_sspi import HttpNegotiateAuth
            return HttpNegotiateAuth()
        except ImportError:
            pass
        try:
            from requests_kerberos import OPTIONAL, HTTPKerberosAuth
            return HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        except ImportError:
            raise SystemExit(
                "ML_AUTH=kerberos needs requests-negotiate-sspi (Windows) "
                "or requests-kerberos (Linux/macOS): pip install -r requirements.txt"
            )
    raise SystemExit(f"Unknown ML_AUTH {auth!r}: use digest, basic or kerberos.")


# ---------- XML -> dict ----------

def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def xml_to_dict(elem: ET.Element) -> Any:
    """Convert an XML element to nested dicts.

    Attributes become @name keys, text becomes #text when the element also has
    children or attributes, and repeated sibling tags collapse into a list.
    """
    node: dict[str, Any] = {}
    for key, val in elem.attrib.items():
        node[f"@{_strip_ns(key)}"] = val

    children = list(elem)
    if children:
        for child in children:
            name = _strip_ns(child.tag)
            value = xml_to_dict(child)
            if name in node:
                if not isinstance(node[name], list):
                    node[name] = [node[name]]
                node[name].append(value)
            else:
                node[name] = value

    text = (elem.text or "").strip()
    if text:
        if node:
            node["#text"] = text
        else:
            return text
    return node if node else None


def parse_xml(payload: bytes) -> dict:
    root = ET.fromstring(payload)
    body = xml_to_dict(root)
    return {_strip_ns(root.tag): body}


def find_key(doc: Any, leaf: str) -> Any:
    """Breadth-first search for the first scalar under a key called `leaf`,
    so 'documentId' also finds systemAttributes.documentId."""
    queue = [doc]
    while queue:
        cur = queue.pop(0)
        items = cur.items() if isinstance(cur, dict) else enumerate(cur) if isinstance(cur, list) else ()
        for key, val in items:
            if key == leaf and not isinstance(val, (dict, list)) and val is not None:
                return val
            if isinstance(val, (dict, list)):
                queue.append(val)
    return None


# ---------- REST client ----------

class MarkLogicError(RuntimeError):
    pass


class MarkLogicClient:
    """Selects documents by collection, directory, string query, or a raw
    MarkLogic structured query, and returns each one as a plain dict."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str = "",
        auth: str = "digest",
        scheme: str = "http",
        timeout: int = 120,
    ) -> None:
        self.base = f"{scheme}://{host}:{port}"
        self.database = database
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = _make_auth(auth, user, password)

    # ---------- internals ----------

    def _params(self, extra: dict | None = None) -> dict:
        params: dict[str, Any] = dict(extra or {})
        if self.database:
            params["database"] = self.database
        return params

    def _check(self, resp: requests.Response, what: str) -> requests.Response:
        if resp.status_code == 401:
            raise MarkLogicError(
                f"{what}: 401 Unauthorized - check user/password/auth type."
            )
        if resp.status_code == 404 and "text/html" in resp.headers.get("Content-Type", ""):
            raise MarkLogicError(
                f"{what}: 404 with an HTML body - this port is not a REST API server. "
                "Use the App-Services port (usually 8000), not Admin (8001)."
            )
        if not resp.ok:
            raise MarkLogicError(f"{what}: HTTP {resp.status_code} -> {_error_text(resp)}")
        return resp

    # ---------- discovery ----------

    def databases(self) -> list[str]:
        """List databases via the Management API on port 8002."""
        mgmt = self.base.rsplit(":", 1)[0] + ":8002"
        resp = self.session.get(
            f"{mgmt}/manage/v2/databases",
            params={"format": "json"},
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        self._check(resp, "list databases")
        items = resp.json()["database-default-list"]["list-items"]["list-item"]
        return sorted(i["nameref"] for i in items)

    def collections(self, limit: int = 200) -> list[str]:
        """List collection URIs present in the database (needs no index)."""
        script = (
            "const out = []; let n = 0;"
            "for (const c of cts.collections()) { out.push(c); if (++n >= LIMIT) break; }"
            "out"
        ).replace("LIMIT", str(limit))
        result = self.eval_js(script)
        # eval returns one part per sequence item; a returned JS array arrives
        # as a single part holding the whole list.
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return sorted(str(c) for c in result)

    def for_database(self, database: str) -> "MarkLogicClient":
        """A view of the same server pointed at another database."""
        other = copy.copy(self)
        other.database = database
        return other

    def eval_js(self, script: str, variables: dict | None = None) -> Any:
        """Run server-side JavaScript and return the decoded result.

        `variables` are passed as external variables; declare each one in the
        script with `var NAME;`.
        """
        form = {"javascript": script}
        if variables:
            form["vars"] = json.dumps(variables)
        resp = self.session.post(
            f"{self.base}/v1/eval",
            data=self._params(form),
            headers={"Accept": "multipart/mixed"},
            timeout=self.timeout,
        )
        self._check(resp, "eval")
        return _parse_multipart(resp)

    def count(self, selector: dict, migrated: bool | None = None) -> int:
        """How many documents match, optionally only one side of the migrated flag."""
        if migrated is None:
            payload, params = self._search_request(selector, start=1, page_length=0)
            return int(self._search(payload, params).get("total", 0))
        if selector.get("q") or selector.get("structured_query"):
            raise MarkLogicError(MIGRATED_NEEDS_LEXICON)
        result = self.eval_js(COUNT_JS, {
            "COLLECTION": selector.get("collection", "") or "",
            "DIRECTORY": selector.get("directory", "") or "",
            "MIGRATED": "true" if migrated else "false",
        })
        while isinstance(result, list) and result:
            result = result[0]
        return int(result)

    # ---------- selection ----------

    def _search_request(
        self, selector: dict, start: int, page_length: int
    ) -> tuple[dict | None, dict]:
        """Build the /v1/search call for a selector from the mapping file."""
        params: dict[str, Any] = {
            "format": "json",
            "start": start,
            "pageLength": page_length,
        }
        structured = selector.get("structured_query")
        if structured:
            return {"search": {"query": structured}}, params
        if selector.get("collection"):
            params["collection"] = selector["collection"]
        if selector.get("directory"):
            params["directory"] = selector["directory"]
        if selector.get("q"):
            params["q"] = selector["q"]
        return None, params

    def _search(self, payload: dict | None, params: dict) -> dict:
        if payload is None:
            resp = self.session.get(
                f"{self.base}/v1/search",
                params=self._params(params),
                headers={"Accept": "application/json"},
                timeout=self.timeout,
            )
        else:
            resp = self.session.post(
                f"{self.base}/v1/search",
                params=self._params(params),
                data=json.dumps(payload),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=self.timeout,
            )
        return self._check(resp, "search").json()

    def iter_results(self, selector: dict, page_length: int = 500) -> Iterator[dict]:
        """Search result rows (uri, format, mimetype, ...) for every match."""
        start = 1
        while True:
            payload, params = self._search_request(selector, start, page_length)
            data = self._search(payload, params)
            results = data.get("results") or []
            yield from results
            total = int(data.get("total", 0))
            start += len(results)
            if not results or start > total:
                return

    def iter_uri_rows(
        self, selector: dict, page_length: int = 1000, migrated: bool | None = None,
        with_migrated: bool = False,
    ) -> Iterator[dict]:
        """Yield {uri, mimetype} for every match, cheapest way available.

        Prefers the URI lexicon, which walks URIs in sorted order and stays fast
        however deep it goes. Falls back to /v1/search paging when the lexicon is
        off or the selector needs a full query.
        """
        if not (selector.get("q") or selector.get("structured_query")):
            try:
                rows = self._uri_chunk(selector, "", page_length, migrated,
                                       with_migrated)
            except MarkLogicError:
                rows = None                      # no URI lexicon: use search
            if rows is not None:
                while rows:
                    yield from rows
                    rows = self._uri_chunk(selector, rows[-1]["uri"], page_length,
                                           migrated, with_migrated)
                return
        if migrated is not None:
            raise MarkLogicError(MIGRATED_NEEDS_LEXICON)
        for row in self.iter_results(selector, page_length=min(page_length, 500)):
            yield {
                "uri": row["uri"],
                "mimetype": row.get("mimetype", ""),
                "format": row.get("format", ""),
            }

    def _uri_chunk(self, selector: dict, after: str, limit: int,
                   migrated: bool | None = None,
                   with_migrated: bool = False) -> list[dict]:
        result = self.eval_js(URIS_JS, {
            "AFTER": after,
            "LIMIT": limit,
            "COLLECTION": selector.get("collection", "") or "",
            "DIRECTORY": selector.get("directory", "") or "",
            # "", "true" or "false": which side of the migrated flag to keep.
            "MIGRATED": "" if migrated is None else ("true" if migrated else "false"),
            "WANT_MIGRATED": "1" if with_migrated else "",
        })
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return result

    # ---------- marking ----------

    def mark_migrated(
        self, uris: list[str], migrated: bool = True, timestamp: str = "",
        chunk_size: int = 100, workers: int = 4,
    ) -> list[dict]:
        """Write the snowflake-migration property section on each document.

        Returns one {uri, migrated, timestamp} per URI, or {uri, error}.
        """
        chunks = [uris[i:i + chunk_size] for i in range(0, len(uris), chunk_size)]

        def run(chunk: list[str]) -> list:
            result = self.eval_js(MARK_JS, {
                "URIS": json.dumps(chunk),
                "MIGRATED": "true" if migrated else "false",
                "STAMP": timestamp or "",
            })
            if len(result) == 1 and isinstance(result[0], list):
                result = result[0]
            return result

        out: list[dict] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for items in pool.map(run, chunks):
                out.extend(items)
        return out

    # ---------- reading ----------

    def family(self, uri: str, links: str = "") -> dict:
        """Everything that belongs with one document, in one server call.

        Returns {master, versions[], links[]}: the document itself (or, given a
        version copy, the document it is a version of), every other version of
        it, and the documents its `links` fields point at - the binary and its
        extracted DOM. Each entry carries uri, kind, mimetype and version.
        """
        result = self.eval_js(FAMILY_JS, {"URI": uri, "LINKS": links or ""})
        while isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            raise MarkLogicError(f"family {uri}: unexpected answer {result!r}")
        return result

    def get_bytes(self, uri: str) -> bytes:
        """One document exactly as MarkLogic stores it - for binaries."""
        resp = self.session.get(
            f"{self.base}/v1/documents",
            params=self._params({"uri": uri}),
            timeout=self.timeout,
        )
        self._check(resp, f"get {uri}")
        return resp.content

    def read_documents(
        self, uris: list[str], chunk_size: int = 100, workers: int = 8
    ) -> Iterator[SourceDocument]:
        """Read documents with their properties and a server-side SHA-256.

        One eval per chunk of URIs, chunks run in parallel, results come back
        in the order the URIs were given.
        """
        chunks = [uris[i:i + chunk_size] for i in range(0, len(uris), chunk_size)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for items in pool.map(self._read_chunk, chunks):
                for item in items:
                    yield SourceDocument.from_eval(item)

    def document_fields(
        self, uris: list[str], fields: dict[str, str], link: str = "",
        debug: bool = False, chunk_size: int = 250, workers: int = 8,
    ) -> dict[str, dict]:
        """Read a few fields per document inside MarkLogic; only values travel back.

        `fields` maps a name to a dotted path, e.g. {"id": "documentId"}. Each
        value comes from the content (JSON by path or key name, XML by element
        name), else the document's properties, else - for a binary - the record
        whose `link` property holds this binary's URI.

        Returns {uri: {kind, values, sources, record, version, version_of}};
        with debug, also top_keys and properties_xml.
        """
        chunks = [uris[i:i + chunk_size] for i in range(0, len(uris), chunk_size)]
        found: dict[str, dict] = {}

        def run(chunk: list[str]) -> list:
            result = self.eval_js(FIELDS_JS, {
                "URIS": json.dumps(chunk),
                "FIELDS": json.dumps({k: v for k, v in fields.items() if v}),
                "LINK": link or "",
                "DEBUG": "1" if debug else "",
            })
            if len(result) == 1 and isinstance(result[0], list):
                result = result[0]
            return result

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for items in pool.map(run, chunks):
                for item in items:
                    found[item["uri"]] = item
        return found

    def _read_chunk(self, uris: list[str]) -> list[dict]:
        result = self.eval_js(READ_JS, {"URIS": json.dumps(uris)})
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return result


# Some JSON files were loaded as binaries (e.g. Library Services uploads).
# Returns the text when a binary holds JSON, else null. Only decodes the whole
# document after a cheap look at its type and first bytes, so PDFs stay unread.
BINARY_JSON_JS = r"""
function binaryJson(uri, doc) {
  let mime = '';
  try { mime = xdmp.uriContentType(uri) || ''; } catch (e) {}
  let head = '';
  try { head = xdmp.binaryDecode(xdmp.subbinary(doc.root, 1, 16), 'UTF-8'); } catch (e) {}
  head = head.replace(/^\uFEFF/, '').trim();
  if (!/json/i.test(mime) && !/^[\[{]/.test(head)) return null;
  let text;
  try { text = xdmp.binaryDecode(doc.root, 'UTF-8').replace(/^\uFEFF/, ''); }
  catch (e) { return null; }
  try { JSON.parse(text); } catch (e) { return null; }
  return text;
}
"""

# What this tool writes into each document's properties once it has been
# migrated. Kept in its own namespace so it can never collide with a property
# the source system already uses.
MIGRATION_NS = "http://ml2sf/snowflake-migration"
MIGRATION_SECTION = "snowflake-migration"

MIGRATED_NEEDS_LEXICON = (
    "--exclude-migrated / --only-migrated need the URI lexicon, which this "
    "database or selector cannot use. Drop --query / structured_query, or turn "
    "the URI lexicon on for the database."
)

# Documents already marked migrated, to keep or to leave out on a later pass.
MIGRATED_QUERY_JS = """
const MIGRATION_NS = '%s';
function migratedQuery() {
  return cts.propertiesFragmentQuery(
    cts.elementValueQuery(fn.QName(MIGRATION_NS, 'migrated'), 'true'));
}
// The flag as written by `load`, read straight off a properties fragment.
function migratedFlag(props) {
  if (!props) return false;
  // documentProperties returns the properties document node, so search downward.
  const hit = fn.head(props.xpath('.//*:snowflake-migration/*:migrated'));
  return hit ? fn.string(hit) === 'true' : false;
}
""" % MIGRATION_NS


# Counts one side of the migrated flag. Walks the URI lexicon rather than
# asking cts.estimate, which counts fragments - including ones deleted but not
# yet merged away - and only bounds a not-query from above. This reads no
# documents and counts exactly what the listing below would return.
COUNT_JS = MIGRATED_QUERY_JS + """
var COLLECTION; var DIRECTORY; var MIGRATED;
const qs = [];
if (COLLECTION) qs.push(cts.collectionQuery(COLLECTION));
if (DIRECTORY) qs.push(cts.directoryQuery(DIRECTORY, 'infinity'));
qs.push(MIGRATED === 'true' ? migratedQuery() : cts.notQuery(migratedQuery()));
let n = 0;
for (const uri of cts.uris(null, [], cts.andQuery(qs))) n++;
n
"""


# Walks the URI lexicon in sorted order, starting after the previous chunk's
# last URI, so paging cost does not grow with depth. The MIME type comes from
# MarkLogic's extension mapping, which needs no document read.
URIS_JS = MIGRATED_QUERY_JS + """
var AFTER; var LIMIT; var COLLECTION; var DIRECTORY; var MIGRATED; var WANT_MIGRATED;
const qs = [];
if (COLLECTION) qs.push(cts.collectionQuery(COLLECTION));
if (DIRECTORY) qs.push(cts.directoryQuery(DIRECTORY, 'infinity'));
if (MIGRATED === 'true') qs.push(migratedQuery());
if (MIGRATED === 'false') qs.push(cts.notQuery(migratedQuery()));
const q = qs.length ? cts.andQuery(qs) : cts.trueQuery();
const out = [];
for (const item of cts.uris(AFTER || null, ['limit=' + (LIMIT + 1)], q)) {
  // cts.uris yields xs.string objects, not JS strings: compare the text, or
  // every page boundary hands back its first URI a second time.
  const uri = fn.string(item);
  if (uri === AFTER) continue;
  let mime = '';
  try { mime = xdmp.uriContentType(uri); } catch (e) { mime = ''; }
  const row = {uri: uri, mimetype: mime, format: ''};
  // Only on request: this is one properties-fragment read per document.
  if (WANT_MIGRATED) row.migrated = migratedFlag(fn.head(xdmp.documentProperties(uri)));
  out.push(row);
  if (out.length >= LIMIT) break;
}
out
"""


# Breadth-first search for the first scalar under a key called `leaf`, so
# 'documentId' also finds systemAttributes.documentId. Wanted by two scripts.
FIND_KEY_JS = """
function findKey(obj, leaf) {
  const queue = [obj];
  while (queue.length) {
    const cur = queue.shift();
    if (cur === null || typeof cur !== 'object') continue;
    for (const k of Object.keys(cur)) {
      const v = cur[k];
      if (k === leaf && v !== null && typeof v !== 'object') return v;
      if (v !== null && typeof v === 'object') queue.push(v);
    }
  }
  return null;
}
"""

# Pulls one field per document without sending the document back.
FIELDS_JS = BINARY_JSON_JS + MIGRATED_QUERY_JS + FIND_KEY_JS + """
var URIS; var FIELDS; var LINK; var DEBUG;
const fields = JSON.parse(FIELDS);          // {column: dotted path}
const links = LINK ? LINK.split(',').map(s => s.trim()).filter(s => s) : [];

// First element with the given local name under a node (namespace ignored).
function named(node, name) {
  // .xpath() cannot bind variables, so the name goes into the expression;
  // only plain element-name characters are allowed through.
  if (!node || !/^[A-Za-z_][A-Za-z0-9_.-]*$/.test(name)) return null;
  const hit = fn.head(node.xpath('.//*[local-name() = "' + name + '"]'));
  return hit ? fn.string(hit) : null;
}
// The document as a JS object when it holds JSON (natively or as text).
function contentOf(doc) {
  if (!doc) return {kind: 'missing', obj: null};
  const kind = xdmp.nodeKind(doc.root);
  if (kind === 'binary') {
    const text = binaryJson(xdmp.nodeUri(doc), doc);
    return {kind: text === null ? kind : 'binary (JSON)', obj: text === null ? null : JSON.parse(text)};
  }
  if (kind === 'object' || kind === 'array') return {kind: kind, obj: doc.toObject()};
  if (kind === 'text') {
    try { return {kind: kind, obj: JSON.parse(fn.string(doc))}; }
    catch (e) { return {kind: kind, obj: null}; }
  }
  return {kind: kind, obj: null};
}
function valueIn(doc, content, path) {
  const parts = path.split('.');
  const leaf = parts[parts.length - 1];
  if (content.obj !== null && typeof content.obj === 'object') {
    let v = content.obj;
    for (const p of parts) { v = (v === null || v === undefined) ? null : v[p]; }
    if (v === null || v === undefined || typeof v === 'object') v = findKey(content.obj, leaf);
    return (v === null || v === undefined) ? null : String(v);
  }
  if (content.kind === 'element') return named(doc, leaf);
  return null;
}

const out = [];
for (const uri of JSON.parse(URIS)) {
  const doc = cts.doc(uri);
  const content = contentOf(doc);
  const props = fn.head(xdmp.documentProperties(uri));
  const item = {uri: uri, kind: content.kind, record: '', values: {}, sources: {},
                migrated: migratedFlag(props),
                // Library Services keeps version details in each copy's properties.
                version: named(props, 'version-id') || '',
                version_of: named(props, 'document-uri') || ''};
  let rec;                                  // undefined = not looked up yet
  let recContent = null;
  for (const name of Object.keys(fields)) {
    const path = fields[name];
    if (!path) continue;
    const leaf = path.split('.').pop();
    let v = valueIn(doc, content, path);
    let src = 'content';
    if (v === null) { v = named(props, leaf); src = 'properties'; }
    // Not in the document itself (a binary, or a document another record
    // describes): use the record whose link field holds this URI - or, for a
    // version copy, the URI of the document it is a version of.
    if (v === null && links.length) {
      if (rec === undefined) {
        rec = null;
        const targets = item.version_of ? [uri, item.version_of] : [uri];
        for (const target of targets) {
          for (const link of links) {
            rec = fn.head(cts.search(cts.jsonPropertyValueQuery(link, target), ['filtered']));
            if (rec) break;
          }
          if (rec) break;
        }
        if (rec) { item.record = xdmp.nodeUri(rec); recContent = contentOf(rec); }
      }
      if (rec) { v = valueIn(rec, recContent, path); src = 'linked record'; }
    }
    item.values[name] = v;
    item.sources[name] = (v === null) ? '' : src;
  }
  if (DEBUG) {
    item.top_keys = (content.obj && typeof content.obj === 'object') ? Object.keys(content.obj) : [];
    item.properties_xml = props ? xdmp.quote(props) : '';
  }
  out.push(item);
}
out
"""


# Runs inside MarkLogic. The hash is taken over exactly the text that is
# returned, so the same text can be re-hashed anywhere downstream.
READ_JS = BINARY_JSON_JS + """
var URIS;
const FORMATS = {element: 'XML', text: 'TEXT', binary: 'BINARY'};
const out = [];
for (const uri of JSON.parse(URIS)) {
  const doc = cts.doc(uri);
  if (!doc) { out.push({uri: uri, error: 'document not found'}); continue; }
  const format = FORMATS[xdmp.nodeKind(doc.root)] || 'JSON';
  const props = fn.head(xdmp.documentProperties(uri));
  const item = {uri: uri, format: format, properties: props ? xdmp.quote(props) : null};
  if (format === 'BINARY') {
    const text = binaryJson(uri, doc);
    if (text === null) {
      item.error = 'binary document (not JSON) - not supported yet';
    } else {
      item.format = 'JSON';
      item.stored_as = 'binary';
      item.text = text;
      item.hash = xdmp.sha256(text, 'hex');
    }
  } else {
    item.text = xdmp.quote(doc);
    item.hash = xdmp.sha256(item.text, 'hex');
  }
  out.push(item);
}
out
"""


# One document's whole family: the master, every version of it, and the
# documents it points at. Library Services keeps a version copy's parent URI in
# its properties, so the versions are found by querying for that URI.
FAMILY_JS = BINARY_JSON_JS + FIND_KEY_JS + """
var URI; var LINKS;
const DLS_NS = 'http://marklogic.com/xdmp/dls';
function named(node, name) {
  if (!node || !/^[A-Za-z_][A-Za-z0-9_.-]*$/.test(name)) return null;
  const hit = fn.head(node.xpath('.//*[local-name() = "' + name + '"]'));
  return hit ? fn.string(hit) : null;
}
function info(uri) {
  const doc = cts.doc(uri);
  const props = fn.head(xdmp.documentProperties(uri));
  let mime = '';
  try { mime = xdmp.uriContentType(uri) || ''; } catch (e) {}
  const kind = doc ? xdmp.nodeKind(doc.root) : '';
  return {
    uri: uri,
    exists: !!doc,
    kind: kind,
    binary_json: kind === 'binary' && binaryJson(uri, doc) !== null,
    mimetype: mime,
    version: named(props, 'version-id') || '',
    version_of: named(props, 'document-uri') || '',
    created: named(props, 'created') || '',
    author: named(props, 'external-user-name') || named(props, 'author') || '',
    properties_xml: props ? xdmp.quote(props) : ''
  };
}
// A version copy names its parent; anything else is its own master.
const self = info(URI);
const master = self.version_of || URI;
const out = {master: info(master), versions: [], links: []};
for (const doc of cts.search(cts.propertiesFragmentQuery(
      cts.elementValueQuery(fn.QName(DLS_NS, 'document-uri'), master)), ['unfiltered'])) {
  const u = xdmp.nodeUri(doc);
  if (u !== master) out.versions.push(info(u));
}
out.versions.sort(function (a, b) {
  const x = parseInt(a.version || '0', 10), y = parseInt(b.version || '0', 10);
  return x === y ? (a.uri < b.uri ? -1 : 1) : x - y;
});

// Documents this one points at, named by the fields in LINKS.
const fields = LINKS ? LINKS.split(',').map(function (f) { return f.trim(); }).filter(Boolean) : [];
if (fields.length && out.master.exists) {
  const doc = cts.doc(master);
  const kind = xdmp.nodeKind(doc.root);
  let obj = null;
  if (kind === 'object' || kind === 'array') obj = doc.toObject();
  else if (kind === 'binary') { const t = binaryJson(master, doc); if (t !== null) obj = JSON.parse(t); }
  else if (kind === 'text') { try { obj = JSON.parse(fn.string(doc)); } catch (e) {} }
  if (obj !== null && typeof obj === 'object') {
    for (const f of fields) {
      const v = findKey(obj, f);
      if (v) { const item = info(String(v)); item.field = f; out.links.push(item); }
    }
  }
}
out
"""


# Replaces any earlier section, so re-running a document does not stack them up.
MARK_JS = """
declareUpdate();                 // server-side JS writes nothing without this
var URIS; var MIGRATED; var STAMP;
const MIGRATION_NS = '%s';
const SECTION = '%s';
const out = [];
for (const uri of JSON.parse(URIS)) {
  if (!fn.docAvailable(uri)) { out.push({uri: uri, error: 'document not found'}); continue; }
  const stamp = STAMP || fn.string(fn.currentDateTime());
  const xml = '<' + SECTION + ' xmlns="' + MIGRATION_NS + '">' +
              '<migrated>' + MIGRATED + '</migrated>' +
              '<migrated-timestamp>' + stamp + '</migrated-timestamp>' +
              '</' + SECTION + '>';
  try {
    xdmp.documentRemoveProperties(uri, fn.QName(MIGRATION_NS, SECTION));
    xdmp.documentAddProperties(uri, [fn.head(xdmp.unquote(xml)).root]);
    out.push({uri: uri, migrated: MIGRATED, timestamp: stamp});
  } catch (e) {
    out.push({uri: uri, error: e.name + ': ' + (e.data || e.message || '')});
  }
}
out
""" % (MIGRATION_NS, MIGRATION_SECTION)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class SourceDocument:
    """One document as read from MarkLogic, ready to be mapped and verified."""
    uri: str
    format: str = ""
    stored_as: str = ""          # set when the content came out of a binary
    text: str | None = None
    ml_hash: str | None = None
    properties: dict = field(default_factory=dict)
    error: str | None = None

    @classmethod
    def from_eval(cls, item: dict) -> "SourceDocument":
        return cls(
            uri=item["uri"],
            format=item.get("format", ""),
            stored_as=item.get("stored_as", ""),
            text=item.get("text"),
            ml_hash=item.get("hash"),
            properties=parse_properties(item.get("properties")),
            error=item.get("error"),
        )

    @property
    def local_hash(self) -> str | None:
        """SHA-256 of the text as received; must equal ml_hash."""
        return sha256_hex(self.text) if self.text is not None else None

    @property
    def hash_ok(self) -> bool:
        return self.ml_hash is not None and self.ml_hash == self.local_hash

    @property
    def size(self) -> int:
        return len(self.text.encode("utf-8")) if self.text is not None else 0

    def content(self) -> dict:
        """The document as a dict, for the mapping."""
        if self.format == "JSON":
            return _as_dict(json.loads(self.text))
        if self.format == "XML":
            return _as_dict(parse_xml(self.text.encode("utf-8")))
        return _as_dict({"text": self.text})


def parse_properties(xml_text: str | None) -> dict:
    """<prop:properties> XML -> {name: value}, namespaces dropped."""
    if not xml_text:
        return {}
    body = parse_xml(xml_text.encode("utf-8")).get("properties")
    return body if isinstance(body, dict) else {}


def _as_dict(doc: Any) -> dict:
    if isinstance(doc, dict) and set(doc) == {"content"}:
        doc = doc["content"]
    return doc if isinstance(doc, dict) else {"value": doc}


def _error_text(resp: requests.Response) -> str:
    """MarkLogic's own error line, instead of a whole HTML or JSON error page."""
    body = resp.text or ""
    try:
        err = resp.json().get("errorResponse", {})
        if err:
            return f"{err.get('messageCode', '')}: {err.get('message', '')}".strip(": ")
    except ValueError:
        pass
    match = re.search(r"<dt>(.*?)</dt>", body, re.S)
    if match:
        return html.unescape(re.sub(r"\s+", " ", match.group(1))).strip()
    return body[:400]


def _parse_multipart(resp: requests.Response) -> Any:
    """Decode MarkLogic's multipart/mixed eval response."""
    ctype = resp.headers.get("Content-Type", "")
    if "boundary=" not in ctype:
        return resp.text
    boundary = ctype.split("boundary=")[1].strip().strip('"')
    out: list[Any] = []
    for part in resp.content.split(f"--{boundary}".encode()):
        if b"\r\n\r\n" not in part:
            continue
        body = part.split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n-")
        if not body:
            continue
        text = body.decode("utf-8", "replace")
        try:
            out.append(json.loads(text))
        except json.JSONDecodeError:
            out.append(text)
    return out
