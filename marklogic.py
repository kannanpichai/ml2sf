"""Everything on the MarkLogic side: the REST client, and turning whatever it
returns (JSON, XML or text) into a plain dict that can be walked by path.

Makes no assumptions about document shape.
"""
from __future__ import annotations

import html
import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Iterator
from xml.etree import ElementTree as ET

import requests
import requests.adapters
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
        # Shared by every worker thread (requests' pool is thread-safe); sized so
        # 16 workers x 4 requests each reuse connections instead of dropping them.
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=64)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

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

    # ---------- selection ----------

    def inventory(self, top: int = 20) -> dict:
        """What the database holds, from the indexes (see INVENTORY_JS)."""
        result = self.eval_js(INVENTORY_JS, {"TOP": top})
        while isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            raise MarkLogicError(f"inventory: unexpected answer {result!r}")
        return result

    def count_documents(self, field: str, migrated: bool) -> int:
        """How many documents (see DOCS_QUERY_JS) are, or are not, migrated."""
        result = self.eval_js(DOCS_COUNT_JS, {"FIELD": field,
                                              "MIGRATED": "true" if migrated else "false"})
        while isinstance(result, list) and result:
            result = result[0]
        return int(result or 0)

    def plan_page(self, after: str, limit: int, field: str, created: str = "",
                  links: str = "") -> dict:
        """One page of documents still to migrate (see PLAN_PAGE_JS):
        {rows: [{uri, id, created, links}], last}."""
        result = self.eval_js(PLAN_PAGE_JS, {"AFTER": after, "LIMIT": limit, "FIELD": field,
                                             "CREATED": created or "", "LINKS": links or ""})
        while isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            raise MarkLogicError(f"plan: unexpected answer {result!r}")
        return result

    def no_id_page(self, after: str, limit: int, field: str) -> list[str]:
        """One page of unmigrated originals holding no document ID."""
        result = self.eval_js(NO_ID_PAGE_JS, {"AFTER": after, "LIMIT": limit, "FIELD": field})
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return [str(u) for u in result]

    def migrated_among(self, uris: list[str]) -> set[str]:
        """The URIs among these that are already marked migrated."""
        if not uris:
            return set()
        result = self.eval_js(MIGRATED_AMONG_JS, {"URIS": json.dumps(uris)})
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return {str(u) for u in result}

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

    def demark(self, uris: list[str], chunk_size: int = 100, workers: int = 4) -> list[dict]:
        """Remove the migration section from each URI's properties.
        Returns one {uri, removed} per URI (removed=False: it had none), or {uri, error}."""
        chunks = [uris[i:i + chunk_size] for i in range(0, len(uris), chunk_size)]

        def run(chunk: list[str]) -> list:
            result = self.eval_js(DEMARK_JS, {"URIS": json.dumps(chunk)})
            if len(result) == 1 and isinstance(result[0], list):
                result = result[0]
            return result

        out: list[dict] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for items in pool.map(run, chunks):
                out.extend(items)
        return out

    def marked_page(self, after: str, limit: int) -> list[str]:
        """One page of the records carrying the migration section (any value)."""
        result = self.eval_js(MARKED_PAGE_JS, {"AFTER": after, "LIMIT": limit})
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return [str(u) for u in result]

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

    def find_by_id(self, doc_id: str, field: str, links: str = "") -> dict:
        """The current envelope URI(s) for a document ID (see FIND_BY_ID_JS):
        {matched_by: 'field'|'uri', hits: n, envelopes: [uri, ...]}."""
        result = self.eval_js(FIND_BY_ID_JS, {"ID": doc_id, "FIELD": field, "LINKS": links or ""})
        while isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            raise MarkLogicError(f"find {doc_id}: unexpected answer {result!r}")
        return result

    def discover(self, uri: str, created: str = "") -> list[dict]:
        """Every version of the envelope `uri` belongs to, with its timestamps
        and the documents its fields point at (see DISCOVER_JS)."""
        result = self.eval_js(DISCOVER_JS, {"URI": uri, "CREATED": created or ""})
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
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

# Documents already marked migrated, to keep or to leave out on a later pass.
MIGRATED_QUERY_JS = """
const MIGRATION_NS = '%s';
function migratedQuery() {
  return cts.propertiesFragmentQuery(
    cts.elementValueQuery(fn.QName(MIGRATION_NS, 'migrated'), 'true'));
}
// The flag as written by `migrate`, read straight off a properties fragment.
function migratedFlag(props) {
  if (!props) return false;
  // documentProperties returns the properties document node, so search downward.
  const hit = fn.head(props.xpath('.//*:snowflake-migration/*:migrated'));
  return hit ? fn.string(hit) === 'true' : false;
}
""" % MIGRATION_NS


# Which of these URIs are already marked migrated - checked just before a
# document is processed, so a resumed run never redoes one.
MIGRATED_AMONG_JS = MIGRATED_QUERY_JS + """
var URIS;
const out = [];
for (const uri of JSON.parse(URIS)) {
  if (migratedFlag(fn.head(xdmp.documentProperties(uri)))) out.push(uri);
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


# ---------- the bulk run: documents, found without any path ----------

# What makes a record one of "the documents": it holds a document ID (FIELD, at
# any depth) and is an original - not a Library Services version copy, which
# names its original in dls:document-uri. MIGRATED picks a side of our flag.
DOCS_QUERY_JS = MIGRATED_QUERY_JS + """
const DLS_NS = 'http://marklogic.com/xdmp/dls';
function hasIdQuery(field) {
  const leaf = field.split('.').pop();
  return cts.orQuery([cts.jsonPropertyScopeQuery(leaf, cts.trueQuery()),
                      cts.elementQuery(fn.QName('', leaf), cts.trueQuery())]);
}
function versionCopyQuery() {
  return cts.propertiesFragmentQuery(
    cts.elementQuery(fn.QName(DLS_NS, 'document-uri'), cts.trueQuery()));
}
function flagQuery(migrated) {
  return migrated === 'true' ? migratedQuery() : cts.notQuery(migratedQuery());
}
function docsQuery(field, migrated) {
  return cts.andQuery([hasIdQuery(field), cts.notQuery(versionCopyQuery()), flagQuery(migrated)]);
}
"""


# What the database holds, for `status --detail`: every number resolved from the
# indexes (cts.estimate), so nothing is read and it stays fast at any size.
# Collections need the collection lexicon; without it they are left out.
INVENTORY_JS = DOCS_QUERY_JS + """
var TOP;
const est = function (q) { return cts.estimate(q); };
const out = {
  records: est(cts.trueQuery()),
  formats: {},
  version_copies: est(versionCopyQuery()),
  migrated_records: est(migratedQuery()),
  collections: null
};
// cts.documentFormatQuery is newer than some servers; the format-* search
// options do the same on older ones. If neither works, formats are left out.
for (const f of ['json', 'xml', 'text', 'binary']) {
  try {
    out.formats[f] = (typeof cts.documentFormatQuery === 'function')
      ? est(cts.documentFormatQuery(f))
      : cts.estimate(cts.trueQuery(), ['format-' + f]);
  } catch (e) {
    out.formats_error = 'counting by format is not supported by this server';
    out.formats = {};
    break;
  }
}
try {
  const all = [];
  for (const c of cts.collections()) {
    all.push({name: String(c), count: est(cts.collectionQuery(c))});
    if (all.length >= 5000) break;                // plenty to rank the biggest
  }
  all.sort(function (a, b) { return b.count - a.count; });
  out.collections_total = all.length;
  out.collections = all.slice(0, TOP);
} catch (e) {
  out.collections_error = 'collection lexicon is off for this database';
}
out
"""


# How many documents are on one side of the flag. Walks the URI lexicon: reads
# no document, and counts exactly what the plan below pages through.
DOCS_COUNT_JS = DOCS_QUERY_JS + """
var FIELD; var MIGRATED;
let n = 0;
for (const uri of cts.uris(null, [], docsQuery(FIELD, MIGRATED))) n++;
n
"""


# One page of the documents still to migrate, each with its ID, its creation
# time (the CREATED field, else the Library Services created time) and the URIs
# its link fields name - which tell a document apart from a record that only
# hangs off one (a DOM holding the same ID, say).
PLAN_PAGE_JS = DOCS_QUERY_JS + BINARY_JSON_JS + FIND_KEY_JS + """
var AFTER; var LIMIT; var FIELD; var CREATED; var LINKS;
const idLeaf = FIELD.split('.').pop();
const createdLeaf = CREATED ? CREATED.split('.').pop() : '';
const linkFields = LINKS ? LINKS.split(',').map(function (f) { return f.trim(); }).filter(Boolean) : [];
const rows = [];
let last = '', seen = 0;
for (const item of cts.uris(AFTER || null, ['limit=' + (LIMIT + 1)], docsQuery(FIELD, 'false'))) {
  const uri = fn.string(item);
  if (uri === AFTER) continue;
  last = uri;
  seen++;
  const doc = cts.doc(uri);
  let obj = null;
  if (doc) {
    const kind = xdmp.nodeKind(doc.root);
    if (kind === 'object' || kind === 'array') obj = doc.toObject();
    else if (kind === 'binary') { const t = binaryJson(uri, doc); if (t !== null) obj = JSON.parse(t); }
  }
  const row = {uri: uri, id: '', created: '', links: []};
  if (obj !== null && typeof obj === 'object') {
    const id = findKey(obj, idLeaf);
    if (id !== null) row.id = String(id);
    const created = createdLeaf ? findKey(obj, createdLeaf) : null;
    if (created !== null) row.created = String(created);
    for (const f of linkFields) { const v = findKey(obj, f); if (v) row.links.push(String(v)); }
  } else if (doc) {
    // XML: the ID by element name.
    const hit = fn.head(doc.xpath('//*[local-name() = "' + idLeaf + '"]'));
    if (hit) row.id = fn.string(hit);
  }
  if (!row.created) {
    const props = fn.head(xdmp.documentProperties(uri));
    const c = props ? fn.head(props.xpath('.//*:version/*:created')) : null;
    if (c) row.created = fn.string(c);
  }
  rows.push(row);
  if (seen >= LIMIT) break;
}
({rows: rows, last: last})
"""


# One page of originals that hold NO document ID and are not migrated - PDFs,
# DOMs and anything else. The plan keeps those no document points at, so
# nothing is left behind unnoticed.
NO_ID_PAGE_JS = DOCS_QUERY_JS + """
var AFTER; var LIMIT; var FIELD;
const q = cts.andQuery([cts.notQuery(hasIdQuery(FIELD)), cts.notQuery(versionCopyQuery()),
                        flagQuery('false')]);
const uris = [];
for (const item of cts.uris(AFTER || null, ['limit=' + (LIMIT + 1)], q)) {
  const uri = fn.string(item);
  if (uri === AFTER) continue;
  uris.push(uri);
  if (uris.length >= LIMIT) break;
}
uris
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
function binaryHash(doc, kind) {
  if (!doc || kind !== 'binary') return '';
  try { return xdmp.sha256(doc.root, 'hex'); } catch (e) { return ''; }
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
    replaced: named(props, 'replaced') || '',
    author: named(props, 'external-user-name') || named(props, 'author') || '',
    properties_xml: props ? xdmp.quote(props) : '',
    // Hashes computed here, over exactly what this call sends back, so the
    // copy on disk can be checked against them. xdmp.sha256 takes a string or
    // a binary node and refuses anything else, hence the try.
    properties_hash: props ? xdmp.sha256(xdmp.quote(props), 'hex') : '',
    binary_hash: binaryHash(doc, kind)
  };
}
// A version copy names its parent; anything else is its own master.
const self = info(URI);
const master = self.version_of || URI;
// Library Services version copies of `uri`, oldest first.
function versionsOf(uri) {
  const found = [];
  for (const doc of cts.search(cts.propertiesFragmentQuery(
        cts.elementValueQuery(fn.QName(DLS_NS, 'document-uri'), uri)), ['unfiltered'])) {
    const u = xdmp.nodeUri(doc);
    if (u !== uri) found.push(info(u));
  }
  found.sort(function (a, b) {
    const x = parseInt(a.version || '0', 10), y = parseInt(b.version || '0', 10);
    return x === y ? (a.uri < b.uri ? -1 : 1) : x - y;
  });
  return found;
}
const out = {master: info(master), versions: versionsOf(master), links: []};

// Documents each envelope points at, named by the fields in LINKS. Every
// version is read, not just the master: a version can point at its own binary.
// env.refs keeps field -> URI per envelope; out.links holds each URI once,
// with its own version copies - a binary is versioned like the envelope.
const fields = LINKS ? LINKS.split(',').map(function (f) { return f.trim(); }).filter(Boolean) : [];
function refsOf(uri) {
  const refs = {};
  const doc = cts.doc(uri);
  if (!doc || !fields.length) return refs;
  const kind = xdmp.nodeKind(doc.root);
  let obj = null;
  if (kind === 'object' || kind === 'array') obj = doc.toObject();
  else if (kind === 'binary') { const t = binaryJson(uri, doc); if (t !== null) obj = JSON.parse(t); }
  else if (kind === 'text') { try { obj = JSON.parse(fn.string(doc)); } catch (e) {} }
  if (obj !== null && typeof obj === 'object') {
    for (const f of fields) {
      const v = findKey(obj, f);
      if (v) refs[f] = String(v);
    }
  }
  return refs;
}
const seen = {};
for (const env of [out.master, ...out.versions]) {
  env.refs = env.exists ? refsOf(env.uri) : {};
  for (const f of Object.keys(env.refs)) {
    const u = env.refs[f];
    if (seen[u]) continue;
    seen[u] = true;
    const item = info(u); item.field = f; item.versions = versionsOf(u); out.links.push(item);
  }
}
out
"""


# The current envelope(s) holding a document ID, without knowing its URI: the
# documents whose ID field has that value, version copies mapped back to their
# current document, less anything another hit links to (the DOM carries the ID
# too, but the envelope points at it). No hit on the field: URIs containing it.
FIND_BY_ID_JS = BINARY_JSON_JS + FIND_KEY_JS + """
var ID; var FIELD; var LINKS;
const DLS_NS = 'http://marklogic.com/xdmp/dls';
const leaf = FIELD.split('.').pop();
let matchedBy = 'field';
let hits = cts.uris(null, null, cts.orQuery([
  cts.jsonPropertyValueQuery(leaf, ID),
  cts.elementValueQuery(fn.QName('', leaf), ID)])).toArray().map(String);
if (!hits.length) {
  matchedBy = 'uri';
  hits = cts.uriMatch('*' + ID + '*').toArray().map(String);
}
function currentOf(uri) {
  const props = fn.head(xdmp.documentProperties(uri));
  const parent = props ? fn.head(props.xpath('.//*:document-uri')) : null;
  return parent ? fn.string(parent) : uri;
}
function content(uri) {
  const doc = cts.doc(uri);
  if (!doc) return null;
  const kind = xdmp.nodeKind(doc.root);
  if (kind === 'object' || kind === 'array') return doc.toObject();
  if (kind === 'binary') { const t = binaryJson(uri, doc); return t === null ? null : JSON.parse(t); }
  return null;
}
const current = [];
for (const u of hits) { const c = currentOf(u); if (current.indexOf(c) < 0) current.push(c); }
const linked = {};
const fields = LINKS ? LINKS.split(',').map(function (f) { return f.trim(); }).filter(Boolean) : [];
for (const u of current) {
  const obj = content(u);
  if (obj === null || typeof obj !== 'object') continue;
  for (const f of fields) { const v = findKey(obj, f); if (v) linked[String(v)] = true; }
}
({matched_by: matchedBy, hits: hits.length,
  envelopes: current.filter(function (u) { return !linked[u]; })})
"""


# Read-only look at how one document's pieces hang together: every version of
# the envelope with every timestamp MarkLogic has for it, every field in it that
# holds the URI of another document, and that document's own versions. Used to
# decide how a binary is matched to an envelope version.
DISCOVER_JS = BINARY_JSON_JS + FIND_KEY_JS + """
var URI; var CREATED;
const DLS_NS = 'http://marklogic.com/xdmp/dls';
function named(node, name) {
  if (!node) return '';
  const hit = fn.head(node.xpath('.//*[local-name() = "' + name + '"]'));
  return hit ? fn.string(hit) : '';
}
function stamps(uri) {
  const doc = cts.doc(uri);
  const props = fn.head(xdmp.documentProperties(uri));
  let mime = '';
  try { mime = xdmp.uriContentType(uri) || ''; } catch (e) {}
  return {
    uri: uri, exists: !!doc, kind: doc ? xdmp.nodeKind(doc.root) : '', mimetype: mime,
    version_id: named(props, 'version-id'), version_of: named(props, 'document-uri'),
    dls_created: named(props, 'created'), dls_replaced: named(props, 'replaced'),
    last_modified: named(props, 'last-modified'),
    properties_xml: props ? xdmp.quote(props) : '',
    property_names: props ? props.xpath('./*/*').toArray().map(function (n) {
      return fn.string(fn.nodeName(n)); }) : []
  };
}
function versionsOf(uri) {
  const out = [];
  for (const d of cts.search(cts.propertiesFragmentQuery(
        cts.elementValueQuery(fn.QName(DLS_NS, 'document-uri'), uri)), ['unfiltered'])) {
    const u = xdmp.nodeUri(d);
    if (u !== uri) out.push(stamps(u));
  }
  return out;
}
function content(uri) {
  const doc = cts.doc(uri);
  if (!doc) return null;
  const kind = xdmp.nodeKind(doc.root);
  if (kind === 'object' || kind === 'array') return doc.toObject();
  if (kind === 'binary') { const t = binaryJson(uri, doc); return t === null ? null : JSON.parse(t); }
  if (kind === 'text') { try { return JSON.parse(fn.string(doc)); } catch (e) {} }
  return null;
}
// Every string field that names a document that exists: path -> URI.
function refs(obj) {
  const out = [];
  const queue = [[obj, '']];
  while (queue.length && out.length < 50) {
    const [cur, path] = queue.shift();
    if (cur === null || typeof cur !== 'object') continue;
    for (const k of Object.keys(cur)) {
      const v = cur[k], p = path ? path + '.' + k : k;
      if (typeof v === 'string' && v.charAt(0) === '/' && v.length < 1000 && fn.docAvailable(v)) {
        const r = stamps(v); r.field = p; r.versions = versionsOf(v); out.push(r);
      } else if (v !== null && typeof v === 'object') queue.push([v, p]);
    }
  }
  return out;
}
const first = stamps(URI);
const master = first.version_of || URI;
const out = [];
for (const env of [stamps(master), ...versionsOf(master)]) {
  const obj = content(env.uri);
  env.created_field = obj && CREATED ? String(findKey(obj, CREATED.split('.').pop()) || '') : '';
  env.refs = obj ? refs(obj) : [];
  out.push(env);
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


# Takes the migration section off each document's properties again - not set to
# false, removed - and says whether it was there.
DEMARK_JS = """
declareUpdate();
var URIS;
const name = fn.QName('%s', '%s');
const out = [];
for (const uri of JSON.parse(URIS)) {
  const props = fn.head(xdmp.documentProperties(uri));
  const had = !!(props && fn.head(props.xpath('.//*:%s')));
  try {
    if (had) xdmp.documentRemoveProperties(uri, name);
    out.push({uri: uri, removed: had});
  } catch (e) {
    out.push({uri: uri, error: e.name + ': ' + (e.data || e.message || '')});
  }
}
out
""" % (MIGRATION_NS, MIGRATION_SECTION, MIGRATION_SECTION)


# One page of every record carrying the migration section, true or false.
MARKED_PAGE_JS = """
var AFTER; var LIMIT;
const q = cts.propertiesFragmentQuery(
  cts.elementQuery(fn.QName('%s', '%s'), cts.trueQuery()));
const uris = [];
for (const item of cts.uris(AFTER || null, ['limit=' + (LIMIT + 1)], q)) {
  const uri = fn.string(item);
  if (uri === AFTER) continue;
  uris.push(uri);
  if (uris.length >= LIMIT) break;
}
uris
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
