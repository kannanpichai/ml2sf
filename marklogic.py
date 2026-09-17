"""Everything on the MarkLogic side: the REST client, and turning whatever it
returns (JSON, XML or text) into a plain dict that can be walked by path.

Makes no assumptions about document shape.
"""
from __future__ import annotations

import copy
import hashlib
import json
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


# ---------- path walking ----------

def dig(doc: Any, path: str) -> Any:
    """Walk a dotted path. Supports [] to mean 'collect across a list'.

    'party.legalName'      -> scalar
    'officers[].name'      -> list of every officer's name
    """
    cur: Any = doc
    for part in path.split("."):
        collect = part.endswith("[]")
        if collect:
            part = part[:-2]
        if isinstance(cur, list):
            out = []
            for item in cur:
                val = dig(item, part + ("[]" if collect else ""))
                if isinstance(val, list):
                    out.extend(val)
                elif val is not None:
                    out.append(val)
            cur = out or None
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
        if cur is None:
            return None
        if collect and not isinstance(cur, list):
            cur = [cur]
    return cur


def walk_paths(doc: Any, prefix: str = "") -> Iterator[tuple[str, Any]]:
    """Yield (dotted_path, value) for every leaf in the document.

    Lists are described once with a [] marker rather than per index, so 100
    officers produce one 'officers[].name' path, not 100.
    """
    if isinstance(doc, dict):
        for key, val in doc.items():
            path = f"{prefix}.{key}" if prefix else key
            yield from walk_paths(val, path)
    elif isinstance(doc, list):
        if not doc:
            yield (f"{prefix}[]", [])
        for item in doc:
            yield from walk_paths(item, f"{prefix}[]")
    else:
        yield (prefix, doc)


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
            raise MarkLogicError(f"{what}: HTTP {resp.status_code} -> {resp.text[:400]}")
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

    def count(self, selector: dict) -> int:
        payload, params = self._search_request(selector, start=1, page_length=0)
        return int(self._search(payload, params).get("total", 0))

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

    def iter_uris(self, selector: dict, page_length: int = 500) -> Iterator[str]:
        for row in self.iter_results(selector, page_length):
            yield row["uri"]

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

    def iter_uri_rows(self, selector: dict, page_length: int = 1000) -> Iterator[dict]:
        """Yield {uri, mimetype} for every match, cheapest way available.

        Prefers the URI lexicon, which walks URIs in sorted order and stays fast
        however deep it goes. Falls back to /v1/search paging when the lexicon is
        off or the selector needs a full query.
        """
        if not (selector.get("q") or selector.get("structured_query")):
            try:
                rows = self._uri_chunk(selector, "", page_length)
            except MarkLogicError:
                rows = None                      # no URI lexicon: use search
            if rows is not None:
                while rows:
                    yield from rows
                    rows = self._uri_chunk(selector, rows[-1]["uri"], page_length)
                return
        for row in self.iter_results(selector, page_length=min(page_length, 500)):
            yield {
                "uri": row["uri"],
                "mimetype": row.get("mimetype", ""),
                "format": row.get("format", ""),
            }

    def _uri_chunk(self, selector: dict, after: str, limit: int) -> list[dict]:
        result = self.eval_js(URIS_JS, {
            "AFTER": after,
            "LIMIT": limit,
            "COLLECTION": selector.get("collection", "") or "",
            "DIRECTORY": selector.get("directory", "") or "",
        })
        if len(result) == 1 and isinstance(result[0], list):
            result = result[0]
        return result

    # ---------- reading ----------

    def get_document(self, uri: str) -> dict:
        """Fetch one document as a dict, whatever its stored format."""
        resp = self.session.get(
            f"{self.base}/v1/documents",
            params=self._params({"uri": uri}),
            timeout=self.timeout,
        )
        self._check(resp, f"get {uri}")
        ctype = resp.headers.get("Content-Type", "").lower()
        if "json" in ctype:
            return _as_dict(resp.json())
        if "xml" in ctype:
            return _as_dict(parse_xml(resp.content))
        return _as_dict({"text": resp.text})

    def get_documents(
        self, uris: list[str], workers: int = 8
    ) -> Iterator[tuple[str, dict]]:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            yield from zip(uris, pool.map(self.get_document, uris))

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


# Walks the URI lexicon in sorted order, starting after the previous chunk's
# last URI, so paging cost does not grow with depth. The MIME type comes from
# MarkLogic's extension mapping, which needs no document read.
URIS_JS = """
var AFTER; var LIMIT; var COLLECTION; var DIRECTORY;
const qs = [];
if (COLLECTION) qs.push(cts.collectionQuery(COLLECTION));
if (DIRECTORY) qs.push(cts.directoryQuery(DIRECTORY, 'infinity'));
const q = qs.length ? cts.andQuery(qs) : cts.trueQuery();
const out = [];
for (const uri of cts.uris(AFTER || null, ['limit=' + (LIMIT + 1)], q)) {
  if (uri === AFTER) continue;
  let mime = '';
  try { mime = xdmp.uriContentType(uri); } catch (e) { mime = ''; }
  out.push({uri: uri, mimetype: mime, format: ''});
  if (out.length >= LIMIT) break;
}
out
"""


# Runs inside MarkLogic. The hash is taken over exactly the text that is
# returned, so the same text can be re-hashed anywhere downstream.
READ_JS = """
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
    item.error = 'binary documents are not supported yet';
  } else {
    item.text = xdmp.quote(doc);
    item.hash = xdmp.sha256(item.text, 'hex');
  }
  out.push(item);
}
out
"""


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class SourceDocument:
    """One document as read from MarkLogic, ready to be mapped and verified."""
    uri: str
    format: str = ""
    text: str | None = None
    ml_hash: str | None = None
    properties: dict = field(default_factory=dict)
    error: str | None = None

    @classmethod
    def from_eval(cls, item: dict) -> "SourceDocument":
        return cls(
            uri=item["uri"],
            format=item.get("format", ""),
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
