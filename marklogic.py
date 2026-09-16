"""Everything on the MarkLogic side: the REST client, and turning whatever it
returns (JSON, XML or text) into a plain dict that can be walked by path.

Makes no assumptions about document shape.
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterator
from xml.etree import ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth


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
        self.session.auth = (
            HTTPDigestAuth(user, password) if auth == "digest"
            else HTTPBasicAuth(user, password)
        )

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

    def eval_js(self, script: str) -> Any:
        """Run server-side JavaScript and return the decoded result."""
        resp = self.session.post(
            f"{self.base}/v1/eval",
            data=self._params({"javascript": script}),
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
        start = 1
        while True:
            payload, params = self._search_request(selector, start, page_length)
            data = self._search(payload, params)
            results = data.get("results") or []
            for row in results:
                yield row["uri"]
            total = int(data.get("total", 0))
            start += len(results)
            if not results or start > total:
                return

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
            doc = resp.json()
        elif "xml" in ctype:
            doc = parse_xml(resp.content)
        else:
            doc = {"text": resp.text}
        if isinstance(doc, dict) and set(doc) == {"content"}:
            doc = doc["content"]
        return doc if isinstance(doc, dict) else {"value": doc}

    def get_documents(
        self, uris: list[str], workers: int = 8
    ) -> Iterator[tuple[str, dict]]:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            yield from zip(uris, pool.map(self.get_document, uris))


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
