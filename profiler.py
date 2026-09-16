"""Sample a set of documents and infer their schema.

Produces, for every JSON path seen: how often it appears, which types it holds,
whether it repeats, and example values. That profile drives the generated
mapping file, so an unknown corpus becomes a reviewable YAML in one command.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable

from marklogic import walk_paths

# Inferred type -> Snowflake type
SF_TYPE = {
    "boolean": "BOOLEAN",
    "integer": "NUMBER",
    "float": "FLOAT",
    "date": "DATE",
    "timestamp": "TIMESTAMP_NTZ",
    "string": "STRING",
    "array": "VARIANT",
    "object": "VARIANT",
    "null": "STRING",
}

# Widening order: when a path holds mixed types, pick the safest common one.
RANK = ["null", "boolean", "integer", "float", "date", "timestamp", "string", "array", "object"]


def infer_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    text = str(value).strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return "date"
    if 19 <= len(text) <= 32 and text[4] == "-" and ("T" in text or " " in text):
        return "timestamp"
    return "string"


class PathStats:
    __slots__ = ("count", "types", "repeats", "samples", "nulls", "docs")

    def __init__(self) -> None:
        self.count = 0   # total occurrences (a repeating path counts many per doc)
        self.docs = 0    # documents in which the path held a non-null value
        self.nulls = 0
        self.types: set[str] = set()
        self.repeats = False
        self.samples: list[Any] = []

    def add(self, value: Any) -> None:
        self.count += 1
        kind = infer_type(value)
        if kind == "null":
            self.nulls += 1
        else:
            self.types.add(kind)
        if len(self.samples) < 3 and value not in (None, "") and value not in self.samples:
            self.samples.append(value)

    @property
    def sf_type(self) -> str:
        if self.repeats:
            return "VARIANT"
        if not self.types:
            return "STRING"
        widest = max(self.types, key=lambda t: RANK.index(t))
        return SF_TYPE.get(widest, "STRING")


def profile(docs: Iterable[dict]) -> tuple[dict[str, PathStats], int]:
    """Walk every document, returning per-path statistics and the doc count."""
    stats: dict[str, PathStats] = defaultdict(PathStats)
    total = 0
    for doc in docs:
        total += 1
        seen_this_doc: dict[str, int] = defaultdict(int)
        non_null: set[str] = set()
        for path, value in walk_paths(doc):
            if not path:
                continue
            seen_this_doc[path] += 1
            stats[path].add(value)
            if value is not None and value != [] and value != "":
                non_null.add(path)
        for path in non_null:
            stats[path].docs += 1
        for path, n in seen_this_doc.items():
            if n > 1 or "[]" in path:
                stats[path].repeats = True
    return _unify_cardinality(dict(stats)), total


def _unify_cardinality(stats: dict[str, PathStats]) -> dict[str, PathStats]:
    """Collapse 'a.b.c' into 'a.b[].c' when both appear.

    An XML element that occurs once in one document and twice in another parses
    as a scalar then a list, which would otherwise yield two columns for one
    logical field. The repeating form wins because dig() promotes a scalar to a
    single-item list, so it reads both correctly.
    """
    groups: dict[str, list[str]] = {}
    for path in stats:
        groups.setdefault(path.replace("[]", ""), []).append(path)

    merged: dict[str, PathStats] = {}
    for variants in groups.values():
        if len(variants) == 1:
            merged[variants[0]] = stats[variants[0]]
            continue
        canonical = max(variants, key=lambda p: p.count("[]"))
        winner = stats[canonical]
        for other in variants:
            if other == canonical:
                continue
            src_stats = stats[other]
            winner.count += src_stats.count
            winner.docs += src_stats.docs
            winner.nulls += src_stats.nulls
            winner.types |= src_stats.types
            for sample in src_stats.samples:
                if len(winner.samples) < 3 and sample not in winner.samples:
                    winner.samples.append(sample)
        winner.repeats = True
        merged[canonical] = winner
    return merged


def report(stats: dict[str, PathStats], total: int) -> str:
    rows = []
    width = max((len(p) for p in stats), default=10)
    width = min(max(width, 20), 60)
    rows.append(f"{'PATH'.ljust(width)}     {'SF TYPE':<14} {'FILL':>6}  EXAMPLE")
    rows.append("-" * (width + 45))
    for path in sorted(stats):
        st = stats[path]
        fill = _fill(st, total) * 100
        example = ""
        if st.samples:
            example = str(st.samples[0])
            if len(example) > 40:
                example = example[:37] + "..."
        flag = "[]" if st.repeats else "  "
        rows.append(
            f"{path[:width].ljust(width)} {flag} {st.sf_type:<14} {fill:5.0f}%  {example}"
        )
    return "\n".join(rows)


def suggest_columns(
    stats: dict[str, PathStats], total: int, min_fill: float = 0.0
) -> dict[str, dict]:
    """Turn the profile into a column mapping, best candidates first.

    Repeating and object-valued paths are kept but typed VARIANT, so nothing is
    silently dropped - an analyst can delete the rows they do not want.
    """
    used: set[str] = set()
    columns: dict[str, dict] = {}
    for path in sorted(stats, key=lambda p: (-_fill(stats[p], total), p)):
        st = stats[path]
        if _fill(st, total) < min_fill:
            continue
        col = path_to_column(path, used)
        entry = {"path": path, "type": st.sf_type}
        if st.repeats:
            entry["note"] = "repeating - stored as VARIANT array"
        columns[col] = entry
    return columns


def _fill(st: PathStats, total: int) -> float:
    """Fraction of documents in which this path held a real value."""
    return min(st.docs / total, 1.0) if total else 0.0


# ---------- naming ----------

_SAFE = re.compile(r"[^A-Za-z0-9_]+")


def path_to_column(path: str, used: set[str] | None = None) -> str:
    """Turn a dotted path into a legal, readable Snowflake column name.

    'party.legalName' -> PARTY_LEGAL_NAME ; collisions get a numeric suffix.
    """
    cleaned = path.replace("[]", "").replace("@", "").replace("#", "")
    parts: list[str] = []
    for seg in cleaned.split("."):
        seg = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", seg)
        parts.append(_SAFE.sub("_", seg).strip("_"))
    name = "_".join(p for p in parts if p).upper()
    name = re.sub(r"_+", "_", name).strip("_") or "FIELD"
    if name[0].isdigit():
        name = "F_" + name
    if used is not None:
        base, n = name, 2
        while name in used:
            name = f"{base}_{n}"
            n += 1
        used.add(name)
    return name
