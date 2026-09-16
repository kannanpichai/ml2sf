"""Configuration: connection settings from .env, and the YAML mapping model.

No secrets live in code, and nothing schema-specific does either - a new client
is a new mappings/*.yml file, not a new script.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from marklogic import dig

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
MAPPINGS_DIR = ROOT / "mappings"


# ---------- .env settings ----------

def _req(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val or val == "CHANGE_ME":
        raise SystemExit(
            f"[config] {name} is not set in {ROOT / '.env'}. "
            "Fill it in before running this command."
        )
    return val


class ML:
    HOST = os.getenv("ML_HOST", "localhost")
    PORT = int(os.getenv("ML_PORT", "8000"))
    DATABASE = os.getenv("ML_DATABASE", "Documents").strip()
    AUTH = os.getenv("ML_AUTH", "digest").lower()
    SCHEME = os.getenv("ML_SCHEME", "http")

    @staticmethod
    def user() -> str:
        return _req("ML_USER")

    @staticmethod
    def password() -> str:
        return _req("ML_PASSWORD")


class SF:
    ROLE = os.getenv("SF_ROLE", "").strip()

    @staticmethod
    def conn_kwargs() -> dict:
        kw = {
            "account": _req("SF_ACCOUNT"),
            "user": _req("SF_USER"),
            "password": _req("SF_PASSWORD"),
            "warehouse": _req("SF_WAREHOUSE"),
            "database": _req("SF_DATABASE"),
            "schema": _req("SF_SCHEMA"),
        }
        if SF.ROLE:
            kw["role"] = SF.ROLE
        return kw


# ---------- mapping file ----------

@dataclass
class Mapping:
    """A YAML description of one MarkLogic -> Snowflake migration."""
    name: str
    source: dict = field(default_factory=dict)
    target: dict = field(default_factory=dict)
    columns: dict[str, dict] = field(default_factory=dict)
    raw: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path_or_name: str) -> "Mapping":
        path = Path(path_or_name)
        if not path.exists():
            path = MAPPINGS_DIR / f"{path_or_name}.yml"
        if not path.exists():
            raise SystemExit(
                f"Mapping not found: {path_or_name}. "
                f"Run 'python migrate.py profile' to generate one."
            )
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls(
            name=data.get("name", path.stem),
            source=data.get("source") or {},
            target=data.get("target") or {},
            columns=data.get("columns") or {},
            raw=data.get("raw") or {},
        )

    @classmethod
    def default(cls, name: str, source: dict, columns: dict, table: str) -> "Mapping":
        return cls(
            name=name,
            source=source,
            target={"table": table, "key": "DOC_URI", "mode": "merge"},
            raw={"include": True, "column": "RAW_DOC"},
            columns=columns,
        )

    def save(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        body = {
            "name": self.name,
            "source": self.source,
            "target": self.target,
            "raw": self.raw,
            "columns": self.columns,
        }
        path.write_text(
            yaml.safe_dump(body, sort_keys=False, allow_unicode=True, width=100),
            encoding="utf-8",
        )
        return path

    @property
    def table(self) -> str:
        return str(self.target.get("table", self.name)).upper()

    @property
    def key_column(self) -> str:
        return str(self.target.get("key", "DOC_URI")).upper()

    @property
    def mode(self) -> str:
        return str(self.target.get("mode", "merge")).lower()

    @property
    def raw_column(self) -> str | None:
        if self.raw.get("include", True) is False:
            return None
        return str(self.raw.get("column", "RAW_DOC")).upper()

    def sf_columns(self) -> list[tuple[str, str]]:
        """Full ordered column list for the target table."""
        cols: list[tuple[str, str]] = [(self.key_column, "STRING")]
        for name, spec in self.columns.items():
            cols.append((name.upper(), str(spec.get("type", "STRING")).upper()))
        if self.raw_column:
            cols.append((self.raw_column, "VARIANT"))
        cols.append(("EXTRACTED_AT", "TIMESTAMP_NTZ"))
        return cols

    def row_for(self, uri: str, doc: dict, extracted_at: str) -> dict[str, Any]:
        """Apply the mapping to one document."""
        row: dict[str, Any] = {self.key_column: uri}
        for name, spec in self.columns.items():
            value = dig(doc, spec["path"])
            if value is None and "default" in spec:
                value = spec["default"]
            row[name.upper()] = value
        if self.raw_column:
            row[self.raw_column] = json.dumps(doc, ensure_ascii=False)
        row["EXTRACTED_AT"] = extracted_at
        return row
