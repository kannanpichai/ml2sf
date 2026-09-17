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
    """Snowflake settings.

    Three ways to authenticate, in the order they are tried:
      1. key pair  - SF_CERT_KEY (credentials vault) or SF_PRIVATE_KEY (PEM file)
      2. SSO       - SF_AUTHENTICATOR=externalbrowser
      3. password  - SF_PASSWORD
    """
    ROLE = os.getenv("SF_ROLE", "").strip()
    SCHEMA = os.getenv("SF_SCHEMA", "").strip()
    HOST = os.getenv("SF_HOST", "").strip().rstrip("/")
    PORT = os.getenv("SF_PORT", "").strip()
    AUTHENTICATOR = os.getenv("SF_AUTHENTICATOR", "").strip()
    PRIVATE_KEY = os.getenv("SF_PRIVATE_KEY", "").strip()
    PRIVATE_KEY_PASSPHRASE = os.getenv("SF_PRIVATE_KEY_PASSPHRASE", "")
    # Secure Credentials Vault, as sf.datasource.{namespace,certkey,passphrasekey}.name
    NAMESPACE = os.getenv("SF_NAMESPACE", "").strip()
    CERT_KEY = os.getenv("SF_CERT_KEY", "").strip()
    PASSPHRASE_KEY = os.getenv("SF_PASSPHRASE_KEY", "").strip()
    SCV_MODULE = os.getenv("SF_SCV_MODULE", "scvlib").strip()
    SCV_MS_ENV = os.getenv("SF_SCV_MS_ENV", "genpop").strip()
    SCV_ENV = os.getenv("SF_SCV_ENV", "prod").strip()
    PROXY_HOST = os.getenv("SF_PROXY_HOST", "").strip()
    PROXY_PORT = os.getenv("SF_PROXY_PORT", "").strip()
    LOGIN_TIMEOUT = os.getenv("SF_LOGIN_TIMEOUT", "60").strip()

    @staticmethod
    def hostname() -> str:
        """SF_HOST without scheme, path or port."""
        return SF.HOST.split("://")[-1].split("/")[0].split(":")[0]

    @staticmethod
    def account() -> str:
        """SF_ACCOUNT, or the account name derived from SF_HOST."""
        if os.getenv("SF_ACCOUNT", "").strip() or not SF.HOST:
            return _req("SF_ACCOUNT")
        return SF.hostname().split(".snowflakecomputing.com")[0]

    @staticmethod
    def auth_method() -> str:
        if SF.CERT_KEY:
            return "key pair (from the credentials vault)"
        if SF.PRIVATE_KEY:
            return "key pair (from a file)"
        if SF.AUTHENTICATOR.lower() == "externalbrowser":
            return "SSO (browser)"
        return "password"

    @staticmethod
    def _pem_and_passphrase() -> tuple[bytes, bytes | None]:
        """The PEM key and its passphrase, from the vault or from a file."""
        if SF.CERT_KEY:
            try:
                module = __import__(SF.SCV_MODULE, fromlist=["SecureCredentialsVault"])
                SecureCredentialsVault = module.SecureCredentialsVault
            except (ImportError, AttributeError):
                raise SystemExit(
                    f"[config] SF_CERT_KEY is set but {SF.SCV_MODULE!r} is not installed here. "
                    "Install it from the internal package index, set SF_SCV_MODULE if the "
                    "module has another name, or use SF_PRIVATE_KEY with a PEM file."
                )
            scv = SecureCredentialsVault(ms_env=SF.SCV_MS_ENV, scv_env=SF.SCV_ENV, verbose=False)
            pem = scv.tell_key(namespace=SF.NAMESPACE, key_name=SF.CERT_KEY)
            passphrase = scv.tell_key(namespace=SF.NAMESPACE, key_name=SF.PASSPHRASE_KEY)                 if SF.PASSPHRASE_KEY else ""
            return pem.encode() if isinstance(pem, str) else pem,                 (passphrase.encode() if isinstance(passphrase, str) else passphrase) or None

        path = Path(SF.PRIVATE_KEY).expanduser()
        if not path.exists():
            raise SystemExit(f"[config] SF_PRIVATE_KEY file not found: {path}")
        return path.read_bytes(), SF.PRIVATE_KEY_PASSPHRASE.encode() or None

    @staticmethod
    def private_key_bytes() -> bytes:
        """The private key in the DER form the Snowflake connector expects."""
        from cryptography.hazmat.primitives import serialization

        pem, passphrase = SF._pem_and_passphrase()
        try:
            key = serialization.load_pem_private_key(pem, password=passphrase)
        except TypeError:
            raise SystemExit("[config] this private key is encrypted: set its passphrase "
                             "(SF_PRIVATE_KEY_PASSPHRASE, or SF_PASSPHRASE_KEY in the vault).")
        except ValueError as exc:
            raise SystemExit(f"[config] cannot read the Snowflake private key: {exc}")
        return key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @staticmethod
    def conn_kwargs() -> dict:
        kw: dict[str, Any] = {
            "account": SF.account(),
            "user": _req("SF_USER"),
            "warehouse": _req("SF_WAREHOUSE"),
            "database": _req("SF_DATABASE"),
        }
        if SF.SCHEMA:
            kw["schema"] = SF.SCHEMA
        if SF.PRIVATE_KEY or SF.CERT_KEY:
            kw["private_key"] = SF.private_key_bytes()
        elif SF.AUTHENTICATOR.lower() == "externalbrowser":
            kw["authenticator"] = SF.AUTHENTICATOR
        else:
            if SF.AUTHENTICATOR:
                kw["authenticator"] = SF.AUTHENTICATOR
            kw["password"] = _req("SF_PASSWORD")
        if SF.ROLE:
            kw["role"] = SF.ROLE
        if SF.HOST:
            kw["host"] = SF.hostname()
        if SF.PORT:
            kw["port"] = int(SF.PORT)
        if SF.PROXY_HOST:
            kw["proxy_host"] = SF.PROXY_HOST
        if SF.PROXY_PORT:
            kw["proxy_port"] = int(SF.PROXY_PORT)
        if SF.LOGIN_TIMEOUT:
            kw["login_timeout"] = int(SF.LOGIN_TIMEOUT)
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
