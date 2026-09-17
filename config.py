"""Configuration: connection settings read from .env.

No secrets live in code.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


class STAGE:
    """Where staged documents go: one folder per document, named by its ID.

    DIR is the local tree the files are written to. Where that tree belongs on
    the far side is the Snowflake stage SF_STAGE, in the database and schema
    already configured as SF_DATABASE / SF_SCHEMA - so the location is named
    once, not twice.
    """
    DIR = Path(os.getenv("STAGE_DIR", "").strip() or (OUTPUT_DIR / "stage"))

    @staticmethod
    def snowflake_path(*parts: str) -> str:
        """The stage as Snowflake spells it: @DB.SCHEMA.STAGE/a/b."""
        return "/".join([SF.stage(), *[p.strip("/") for p in parts if p]])


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
    # Where each document keeps its own ID, and which field in a record holds
    # the URI of the binary it describes. Empty = not used.
    ID_PATH = os.getenv("ML_ID_PATH", "").strip()
    LINK_PATH = os.getenv("ML_LINK_PATH", "").strip()
    CREATED_PATH = os.getenv("ML_CREATED_PATH", "").strip()

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
    # Named stage the staged files are uploaded into. A bare name is qualified
    # with SF_DATABASE and SF_SCHEMA; give it dots to point somewhere else.
    STAGE = os.getenv("SF_STAGE", "").strip()
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
    def stage() -> str:
        """The stage, fully qualified: @DB.SCHEMA.STAGE."""
        if not SF.STAGE:
            raise SystemExit(
                f"[config] SF_STAGE is not set in {ROOT / '.env'}. "
                "Set it to the stage these files belong in, e.g. "
                "GDX_MARKLOGIC_DOCUMENTS"
            )
        name = SF.STAGE.lstrip("@").replace("/", ".").strip(".")
        if "." in name:                       # already qualified: use as given
            return "@" + name
        database = os.getenv("SF_DATABASE", "").strip()
        if not database or not SF.SCHEMA:
            raise SystemExit(
                f"[config] SF_STAGE={SF.STAGE} needs SF_DATABASE and SF_SCHEMA "
                f"in {ROOT / '.env'} to say where the stage lives - or give "
                "SF_STAGE the full DB.SCHEMA.STAGE name."
            )
        return f"@{database}.{SF.SCHEMA}.{name}"

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
