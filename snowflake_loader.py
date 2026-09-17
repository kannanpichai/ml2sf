"""The Snowflake side: is it reachable, can we log in, and uploading to the stage."""
from __future__ import annotations

from pathlib import Path

SESSION_SQL = """
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER(), CURRENT_ROLE(),
       CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()
"""
SESSION_FIELDS = ["account", "region", "user", "role",
                  "warehouse", "database", "schema", "version"]


def reach_check(host: str, port: int = 443, proxy_host: str = "",
                 proxy_port: str = "", timeout: int = 10) -> list[tuple[str, bool, str]]:
    """Can we get to the Snowflake endpoint at all? Steps, each ok/not ok.

    Any HTTP answer counts as reachable - 401/403/404 all prove the endpoint
    is there and only the credentials are missing.
    """
    import socket

    import requests

    steps: list[tuple[str, bool, str]] = []
    target = (proxy_host, int(proxy_port or 8080)) if proxy_host else (host, port)
    label = "proxy" if proxy_host else "host"

    try:
        ip = socket.gethostbyname(target[0])
        steps.append((f"DNS lookup ({label} {target[0]})", True, ip))
    except OSError as exc:
        steps.append((f"DNS lookup ({label} {target[0]})", False, str(exc)))
        return steps

    try:
        with socket.create_connection(target, timeout=timeout):
            steps.append((f"TCP connect {target[0]}:{target[1]}", True, "open"))
    except OSError as exc:
        steps.append((f"TCP connect {target[0]}:{target[1]}", False, str(exc)))
        return steps

    proxies = ({"https": f"http://{proxy_host}:{proxy_port or 8080}"} if proxy_host else None)
    url = f"https://{host}:{port}/"
    try:
        resp = requests.get(url, timeout=timeout, proxies=proxies)
        steps.append((f"HTTPS to {host}", True,
                      f"HTTP {resp.status_code} (any answer means it is reachable)"))
    except requests.exceptions.SSLError as exc:
        steps.append((f"HTTPS to {host}", False, f"TLS problem: {exc}"))
    except requests.exceptions.RequestException as exc:
        steps.append((f"HTTPS to {host}", False, str(exc)))
    return steps


def check_connection(conn_kwargs: dict, stage: str = "") -> dict:
    """Connect, report what the session resolves to, optionally test the stage.

    With a stage (@DB.SCHEMA.STAGE), a small file is uploaded to it, listed
    and removed again - the same PUT the migration uses, so it needs only
    READ and WRITE on that stage, no table privileges.
    """
    import tempfile

    import snowflake.connector

    out: dict[str, str] = {}
    with snowflake.connector.connect(**conn_kwargs) as conn:
        cur = conn.cursor()
        values = cur.execute(SESSION_SQL).fetchone() or ()
        out.update({k: ("" if v is None else str(v))
                    for k, v in zip(SESSION_FIELDS, values)})
        if stage:
            target = f"{stage}/_ml2sf_connection_test"
            with tempfile.TemporaryDirectory() as tmp:
                probe = Path(tmp) / "ml2sf_connection_test.txt"
                probe.write_text("ml2sf connection test\n", encoding="utf-8")
                try:
                    cur.execute(f"PUT 'file://{probe.as_posix()}' '{target}' "
                                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                    listed = cur.execute(f"LIST '{target}'").fetchall()
                    out["write test"] = (f"ok ({len(listed)} file uploaded to {target}, "
                                         "then removed)")
                finally:
                    cur.execute(f"REMOVE '{target}'")
    return out


def connect(conn_kwargs: dict):
    import snowflake.connector

    return snowflake.connector.connect(**conn_kwargs)


def upload_folder(cur, folder: Path, target: str) -> dict:
    """Make the stage folder `target` (@DB.SCHEMA.STAGE/<documentuuid>) hold
    exactly the files under `folder`, subfolders included.

    Whatever an earlier run left there is removed first, so a re-run replaces
    the document rather than mixing old and new files. Files go up as they are
    (no compression), and the stage is listed afterwards to prove every one
    arrived; the listing (size and md5 per file) is returned for checking.
    """
    files = sorted(p for p in folder.rglob("*") if p.is_file())
    wanted = {p.relative_to(folder).as_posix() for p in files}
    failed: list[str] = []
    put: dict[str, dict] = {}                 # name -> what PUT says it read and sent

    cur.execute(f"REMOVE '{target}/'")
    # One PUT per file: a wildcard would also match the subfolders, and PUT
    # rejects a directory (253006 "Not a file but a directory").
    for path in files:
        sub = path.parent.relative_to(folder).as_posix()
        dest = target if sub == "." else f"{target}/{sub}"
        rows = cur.execute(f"PUT 'file://{path.as_posix()}' '{dest}/' "
                           "AUTO_COMPRESS=FALSE OVERWRITE=TRUE").fetchall()
        # source, target, source_size, target_size, source_compression,
        # target_compression, status, message
        for row in rows:
            if str(row[6]).upper() != "UPLOADED":
                failed.append(f"{row[0]}: {row[6]} {row[7] or ''}".strip())
            put[path.relative_to(folder).as_posix()] = {
                "source_size": row[2], "target_size": row[3],
                "compression": f"{row[4]}->{row[5]}"}

    # LIST names read <stage>/<documentuuid>/<file>; keep the part below the
    # document folder so it compares with the local names.
    # LIST rows: name, size, md5, last_modified.
    doc_folder = target.split("/", 1)[1]
    listed: dict[str, dict] = {}
    for row in cur.execute(f"LIST '{target}/'").fetchall():
        below_stage = str(row[0]).split("/", 1)[-1]
        listed[below_stage[len(doc_folder) + 1:]] = {"size": int(row[1]),
                                                     "md5": str(row[2] or "")}
    return {
        "files": len(wanted),
        "failed": failed,
        "missing": sorted(wanted - set(listed)),
        "extra": sorted(set(listed) - wanted),
        "listed": listed,
        "put": put,
    }


# ---------- checking the stage copy ----------

# A Python UDF that reads a staged file inside Snowflake - after Snowflake has
# decrypted it - and returns its SHA-256. It is the only way to hash what a
# client-side encrypted stage (SHOW STAGES type INTERNAL) holds: LIST reports
# the MD5 of the encrypted copy there. Its SQL lives in sql/stage_sha256.sql,
# for an admin to run once; that file names it by its default name.
HASH_FUNCTION_FILE = Path(__file__).resolve().parent / "sql" / "stage_sha256.sql"
_DEFAULT_HASH_FUNCTION = "GDX_DOCUMENTS_DB.GDX_DOCUMENTS.STAGE_SHA256"


def hash_function_sql(name: str) -> str:
    """sql/stage_sha256.sql, with the function under `name` if that differs."""
    return HASH_FUNCTION_FILE.read_text(encoding="utf-8").replace(_DEFAULT_HASH_FUNCTION, name)


def hash_function_exists(cur, name: str) -> bool:
    """Is the UDF `name` (DB.SCHEMA.FUNCTION) there and visible to this role?"""
    schema, _, function = name.rpartition(".")
    rows = cur.execute(f"SHOW USER FUNCTIONS LIKE {_literal(function.strip(chr(34)))} "
                       f"IN SCHEMA {schema}").fetchall()
    return bool(rows)


def stage_sha256(cur, function: str, target: str, names: list[str],
                 batch: int = 200) -> dict[str, str]:
    """name -> SHA-256 of <target>/<name> (target = @DB.SCHEMA.STAGE/<documentuuid>),
    computed inside Snowflake by the UDF `function`."""
    stage, doc_folder = target.split("/", 1)
    found: dict[str, str] = {}
    for i in range(0, len(names), batch):
        values = ", ".join(f"({_literal(doc_folder + '/' + n)})" for n in names[i:i + batch])
        sql = (f"SELECT column1, {function}(BUILD_STAGE_FILE_URL({stage}, column1)) "
               f"FROM VALUES {values}")
        for path, sha in cur.execute(sql).fetchall():
            found[str(path)[len(doc_folder) + 1:]] = str(sha or "").lower()
    return found


# ---------- migration control table ----------

# What `migrate` writes per row (MIG_STATUS is always CREATED); the audit columns (CREATE_/UPDATE_USER_ID, _TS)
# are filled in by the MERGE itself.
CONTROL_KEY = ["DOCUMENT_ID", "VERSION_ID"]
CONTROL_COLUMNS = CONTROL_KEY + [
    "ENVELOPE_FILE_NAME", "ENVELOPE_FILE_URI",
    "PROPERTY_FILE_NAME", "PROPERTY_FILE_URI",
    "BINARY_FILE_NAME", "BINARY_FILE_URI",
    "DOM_FILE_NAME", "DOM_FILE_URI",
    "CONVERTED_PDF_FILE_NAME", "CONVERTED_PDF_FILE_URI",
    "TRANSLATED_PDF_FILE_NAME", "TRANSLATED_PDF_FILE_URI",
    "DOC_ML_HASH_SHA256", "DOC_SF_HASH_SHA256",
    "MIG_STATUS", "COMMENT",
]
# Column widths from the table definition; a longer value is refused, not cut.
CONTROL_WIDTHS = {"DOCUMENT_ID": 36, "DOC_ML_HASH_SHA256": 100, "DOC_SF_HASH_SHA256": 100,
                  "MIG_STATUS": 50, "COMMENT": 1000,
                  **{c: 100 for c in CONTROL_COLUMNS if c.endswith("_FILE_NAME")},
                  **{c: 1000 for c in CONTROL_COLUMNS if c.endswith("_FILE_URI")}}


def refresh_directory(cur, stage: str) -> int:
    """ALTER STAGE ... REFRESH: bring the stage's directory table - what the
    Snowflake UI's stage page and DIRECTORY(@stage) read - up to date with the
    files uploaded. Internal stages never do this by themselves. Returns how
    many files the refresh registered or removed."""
    rows = cur.execute(f"ALTER STAGE {stage.lstrip('@')} REFRESH").fetchall()
    return len(rows)


def check_control_table(cur, table: str) -> None:
    """Fail early if the table is not there or this role cannot read it."""
    cur.execute(f"SELECT 1 FROM {table} LIMIT 0")


def _merge_sql(table: str) -> str:
    """Insert a row, or update it if (DOCUMENT_ID, VERSION_ID) is already there
    - Snowflake does not enforce the primary key, so a re-run must not simply
    insert again. Values are %(COLUMN)s placeholders."""
    source = ", ".join(f"%({c})s AS {c}" for c in CONTROL_COLUMNS)
    user = "LEFT(CURRENT_USER(), 24)"
    updates = ", ".join(f"t.{c} = s.{c}" for c in CONTROL_COLUMNS if c not in CONTROL_KEY)
    return (f"MERGE INTO {table} t USING (SELECT {source}) s "
            f"ON t.DOCUMENT_ID = s.DOCUMENT_ID AND t.VERSION_ID = s.VERSION_ID "
            f"WHEN MATCHED THEN UPDATE SET {updates}, "
            f"t.UPDATE_USER_ID = {user}, t.UPDATE_TS = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(CONTROL_COLUMNS)}, CREATE_USER_ID, CREATE_TS) "
            f"VALUES ({', '.join('s.' + c for c in CONTROL_COLUMNS)}, {user}, CURRENT_TIMESTAMP())")


def merge_control_rows(cur, table: str, rows: list[dict]) -> None:
    """Write each row to the control table (see _merge_sql)."""
    sql = _merge_sql(table)
    for row in rows:
        cur.execute(sql, {c: row.get(c) for c in CONTROL_COLUMNS})


def _literal(value) -> str:
    """A value as a Snowflake SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def control_rows_sql(table: str, rows: list[dict]) -> str:
    """The same MERGE statements merge_control_rows runs, as text to run by hand."""
    sql = _merge_sql(table)
    return "".join(sql % {c: _literal(row.get(c)) for c in CONTROL_COLUMNS} + ";\n"
                   for row in rows)
