"""The Snowflake side: is it reachable, and can we log in?

Loading is not built yet; this only proves the connection works.
"""
from __future__ import annotations

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


def check_connection(conn_kwargs: dict, write_test: bool = False) -> dict:
    """Connect, report what the session resolves to, optionally test writing."""
    import snowflake.connector

    out: dict[str, str] = {}
    with snowflake.connector.connect(**conn_kwargs) as conn:
        cur = conn.cursor()
        values = cur.execute(SESSION_SQL).fetchone() or ()
        out.update({k: ("" if v is None else str(v))
                    for k, v in zip(SESSION_FIELDS, values)})
        if write_test:
            table = "ML2SF_CONNECTION_TEST"
            try:
                cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {table} (CHECKED_AT TIMESTAMP_NTZ)")
                cur.execute(f"INSERT INTO {table} VALUES (CURRENT_TIMESTAMP())")
                rows = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                out["write test"] = f"ok ({rows[0]} row created, table dropped)"
            finally:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
    return out
