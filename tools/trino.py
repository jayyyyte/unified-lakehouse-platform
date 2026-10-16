"""Minimal Trino client over its REST protocol, for tools/ that read the lakehouse.

    from tools.trino import query
    rows = query("SELECT count(*) FROM iceberg.bronze.cdc_events")

Why not the `trino` package: the same reason tools/dbsession.py shells out instead of
importing a driver -- the repository runs on a bare Python plus Docker. Why not
`docker compose exec trino trino --execute`: every call would start a JVM CLI inside the
container, seconds per query, which is more than some of the things being measured.

The protocol is small: POST the statement to /v1/statement, then follow `nextUri` until
it disappears, collecting `data` pages on the way. Values arrive already JSON-typed
(numbers as numbers, timestamps and decimals as strings).
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mosaic import host_port  # noqa: E402

USER = "mosaic-tools"


class TrinoError(RuntimeError):
    pass


def _send(req: urllib.request.Request) -> dict:
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise TrinoError(f"HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:300]}") from e


def query_with_columns(sql: str, timeout: float = 600.0) -> tuple[list[str], list[list]]:
    """Run one statement; return (column names, rows). Raises TrinoError on failure."""
    headers = {"X-Trino-User": USER}
    url = f"http://localhost:{host_port('TRINO_PORT')}/v1/statement"
    doc = _send(urllib.request.Request(url, data=sql.encode("utf-8"),
                                       headers=headers, method="POST"))
    columns: list[str] = []
    rows: list[list] = []
    deadline = time.time() + timeout
    while True:
        if doc.get("error"):
            err = doc["error"]
            raise TrinoError(f"{err.get('errorName', 'ERROR')}: {err.get('message')}")
        if not columns and doc.get("columns"):
            columns = [c["name"] for c in doc["columns"]]
        rows.extend(doc.get("data") or [])
        nxt = doc.get("nextUri")
        if not nxt:
            return columns, rows
        if time.time() > deadline:
            raise TrinoError(f"query still running after {timeout:.0f}s")
        doc = _send(urllib.request.Request(nxt, headers=headers))


def query(sql: str, timeout: float = 600.0) -> list[list]:
    return query_with_columns(sql, timeout)[1]


def scalar(sql: str, timeout: float = 600.0):
    rows = query(sql, timeout)
    return rows[0][0] if rows and rows[0] else None
