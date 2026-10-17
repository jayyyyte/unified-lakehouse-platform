"""Database access for platform jobs running inside the stack (the `jobs` image).

Two engines, reached the way each is cheapest:

- Trino over its REST protocol (stdlib only): POST the statement, follow `nextUri`.
  Column *types* are returned alongside the rows, which is what lets the index writer
  create its table without being told the domain's schema.
- PostgreSQL through psycopg 3, for the vector store and the platform metadata store.
  Trino's PostgreSQL connector cannot read a pgvector column, so the index is never
  read through Trino.

Connection details come from the environment the compose service sets; defaults match
docker/compose.yaml.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request

TRINO_URL = (f"http://{os.environ.get('MOSAIC_TRINO_HOST', 'trino')}:"
             f"{os.environ.get('MOSAIC_TRINO_PORT', '8080')}/v1/statement")
TRINO_USER = os.environ.get("MOSAIC_TRINO_USER", "mosaic-jobs")

PG_HOST = os.environ.get("MOSAIC_PG_HOST", "postgres")
PG_PORT = int(os.environ.get("MOSAIC_PG_PORT", "5432"))
PG_USER = os.environ.get("MOSAIC_PG_USER", "cdc")
PG_PASSWORD = os.environ.get("MOSAIC_PG_PASSWORD", "cdcpw")
PLATFORM_DB = os.environ.get("MOSAIC_PLATFORM_DB", "mosaic_platform")


class TrinoError(RuntimeError):
    pass


def _send(req: urllib.request.Request) -> dict:
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise TrinoError(f"HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:300]}") from e


def trino(sql: str, timeout: float = 900.0) -> tuple[list[tuple[str, str]], list[list]]:
    """Run one statement; return ([(column, trino_type)], rows)."""
    headers = {"X-Trino-User": TRINO_USER}
    doc = _send(urllib.request.Request(TRINO_URL, data=sql.encode("utf-8"),
                                       headers=headers, method="POST"))
    columns: list[tuple[str, str]] = []
    rows: list[list] = []
    deadline = time.time() + timeout
    while True:
        if doc.get("error"):
            err = doc["error"]
            raise TrinoError(f"{err.get('errorName', 'ERROR')}: {err.get('message')}")
        if not columns and doc.get("columns"):
            columns = [(c["name"], c["type"]) for c in doc["columns"]]
        rows.extend(doc.get("data") or [])
        nxt = doc.get("nextUri")
        if not nxt:
            return columns, rows
        if time.time() > deadline:
            raise TrinoError(f"statement still running after {timeout:.0f}s")
        doc = _send(urllib.request.Request(nxt, headers=headers))


def pg(database: str, autocommit: bool = False):
    """A psycopg connection to a database in the PostgreSQL container."""
    import psycopg
    return psycopg.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD,
                           dbname=database, autocommit=autocommit)


def parse_ts(value) -> dt.datetime | None:
    """A Trino timestamp value as an aware UTC datetime.

    Trino renders `timestamp with time zone` as '2026-10-05 04:20:01.123456 UTC' and a
    plain `timestamp` with no zone at all. Every row timestamp registered today is
    `timestamp(6) with time zone` (Spark's TIMESTAMP lands in Iceberg as timestamptz),
    so a zoneless value would have to come from a future source; it is read as UTC, the
    zone every writer in this stack uses. psycopg returns datetimes; those pass through.
    """
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
    parsed = dt.datetime.fromisoformat(str(value).replace(" UTC", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


# Trino type -> PostgreSQL column type, for columns the index copies from its source.
_SIMPLE = {
    "bigint": "BIGINT", "integer": "INTEGER", "smallint": "SMALLINT", "tinyint": "SMALLINT",
    "double": "DOUBLE PRECISION", "real": "REAL", "boolean": "BOOLEAN", "date": "DATE",
}


def pg_type(trino_type: str) -> str:
    t = trino_type.lower()
    if t in _SIMPLE:
        return _SIMPLE[t]
    if t.startswith("decimal"):
        return "NUMERIC" + t[len("decimal"):]
    if t.startswith("varchar") or t.startswith("char"):
        return "TEXT"
    if t.startswith("timestamp") and t.endswith("with time zone"):
        return "TIMESTAMPTZ"
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    raise ValueError(f"no PostgreSQL mapping for Trino type '{trino_type}'")
