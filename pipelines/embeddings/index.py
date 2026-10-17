"""Build (refresh) an embedding index, and report on the one that exists.

A refresh is one full rebuild, published atomically (ADR 0006, M4-C2):

  1. guard     the index's text columns are checked against the tag store (N3, M4-B3)
  2. read      one Trino statement against the source dataset, pinned to the Iceberg
               snapshot that was current when the refresh began, returning the text,
               the snapshot columns, and the dataset's watermark max(row timestamp)
  3. embed     the model baked into the image, at a pinned revision
  4. publish   a new table is filled and indexed beside the live one, then swapped in
               with DROP + RENAME inside one transaction. Readers see the old index
               until the commit and the new one after it -- never a half-built one.

Every row of one refresh carries the same refreshed_at: the *watermark of the data
read*, not the wall clock of the job (M4-A3). A refresh that reads a Gold table last
built an hour ago produces an index that is honestly an hour old the moment it lands;
stamping the job time instead would hide exactly the staleness N1 exists to expose.
The job's wall clock is kept separately as embedded_at, for diagnosis.

Only rows matching the spec's `eligible` predicate are indexed, and the spec's
`snapshot` columns are copied beside each vector (M4-A1). The index is therefore a
picture of which entities qualified, and on what terms, at refreshed_at -- which is
what lets it be confidently wrong about them later.

This module never names a domain: tables, columns and the predicate all come from
the spec as opaque strings.
"""
from __future__ import annotations

import datetime as dt
import time

from mosaic_platform.governance import guard
from mosaic_platform.runtime import db
from mosaic_platform.spec.retrieval_spec import EmbeddingIndex

from . import ledger
from .model import Encoder, revision, to_pgvector

E = EmbeddingIndex


def _current_snapshot(relation: str) -> tuple[int, str] | None:
    """The Iceberg snapshot a read should pin to, or None for a non-Iceberg source."""
    catalog, schema, table = relation.split(".")
    if catalog != "iceberg":
        return None
    _, rows = db.trino(f'SELECT snapshot_id, CAST(committed_at AS varchar) '
                       f'FROM {catalog}.{schema}."{table}$snapshots" '
                       f'ORDER BY committed_at DESC LIMIT 1')
    return (int(rows[0][0]), rows[0][1]) if rows else None


def read_source(ix: EmbeddingIndex) -> dict:
    src = ix.source
    if src.engine != "trino":
        raise RuntimeError(f"index source must be a Trino dataset, got '{src.id}'")
    ts = src.row_timestamp_column
    if not ts:
        raise RuntimeError(f"source '{src.id}' declares no row_timestamp_column; an index "
                           f"cannot be stamped with a watermark without one")
    snap = _current_snapshot(src.relation)
    pinned = f" FOR VERSION AS OF {snap[0]}" if snap else ""
    cols = [ix.key, *ix.text, *ix.snapshot]
    inner = ", ".join(cols)
    sql = (f"SELECT {inner}, _wm, _n_all FROM ("
           f"SELECT {inner}, max({ts}) OVER () AS _wm, count(*) OVER () AS _n_all, "
           f"({ix.eligible or 'TRUE'}) AS _eligible FROM {src.relation}{pinned}"
           f") WHERE _eligible")
    columns, rows = db.trino(sql)
    if not rows:
        raise RuntimeError(f"no eligible rows in '{src.id}'"
                           + (f" under `{ix.eligible}`" if ix.eligible else "")
                           + "; refusing to publish an empty index")
    types = dict(columns)
    n = len(cols)
    return {
        "columns": cols,
        "types": {c: types[c] for c in cols},
        "rows": [r[:n] for r in rows],
        "watermark": db.parse_ts(rows[0][n]),
        "rows_source": int(rows[0][n + 1]),
        "snapshot_id": snap[0] if snap else None,
        "snapshot_committed_at": snap[1] if snap else None,
    }


def _text(row: list, ix: EmbeddingIndex) -> str:
    parts = row[1:1 + len(ix.text)]
    return ". ".join(str(p).strip() for p in parts if p is not None and str(p).strip())


def _previous(conn, ix: EmbeddingIndex) -> dt.datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (ix.target.table,))
        if cur.fetchone()[0] is None:
            return None
        cur.execute(f"SELECT max({E.REFRESHED_AT}) FROM {ix.target.table}")
        prev = cur.fetchone()[0]
        # psycopg returns the session's zone (+07 here); report in UTC like everything else.
        return prev.astimezone(dt.timezone.utc) if prev else None


def publish(conn, ix: EmbeddingIndex, data: dict, vectors, embedded_at: dt.datetime) -> dict:
    schema, table = ix.target.table.split(".")
    build = f"{table}__build"
    key_type = db.pg_type(data["types"][ix.key])
    snap_defs = [f"{c} {db.pg_type(data['types'][c])}" for c in ix.snapshot]
    copy_cols = [ix.key, E.EMBEDDING, *ix.snapshot, E.REFRESHED_AT, E.EMBEDDED_AT]
    snap_pos = [data["columns"].index(c) for c in ix.snapshot]
    timings = {}
    with conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = '30s'")
        cur.execute("SET LOCAL maintenance_work_mem = '512MB'")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{build}")
        cur.execute(
            f"CREATE TABLE {schema}.{build} ("
            f"{ix.key} {key_type} PRIMARY KEY, "
            f"{E.EMBEDDING} vector({ix.dimensions}) NOT NULL"
            + "".join(f", {d}" for d in snap_defs)
            + f", {E.REFRESHED_AT} TIMESTAMPTZ NOT NULL, {E.EMBEDDED_AT} TIMESTAMPTZ NOT NULL)")
        t0 = time.time()
        with cur.copy(f"COPY {schema}.{build} ({', '.join(copy_cols)}) FROM STDIN") as cp:
            for row, vec in zip(data["rows"], vectors):
                cp.write_row([row[0], to_pgvector(vec), *[row[i] for i in snap_pos],
                              data["watermark"], embedded_at])
        timings["copy"] = time.time() - t0
        t0 = time.time()
        cur.execute(f"CREATE INDEX {build}_hnsw ON {schema}.{build} USING hnsw "
                    f"({E.EMBEDDING} vector_cosine_ops) "
                    f"WITH (m = {ix.m}, ef_construction = {ix.ef_construction})")
        timings["hnsw_build"] = time.time() - t0
        cur.execute(f"ANALYZE {schema}.{build}")
        # The swap. DDL is transactional in PostgreSQL, so these four statements and
        # everything above become visible together at COMMIT, or not at all.
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        cur.execute(f"ALTER TABLE {schema}.{build} RENAME TO {table}")
        cur.execute(f"ALTER INDEX {schema}.{build}_hnsw RENAME TO {table}_hnsw")
        cur.execute(f"ALTER INDEX {schema}.{build}_pkey RENAME TO {table}_pkey")
    t0 = time.time()
    conn.commit()
    timings["commit"] = time.time() - t0
    return timings


def refresh(spec, ix: EmbeddingIndex, guard_only: bool = False) -> int:
    t_start = time.time()
    event = {"event": "refresh", "domain": spec.domain, "index": ix.id, "model": ix.model,
             "model_revision": revision(ix.model), "source": ix.source.id,
             "target": ix.target.id}

    with db.pg(db.PLATFORM_DB) as pconn:
        g = guard.check_embedding_inputs(pconn, ix.source.id, ix.text)
    print(f"  guard     {g.describe()}")
    event["guard"] = {"ok": g.ok, "checked": list(g.checked_columns),
                      "blocked": [list(b) for b in g.blocked], "tags_known": g.tags_known}
    if not g.ok:
        event["outcome"] = "refused_by_guard"
        ledger.append(event)
        return 2
    if guard_only:
        return 0

    t0 = time.time()
    data = read_source(ix)
    t_read = time.time() - t0
    print(f"  read      {len(data['rows'])} eligible of {data['rows_source']} row(s) in "
          f"{ix.source.id}" + (f" @ snapshot {data['snapshot_id']}" if data["snapshot_id"] else "")
          + f"  ({t_read:.1f}s)")
    print(f"            watermark max({ix.source.row_timestamp_column}) = {data['watermark']}")

    t0 = time.time()
    enc = Encoder(ix.model, ix.dimensions)
    t_load = time.time() - t0
    t0 = time.time()
    texts = [_text(r, ix) for r in data["rows"]]
    vectors = enc.encode(texts)
    t_embed = time.time() - t0
    print(f"  embed     {len(texts)} text(s) with {ix.model} @ {enc.revision[:12]}  "
          f"({t_embed:.1f}s, {len(texts) / max(t_embed, 1e-9):.0f}/s; model load {t_load:.1f}s)")

    embedded_at = dt.datetime.now(dt.timezone.utc)
    with db.pg(ix.target.database) as conn:
        previous = _previous(conn, ix)
        conn.rollback()   # _previous ran in a transaction; publish starts its own
        timings = publish(conn, ix, data, vectors, embedded_at)
    print(f"  publish   {ix.target.id}: copy {timings['copy']:.1f}s, hnsw build "
          f"{timings['hnsw_build']:.1f}s (m={ix.m}, ef_construction={ix.ef_construction}), "
          f"swap committed")

    age = (embedded_at - data["watermark"]).total_seconds()
    print(f"  index     refreshed_at = {data['watermark']} -- already {age:.0f}s old on "
          f"landing (SLA {ix.freshness_sla_seconds}s)"
          + (f"; replaces an index stamped {previous}" if previous else "; first build"))

    event.update({
        "outcome": "ok",
        "source_snapshot_id": data["snapshot_id"],
        "source_snapshot_committed_at": data["snapshot_committed_at"],
        "rows_source": data["rows_source"], "rows_eligible": len(data["rows"]),
        "rows_embedded": len(texts), "eligible": ix.eligible,
        "refreshed_at": data["watermark"].isoformat(),
        "embedded_at": embedded_at.isoformat(),
        "age_on_landing_s": round(age, 3),
        "previous_refreshed_at": previous.isoformat() if previous else None,
        "dimensions": ix.dimensions, "hnsw": {"m": ix.m, "ef_construction": ix.ef_construction},
        "seconds": {"read": round(t_read, 3), "model_load": round(t_load, 3),
                    "embed": round(t_embed, 3),
                    **{k: round(v, 3) for k, v in timings.items()},
                    "total": round(time.time() - t_start, 3)},
    })
    ledger.append(event)
    return 0


def status(spec, ix: EmbeddingIndex) -> int:
    with db.pg(ix.target.database) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (ix.target.table,))
        if cur.fetchone()[0] is None:
            print(f"  {ix.target.id}: not built -- python mosaic.py embed refresh")
            return 1
        cur.execute(f"SELECT count(*), min({E.REFRESHED_AT}), max({E.REFRESHED_AT}), "
                    f"max({E.EMBEDDED_AT}), now() FROM {ix.target.table}")
        n, rmin, rmax, emb, now = cur.fetchone()
        cur.execute("SELECT indexdef FROM pg_indexes WHERE schemaname || '.' || tablename = %s "
                    "AND indexdef ILIKE '%%hnsw%%'", (ix.target.table,))
        idx = cur.fetchone()
    age = (now - rmax).total_seconds() if rmax else None
    print(f"  index        {ix.id} -> {ix.target.id}")
    print(f"  rows         {n}")
    print(f"  refreshed_at {rmax}" + ("" if rmin == rmax else f"  (min {rmin}: NOT uniform)"))
    print(f"  embedded_at  {emb}")
    print(f"  age          {age:.0f}s against an SLA of {ix.freshness_sla_seconds}s"
          f" -> s_emb = {age / ix.freshness_sla_seconds:.2f}  (GR1 Eq. 5.1)" if age is not None
          else "  age          -")
    print(f"  hnsw         {idx[0] if idx else 'MISSING'}")
    return 0 if idx and rmin == rmax else 1
