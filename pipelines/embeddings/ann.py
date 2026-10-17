"""Read side of an index: one ANN query, and an HNSW-vs-exact recall check.

`query` is the slow channel as N2 will see it: the top-k by cosine similarity, each hit
returned with what the index believed about it (the snapshot columns) and when
(refreshed_at). It deliberately does not consult any live source -- that is the point
of the channel being stale.

`recall` answers "is the HNSW index actually approximating the exact answer?". Query
vectors are drawn from the index itself with a seeded hash, so the check needs no
domain text. Each path's plan is EXPLAINed first: an "HNSW" path that silently fell
back to a sequential scan would report a perfect, meaningless recall of 1.0 -- which is
exactly what the first unpinned run did (see _set_path).
"""
from __future__ import annotations

import json
import statistics
import time

from mosaic_platform.runtime import db
from mosaic_platform.spec.retrieval_spec import EmbeddingIndex

from . import ledger
from .model import Encoder, to_pgvector

E = EmbeddingIndex
RESULT_MARKER = "@@RESULT "   # machine-readable line for host-side tools


def _search_sql(ix: EmbeddingIndex, select_snapshot: bool = True) -> str:
    extra = "".join(f", {c}" for c in ix.snapshot) if select_snapshot else ""
    return (f"SELECT {ix.key}, 1 - ({E.EMBEDDING} <=> %(q)s::vector) AS similarity{extra}, "
            f"{E.REFRESHED_AT} FROM {ix.target.table} "
            f"ORDER BY {E.EMBEDDING} <=> %(q)s::vector LIMIT %(k)s")


def _set_path(cur, exact: bool, ef_search: int) -> None:
    """Pin the access path; never leave it to the cost model.

    Measured on the first M4 build (946 rows): the planner costs a sequential scan + sort
    at 230 and the HNSW scan at 417, so an unpinned "ANN" query is silently *exact*.
    The channel is approximate by definition (GR1 section 3.4), and E3/E4 compare the two
    paths, so each is forced: HNSW = sequential scans off; exact = index scans off.
    """
    cur.execute(f"SET LOCAL hnsw.ef_search = {int(ef_search)}")
    cur.execute(f"SET LOCAL enable_indexscan = {'off' if exact else 'on'}")
    cur.execute(f"SET LOCAL enable_seqscan = {'on' if exact else 'off'}")


def _uses_hnsw(cur, ix: EmbeddingIndex, qvec: str, k: int) -> bool:
    cur.execute("EXPLAIN " + _search_sql(ix, select_snapshot=False), {"q": qvec, "k": k})
    plan = "\n".join(r[0] for r in cur.fetchall())
    return "_hnsw" in plan


def query(spec, ix: EmbeddingIndex, text: str, k: int, ef_search: int, exact: bool,
          as_json: bool) -> int:
    enc = Encoder(ix.model, ix.dimensions)
    qvec = to_pgvector(enc.encode([text])[0])
    with db.pg(ix.target.database) as conn, conn.cursor() as cur:
        _set_path(cur, exact, ef_search)
        used_index = _uses_hnsw(cur, ix, qvec, k)
        t0 = time.perf_counter()
        cur.execute(_search_sql(ix), {"q": qvec, "k": k})
        rows = cur.fetchall()
        ms = (time.perf_counter() - t0) * 1000
        cur.execute("SELECT now()")
        now = cur.fetchone()[0]
    hits = []
    for rank, r in enumerate(rows, 1):
        snap = dict(zip(ix.snapshot, r[2:2 + len(ix.snapshot)]))
        refreshed = r[-1]
        hits.append({"rank": rank, "key": r[0], "similarity": round(float(r[1]), 4),
                     "snapshot": {k_: (float(v) if hasattr(v, "as_tuple") else v)
                                  for k_, v in snap.items()},
                     "refreshed_at": refreshed.isoformat(),
                     "age_s": round((now - refreshed).total_seconds(), 1)})
    path = "exact" if exact else f"hnsw ef_search={ef_search}"
    print(f"  {ix.id}: top-{k} for {text!r}  [{path}; plan uses hnsw index: {used_index}; "
          f"{ms:.1f} ms]")
    for h in hits:
        snap = " ".join(f"{c}={v}" for c, v in h["snapshot"].items())
        print(f"   {h['rank']:>3}  {ix.key}={h['key']:<8} sim={h['similarity']:.4f}  {snap}  "
              f"(as of {h['refreshed_at'][:19]}, {h['age_s']:.0f}s ago)")
    if as_json:
        print(RESULT_MARKER + json.dumps({"index": ix.id, "key": ix.key, "text": text, "k": k,
                                          "path": path, "uses_hnsw": used_index,
                                          "latency_ms": round(ms, 3), "hits": hits}))
    return 0


def _pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


def recall(spec, ix: EmbeddingIndex, n_queries: int, k: int, ef_values: list[int],
           seed: str) -> int:
    with db.pg(ix.target.database) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {ix.target.table}")
        size = cur.fetchone()[0]
        cur.execute(f"SELECT {E.EMBEDDING}::text FROM {ix.target.table} "
                    f"ORDER BY md5({ix.key}::text || %s) LIMIT %s", (seed, n_queries))
        queries = [r[0] for r in cur.fetchall()]
        conn.commit()

        def run(exact: bool, ef: int):
            results, lat = [], []
            with conn.transaction(), conn.cursor() as c:
                _set_path(c, exact, ef)
                plan_ok = _uses_hnsw(c, ix, queries[0], k) != exact
                for q in queries:
                    t0 = time.perf_counter()
                    c.execute(_search_sql(ix, select_snapshot=False), {"q": q, "k": k})
                    results.append([r[0] for r in c.fetchall()])
                    lat.append((time.perf_counter() - t0) * 1000)
            return results, lat, plan_ok

        truth, exact_lat, exact_plan = run(True, 40)
        print(f"  {ix.id}: {len(queries)} query vector(s) sampled from the index "
              f"({size} rows), k={k}")
        print(f"  exact          p50 {_pct(exact_lat, .5):6.2f} ms  p95 "
              f"{_pct(exact_lat, .95):6.2f} ms   plan is a sequential scan: {exact_plan}")
        sweep, ok = [], exact_plan
        for ef in ef_values:
            got, lat, plan_ok = run(False, ef)
            per_q = [len(set(g) & set(t)) / len(t) for g, t in zip(got, truth)]
            mean_r, min_r = statistics.fmean(per_q), min(per_q)
            ok &= plan_ok
            print(f"  hnsw ef={ef:<5} p50 {_pct(lat, .5):6.2f} ms  p95 {_pct(lat, .95):6.2f} ms   "
                  f"recall@{k} mean {mean_r:.4f} min {min_r:.2f}   plan uses hnsw: {plan_ok}")
            sweep.append({"ef_search": ef, "recall_mean": round(mean_r, 5),
                          "recall_min": round(min_r, 4), "p50_ms": round(_pct(lat, .5), 3),
                          "p95_ms": round(_pct(lat, .95), 3), "plan_uses_hnsw": plan_ok})
    if size <= 10_000:
        print(f"  note: at {size} rows an exact scan is already cheap and HNSW recall is near "
              f"1.0 by construction; the recall/latency trade-off is E3's, at `full` scale.")
    ledger.append({"event": "recall", "domain": spec.domain, "index": ix.id, "rows": size,
                   "queries": len(queries), "k": k, "seed": seed,
                   "hnsw": {"m": ix.m, "ef_construction": ix.ef_construction},
                   "exact": {"p50_ms": round(_pct(exact_lat, .5), 3),
                             "p95_ms": round(_pct(exact_lat, .95), 3),
                             "plan_is_seq_scan": exact_plan},
                   "sweep": sweep, "plans_ok": ok})
    if not ok:
        print("  FAIL: a path did not use the plan it claims to measure")
        return 1
    return 0
