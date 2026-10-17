"""M4 acceptance checks for the slow channel's index (ADR 0006, decision M4-D3).

    python mosaic.py embed-verify [--with-refresh]

Six checks, each able to fail:

  1. counts      index rows == the refresh's eligible rows, re-counted in Trino against
                 the very Iceberg snapshot the refresh pinned; refreshed_at uniform and
                 equal to max(_src_ts) of that snapshot (M4-A3, M4-C2)
  2. semantics   free-text queries land in the right top-level category (the corpus is
                 compositional, so a broken model or text join shows up here)
  3. recall      HNSW vs exact recall@10, with each path's plan checked
  4. guard       negative control: tag a text column `sensitive`, the refresh must refuse
                 and leave the published index untouched; untag, it must pass (M4-B3)
  5. no drift    before any lever is pulled, the index and the live source agree
  6. L0          stock out the best live hit of a query; the index still returns it, as
                 in stock, while PostgreSQL says 0. The stale hit from docs/PROBLEM.md,
                 reproduced end to end for the first time. Then restored.

--with-refresh adds the other half: land the change through Bronze, rebuild Silver/Gold,
refresh the index, and show the stale hit gone -- i.e. the staleness was the refresh
cadence and nothing else.

Domain-aware on purpose: like tools/loadgen, this is a fixture for the ecommerce domain,
and it lives in tools/, outside the abstraction gate.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from mosaic import _compose_cmd, compose, docker_env  # noqa: E402
from tools.dbsession import PostgresSession  # noqa: E402
from tools.trino import query as trino, scalar as trino_scalar  # noqa: E402

LEDGER = ROOT / "experiments" / "results" / "embedding_events.jsonl"
GOLD = "iceberg.gold.product_catalog"
MARKER = "@@RESULT "

# (query, expected top-level category). The corpus generates name, description and
# leaf category together from one vocabulary neighbourhood (CHANGES-FROM-GR1 C9).
SEMANTIC_PROBES = [
    ("waterproof hiking backpack", "Outdoor & Hiking"),
    ("wireless noise cancelling headphones for commuting", "Audio & Headphones"),
    ("cast iron skillet for searing", "Kitchen & Dining"),
]
SEMANTIC_MIN = 7          # of 10
RECALL_MIN = 0.95         # mean recall@10 at the default ef_search
L0_QUERY = SEMANTIC_PROBES[0][0]


class Check:
    def __init__(self):
        self.results: list[dict] = []

    def record(self, name: str, ok: bool, detail: str, **data) -> bool:
        self.results.append({"check": name, "ok": ok, "detail": detail, **data})
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<10} {detail}")
        return ok


# ------------------------------------------------------------------ plumbing
def jobs(*args: str) -> tuple[int, str]:
    """Run a job in the jobs image, capturing its output."""
    cmd = _compose_cmd(["jobs"]) + ["run", "--rm", "-T", "jobs", *args]
    r = subprocess.run(cmd, capture_output=True, text=True, env=docker_env())
    return r.returncode, r.stdout + r.stderr


def ann(text: str, k: int = 10) -> dict:
    rc, out = jobs("embeddings", "query", "--domain", "ecommerce", "--text", text,
                   "--k", str(k), "--json")
    for line in out.splitlines():
        if line.startswith(MARKER):
            return json.loads(line[len(MARKER):])
    raise RuntimeError(f"query produced no result (rc={rc}):\n{out[-1500:]}")


def last_event(kind: str) -> dict | None:
    if not LEDGER.exists():
        return None
    rows = [json.loads(l) for l in LEDGER.read_text(encoding="utf-8").splitlines() if l.strip()]
    rows = [r for r in rows if r.get("event") == kind and r.get("outcome", "ok") == "ok"]
    return rows[-1] if rows else None


def live_stock(pg: PostgresSession, ids) -> dict[int, int]:
    ids = list(ids)
    if not ids:
        return {}
    rows = pg.query_rows(f"SELECT product_id, stock_qty FROM inventory "
                         f"WHERE product_id IN ({','.join(map(str, ids))})")
    return {int(r[0]): int(r[1]) for r in rows}


def roots(pg: PostgresSession, ids) -> dict[int, str]:
    rows = pg.query_rows(
        "SELECT p.id, COALESCE(r.name, c.name) FROM products p "
        "JOIN categories c ON c.id = p.category_id "
        "LEFT JOIN categories r ON r.id = c.parent_id "
        f"WHERE p.id IN ({','.join(map(str, ids))})")
    return {int(r[0]): r[1] for r in rows}


def stale_hits(result: dict, live: dict[int, int]) -> list[int]:
    """Hits the index presents as in stock that the live source says are not."""
    return [h["key"] for h in result["hits"]
            if (h["snapshot"].get("stock_qty") or 0) > 0 and live.get(h["key"], 0) == 0]


def index_state(pg: PostgresSession) -> tuple[int, float, float, float]:
    """(rows, min/max refreshed_at, max embedded_at), timestamps as epoch seconds so no
    session time zone gets between the comparison and the truth."""
    r = pg.query_rows("SELECT count(*), extract(epoch FROM min(refreshed_at)), "
                      "extract(epoch FROM max(refreshed_at)), extract(epoch FROM max(embedded_at)) "
                      "FROM product_embeddings")[0]
    return int(r[0]), float(r[1]), float(r[2]), float(r[3])


# -------------------------------------------------------------------- checks
def check_counts(c: Check, pg: PostgresSession) -> bool:
    ev = last_event("refresh")
    if ev is None:
        return c.record("counts", False, "no successful refresh in the ledger -- "
                        "python mosaic.py embed refresh")
    n, rmin, rmax, _ = index_state(pg)
    snap = ev["source_snapshot_id"]
    eligible = trino_scalar(f"SELECT count(*) FROM {GOLD} FOR VERSION AS OF {snap} "
                            f"WHERE {ev['eligible']}")
    total = trino_scalar(f"SELECT count(*) FROM {GOLD} FOR VERSION AS OF {snap}")
    wm = trino_scalar(f"SELECT CAST(max(_src_ts) AS varchar) FROM {GOLD} FOR VERSION AS OF {snap}")
    wm_dt = dt.datetime.fromisoformat(wm.replace(" UTC", "+00:00"))
    stamp = dt.datetime.fromisoformat(ev["refreshed_at"])
    ok = (n == ev["rows_embedded"] == eligible and rmin == rmax
          and wm_dt == stamp and abs(rmax - stamp.timestamp()) < 1e-3)
    return c.record("counts", ok,
                    f"index {n} rows = ledger {ev['rows_embedded']} = Gold@{snap} eligible "
                    f"{eligible} (of {total}); refreshed_at uniform={rmin == rmax}, "
                    f"= max(_src_ts)@snapshot {wm_dt == stamp}",
                    index_rows=n, eligible=eligible, total=total, snapshot=snap,
                    refreshed_at=ev["refreshed_at"], gold_watermark=wm)


def check_semantics(c: Check, pg: PostgresSession) -> bool:
    ok_all, details = True, []
    for text, expected in SEMANTIC_PROBES:
        res = ann(text)
        ids = [h["key"] for h in res["hits"]]
        r = roots(pg, ids)
        hit = sum(1 for i in ids if r.get(i) == expected)
        ok_all &= hit >= SEMANTIC_MIN and res["uses_hnsw"]
        details.append({"query": text, "expected": expected, "in_expected": hit,
                        "roots": [r.get(i) for i in ids], "uses_hnsw": res["uses_hnsw"]})
        print(f"            {text!r}: {hit}/10 in {expected!r}  (hnsw plan: {res['uses_hnsw']})")
    return c.record("semantics", ok_all,
                    f"each probe needs >= {SEMANTIC_MIN}/10 in its expected category",
                    probes=details)


def check_recall(c: Check) -> bool:
    rc, out = jobs("embeddings", "recall", "--domain", "ecommerce", "--queries", "100",
                   "--k", "10", "--ef", "10", "--ef", "40", "--ef", "100")
    print("\n".join("            " + l.strip() for l in out.splitlines()
                    if l.strip().startswith(("exact", "hnsw", "note"))))
    ev = last_event("recall")
    at40 = next((s for s in (ev or {}).get("sweep", []) if s["ef_search"] == 40), None)
    ok = rc == 0 and ev is not None and ev["plans_ok"] and at40 and at40["recall_mean"] >= RECALL_MIN
    return c.record("recall", bool(ok),
                    f"mean recall@10 at ef_search=40: {at40['recall_mean'] if at40 else '?'} "
                    f"(need >= {RECALL_MIN}); plans checked: {ev and ev['plans_ok']}",
                    sweep=(ev or {}).get("sweep"))


def check_guard(c: Check, pg: PostgresSession) -> bool:
    before = index_state(pg)
    plat = PostgresSession(database="mosaic_platform")
    tag = (f"INSERT INTO platform.column_tags (dataset_id, column_name, tag, origin) "
           f"VALUES ('{GOLD}', 'description', 'sensitive', 'declared') ON CONFLICT DO NOTHING")
    untag = (f"DELETE FROM platform.column_tags WHERE dataset_id = '{GOLD}' "
             f"AND column_name = 'description' AND tag = 'sensitive'")
    try:
        plat.query(tag)
        rc_tagged, out = jobs("embeddings", "refresh", "--domain", "ecommerce")
        refused = rc_tagged == 2 and "REFUSED" in out
    finally:
        plat.query(untag)
    after = index_state(pg)
    rc_clean, _ = jobs("embeddings", "refresh", "--domain", "ecommerce", "--guard-only")
    ok = refused and before == after and rc_clean == 0
    return c.record("guard", ok,
                    f"tagged description -> refresh exit {rc_tagged}"
                    f"{' REFUSED' if refused else ''}, index untouched: {before == after}; "
                    f"untagged -> guard exit {rc_clean}")


def wait_bronze(since_iso: str, timeout: float = 120.0) -> float:
    t0 = time.time()
    while time.time() - t0 < timeout:
        n = trino_scalar(
            "SELECT count(*) FROM iceberg.bronze.cdc_events "
            "WHERE topic = 'mosaic.postgres.public.inventory' "
            f"AND _src_ts >= from_iso8601_timestamp('{since_iso}')")
        if n:
            return time.time() - t0
        time.sleep(3)
    raise RuntimeError(f"the stock-out did not reach Bronze within {timeout:.0f}s")


def check_l0(c: Check, pg: PostgresSession, with_refresh: bool) -> bool:
    res = ann(L0_QUERY)
    live = live_stock(pg, [h["key"] for h in res["hits"]])
    drift = stale_hits(res, live)
    c.record("no drift", not drift,
             f"before the lever: {len(drift)} of top-10 contradicted by the live source",
             stale=drift)
    target = next((h for h in res["hits"] if live.get(h["key"], 0) > 0
                   and (h["snapshot"].get("stock_qty") or 0) > 0), None)
    if target is None:
        return c.record("L0", False, "no in-stock hit to stock out")
    pid, rank, prev_qty = target["key"], target["rank"], live[target["key"]]

    since = pg.scalar("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
    rc = subprocess.call([sys.executable, "-m", "tools.loadgen.run", "stockout",
                          "--product-id", str(pid)], cwd=str(ROOT), stdout=subprocess.DEVNULL)
    try:
        after_live = live_stock(pg, [pid]).get(pid)
        res2 = ann(L0_QUERY)
        still = next((h for h in res2["hits"] if h["key"] == pid), None)
        live2 = live_stock(pg, [h["key"] for h in res2["hits"]])
        stale = stale_hits(res2, live2)
        ok = (rc == 0 and after_live == 0 and still is not None
              and (still["snapshot"].get("stock_qty") or 0) > 0 and pid in stale)
        c.record("L0", ok,
                 f"stocked out #{rank} product {pid} (live {prev_qty} -> {after_live}); index "
                 f"still returns it at #{still['rank'] if still else '-'} with stock_qty="
                 f"{still['snapshot'].get('stock_qty') if still else '-'} as of "
                 f"{still['refreshed_at'][:19] if still else '-'}; stale-hit rate@10 = "
                 f"{len(stale)}/10",
                 product_id=pid, rank=rank, previous_qty=prev_qty,
                 snapshot=still["snapshot"] if still else None,
                 age_s=still["age_s"] if still else None, stale=stale)
        if not with_refresh:
            return ok

        print(f"            ...landing the stock-out through Bronze (since {since})")
        waited = wait_bronze(since)
        print(f"            in Bronze after {waited:.0f}s; rebuilding Silver -> Gold catalog")
        if compose("run", "--rm", "dbt", "build", "-s", "+product_catalog",
                   profiles=["transform"]) != 0:
            return c.record("L0 fixed", False, "dbt build failed")
        rc_r, out = jobs("embeddings", "refresh", "--domain", "ecommerce")
        print("\n".join("            " + l.strip() for l in out.splitlines()
                        if l.strip().startswith(("read", "index", "publish"))))
        res3 = ann(L0_QUERY)
        live3 = live_stock(pg, [h["key"] for h in res3["hits"]])
        gone = all(h["key"] != pid for h in res3["hits"])
        stale3 = stale_hits(res3, live3)
        return c.record("L0 fixed", rc_r == 0 and gone and not stale3,
                        f"after Gold rebuild + refresh: product {pid} in top-10: {not gone}; "
                        f"stale-hit rate@10 = {len(stale3)}/10",
                        stale=stale3) and ok
    finally:
        subprocess.call([sys.executable, "-m", "tools.loadgen.run", "restock",
                         "--product-id", str(pid), "--qty", str(prev_qty)],
                        cwd=str(ROOT), stdout=subprocess.DEVNULL)
        print(f"            restored product {pid} to stock {prev_qty}"
              + (" (the index now under-states it until the next refresh -- the same "
                 "staleness, the other way round)" if with_refresh else ""))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="embed-verify", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domain", default="ecommerce", choices=["ecommerce"])
    ap.add_argument("--with-refresh", action="store_true")
    a = ap.parse_args(argv)

    c = Check()
    pg = PostgresSession()
    started = time.time()
    try:
        check_counts(c, pg)
        check_semantics(c, pg)
        check_recall(c)
        check_guard(c, pg)
        check_l0(c, pg, a.with_refresh)
    except Exception as e:   # noqa: BLE001 -- a crash is a failed run, and is ledgered as one
        c.record("crash", False, f"{type(e).__name__}: {str(e).splitlines()[0][:200]}")
    passed = all(r["ok"] for r in c.results)
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                            "event": "verify", "domain": a.domain,
                            "with_refresh": a.with_refresh, "passed": passed,
                            "seconds": round(time.time() - started, 1),
                            "checks": c.results}, default=str) + "\n")
    print(f"\n  {'ALL CHECKS PASSED' if passed else 'FAILED'}   ledger: "
          f"{LEDGER.relative_to(ROOT).as_posix()}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
