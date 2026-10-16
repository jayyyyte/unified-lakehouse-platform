"""Bronze, measured: exactly-once landing across both replay sources, and end-to-end latency.

    python -m tools.bronze.verify exactly-once [--domain ecommerce]
    python -m tools.bronze.verify replay       [--domain ecommerce] [--rate 20]
    python -m tools.bronze.verify latency      [--domain ecommerce] [--rate 20] [--duration 120]

GR1 section 4.2 asks Bronze to "apply deduplication using the source offset as an
idempotency key". M2 showed why that is not decoration: Kafka Connect is at-least-once
with a replay window (CHANGES-FROM-GR1 A8). Bronze sees duplicates from two independent
sources, and each sub-command here proves one of them is absorbed:

`exactly-once`  Connect-side replay. Run `python mosaic.py restart-safety` first, with the
                Bronze stream up; it SIGKILLs the Connect worker under load and records,
                per table, the contiguous key range the source wrote. This then requires
                Bronze to hold *exactly one* insert event per key in that range -- no fewer
                (no gap end to end) and no more (every replayed insert dropped).

`replay`        Spark-side replay. SIGKILLs the Bronze stream under load, then makes sure
                the batch in flight is re-run: if the checkpoint already recorded it, its
                commit marker is withheld, which is exactly the state a crash between the
                Iceberg commit and the checkpoint commit leaves behind. The re-run must
                write nothing new, and the same exactly-one-insert-per-key check must hold.

`latency`       GR1 Table 4.4's first KPI, blocked until Bronze existed: time from source
                commit to row availability in Iceberg Bronze. Availability is the commit
                time of the Iceberg snapshot that published the row, joined exactly through
                `_ingest_ts` = the snapshot's `mosaic.ingest_ts_us`. Split into capture
                (source commit -> Kafka) and landing (Kafka -> Bronze snapshot).

The insert-range proof is independent of Bronze's own idempotency key on purpose: a key
is inserted at the source exactly once, so a second op=c for it can only be a replay, and
a missing one can only be a gap. Checking Bronze with the key it deduplicates on would be
circular.

Domain-free: tables, topics and key columns come from the connectors' declared config and
the live catalog via tools.cdc.restart.topology(), the same derivation M2 uses.
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from mosaic import _bronze_status, _compose_cmd, docker_env  # noqa: E402
from tools.cdc.restart import LoadThread, _bracket, _docker, topology  # noqa: E402
from tools.dbsession import MySqlSession, PostgresSession  # noqa: E402
from tools.loadgen.workload import Workload  # noqa: E402
from tools.trino import query, query_with_columns, scalar  # noqa: E402

TABLE = "iceberg.bronze.cdc_events"
SNAPSHOTS = 'iceberg.bronze."cdc_events$snapshots"'
SPARK = "mosaic-spark"
CHECKPOINT = "/opt/mosaic/state/checkpoints/bronze"
LEDGER = ROOT / "experiments" / "results" / "bronze_events.jsonl"
RESTART_LEDGER = ROOT / "experiments" / "results" / "restart_events.jsonl"


# ------------------------------------------------------------------- helpers
def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts(dt: datetime) -> str:
    """A Trino TIMESTAMP WITH TIME ZONE literal."""
    return f"TIMESTAMP '{dt.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S.%f} UTC'"


def _append_ledger(event: dict) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    event.setdefault("recorded_at", _utc())
    with LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, default=str) + "\n")
    print(f"\n  recorded to {LEDGER.relative_to(ROOT).as_posix()}")


def stream_live() -> tuple[bool, dict | None]:
    status = _bronze_status()
    if not status:
        return False, None
    return bool(status.get("active")) and time.time() - status.get("written_at", 0) < 60, status


def require_live() -> bool:
    live, _ = stream_live()
    if not live:
        print("the Bronze stream is not running -- python mosaic.py bronze")
    return live


def bronze_inserts(table: str, key_column: str, lo: int, hi: int) -> tuple[int, int]:
    """(insert events, distinct keys) Bronze holds for keys lo+1..hi of one table."""
    rows = query(f"""
        SELECT count(*), count(DISTINCT k) FROM (
            SELECT CAST(json_extract_scalar(record_key, '$.{key_column}') AS bigint) AS k
            FROM {TABLE} WHERE source_table = '{table}' AND op = 'c') t
        WHERE k BETWEEN {lo + 1} AND {hi}""")
    return int(rows[0][0]), int(rows[0][1])


def max_landed_key(table: str, key_column: str) -> int:
    v = scalar(f"""
        SELECT max(CAST(json_extract_scalar(record_key, '$.{key_column}') AS bigint))
        FROM {TABLE} WHERE source_table = '{table}' AND op = 'c'""")
    return int(v) if v is not None else 0


def wait_landed(ranges: dict, timeout: float) -> tuple[bool, float]:
    """Block until every table's last written key is in Bronze.

    Sufficient for the whole range: each captured table is one single-partition topic,
    Kafka keeps source commit order within it, and a micro-batch lands atomically.
    """
    t0 = time.perf_counter()
    pending = dict(ranges)
    while pending and time.perf_counter() - t0 < timeout:
        for table, r in list(pending.items()):
            if max_landed_key(table, r["key_column"]) >= r["hi"]:
                del pending[table]
        if pending:
            time.sleep(5)
    return not pending, time.perf_counter() - t0


def check_ranges(ranges: dict, kafka: dict | None = None) -> tuple[bool, dict]:
    """Exactly one Bronze insert event per source key, for every bracketed table."""
    width = max(len(t) for t in ranges)
    print(f"  {'table'.ljust(width)}  {'key range':>15}  {'rows':>6}  "
          + (f"{'kafka c':>7}  " if kafka else "")
          + f"{'bronze c':>8}  {'distinct':>8}  verdict")
    ok, out = True, {}
    for table, r in ranges.items():
        expected = r["hi"] - r["lo"]
        n, distinct = bronze_inserts(table, r["key_column"], r["lo"], r["hi"])
        exact = n == distinct == expected
        ok &= exact
        verdict = "exact" if exact else (
            f"GAP {expected - distinct}" if distinct < expected else f"EXTRA {n - distinct}")
        out[table] = {**r, "expected": expected, "bronze_inserts": n,
                      "bronze_distinct": distinct, "exact": exact}
        k = ""
        if kafka:
            k = f"{expected + kafka.get(table, {}).get('replayed_inserts', 0):>7}  "
        print(f"  {table.ljust(width)}  {r['lo'] + 1:>7}..{r['hi']:<7}  {expected:>6}  {k}"
              f"{n:>8}  {distinct:>8}  {verdict}")
    return ok, out


def _ranges_from(captured) -> dict:
    return {c.table: {"key_column": c.key_column, "lo": c.lo, "hi": c.hi}
            for c in captured if c.key_column and c.lo is not None
            and c.hi is not None and c.hi > c.lo}


def _open_load(domain: str, rate: float, seed: int | None):
    captured = topology(domain)
    my, pg = MySqlSession().open(), PostgresSession().open()
    sessions = {"mysql": my, "postgres": pg}
    wl = Workload(my, pg, random.Random(seed) if seed is not None else random.Random())
    wl.prime()
    return captured, sessions, LoadThread(wl, rate)


# ------------------------------------------------------------- exactly-once
def cmd_exactly_once(a: argparse.Namespace) -> int:
    runs = []
    if RESTART_LEDGER.exists():
        for line in RESTART_LEDGER.read_text(encoding="utf-8").splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("kind") == "restart-safety" and e.get("ranges"):
                runs.append(e)
    if not runs:
        print("no restart-safety run with per-table ranges in the ledger.")
        print("run one with the Bronze stream up:  python mosaic.py restart-safety")
        return 1
    run = runs[-1]
    if not require_live():
        return 1
    ranges = run["ranges"]
    replayed = sum(r.get("replayed", 0) for r in ranges.values())
    print(f"exactly-once: restart-safety run of {run['recorded_at']} "
          f"({run['mode']}, verdict {run['verdict']})")
    print(f"  Kafka received {replayed} replayed event(s) across the bracketed tables, "
          f"{sum(r.get('replayed_inserts', 0) for r in ranges.values())} of them inserts\n")

    landed, waited = wait_landed(ranges, a.timeout)
    if not landed:
        print(f"  TIMED OUT after {waited:.0f}s waiting for Bronze to land the range")
    ok, detail = check_ranges(ranges, kafka=ranges)
    ok &= landed
    print()
    print("  PASS -- every source insert is in Bronze exactly once; every replayed copy "
          "was dropped." if ok else "  FAIL -- see the verdict column.")
    _append_ledger({"kind": "exactly-once", "domain": a.domain,
                    "verdict": "pass" if ok else "fail",
                    "restart_run": run["recorded_at"], "kafka_replayed": replayed,
                    "tables": detail})
    return 0 if ok else 1


# ------------------------------------------------------------------- replay
def _checkpoint_sh(script: str) -> subprocess.CompletedProcess:
    """Run a shell snippet against the checkpoint volume in a throwaway container."""
    return subprocess.run(_compose_cmd() + ["run", "--rm", "--no-deps", "--entrypoint",
                                            "sh", "spark", "-c", script],
                          capture_output=True, text=True, env=docker_env())


def _batches(listing: str) -> list[int]:
    return sorted(int(n) for n in listing.split() if n.isdigit())


def cmd_replay(a: argparse.Namespace) -> int:
    if not require_live():
        return 1
    t_start = datetime.now(timezone.utc)
    captured, sessions, load = _open_load(a.domain, a.rate, a.seed)
    print(f"replay: Spark-side crash under load, domain={a.domain}, rate={a.rate}/s\n")

    _bracket(sessions, captured, "lo")
    load.start()
    print(f"[1/5] load      {a.warmup}s of writes with the stream healthy")
    time.sleep(a.warmup)

    r = _docker("kill", SPARK)
    t_kill = time.time()
    print(f"[2/5] crash     docker kill {SPARK}: {'ok' if r.returncode == 0 else r.stderr.strip()}")
    time.sleep(a.outage)

    listing = _checkpoint_sh(f"cd {CHECKPOINT} && echo OFFSETS $(ls offsets) "
                             f"&& echo COMMITS $(ls commits)")
    out = listing.stdout
    offsets = _batches(out.split("OFFSETS", 1)[-1].split("COMMITS", 1)[0])
    commits = _batches(out.split("COMMITS", 1)[-1])
    if not offsets:
        print(f"cannot read the checkpoint: {listing.stderr.strip()[:300]}")
        load.stop()
        return 1
    batch = offsets[-1]
    landed_before = int(scalar(
        f"SELECT count(*) FROM {SNAPSHOTS} WHERE summary['mosaic.batch_id'] = '{batch}' "
        f"AND committed_at >= {_ts(t_start)}") or 0)
    if batch in commits:
        mv = _checkpoint_sh(f"cd {CHECKPOINT} && mkdir -p commits.withheld && "
                            f"mv commits/{batch} commits/.{batch}.crc commits.withheld/")
        how = ("checkpoint had committed it; commit marker withheld -> forced re-run"
               if mv.returncode == 0 else f"could not withhold: {mv.stderr.strip()[:200]}")
        if mv.returncode != 0:
            load.stop()
            print(f"[3/5] state     batch {batch}: {how}")
            return 1
    else:
        how = "crash landed mid-batch; Spark re-runs it on its own"
    print(f"[3/5] state     batch {batch}: in Iceberg before the crash: "
          f"{'yes' if landed_before else 'no'}; {how}")

    load.stop()
    load.join(timeout=10)
    _bracket(sessions, captured, "hi")
    for s in sessions.values():
        s.close()

    r = _docker("start", SPARK)
    t_restart = time.time()
    print(f"[4/5] restart   docker start {SPARK}: {'ok' if r.returncode == 0 else r.stderr.strip()}"
          f"  ({t_restart - t_kill:.0f}s after the kill, {load.ops} ops written)")
    deadline = time.time() + a.timeout
    status = None
    while time.time() < deadline:
        live, status = stream_live()
        started = status and datetime.fromisoformat(status["started_at"]).timestamp()
        if live and started and started > t_kill:
            break
        time.sleep(5)
    ranges = _ranges_from(captured)
    landed, waited = wait_landed(ranges, a.timeout)
    _, status = stream_live()
    totals = (status or {}).get("totals", {})
    snaps = int(scalar(
        f"SELECT count(*) FROM {SNAPSHOTS} WHERE summary['mosaic.batch_id'] = '{batch}' "
        f"AND committed_at >= {_ts(t_start)}") or 0)
    print(f"[5/5] proof     caught up {time.time() - t_restart:.0f}s after restart; since "
          f"restart: {totals.get('rows_in', 0)} in, {totals.get('written', 0)} written, "
          f"{totals.get('dup_in_bronze', 0)} already in Bronze, "
          f"{totals.get('dup_in_batch', 0)} in-batch\n")

    ok, detail = check_ranges(ranges)
    once = snaps == 1 if landed_before else snaps <= 1
    replay_absorbed = (totals.get("dup_in_bronze", 0) > 0) if landed_before else True
    ok = ok and landed and once and replay_absorbed
    print(f"\n  Iceberg snapshots for batch {batch}: {snaps} (must be 1 -- a re-run that "
          f"finds everything landed commits nothing)")
    if landed_before:
        print(f"  replayed batch: {totals.get('dup_in_bronze', 0)} row(s) recognised as "
              f"already in Bronze")
    print()
    print("  PASS -- the Spark-side replay wrote nothing twice and lost nothing."
          if ok else "  FAIL -- see above.")
    _append_ledger({"kind": "spark-replay", "domain": a.domain,
                    "verdict": "pass" if ok else "fail", "batch": batch,
                    "landed_before_crash": bool(landed_before), "how": how,
                    "snapshots_for_batch": snaps, "ops": load.ops,
                    "outage_s": round(t_restart - t_kill, 1), "totals_since_restart": totals,
                    "tables": detail})
    return 0 if ok else 1


# ------------------------------------------------------------------ latency
LATENCY_SQL = """
WITH s AS (
    SELECT committed_at,
           from_unixtime_nanos(CAST(summary['mosaic.ingest_ts_us'] AS bigint) * 1000) AS ing
    FROM {snapshots}
    WHERE summary['mosaic.ingest_ts_us'] IS NOT NULL AND committed_at >= {t0}
), e AS (
    SELECT regexp_extract(b.topic, '^(.+)\\.[^.]+\\.[^.]+$', 1) AS source,
           -- double, not decimal: the REST protocol returns decimals as strings
           CAST(date_diff('millisecond', b._src_ts, s.committed_at) AS double) / 1000 AS total,
           CAST(date_diff('millisecond', b._src_ts, b.kafka_ts) AS double) / 1000 AS capture,
           CAST(date_diff('millisecond', b.kafka_ts, s.committed_at) AS double) / 1000 AS landing
    FROM {table} b JOIN s ON b._ingest_ts = s.ing
    WHERE b.op <> 'r' AND b._src_ts BETWEEN {t0} AND {t1}
)
SELECT source, count(*) AS events,
       approx_percentile(total, 0.5)  AS p50,  approx_percentile(total, 0.95) AS p95,
       approx_percentile(total, 0.99) AS p99,  max(total) AS max,
       approx_percentile(capture, 0.5) AS capture_p50,
       approx_percentile(capture, 0.95) AS capture_p95,
       approx_percentile(landing, 0.5) AS landing_p50,
       approx_percentile(landing, 0.95) AS landing_p95
FROM e GROUP BY source ORDER BY source
"""


def cmd_latency(a: argparse.Namespace) -> int:
    if not require_live():
        return 1
    captured, sessions, load = _open_load(a.domain, a.rate, a.seed)
    print(f"latency: source commit -> Iceberg Bronze, {a.duration}s at {a.rate}/s\n")
    _bracket(sessions, captured, "lo")
    t0 = datetime.now(timezone.utc)
    load.start()
    time.sleep(a.duration)
    load.stop()
    load.join(timeout=10)
    t1 = datetime.now(timezone.utc)
    _bracket(sessions, captured, "hi")
    for s in sessions.values():
        s.close()
    if load.error:
        print(f"  load generator stopped early: {load.error}")
    landed, waited = wait_landed(_ranges_from(captured), a.timeout)
    # Two more triggers past the last write so trailing UPDATEs (no key to wait on) land.
    time.sleep(25)

    cols, rows = query_with_columns(LATENCY_SQL.format(
        snapshots=SNAPSHOTS, table=TABLE, t0=_ts(t0 - timedelta(seconds=1)), t1=_ts(t1)))
    print(f"  {load.ops} operations written; ranges landed: {'yes' if landed else 'NO'}\n")
    print("  seconds        events     p50     p95     p99     max   | capture p50/p95"
          "  | landing p50/p95")
    result = {}
    for r in rows:
        d = dict(zip(cols, r))
        result[d["source"]] = d
        print(f"  {d['source']:<14} {d['events']:>6}  {d['p50']:>6.2f}  {d['p95']:>6.2f}  "
              f"{d['p99']:>6.2f}  {d['max']:>6.2f}   | {d['capture_p50']:>5.2f} / "
              f"{d['capture_p95']:<5.2f}    | {d['landing_p50']:>5.2f} / {d['landing_p95']:.2f}")
    print("\n  capture = source commit -> Kafka record; landing = Kafka -> Iceberg snapshot"
          " commit.\n  Snapshot reads (op=r) are excluded: their source time is when the"
          " snapshot read the\n  row, and on MySQL it has one-second resolution. Streaming"
          " events carry sub-ms source\n  times on both engines (MySQL 8's"
          " original_commit_timestamp).")
    ok = landed and bool(rows)
    _append_ledger({"kind": "latency", "domain": a.domain,
                    "verdict": "pass" if ok else "inconclusive",
                    "rate": a.rate, "duration_s": a.duration, "ops": load.ops,
                    "window": [t0.isoformat(), t1.isoformat()], "by_source": result})
    return 0 if ok else 1


# --------------------------------------------------------------------- main
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    eo = sub.add_parser("exactly-once", help="Bronze vs the last restart-safety run")
    rp = sub.add_parser("replay", help="SIGKILL the Bronze stream under load; prove no dup/gap")
    rp.add_argument("--warmup", type=int, default=30)
    rp.add_argument("--outage", type=int, default=20)
    lt = sub.add_parser("latency", help="source commit -> Iceberg Bronze, under load")
    lt.add_argument("--duration", type=int, default=120)
    for sp in (eo, rp, lt):
        sp.add_argument("--domain", default="ecommerce")
        sp.add_argument("--timeout", type=float, default=300.0)
    for sp in (rp, lt):
        sp.add_argument("--rate", type=float, default=20.0)
        sp.add_argument("--seed", type=int, default=None)

    a = p.parse_args()
    return {"exactly-once": cmd_exactly_once, "replay": cmd_replay,
            "latency": cmd_latency}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
