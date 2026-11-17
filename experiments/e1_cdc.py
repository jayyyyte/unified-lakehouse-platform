"""E1 -- the CDC comparison of GR1 section 4.4, run rather than designed.

    python mosaic.py e1 [--phase all|latency|throughput|restart|ddl|wal|report]

GR1 Table 4.4 names five key performance indicators and says they are "to be measured
once the pipeline is implemented in GR2". This is that measurement. One runner, both
sources, the same workload, the same clock, one ledger:

  latency      source commit -> Iceberg Bronze, per source, at a fixed rate
               (tools/bronze/verify.py's measurement, re-run so E1 has its own entry)
  throughput   a rate ladder; per step: what the sources actually committed, events/s
               each connector put on Kafka, and the latency percentiles at that rate.
               The ladder shows *where* each mechanism saturates, which one number
               would hide. The load generator's send rate is not its commit rate
               (CLAUDE.md), so the committed figure is re-counted from the sources.
  restart      tools/cdc/restart.py's kill-under-load run, which since E1 attributes
               catch-up time to each connector separately (its topics going flat).
  ddl          the KPI nothing had exercised: ALTER TABLE at the source while the
               connector streams. One captured table per source gains a nullable
               column, one row is updated through it, and the probe follows the event
               through Kafka (the after image), the MySQL schema-history and
               schema-change topics, Bronze (envelope kept verbatim) and Silver (dbt
               `append_new_columns`). Then the column is dropped and that Silver model
               is rebuilt with --full-refresh, because `append_new_columns` never
               removes a column. Everything is restored; the run checks that it was.
  wal          tools/cdc/restart.py's wal-guard run (PostgreSQL only, by definition),
               plus the slot series the restart run recorded under load.
  report       experiments/results/e1_cdc_report.md from the latest entry of each
               phase: GR1 Table 4.4 with numbers in it. The .jsonl ledgers are
               gitignored; the report is the artefact that goes into the repository.

Nothing here names a domain. Tables, topics, key columns and engines come from the
connector configs through tools/cdc (topology()); the probe column name is the only
identifier this file invents, and it is removed again before the run ends.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mosaic import _bronze_status, compose  # noqa: E402
from tools.bronze.verify import (LATENCY_SQL, SNAPSHOTS, TABLE, _ranges_from, _ts,  # noqa: E402
                                 require_live, wait_landed)
from tools.cdc.restart import (LoadThread, _bracket, _heartbeat_relation, cmd_restart,  # noqa: E402
                               cmd_wal_guard, connector_state, consume_range,
                               end_offsets_many, is_mysql, load_connectors, topology)
from tools.cdc.restart import LEDGER as RESTART_LEDGER  # noqa: E402
from tools.cdc.verify import list_topics  # noqa: E402
from tools.dbsession import MySqlSession, PostgresSession  # noqa: E402
from tools.loadgen.workload import Workload  # noqa: E402
from tools.trino import query, query_with_columns, scalar  # noqa: E402

LEDGER = ROOT / "experiments" / "results" / "e1_cdc_events.jsonl"
REPORT = ROOT / "experiments" / "results" / "e1_cdc_report.md"
PROBE_COLUMN = "e1_ddl_probe"
POLL_SECONDS = 2.0
FLAT_SAMPLES = 3        # consecutive unchanged offset polls that mean "drained"
ENGINES = ("mysql", "postgres")


# ------------------------------------------------------------------- helpers
def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append(event: dict) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    event.setdefault("recorded_at", _utc())
    with LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, default=str) + "\n")
    print(f"  recorded to {LEDGER.relative_to(ROOT).as_posix()}")


def _entries(path: Path, kind: str) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("kind") == kind:
            out.append(e)
    return out


def _latest(path: Path, kind: str) -> dict | None:
    entries = _entries(path, kind)
    return entries[-1] if entries else None


def _workload(domain: str):
    """Captured tables minus the connector's own heartbeat relation, and the heartbeat."""
    captured = topology(domain)
    heartbeat = None
    for _, config in load_connectors(domain):
        if not is_mysql(config):
            heartbeat = _heartbeat_relation(config)
    work = [c for c in captured if c.relation != heartbeat]
    return captured, work, heartbeat


def _open_sessions():
    return {"mysql": MySqlSession().open(), "postgres": PostgresSession().open()}


def _drain(topics: list[str], timeout: float) -> tuple[float, dict]:
    """Poll until the topics stop advancing; return (seconds until flat, final offsets)."""
    t0 = time.perf_counter()
    t_last = t0
    snap = end_offsets_many(topics)
    prev = sum(sum(p.values()) for p in snap.values())
    flat = 0
    while flat < FLAT_SAMPLES and time.perf_counter() - t0 < timeout:
        time.sleep(POLL_SECONDS)
        snap = end_offsets_many(topics)
        cur = sum(sum(p.values()) for p in snap.values())
        if cur != prev:
            prev, flat, t_last = cur, 0, time.perf_counter()
        else:
            flat += 1
    return t_last - t0, snap


def _delta_by_engine(work, before: dict, after: dict) -> dict[str, int]:
    out = {e: 0 for e in ENGINES}
    for c in work:
        b = sum(before.get(c.topic, {}).values())
        a = sum(after.get(c.topic, {}).values())
        out[c.engine] += max(0, a - b)
    return out


def _source_key(topic_source: str) -> str:
    """'mosaic.mysql' -> 'mysql'; the LATENCY_SQL source column is the topic prefix."""
    return "mysql" if "mysql" in topic_source else "postgres"


# ---------------------------------------------------------------- load phase
def measure_load(domain: str, rate: float, duration: int, seed: int | None,
                 timeout: float) -> dict:
    """One window of load: committed rows, Kafka events/s per source, Bronze latency."""
    captured, work, _ = _workload(domain)
    topics = [c.topic for c in work]
    sessions = _open_sessions()
    wl = Workload(sessions["mysql"], sessions["postgres"],
                  random.Random(seed) if seed is not None else random.Random())
    wl.prime()
    load = LoadThread(wl, rate)

    _bracket(sessions, captured, "lo")
    off0 = end_offsets_many(topics)
    t0 = datetime.now(timezone.utc)
    p0 = time.perf_counter()
    load.start()
    time.sleep(duration)
    load.stop()
    load.join(timeout=10)
    sent_s = time.perf_counter() - p0
    # close() drains the client pipes: everything the generator queued is committed by
    # the time it returns, so the commit window ends here, not when sending stopped.
    for s in sessions.values():
        s.close()
    t1 = datetime.now(timezone.utc)
    commit_s = time.perf_counter() - p0
    fresh = _open_sessions()
    _bracket(fresh, captured, "hi")
    for s in fresh.values():
        s.close()
    if load.error:
        print(f"  load generator stopped early: {load.error}")

    kafka_s, off1 = _drain(topics, timeout)
    events = _delta_by_engine(work, off0, off1)
    inserted = {c.table: c.hi - c.lo for c in captured
                if c.key_column and c.lo is not None and c.hi is not None}
    rows_by_engine = {e: sum(c.hi - c.lo for c in captured
                             if c.engine == e and c.key_column and c.lo is not None
                             and c.hi is not None) for e in ENGINES}

    landed, waited = wait_landed(_ranges_from(captured), timeout)
    time.sleep(25)   # two more triggers so trailing UPDATEs (no key to wait on) land
    cols, rows = query_with_columns(LATENCY_SQL.format(
        snapshots=SNAPSHOTS, table=TABLE, t0=_ts(t0 - timedelta(seconds=1)), t1=_ts(t1)))
    latency = {}
    for r in rows:
        d = dict(zip(cols, r))
        latency[_source_key(d["source"])] = {k: (v if k == "source" else float(v))
                                             for k, v in d.items()}
    window_s = max(commit_s, kafka_s)
    step = {
        "rate_requested": rate, "duration_s": duration, "ops_sent": load.ops,
        "sent_window_s": round(sent_s, 2), "commit_window_s": round(commit_s, 2),
        "ops_per_s_sustained": round(load.ops / commit_s, 2) if commit_s else None,
        "rows_inserted": inserted, "rows_inserted_by_engine": rows_by_engine,
        "kafka_events": events,
        "kafka_events_per_s": {e: round(events[e] / window_s, 2) for e in ENGINES},
        "kafka_flat_after_s": round(kafka_s, 2),
        "window": [t0.isoformat(), t1.isoformat()],
        "landed": landed, "landed_wait_s": round(waited, 1),
        "latency": latency,
    }
    return step


def _print_step(s: dict) -> None:
    print(f"  requested {s['rate_requested']:>5.0f}/s  sent {s['ops_sent']:>5} ops in "
          f"{s['sent_window_s']:.0f}s, committed in {s['commit_window_s']:.0f}s "
          f"-> sustained {s['ops_per_s_sustained']} ops/s")
    for e in ENGINES:
        lat = s["latency"].get(e, {})
        p50 = lat.get("p50"); p95 = lat.get("p95"); p99 = lat.get("p99")
        cap = lat.get("capture_p50")
        print(f"    {e:<9} rows +{s['rows_inserted_by_engine'][e]:<6} kafka events "
              f"{s['kafka_events'][e]:<6} = {s['kafka_events_per_s'][e]:>7} ev/s   "
              f"bronze p50/p95/p99 "
              f"{'-' if p50 is None else f'{p50:.2f}'}/"
              f"{'-' if p95 is None else f'{p95:.2f}'}/"
              f"{'-' if p99 is None else f'{p99:.2f}'} s   "
              f"capture p50 {'-' if cap is None else f'{cap:.2f}'} s")


def phase_latency(a: argparse.Namespace) -> int:
    if not require_live():
        return 1
    print(f"E1 latency: {a.duration}s at {a.rate}/s, both sources, commit -> Bronze\n")
    step = measure_load(a.domain, a.rate, a.duration, a.seed, a.timeout)
    _print_step(step)
    ok = step["landed"] and all(e in step["latency"] for e in ENGINES)
    _append({"kind": "e1-latency", "domain": a.domain,
             "verdict": "pass" if ok else "inconclusive", **step})
    return 0 if ok else 1


def phase_throughput(a: argparse.Namespace) -> int:
    if not require_live():
        return 1
    ladder = [float(x) for x in a.ladder.split(",")]
    print(f"E1 throughput: rate ladder {ladder} x {a.step_duration}s each\n")
    steps = []
    for rate in ladder:
        step = measure_load(a.domain, rate, a.step_duration, a.seed, a.timeout)
        _print_step(step)
        steps.append(step)
    ok = all(s["landed"] for s in steps)
    _append({"kind": "e1-throughput", "domain": a.domain,
             "verdict": "pass" if ok else "inconclusive",
             "ladder": ladder, "step_duration_s": a.step_duration, "steps": steps})
    return 0 if ok else 1


# ------------------------------------------------------------- restart / wal
def phase_restart(a: argparse.Namespace) -> int:
    print("E1 restart recovery: tools/cdc/restart.py restart, per-connector catch-up\n")
    started = _utc()
    rc = cmd_restart(argparse.Namespace(
        domain=a.domain, mode="kill", rate=a.rate, warmup=20.0, stall=45.0, drain=20.0,
        sample=5.0, seed=a.seed, flush_interval_ms=60000,
        recovery_timeout=240.0, offset_timeout=240.0))
    run = _latest(RESTART_LEDGER, "restart-safety")
    if run is None or run.get("recorded_at", "") < started:
        print("  no restart-safety entry was recorded for this run")
        return 1
    series = run.get("slot_series", [])
    stalled = [s for s in series if s["phase"] in ("pre-kill", "stalled")]
    wal = {}
    for slot in (run.get("slots_after") or {}):
        pre = next((s["slots"][slot] for s in series if s["phase"] == "pre-kill"), None)
        last = stalled[-1]["slots"][slot] if stalled else None
        wal[slot] = {
            "flush_lag_bytes_under_load": pre["flush_lag_bytes"] if pre else None,
            "retained_wal_growth_during_outage_bytes":
                (last["retained_wal_bytes"] - pre["retained_wal_bytes"]) if pre and last else None,
            "flush_lag_bytes_after_recovery": run["slots_after"][slot]["flush_lag_bytes"],
            "retained_wal_bytes_after_recovery": run["slots_after"][slot]["retained_wal_bytes"],
        }
    _append({"kind": "e1-restart", "domain": a.domain, "verdict": run["verdict"],
             "restart_run": run["recorded_at"], "mode": run["mode"],
             "ops_during_stall": run["ops_during_stall"],
             "connectors_running_after_s": run["connectors_running_after_s"],
             "recovery_seconds": run["recovery_seconds"],
             "recovery_by_connector": run.get("recovery_by_connector"),
             "writes_continued_s": run.get("writes_continued_s"),
             "drain_after_last_write_s": run.get("drain_after_last_write_s"),
             "offset_flush_seconds": run.get("offset_flush_seconds"),
             "gaps": run["gaps"], "duplicates": run["duplicates"],
             "ranges": run.get("ranges"), "wal": wal})
    return rc


def phase_wal(a: argparse.Namespace) -> int:
    print("E1 WAL lag: tools/cdc/restart.py wal-guard (captured tables must be idle)\n")
    started = _utc()
    rc = cmd_wal_guard(argparse.Namespace(
        domain=a.domain, mode="kill", idle=a.idle, settle=70.0, flush_interval_ms=60000,
        recovery_timeout=240.0, offset_timeout=240.0))
    run = _latest(RESTART_LEDGER, "wal-guard")
    if run is None or run.get("recorded_at", "") < started:
        print("  no wal-guard entry was recorded for this run")
        return 1
    _append({"kind": "e1-wal", "domain": a.domain, "verdict": run["verdict"],
             "wal_run": run["recorded_at"], "idle_seconds": run["idle_seconds"],
             "heartbeat_ms": run["heartbeat_ms"], "series": run["series"]})
    return rc


# ------------------------------------------------------------------ ddl phase
def _probe_target(work):
    """The first captured table with a generated key, per engine (derived, not named)."""
    out = {}
    for c in work:
        if c.key_column and c.engine not in out:
            out[c.engine] = c
    return out


def _schema_topics(domain: str) -> dict[str, list[str]]:
    """MySQL: the schema-history topic and the schema-change topic (= topic.prefix).
    PostgreSQL has neither: pgoutput carries no DDL."""
    out = {e: [] for e in ENGINES}
    existing = list_topics()
    for _, config in load_connectors(domain):
        if is_mysql(config):
            for t in (config.get("schema.history.internal.kafka.topic"),
                      config.get("topic.prefix")):
                if t and t in existing:
                    out["mysql"].append(t)
    return out


def _alter(engine: str, relation: str, sql: str) -> float:
    """Run one DDL statement on its own one-shot client; wall seconds (incl. exec)."""
    session = MySqlSession() if engine == "mysql" else PostgresSession()
    t0 = time.perf_counter()
    session.query(sql.format(t=relation, c=PROBE_COLUMN))
    return round(time.perf_counter() - t0, 3)


def _find_probe(topic: str, start: dict, value: str, timeout: float) -> tuple[float | None, dict | None]:
    """Seconds until an event whose after image carries the probe value shows on the topic."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        end = end_offsets_many([topic]).get(topic, {})
        for msg in consume_range(topic, start, end):
            env = msg.get("payload", msg)
            after = env.get("after") or {}
            if after.get(PROBE_COLUMN) == value:
                return round(time.perf_counter() - t0, 2), env
        time.sleep(POLL_SECONDS)
    return None, None


def _silver_columns(relation: str) -> list[str]:
    rows = query("SELECT column_name FROM iceberg.information_schema.columns "
                 f"WHERE table_schema = 'silver' AND table_name = '{relation}'")
    return [r[0] for r in rows]


def _dbt(*args: str) -> tuple[int, float]:
    t0 = time.perf_counter()
    rc = compose("run", "--rm", "dbt", *args, profiles=["transform"])
    return rc, round(time.perf_counter() - t0, 1)


def phase_ddl(a: argparse.Namespace) -> int:
    if not require_live():
        return 1
    captured, work, _ = _workload(a.domain)
    targets = _probe_target(work)
    if set(targets) != set(ENGINES):
        print(f"need one keyed table per engine, have {list(targets)}")
        return 1
    schema_topics = _schema_topics(a.domain)
    names = sorted({c.connector for c in work})
    print(f"E1 DDL handling: ALTER TABLE ADD COLUMN {PROBE_COLUMN} under {a.rate}/s load")
    for e, c in targets.items():
        print(f"  {e:<9} {c.table}  key {c.key_column}  topic {c.topic}")
    print()

    # Guard: a leftover probe column from an aborted run would make ADD COLUMN fail.
    for e, c in targets.items():
        session = MySqlSession() if e == "mysql" else PostgresSession()
        cols = {r[0] for r in session.query_rows(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{c.relation}'")}
        if PROBE_COLUMN in cols:
            print(f"  {c.relation} already has {PROBE_COLUMN} (aborted run?); dropping first")
            _alter(e, c.relation, "ALTER TABLE {t} DROP COLUMN {c}")

    sessions = _open_sessions()
    wl = Workload(sessions["mysql"], sessions["postgres"],
                  random.Random(a.seed) if a.seed is not None else random.Random())
    wl.prime()
    load = LoadThread(wl, a.rate)
    load.start()
    time.sleep(10)

    result = {}
    all_topics = [c.topic for c in targets.values()] + sum(schema_topics.values(), [])
    off_before = end_offsets_many(all_topics)
    bronze_before = _bronze_status() or {}
    for e in ENGINES:
        c = targets[e]
        r = {"table": c.table, "topic": c.topic, "key_column": c.key_column,
             "connector_before": connector_state(c.connector)}
        add_sql = ("ALTER TABLE {t} ADD COLUMN {c} VARCHAR(32) NULL" if e == "mysql"
                   else "ALTER TABLE {t} ADD COLUMN {c} varchar(32)")
        print(f"[{e}] ALTER TABLE {c.relation} ADD COLUMN {PROBE_COLUMN}")
        r["alter_add_s"] = _alter(e, c.relation, add_sql)
        reader = MySqlSession() if e == "mysql" else PostgresSession()
        key = reader.scalar(f"SELECT MAX({c.key_column}) FROM {c.relation}")
        value = f"e1-{e}"
        t_upd = _utc()
        reader.query(f"UPDATE {c.relation} SET {PROBE_COLUMN} = '{value}' "
                     f"WHERE {c.key_column} = {key}")
        r.update({"probe_key": int(key), "probe_value": value, "update_at": t_upd})
        seen_s, env = _find_probe(c.topic, off_before.get(c.topic, {}), value, a.timeout)
        r["probe_on_kafka_s"] = seen_s
        r["probe_event_op"] = env.get("op") if env else None
        r["probe_before_has_column"] = (PROBE_COLUMN in (env.get("before") or {})) if env else None
        r["connector_after"] = connector_state(c.connector)
        print(f"[{e}] probe row {key}: event on Kafka after "
              f"{'TIMEOUT' if seen_s is None else f'{seen_s}s'}; "
              f"connector {r['connector_after']}")
        result[e] = r

    off_after = end_offsets_many(all_topics)
    for e in ENGINES:
        deltas = {}
        for t in schema_topics[e]:
            b = sum(off_before.get(t, {}).values()); n = sum(off_after.get(t, {}).values())
            deltas[t] = n - b
            if n > b:
                msgs = consume_range(t, off_before.get(t, {}), off_after.get(t, {}))
                ddl = [m.get("payload", m).get("ddl") for m in msgs
                       if isinstance(m.get("payload", m), dict)]
                deltas[t + ".ddl"] = [d for d in ddl if d][:4]
        result[e]["schema_topics"] = deltas
        if deltas:
            print(f"[{e}] schema topics: {json.dumps({k: v for k, v in deltas.items() if not k.endswith('.ddl')})}")

    # Bronze: the envelope is stored verbatim, so the new field must be inside `after`.
    print("\nBronze: waiting for the probe events to land")
    t0 = time.perf_counter()
    pending = dict(targets)
    while pending and time.perf_counter() - t0 < a.timeout:
        for e, c in list(pending.items()):
            # Bronze keeps the whole record value verbatim in `envelope`; the new
            # field is inside its after image.
            n = int(scalar(f"SELECT count(*) FROM {TABLE} WHERE topic = '{c.topic}' "
                           f"AND json_extract_scalar(envelope, '$.after.{PROBE_COLUMN}') = "
                           f"'{result[e]['probe_value']}'"))
            if n >= 1:
                result[e]["bronze_landed_s"] = round(time.perf_counter() - t0, 1)
                del pending[e]
        if pending:
            time.sleep(5)
    for e in pending:
        result[e]["bronze_landed_s"] = None
    status = _bronze_status() or {}

    def batch_id(doc: dict) -> int:
        return int(((doc.get("progress") or {}).get("batchId")) or 0)

    bronze = {"active_before": bronze_before.get("active"), "active_after": status.get("active"),
              "batches": batch_id(status) - batch_id(bronze_before)}
    for e in ENGINES:
        landed = result[e]["bronze_landed_s"]
        print(f"  [{e}] probe in Bronze after {'TIMEOUT' if landed is None else f'{landed}s'}")
    print(f"  stream active: before {bronze['active_before']}, after {bronze['active_after']}")

    load.stop()
    load.join(timeout=10)
    for s in sessions.values():
        s.close()

    # Silver: assemble.py reads columns from the live catalog, dbt is append_new_columns.
    models = [f"silver__{c.relation}" for c in targets.values()]
    print(f"\nSilver: dbt build --select {' '.join(models)} (incremental)")
    rc, secs = _dbt("build", "--select", *models)
    silver = {"incremental_rc": rc, "incremental_s": secs}
    for e, c in targets.items():
        has = PROBE_COLUMN in _silver_columns(c.relation)
        val = None
        if has:
            val = scalar(f"SELECT {PROBE_COLUMN} FROM iceberg.silver.{c.relation} "
                         f"WHERE {c.key_column} = {result[e]['probe_key']}")
        result[e]["silver_has_column_after_incremental"] = has
        result[e]["silver_probe_value"] = val
        print(f"  [{e}] silver.{c.relation}: column {'present' if has else 'ABSENT'}, "
              f"probe value {val!r}")

    # Revert: drop at the source. First an ordinary incremental build, to measure whether
    # the drop propagates on its own (on_schema_change), then --full-refresh regardless,
    # so the restore does not depend on the answer.
    print("\nRevert: DROP COLUMN at both sources, incremental build, then --full-refresh")
    for e, c in targets.items():
        result[e]["alter_drop_s"] = _alter(e, c.relation, "ALTER TABLE {t} DROP COLUMN {c}")
    rc_drop, secs_drop = _dbt("build", "--select", *models)
    silver.update({"after_drop_incremental_rc": rc_drop, "after_drop_incremental_s": secs_drop})
    for e, c in targets.items():
        result[e]["silver_column_gone_after_incremental"] = (
            PROBE_COLUMN not in _silver_columns(c.relation))
        print(f"  [{e}] after DROP, incremental build rc={rc_drop}: column "
              f"{'gone' if result[e]['silver_column_gone_after_incremental'] else 'still there'}")
    rc2, secs2 = _dbt("build", "--full-refresh", "--select", *models)
    silver.update({"full_refresh_rc": rc2, "full_refresh_s": secs2})
    restored = True
    for e, c in targets.items():
        gone = PROBE_COLUMN not in _silver_columns(c.relation)
        src = MySqlSession() if e == "mysql" else PostgresSession()
        n_src = int(src.scalar(f"SELECT count(*) FROM {c.relation}"))
        n_silver = int(scalar(f"SELECT count(*) FROM iceberg.silver.{c.relation}"))
        result[e].update({"silver_column_gone_after_full_refresh": gone,
                          "source_rows": n_src, "silver_rows": n_silver,
                          "connector_final": connector_state(c.connector)})
        restored &= gone and n_src == n_silver and result[e]["connector_final"].startswith("RUNNING")
        print(f"  [{e}] silver.{c.relation}: column {'gone' if gone else 'STILL THERE'}, "
              f"rows source {n_src} / silver {n_silver}, connector {result[e]['connector_final']}")

    ok = restored and rc == 0 and rc2 == 0 and all(
        result[e]["probe_on_kafka_s"] is not None and result[e]["bronze_landed_s"] is not None
        and result[e]["silver_has_column_after_incremental"]
        and result[e]["connector_after"].startswith("RUNNING") for e in ENGINES)
    print("\n  PASS -- both connectors streamed through the DDL, the new column reached "
          "Kafka, Bronze and Silver,\n  and the schema is restored." if ok
          else "\n  FAIL -- see the lines above.")
    _append({"kind": "e1-ddl", "domain": a.domain, "verdict": "pass" if ok else "fail",
             "probe_column": PROBE_COLUMN, "rate": a.rate, "connectors": names,
             "by_engine": result, "bronze": bronze, "silver": silver})
    return 0 if ok else 1


# --------------------------------------------------------------------- report
def _f(v, nd=2, unit="") -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.{nd}f}{unit}"
    return f"{v:,}{unit}" if isinstance(v, int) else f"{v}{unit}"


def phase_report(a: argparse.Namespace) -> int:
    lat = _latest(LEDGER, "e1-latency")
    thr = _latest(LEDGER, "e1-throughput")
    rst = _latest(LEDGER, "e1-restart")
    ddl = _latest(LEDGER, "e1-ddl")
    wal = _latest(LEDGER, "e1-wal")
    missing = [k for k, v in (("latency", lat), ("throughput", thr), ("restart", rst),
                              ("ddl", ddl), ("wal", wal)) if v is None]
    if missing:
        print(f"no ledger entry yet for: {', '.join(missing)} -- the report marks them.")

    L = []
    L.append("# E1 — So sánh CDC: MySQL binlog vs PostgreSQL WAL (GR1 Table 4.4)\n")
    L.append(f"*Sinh bởi `python mosaic.py e1 --phase report` lúc {_utc()[:19]}Z. Mọi số ở đây đọc từ "
             "entry mới nhất của mỗi phase trong `experiments/results/e1_cdc_events.jsonl` (gitignored); "
             "file này là bản ghi vào repo. Giao thức: "
             "[ADR 0007](../../docs/adr/0007-e1-cdc-comparison.md).*\n")
    L.append("Cùng một workload (`tools/loadgen`), cùng đồng hồ, cùng stack; mỗi KPI đo trên cả hai "
             "nguồn trong cùng một phiên. Quy mô `small`. Ledger giữ cả lần thất bại.\n")

    L.append("## Table 4.4 với số thật\n")
    L.append("| Indicator | MySQL 8.0 (binlog) | PostgreSQL 15 (WAL / pgoutput) | Ghi chú |")
    L.append("|---|---|---|---|")
    if lat:
        m, p = lat["latency"].get("mysql", {}), lat["latency"].get("postgres", {})
        L.append(f"| End-to-end latency, commit -> Bronze, {lat['rate_requested']:.0f} ops/s x "
                 f"{lat['duration_s']} s | p50 **{_f(m.get('p50'))} s**, p95 {_f(m.get('p95'))}, "
                 f"p99 {_f(m.get('p99'))}, max {_f(m.get('max'))} ({_f(int(m.get('events', 0)))} events) "
                 f"| p50 **{_f(p.get('p50'))} s**, p95 {_f(p.get('p95'))}, p99 {_f(p.get('p99'))}, "
                 f"max {_f(p.get('max'))} ({_f(int(p.get('events', 0)))} events) "
                 f"| capture (commit -> Kafka) p50 {_f(m.get('capture_p50'))} / "
                 f"{_f(p.get('capture_p50'))} s; phần còn lại là trigger 10 s của Bronze |")
    else:
        L.append("| End-to-end latency | chưa đo | chưa đo | |")
    if thr:
        for s in thr["steps"]:
            km, kp = s["kafka_events_per_s"]["mysql"], s["kafka_events_per_s"]["postgres"]
            lm, lp = s["latency"].get("mysql", {}), s["latency"].get("postgres", {})
            L.append(f"| Throughput @ {s['rate_requested']:.0f} ops/s requested "
                     f"(sustained {_f(s['ops_per_s_sustained'])} ops/s) "
                     f"| **{_f(km)} ev/s**, p95 {_f(lm.get('p95'))} s "
                     f"| **{_f(kp)} ev/s**, p95 {_f(lp.get('p95'))} s "
                     f"| rows +{_f(s['rows_inserted_by_engine']['mysql'])} / "
                     f"+{_f(s['rows_inserted_by_engine']['postgres'])}; "
                     f"{s['ops_sent']} ops sent in {s['sent_window_s']:.0f} s, committed in "
                     f"{s['commit_window_s']:.0f} s |")
    else:
        L.append("| Throughput | chưa đo | chưa đo | |")
    if rst:
        by = rst.get("recovery_by_connector") or {}
        my = next((v for k, v in by.items() if "mysql" in k), None)
        pg = next((v for k, v in by.items() if "postgres" in k), None)
        # The underlying restart-safety entry carries the split between "load still
        # writing" and "pure catch-up"; older e1-restart entries did not copy it.
        run = next((e for e in _entries(RESTART_LEDGER, "restart-safety")
                    if e.get("recorded_at") == rst.get("restart_run")), {})
        writes = rst.get("writes_continued_s", run.get("writes_continued_s"))
        tail = rst.get("drain_after_last_write_s", run.get("drain_after_last_write_s"))
        same = my is not None and my == pg
        L.append(f"| Restart recovery (SIGKILL worker 45 s under load, writes continue) "
                 f"| topics flat **{_f(my)} s** after restart "
                 f"| topics flat **{_f(pg)} s** after restart "
                 f"| connectors RUNNING after {_f(rst['connectors_running_after_s'])} s; tải còn ghi "
                 f"{_f(writes)} s sau restart, topic phẳng {_f(tail)} s sau lần ghi cuối"
                 f"{' (hai connector phẳng ở cùng một lần poll: catch-up bị chặn bởi lúc tải dừng, không bởi cơ chế)' if same else ''}; "
                 f"{rst['ops_during_stall']} ops during outage; gaps {rst['gaps']}, "
                 f"replayed {rst['duplicates']} (at-least-once, A8); committed offset passed "
                 f"the kill point after {_f(rst.get('offset_flush_seconds'))} s |")
    else:
        L.append("| Restart recovery | chưa đo | chưa đo | |")
    if ddl:
        bm, bp = ddl["by_engine"]["mysql"], ddl["by_engine"]["postgres"]

        def cell(b: dict) -> str:
            return (f"ALTER {_f(b.get('alter_add_s'))} s; probe on Kafka after "
                    f"**{_f(b.get('probe_on_kafka_s'))} s**; connector {b.get('connector_after')}; "
                    f"Bronze {_f(b.get('bronze_landed_s'))} s; Silver column after incremental build: "
                    f"{'yes' if b.get('silver_has_column_after_incremental') else 'NO'}; "
                    f"gone after DROP + incremental: "
                    f"{'yes' if b.get('silver_column_gone_after_incremental') else 'NO'}; "
                    f"restored: {'yes' if b.get('silver_column_gone_after_full_refresh') else 'NO'}")
        st = bm.get("schema_topics", {})
        hist = {k: v for k, v in st.items() if not k.endswith(".ddl")}
        L.append(f"| DDL handling (ADD COLUMN under load, UPDATE one row, DROP + full-refresh) "
                 f"| {cell(bm)} | {cell(bp)} "
                 f"| MySQL schema topics got {json.dumps(hist)} new message(s); "
                 f"pgoutput emits no DDL event, the connector picks the column up from the next "
                 f"Relation message. dbt incremental (ADD) {_f(ddl['silver']['incremental_s'])} s, "
                 f"incremental (DROP) {_f(ddl['silver'].get('after_drop_incremental_s'))} s, "
                 f"full-refresh {_f(ddl['silver']['full_refresh_s'])} s. Lần chạy đầu fail ở Silver "
                 f"(A15, ADR 0007 E1-B6) |")
    else:
        L.append("| DDL handling | chưa đo | chưa đo | |")
    if wal or rst:
        w = (rst or {}).get("wal", {})
        slot = next(iter(w.values()), {}) if w else {}
        guard = ""
        if wal:
            # wal-guard samples twice per phase: the first and last "idle-connector-up"
            # samples bracket the idle window with the worker alive.
            up = [s for s in wal.get("series", []) if s.get("phase") == "idle-connector-up"]
            try:
                s0, s1 = up[0], up[-1]
                name = next(iter(s0["slots"]))
                adv = s1["slots"][name]["flush_bytes"] - s0["slots"][name]["flush_bytes"]
                guard = (f"idle {wal['idle_seconds']:.0f} s with worker alive: confirmed_flush "
                         f"advanced {_f(adv)} B (heartbeat {wal['heartbeat_ms']} ms, verdict "
                         f"{wal['verdict']}); ")
            except (KeyError, IndexError, StopIteration):
                guard = f"wal-guard verdict {wal['verdict']}; "
        L.append(f"| WAL lag (PostgreSQL only) | n/a (binlog retention by expire_logs_days) "
                 f"| under load: flush lag {_f(slot.get('flush_lag_bytes_under_load'))} B; "
                 f"retained WAL grew {_f(slot.get('retained_wal_growth_during_outage_bytes'))} B during the "
                 f"45 s outage; after recovery lag {_f(slot.get('flush_lag_bytes_after_recovery'))} B, "
                 f"retained {_f(slot.get('retained_wal_bytes_after_recovery'))} B "
                 f"| {guard}wal_keep_size bounds retention at 512 MB |")
    else:
        L.append("| WAL lag | n/a | chưa đo | |")

    L.append("\n## Đọc thế nào\n")
    L.append("- *Latency*: `_src_ts` (commit ở nguồn) → `committed_at` của Iceberg snapshot chứa dòng đó. "
             "Snapshot read (`op=r`) bị loại. *Capture* = commit → Kafka record; phần còn lại là trigger "
             "của Spark.")
    L.append("- *Throughput*: số event mỗi connector đưa lên Kafka, chia cho cửa sổ commit (tới lúc topic "
             "ngừng tăng). `sustained` là số ops thật sự commit chia cho thời gian commit, không phải tốc độ "
             "gửi (CLAUDE.md). Một op không phải một event: `new_order` = 3 insert MySQL + 1 update "
             "PostgreSQL. Latency p95 gần như không đổi qua các bậc, nên **nút cổ chai là tốc độ commit của "
             "nguồn, không phải CDC**.")
    L.append("- *Restart recovery*: thời điểm topic của từng connector ngừng tăng, trừ thời điểm worker sống "
             "lại (poll 2 s). Tải vẫn ghi trong 20 s đầu sau restart (GR1: catch-up *while writes "
             "continue*), nên con số gồm cả thời gian tải còn chạy; phần thuần đuổi kịp là \"phẳng sau lần "
             "ghi cuối\".")
    L.append("- *DDL*: cột `e1_ddl_probe` (nullable) thêm vào một bảng có key tự sinh của mỗi nguồn, dưới "
             "tải; một dòng được UPDATE qua cột đó; probe được tìm trong after-image trên Kafka, trong "
             "`envelope` của Bronze, và trong Silver sau một `dbt build` incremental. Sau đó DROP ở nguồn, "
             "build incremental lần nữa (cột phải tự mất, `on_schema_change: sync_all_columns`), rồi "
             "`--full-refresh` để hoàn nguyên. Lần chạy đầu fail ở Silver: xem ADR 0007 E1-B6 và "
             "CHANGES-FROM-GR1 A15.")
    L.append("- *WAL lag*: `pg_replication_slots` lấy mẫu mỗi 5 s trong lần chạy restart (dưới tải); "
             "`wal-guard` chạy riêng khi các bảng được capture đứng yên (A9).")
    if missing:
        L.append(f"\n**Chưa đo:** {', '.join(missing)}.")
    REPORT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"wrote {REPORT.relative_to(ROOT).as_posix()}")
    return 0 if not missing else 1


# ----------------------------------------------------------------------- main
PHASES = {"latency": phase_latency, "throughput": phase_throughput,
          "restart": phase_restart, "ddl": phase_ddl, "wal": phase_wal,
          "report": phase_report}
ORDER = ["latency", "throughput", "restart", "ddl", "wal", "report"]


def main() -> int:
    p = argparse.ArgumentParser(prog="python mosaic.py e1", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--phase", default="all", choices=["all", *ORDER])
    p.add_argument("--domain", default="ecommerce")
    p.add_argument("--rate", type=float, default=20.0, help="latency / restart / ddl load")
    p.add_argument("--duration", type=int, default=120, help="latency window")
    p.add_argument("--ladder", default="25,50,100", help="throughput rates, ops/s")
    p.add_argument("--step-duration", type=int, default=90)
    p.add_argument("--idle", type=float, default=150.0, help="wal-guard idle window")
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()
    phases = ORDER if a.phase == "all" else [a.phase]
    worst = 0
    for name in phases:
        print(f"\n===== E1 / {name} =====")
        rc = PHASES[name](a)
        worst = max(worst, rc)
        if rc and name != "report":
            print(f"  phase {name} returned {rc}; continuing (the ledger keeps failures)")
    return worst


if __name__ == "__main__":
    sys.exit(main())
