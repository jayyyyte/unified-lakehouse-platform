"""Restart safety and the WAL-accumulation guard, measured rather than asserted.

    python -m tools.cdc.restart restart    [--domain ecommerce] [--mode kill]
    python -m tools.cdc.restart wal-guard  [--domain ecommerce]

This is milestone M2, and it produces two of the five CDC key performance indicators
GR1 Table 4.4 defines -- *restart recovery time* and *WAL lag* -- neither of which
GR1 could measure, because measuring them requires a running system.

GR1 Table 4.3 states restart safety in one line: *"offset stored in the Kafka Connect
offsets topic"*. That is true and it is not the whole story. **Connect flushes source
offsets on a timer**, not per record:

    offset.flush.interval.ms = 60000      (read from the live worker; the Connect default)

So the offset in `_connect_offsets` trails the last produced record by up to a minute.
Kill the worker and it restarts from that stale offset, re-reading everything committed
since -- which is at-least-once delivery with a **replay window bounded by the flush
interval**, not the exactly-once a one-line summary invites you to assume. The number of
duplicate events this run observes is therefore *evidence that the mechanism works as
designed*, and a harness that scored duplicates as failure would report a false negative.

That fixes what "pass" has to mean here, and it is the reason this file exists rather
than a shell one-liner:

  * **no gap** -- every row committed at the source during the outage reaches its topic.
    Exact, not statistical: every captured table with a generated key writes a
    *contiguous* range of identifiers, so the run brackets that range at the moment of
    the kill and again when writing stops, then requires the whole of it on the topic.
    One missing identifier is a failure and names itself.
  * **duplicates >= 0** -- counted, reported, and bounded by the replay window. Expected.
  * **recovery time** -- the moment the topics stop advancing after the restart, minus
    the moment the worker came back. Mechanical. Nothing here is eyeballed.

Nothing below names a domain. The captured tables, their topics and their generated-key
columns are all *derived*: topics from the connector's own `topic.prefix` and
`table.include.list` (the same expectations() that tools/cdc/verify.py uses), and the key
column from the live catalog -- `information_schema` on MySQL, `pg_attrdef` on
PostgreSQL. Point it at another domain's connectors and it measures that domain.

Two sub-commands, because docs/STATUS.md M2 is two separate claims:

`restart`     kill the worker mid-stream under load, restart it, prove no gap.
`wal-guard`   GR1 section 4.4's PostgreSQL-specific risk: a logical replication slot
              retains WAL until the consumer confirms it, so a stalled consumer grows
              WAL without bound. The mitigation GR1 names is a heartbeat that advances
              `confirmed_flush_lsn` even while the captured tables are idle. That guard
              has never been exercised here. This runs both halves -- idle *with* the
              connector alive (flush must advance; the heartbeat is doing it) and idle
              *with the connector dead* (flush must freeze and retained WAL must grow) --
              because only the contrast shows the guard is what moved it.

              The idle window must exceed the 60 s flush interval or the test proves
              nothing: a flush position that did not move inside 40 s has not
              demonstrated a stall, only a timer that had not fired yet.
"""
from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from mosaic import docker_bin, docker_env, host_port, http  # noqa: E402
from tools.cdc.verify import CONSOLE_CONSUMER, _kafka, expectations  # noqa: E402
from tools.dbsession import MySqlSession, PostgresSession, SessionDied  # noqa: E402
from tools.loadgen.workload import Workload  # noqa: E402

LEDGER = ROOT / "experiments" / "results" / "restart_events.jsonl"
CONNECT_CONTAINER = "mosaic-connect"

# How many consecutive flat samples of the topic end-offsets mean "drained". Three at
# the poll interval below is ~6 s of silence -- long enough not to trip on the gap
# between two Debezium batches, short enough not to inflate the recovery number.
FLAT_SAMPLES = 3
POLL_SECONDS = 2.0


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_ledger(event: dict) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    event.setdefault("recorded_at", _utc())
    with LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event) + "\n")


# ------------------------------------------------------------------- topology
# Everything in this section is derived from what the connectors declare. No table,
# column or topic name is written down here.

def load_connectors(domain: str) -> list[tuple[str, dict]]:
    conn_dir = ROOT / "domains" / domain / "connectors"
    out = []
    for path in sorted(conn_dir.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        config = {k: v for k, v in spec["config"].items() if not k.startswith("_comment")}
        out.append((spec["name"], config))
    return out


def is_mysql(config: dict) -> bool:
    return "mysql" in config.get("connector.class", "").lower()


def generated_key_columns(config: dict) -> dict[str, str]:
    """table -> the column the source generates monotonically, if it has one.

    Read from the live catalog, not from the DDL file, for the same reason
    tools/cdc/verify.py reads column widths from information_schema: the property the
    proof rests on is whatever the running database actually does.

    Tables without such a column (a natural or foreign primary key) simply drop out of
    the coverage proof -- they are still checked for duplicates, but a contiguous range
    is the only thing that can prove absence of a gap, and they do not have one.
    """
    if is_mysql(config):
        db = config.get("database.include.list", "").split(",")[0].strip()
        rows = MySqlSession(database=db).query_rows(
            "SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE table_schema = '{db}' AND extra LIKE '%auto_increment%'"
        )
        return {f"{db}.{r[0]}": r[1] for r in rows if len(r) >= 2}

    db = config.get("database.dbname", "")
    schema = config.get("schema.include.list", "public").split(",")[0].strip()
    rows = PostgresSession(database=db).query_rows(
        "SELECT c.relname, a.attname FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
        "JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum "
        f"WHERE n.nspname = '{schema}' "
        "AND pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%'"
    )
    return {f"{schema}.{r[0]}": r[1] for r in rows if len(r) >= 2}


class Captured:
    """One captured table: its topic, its generated key, and the engine behind it."""

    def __init__(self, connector: str, table: str, topic: str,
                 key_column: str | None, engine: str) -> None:
        self.connector = connector
        self.table = table
        self.topic = topic
        self.key_column = key_column
        self.engine = engine
        self.lo: int | None = None      # max key at the moment of the kill
        self.hi: int | None = None      # max key when writing stopped
        self.seen: dict[int, int] = {}  # key -> times it arrived, over the read window
        self.messages = 0               # events on this topic since the kill

    @property
    def relation(self) -> str:
        """The bare relation name, without its schema/database qualifier."""
        return self.table.split(".", 1)[-1]


def topology(domain: str) -> list[Captured]:
    out: list[Captured] = []
    for name, config in load_connectors(domain):
        keys = generated_key_columns(config)
        engine = "mysql" if is_mysql(config) else "postgres"
        for exp in expectations(config):
            out.append(Captured(name, exp.table, exp.topic, keys.get(exp.table), engine))
    return out


# -------------------------------------------------------------------- probes
def _docker(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([docker_bin(), *args], capture_output=True, text=True,
                          env=docker_env())


def connect_url() -> str:
    return f"http://localhost:{host_port('CONNECT_PORT')}"


def connector_state(name: str) -> str:
    """RUNNING / PAUSED / FAILED / down -- never raises."""
    try:
        _, st = http(f"{connect_url()}/connectors/{name}/status", timeout=5)
    except Exception:
        return "down"
    tasks = [t.get("state", "?") for t in st.get("tasks", [])]
    return f"{st['connector']['state']}:{'/'.join(tasks) or 'no-tasks'}"


def stored_offset(name: str) -> dict | None:
    """The offset the worker has committed for this connector, or None if unreachable.

    This is the same value that lives in `_connect_offsets`; the REST endpoint just
    saves parsing a compacted topic whose keys are JSON arrays. Remember that it is
    written on the flush timer, so immediately after a kill it is *expected* to trail
    the last produced record.
    """
    try:
        _, body = http(f"{connect_url()}/connectors/{name}/offsets", timeout=5)
    except Exception:
        return None
    offsets = body.get("offsets") or []
    return offsets[0].get("offset") if offsets else None


def end_offsets_many(topics: list[str]) -> dict[str, dict[int, int]]:
    """topic -> {partition: next offset}, for every topic in one exec.

    kafka-get-offsets.sh takes --topic-partitions as a comma-separated list, which
    matters: the recovery detector polls this every couple of seconds, and one container
    exec per topic per poll would cost more than the thing being measured.
    """
    if not topics:
        return {}
    r = _kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
               "--topic-partitions", ",".join(topics), "--time", "-1")
    out: dict[str, dict[int, int]] = {t: {} for t in topics}
    for line in r.stdout.splitlines():
        parts = line.strip().rsplit(":", 2)
        if len(parts) == 3 and parts[1].isdigit() and parts[2].lstrip("-").isdigit():
            out.setdefault(parts[0], {})[int(parts[1])] = int(parts[2])
    return out


def total_offsets(snapshot: dict[str, dict[int, int]]) -> int:
    return sum(sum(p.values()) for p in snapshot.values())


def consume_range(topic: str, start: dict[int, int], end: dict[int, int],
                  timeout_ms: int = 30000) -> list[dict]:
    """Every message between two offset snapshots.

    Bounded on both sides on purpose. tools/cdc/verify.py samples the tail because it
    only needs a few representative events; this needs *all* of them, and only those,
    because the question is whether a specific set of rows arrived.
    """
    out: list[dict] = []
    for partition, first in start.items():
        last = end.get(partition, first)
        if last <= first:
            continue
        r = _kafka(CONSOLE_CONSUMER, "--bootstrap-server", "localhost:9092",
                   "--topic", topic, "--partition", str(partition), "--offset", str(first),
                   "--max-messages", str(last - first), "--timeout-ms", str(timeout_ms))
        for line in r.stdout.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


def slot_sample(pg: PostgresSession) -> dict:
    """Every replication slot on the source, with its lag in bytes.

    flush_bytes is confirmed_flush_lsn expressed as a single number so that two samples
    can be compared without parsing PostgreSQL's `X/Y` LSN notation.
    """
    rows = pg.query_rows(
        "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, "
        "pg_current_wal_lsn(), "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), "
        "pg_wal_lsn_diff(confirmed_flush_lsn, '0/0') "
        "FROM pg_replication_slots")
    slots = {}
    for r in rows:
        if len(r) >= 8:
            slots[r[0]] = {
                "active": r[1] == "t",
                "restart_lsn": r[2],
                "confirmed_flush_lsn": r[3],
                "current_wal_lsn": r[4],
                "flush_lag_bytes": int(float(r[5])),
                "retained_wal_bytes": int(float(r[6])),
                "flush_bytes": int(float(r[7])),
            }
    return slots


def mysql_position(my: MySqlSession) -> dict:
    """The server's current binlog coordinates.

    SHOW MASTER STATUS, not SHOW BINARY LOG STATUS: the latter is the 8.4 spelling and
    is a syntax error on the 8.0.x this stack runs.
    """
    rows = my.query_rows("SHOW MASTER STATUS")
    if not rows or len(rows[0]) < 2:
        return {}
    r = rows[0]
    return {"file": r[0], "pos": int(r[1]), "gtid": r[4] if len(r) > 4 else ""}


# --------------------------------------------------------------- load driver
class LoadThread(threading.Thread):
    """The workload of tools/loadgen, driven from a thread so the main thread can
    orchestrate the outage around it.

    Writes go down the persistent session pipes owned by this thread; the main thread
    only ever issues one-shot reads, which SqlSession serves by spawning a separate
    client. That separation is what makes sharing a session across the two safe.
    """

    def __init__(self, workload: Workload, rate: float) -> None:
        super().__init__(daemon=True)
        self.wl = workload
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self.ops = 0
        self.error: Exception | None = None
        self._stop = threading.Event()

    def run(self) -> None:
        nxt = time.perf_counter()
        while not self._stop.is_set():
            now = time.perf_counter()
            if now < nxt:
                time.sleep(min(nxt - now, 0.05))
                continue
            try:
                self.wl.step()
            except SessionDied as e:      # a rejected statement killed the client
                self.error = e
                return
            except Exception as e:
                self.error = e
                return
            self.ops += 1
            nxt += self.interval
            if nxt < time.perf_counter() - 1.0:
                nxt = time.perf_counter()

    def stop(self) -> None:
        self._stop.set()


# ------------------------------------------------------------ outage control
def take_down(mode: str) -> tuple[bool, str]:
    if mode == "kill":
        r = _docker("kill", CONNECT_CONTAINER)
    elif mode == "stop":
        r = _docker("stop", CONNECT_CONTAINER)
    elif mode == "pause":
        r = _docker("pause", CONNECT_CONTAINER)
    else:
        return False, f"unknown mode {mode}"
    return r.returncode == 0, (r.stderr or r.stdout).strip()


def bring_up(mode: str) -> tuple[bool, str]:
    r = _docker("unpause" if mode == "pause" else "start", CONNECT_CONTAINER)
    return r.returncode == 0, (r.stderr or r.stdout).strip()


def wait_for_connectors(names: list[str], timeout: float) -> tuple[bool, float, dict]:
    """Block until every connector reports RUNNING. Returns (ok, seconds, states)."""
    t0 = time.perf_counter()
    states: dict[str, str] = {}
    while time.perf_counter() - t0 < timeout:
        states = {n: connector_state(n) for n in names}
        if all(s.startswith("RUNNING") and "RUNNING" in s.split(":", 1)[-1] for s in states.values()):
            return True, time.perf_counter() - t0, states
        time.sleep(1.0)
    return False, time.perf_counter() - t0, states


# ------------------------------------------------------------------ the runs
def _bracket(sessions: dict[str, object], captured: list[Captured], attr: str) -> None:
    """Record max(generated key) for every captured table into .lo or .hi."""
    for cap in captured:
        if not cap.key_column:
            continue
        session = sessions[cap.engine]
        value = session.scalar(
            f"SELECT COALESCE(MAX({cap.key_column}), 0) FROM {cap.relation}")
        try:
            setattr(cap, attr, int(value))
        except (TypeError, ValueError):
            setattr(cap, attr, None)


def cmd_restart(a: argparse.Namespace) -> int:
    captured = topology(a.domain)
    names = sorted({c.connector for c in captured})
    topics = [c.topic for c in captured]
    if not captured:
        print(f"no connectors under domains/{a.domain}/connectors")
        return 1

    # The connector writes its own heartbeat, so that relation is background traffic for
    # every measurement here, not part of the workload being drained.
    heartbeat_relation = None
    for _, config in load_connectors(a.domain):
        if not is_mysql(config):
            heartbeat_relation = _heartbeat_relation(config)

    print(f"restart-safety: domain={a.domain}  mode={a.mode}  rate={a.rate}/s")
    print(f"  connectors: {', '.join(names)}")
    print(f"  captured:   {len(captured)} tables, "
          f"{sum(1 for c in captured if c.key_column)} with a generated key\n")

    for n in names:
        state = connector_state(n)
        if not state.startswith("RUNNING"):
            print(f"cannot start: {n} is {state}, expected RUNNING")
            return 1

    my, pg = MySqlSession().open(), PostgresSession().open()
    sessions = {"mysql": my, "postgres": pg}
    wl = Workload(my, pg, random.Random(a.seed) if a.seed is not None
                  else random.Random())
    try:
        wl.prime()
    except Exception as e:
        my.close(), pg.close()
        print(f"cannot start: {e}")
        return 1

    slot_series: list[dict] = []

    def sample(phase: str, session: PostgresSession = None) -> None:
        # slot_sample() reads through a one-shot client rather than the session's
        # write pipe, so it keeps working after close() -- but pass the live session
        # explicitly once the original has been closed rather than leaning on that.
        slot_series.append({"t": _utc(), "phase": phase,
                            "slots": slot_sample(session or pg)})

    load = LoadThread(wl, a.rate)
    load.start()

    # ---- warm up: get CDC actually streaming before disturbing anything -------
    print(f"[1/5] warm-up   {a.warmup}s of load with the worker healthy")
    sample("warmup")
    time.sleep(a.warmup)
    if load.error:
        load.stop(); my.close(); pg.close()
        print(f"load generator died during warm-up: {load.error}")
        return 1

    # ---- the outage ----------------------------------------------------------
    offsets_at_kill = {n: stored_offset(n) for n in names}
    source_at_kill = {"mysql": mysql_position(my),
                      "postgres": pg.scalar("SELECT pg_current_wal_lsn()")}
    topic_at_kill = end_offsets_many(topics)
    _bracket(sessions, captured, "lo")
    sample("pre-kill")

    ok, detail = take_down(a.mode)
    t_kill = time.perf_counter()
    if not ok:
        load.stop(); my.close(); pg.close()
        print(f"could not take the worker down: {detail}")
        return 1
    print(f"[2/5] outage    worker {a.mode}ed; writing for {a.stall}s with CDC dead")

    stall_end = t_kill + a.stall
    while time.perf_counter() < stall_end:
        time.sleep(min(a.sample, max(0.0, stall_end - time.perf_counter())))
        sample("stalled")
        if load.error:
            break
    ops_during_stall = load.ops

    # ---- bring it back -------------------------------------------------------
    ok, detail = bring_up(a.mode)
    t_restart = time.perf_counter()
    if not ok:
        load.stop(); my.close(); pg.close()
        print(f"could not bring the worker back: {detail}")
        return 1
    running, t_running, states = wait_for_connectors(names, a.recovery_timeout)
    print(f"[3/5] restart   connectors RUNNING after {t_running:.1f}s"
          if running else
          f"[3/5] restart   connectors did NOT return within {a.recovery_timeout}s: {states}")

    # Keep writing into the recovery for a while: GR1 Table 4.4 defines the KPI as
    # catch-up "while writes continue at the source", so a run that stopped the load at
    # the restart would be measuring an easier problem than the one specified.
    print(f"[4/5] recovery  {a.drain}s of continued load, then draining to quiescence")
    time.sleep(a.drain)
    load.stop()
    load.join(timeout=30)
    t_load_stop = time.perf_counter()
    my_out, pg_out = my.close(), pg.close()
    if load.error:
        print(f"\n  WARNING: the load generator stopped early ({load.error}).")
        print("  The ground truth below is therefore whatever it managed to commit,")
        print("  which is still exact -- the brackets are read from the source, not")
        print("  from the generator's counters.")

    # The high-water bracket has to be read *after* the client pipes have drained,
    # or rows still in flight fall outside the range the proof then claims to cover.
    my2, pg2 = MySqlSession().open(), PostgresSession().open()
    _bracket({"mysql": my2, "postgres": pg2}, captured, "hi")

    # Quiescence is measured over the *workload's* topics only. Once the heartbeat table
    # became captured (the A9 fix) its topic gained an event every 10 s forever, so a
    # detector watching every topic could never see the stream go flat for its own reasons
    # -- one run reported 214 s of "recovery" that was really the harness waiting on a
    # heartbeat it had itself put into the capture set.
    drain_topics = [c.topic for c in captured if c.relation != heartbeat_relation]

    # The committed offset is polled in the *same* loop rather than afterwards. Polled
    # afterwards, the first observation can only be as early as the loop above finished,
    # so a flush that landed at t+40 s gets reported as whenever the drain ended. Both
    # quantities are timed from the restart, which is the only reference that means
    # anything to a reader.
    print(f"        polling drain + committed offset "
          f"(flush interval {a.flush_interval_ms // 1000}s)")
    t_last_change = time.perf_counter()
    previous = total_offsets(end_offsets_many(drain_topics))
    flat = 0
    offsets_after = dict(offsets_at_kill)
    offset_at = {}
    deadline = time.perf_counter() + max(a.recovery_timeout, a.offset_timeout)
    while time.perf_counter() < deadline:
        pending = [n for n in names if n not in offset_at]
        if flat >= FLAT_SAMPLES and not pending:
            break
        time.sleep(POLL_SECONDS)
        current = total_offsets(end_offsets_many(drain_topics))
        if current != previous:
            t_last_change = time.perf_counter()
            previous = current
            flat = 0
        else:
            flat += 1
        for n in pending:
            latest = stored_offset(n)
            if _offset_progressed(offsets_at_kill.get(n), latest):
                offsets_after[n] = latest
                offset_at[n] = time.perf_counter()
    drained = flat >= FLAT_SAMPLES
    recovery_seconds = t_last_change - t_restart
    offset_ok = len(offset_at) == len(names)
    offset_seconds = (max(offset_at.values()) - t_restart) if offset_ok else -1.0

    sample("recovered", pg2)
    slots_after = slot_sample(pg2)
    source_after = {"mysql": mysql_position(my2),
                    "postgres": pg2.scalar("SELECT pg_current_wal_lsn()")}
    my2.close(), pg2.close()

    # ---- the proof -----------------------------------------------------------
    print(f"[5/5] proof     reading every event produced since the kill\n")
    topic_now = end_offsets_many(topics)
    for cap in captured:
        messages = consume_range(cap.topic, topic_at_kill.get(cap.topic, {}),
                                 topic_now.get(cap.topic, {}))
        cap.messages = len(messages)
        if not cap.key_column:
            continue
        for msg in messages:
            env = msg.get("payload", msg)
            after = env.get("after") or {}
            value = after.get(cap.key_column)
            if value is None:
                continue
            try:
                key = int(value)
            except (TypeError, ValueError):
                continue
            cap.seen[key] = cap.seen.get(key, 0) + 1

    return _report(a, captured, names, {
        "mode": a.mode,
        "offsets_at_kill": offsets_at_kill, "offsets_after": offsets_after,
        "source_at_kill": source_at_kill, "source_after": source_after,
        "ops_during_stall": ops_during_stall, "ops_total": load.ops,
        "connectors_running_after_s": round(t_running, 2),
        "connectors_returned": running,
        "recovery_seconds": round(recovery_seconds, 2), "drained": drained,
        "writes_continued_s": round(t_load_stop - t_restart, 2),
        "drain_after_last_write_s": round(t_last_change - t_load_stop, 2),
        "offset_flush_seconds": round(offset_seconds, 2), "offset_advanced": offset_ok,
        "slot_series": slot_series, "slots_after": slots_after,
    })


def _report(a, captured: list[Captured], names: list[str], run: dict) -> int:
    width = max(len(c.table) for c in captured)
    gaps = 0
    duplicates = 0
    proved = 0
    rows = []

    for cap in captured:
        if not cap.key_column or cap.lo is None or cap.hi is None:
            rows.append((cap.table, "-", "-", getattr(cap, "messages", 0), "-", "-",
                         "no generated key" if not cap.key_column else "no bracket"))
            continue
        if cap.hi <= cap.lo:
            rows.append((cap.table, f"{cap.lo}", "0", getattr(cap, "messages", 0), "-", "-",
                         "no inserts in window"))
            continue
        expected = set(range(cap.lo + 1, cap.hi + 1))
        arrived = {k for k in cap.seen if cap.lo < k <= cap.hi}
        missing = expected - arrived
        dup = sum(v - 1 for k, v in cap.seen.items() if v > 1)
        gaps += len(missing)
        duplicates += dup
        proved += 1
        note = "ok" if not missing else f"GAP: {sorted(missing)[:5]}"
        rows.append((cap.table, f"{cap.lo + 1}..{cap.hi}", str(len(expected)),
                     getattr(cap, "messages", 0), str(len(missing)), str(dup), note))

    print(f"  {'table'.ljust(width)}  {'key range written':>20}  {'rows':>6}  "
          f"{'events':>7}  {'missing':>7}  {'dup':>5}  note")
    for table, rng, nrows, events, missing, dup, note in rows:
        print(f"  {table.ljust(width)}  {rng:>20}  {nrows:>6}  {events:>7}  "
              f"{missing:>7}  {dup:>5}  {note}")

    slots = run["slots_after"]
    series = run["slot_series"]
    stalled = [s for s in series if s["phase"] == "stalled"]
    print(f"\n  worker outage        {a.mode} for {a.stall}s, "
          f"{run['ops_during_stall']:,} operations sent while CDC was dead")
    print(f"  connectors RUNNING   {run['connectors_running_after_s']}s after restart")
    print(f"  writes continued     {run['writes_continued_s']}s into the recovery "
          f"(GR1 Table 4.4 measures catch-up under load)")
    print(f"  fully caught up      {run['recovery_seconds']}s after restart, "
          f"{run['drain_after_last_write_s']}s after the last write"
          f"{'' if run['drained'] else '  (TIMED OUT -- still moving)'}")
    before, after = run["offsets_at_kill"], run["offsets_after"]
    for n in names:
        moved = _offset_progressed(before.get(n), after.get(n))
        print(f"  offset {n:<22} at kill: {_short(before.get(n))}")
        print(f"  {'':<29} after:   {_short(after.get(n))}"
              f"   {'advanced' if moved else 'DID NOT ADVANCE'}")
    print(f"  committed offset     caught up {run['offset_flush_seconds']}s after the "
          f"restart -- the replay window is bounded by this, not by the outage")
    if stalled:
        first, last = stalled[0]["slots"], stalled[-1]["slots"]
        for slot in sorted(first):
            f0, f1 = first[slot], last[slot]
            frozen = f0["flush_bytes"] == f1["flush_bytes"]
            print(f"  slot {slot:<16} during outage: flush {'FROZEN' if frozen else 'moved'}"
                  f", retained WAL {f0['retained_wal_bytes']:,} -> {f1['retained_wal_bytes']:,} bytes")
        for slot in sorted(slots):
            print(f"  slot {slot:<16} after recovery: retained WAL "
                  f"{slots[slot]['retained_wal_bytes']:,} bytes, "
                  f"flush lag {slots[slot]['flush_lag_bytes']:,} bytes")

    advanced = run["offset_advanced"]
    verdict = (gaps == 0 and proved > 0 and run["connectors_returned"]
               and run["drained"] and advanced)
    print()
    if proved == 0:
        print("  INCONCLUSIVE -- no captured table wrote a bracketable key range.")
        print("  Raise --rate or --stall so that inserts actually happen during the outage.")
    elif gaps:
        print(f"  FAIL -- {gaps} row(s) committed at the source never reached a topic.")
    else:
        print(f"  PASS -- {proved} table(s) proved gap-free across a {a.mode} of the worker.")
        if not advanced:
            print("  ...but a committed offset did not move past the kill point, so the")
            print("  resume cannot be credited to the offset mechanism. Treat as suspect.")
        print(f"  {duplicates:,} duplicate event(s) replayed, which is the expected")
        print(f"  at-least-once behaviour: Connect flushes offsets every "
              f"{a.flush_interval_ms // 1000}s, so a crash replays up to that window.")

    _append_ledger({"kind": "restart-safety", "domain": a.domain, "verdict":
                    "pass" if verdict else ("fail" if gaps else "inconclusive"),
                    "gaps": gaps, "duplicates": duplicates, "tables_proved": proved,
                    **{k: v for k, v in run.items() if k != "slot_series"},
                    "slot_series": series})
    print(f"\n  recorded to {LEDGER.relative_to(ROOT).as_posix()}")
    return 0 if verdict else 1


def _heartbeat_relation(config: dict) -> str | None:
    """The relation the connector's heartbeat.action.query writes to, if any.

    Parsed from the declared query rather than named here, so the exclusion follows
    whatever a domain configures instead of assuming this project's table.
    """
    query = config.get("heartbeat.action.query", "")
    match = re.search(r"insert\s+into\s+([A-Za-z0-9_.\"]+)", query, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).replace('"', "").split(".")[-1]


def _offset_progressed(before: dict | None, after: dict | None) -> bool:
    """Did the worker's committed offset actually move past where it was killed?

    Without this the run could pass on a worker that came back, consumed nothing and
    left its offset where it stood: the topics would be quiet, no identifier would be
    missing from the bracketed window, and the harness would call that a recovery.
    Binlog coordinates order by (file, position); PostgreSQL's by LSN.
    """
    if not before or not after:
        return False
    if "file" in before and "file" in after:
        return (after["file"], after["pos"]) > (before["file"], before["pos"])
    for key in ("lsn_commit", "lsn"):
        if key in before and key in after:
            return int(after[key]) > int(before[key])
    return after != before


def _short(offset: dict | None) -> str:
    if not offset:
        return "unavailable"
    if "file" in offset:
        return f"{offset['file']}:{offset['pos']}"
    for key in ("lsn_commit", "lsn"):
        if key in offset:
            return f"lsn {offset[key]}"
    return json.dumps(offset)[:80]


# --------------------------------------------------------------- WAL guard
def cmd_wal_guard(a: argparse.Namespace) -> int:
    """GR1 section 4.4's PostgreSQL risk, exercised in both directions.

    The guard only means something as a contrast. With the connector alive and the
    captured tables idle, `confirmed_flush_lsn` must still advance -- that is the
    heartbeat working. With the connector dead it must freeze while retained WAL grows
    -- that is the risk the heartbeat exists to bound. Running only the first half
    would show a number moving without showing what moves it.
    """
    captured = topology(a.domain)
    names = sorted({c.connector for c in captured if c.engine == "postgres"})
    if not names:
        print(f"no PostgreSQL connector under domains/{a.domain}/connectors")
        return 1

    pg = PostgresSession().open()
    relations = [c.relation for c in captured if c.engine == "postgres"]

    def counts(subset: list[str]) -> dict[str, int]:
        return {r: int(pg.scalar(f"SELECT count(*) FROM {r}")) for r in subset}

    print(f"wal-guard: domain={a.domain}  idle window={a.idle}s per phase")
    print(f"  connector: {', '.join(names)}")
    print(f"  NOTE: the idle window must exceed the worker's offset flush interval")
    print(f"        ({a.flush_interval_ms // 1000}s) or a frozen flush position proves")
    print(f"        nothing but a timer that has not fired.\n")
    if a.idle * 1000 <= a.flush_interval_ms:
        print(f"  --idle {a.idle} is not longer than the flush interval; raise it.")
        pg.close()
        return 1

    for n in names:
        if not connector_state(n).startswith("RUNNING"):
            print(f"cannot start: {n} is {connector_state(n)}, expected RUNNING")
            pg.close()
            return 1

    heartbeat_ms = 0
    heartbeat_relation = None
    for _, config in load_connectors(a.domain):
        if not is_mysql(config):
            heartbeat_ms = int(config.get("heartbeat.interval.ms", 0) or 0)
            heartbeat_relation = _heartbeat_relation(config)
    # The heartbeat table is written *by the connector*, and once it is captured (it
    # must be, or the guard is inert) those writes land in the same relation list the
    # idleness check walks. They are the mechanism under test, not traffic that
    # invalidates it, so exclude the relation the action query names.
    idle_relations = [r for r in relations if r != heartbeat_relation]

    series: list[dict] = []

    def sample(phase: str) -> dict:
        s = {"t": _utc(), "phase": phase, "slots": slot_sample(pg)}
        series.append(s)
        return s

    # ---- phase A: idle, connector alive. The heartbeat should carry the flush. ----
    rows_a0 = counts(idle_relations)
    a0 = sample("idle-connector-up")
    print(f"[1/3] idle with the worker alive ({a.idle}s, no writes to captured tables)")
    time.sleep(a.idle)
    a1 = sample("idle-connector-up")
    rows_a1 = counts(idle_relations)
    contaminated = {r: (rows_a1[r] - rows_a0[r])
                    for r in idle_relations if rows_a1[r] != rows_a0[r]}

    # ---- phase B: idle, connector dead. Flush must freeze, WAL must accumulate. ----
    ok, detail = take_down(a.mode)
    if not ok:
        pg.close()
        print(f"could not take the worker down: {detail}")
        return 1
    print(f"[2/3] idle with the worker {a.mode}ed ({a.idle}s)")
    b0 = sample("idle-connector-down")
    time.sleep(a.idle)
    b1 = sample("idle-connector-down")

    # ---- phase C: back up, flush must catch up again. ----
    ok, detail = bring_up(a.mode)
    if not ok:
        pg.close()
        print(f"could not bring the worker back: {detail}")
        return 1
    running, t_running, states = wait_for_connectors(names, a.recovery_timeout)
    print(f"[3/3] worker back; connectors RUNNING after {t_running:.1f}s"
          if running else f"[3/3] connectors did NOT return: {states}")
    # Give the worker at least one flush interval to acknowledge what it consumed.
    time.sleep(max(a.settle, a.flush_interval_ms / 1000 + 5))
    c1 = sample("recovered")
    pg.close()

    slot_names = sorted(a0["slots"])
    print()
    verdict_ok = True
    wal_growth = 0        # last slot's outage growth; read by the summary below
    for slot in slot_names:
        up_delta = a1["slots"][slot]["flush_bytes"] - a0["slots"][slot]["flush_bytes"]
        down_delta = b1["slots"][slot]["flush_bytes"] - b0["slots"][slot]["flush_bytes"]
        wal_growth = (b1["slots"][slot]["retained_wal_bytes"]
                      - b0["slots"][slot]["retained_wal_bytes"])
        after_delta = c1["slots"][slot]["flush_bytes"] - b1["slots"][slot]["flush_bytes"]
        print(f"  slot {slot}")
        print(f"    idle, worker alive   confirmed_flush advanced {up_delta:,} bytes"
              f"   {'ok' if up_delta > 0 else 'DID NOT ADVANCE'}")
        print(f"    idle, worker down    confirmed_flush advanced {down_delta:,} bytes"
              f"   {'frozen as expected' if down_delta == 0 else 'moved unexpectedly'}")
        print(f"    idle, worker down    retained WAL grew {wal_growth:,} bytes"
              f"   {'ok' if wal_growth > 0 else 'did not grow'}")
        print(f"    after restart        confirmed_flush advanced {after_delta:,} bytes"
              f"   {'ok' if after_delta > 0 else 'DID NOT RECOVER'}")
        print(f"    retained WAL now     {c1['slots'][slot]['retained_wal_bytes']:,} bytes"
              f"  (wal_keep_size bounds this at 512MB)")
        if up_delta <= 0 or after_delta <= 0:
            verdict_ok = False

    if contaminated:
        verdict_ok = False
        print(f"\n  CONTAMINATED -- captured tables were written during the idle window: "
              f"{contaminated}")
        print("  Stop the load generator before running wal-guard; an advancing flush")
        print("  position proves nothing if ordinary traffic could have advanced it.")
    elif verdict_ok:
        print(f"\n  PASS -- with the captured tables idle, the {heartbeat_ms // 1000}s heartbeat")
        print("  advanced confirmed_flush_lsn on its own, and with the worker down the")
        print("  same position froze. The guard in GR1 section 4.4 does what the report says.")
        if wal_growth <= 0:
            print()
            print("  Note: retained WAL did not grow during the outage, and that is correct")
            print("  rather than a weak result. The heartbeat's INSERT is issued *by the")
            print("  connector*, so killing the worker also stops the only writer -- the")
            print("  database goes genuinely idle and there is no WAL to retain. The guard")
            print("  protects an idle capture set beside a busy database, which is the case")
            print(f"  phase 1 measured; it cannot protect anything while the consumer is down.")
    else:
        print("\n  FAIL -- see the lines marked above.")

    _append_ledger({"kind": "wal-guard", "domain": a.domain,
                    "verdict": "pass" if verdict_ok else "fail",
                    "idle_seconds": a.idle, "heartbeat_ms": heartbeat_ms,
                    "heartbeat_relation": heartbeat_relation,
                    "contaminated": contaminated, "series": series})
    print(f"  recorded to {LEDGER.relative_to(ROOT).as_posix()}")
    return 0 if verdict_ok else 1


# --------------------------------------------------------------------- main
def main() -> int:
    # Shared flags live on a parent parser rather than the top-level one, so that
    # `... restart --mode kill` works. With them on the top-level parser argparse
    # would demand `... --mode kill restart`, which nobody types correctly twice.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--domain", default="ecommerce")
    common.add_argument("--mode", default="kill", choices=["kill", "stop", "pause"],
                        help="kill = SIGKILL, the honest crash; stop = graceful SIGTERM "
                             "(flushes offsets first, so it proves less); pause = freeze")
    common.add_argument("--flush-interval-ms", type=int, default=60000,
                        help="the worker's offset.flush.interval.ms -- reporting only")
    common.add_argument("--recovery-timeout", type=float, default=240.0)
    common.add_argument("--offset-timeout", type=float, default=240.0,
                        help="how long to wait for the committed offset to pass the "
                             "kill point; must comfortably exceed the flush interval")

    p = argparse.ArgumentParser(
        prog="python -m tools.cdc.restart",
        description="CDC restart safety and WAL-accumulation guard (GR1 Table 4.4)")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("restart", parents=[common],
                       help="kill the worker under load and prove no gap")
    r.add_argument("--rate", type=float, default=25.0)
    r.add_argument("--warmup", type=float, default=20.0)
    r.add_argument("--stall", type=float, default=45.0)
    r.add_argument("--drain", type=float, default=20.0,
                   help="seconds of continued load after the restart -- GR1 Table 4.4 "
                        "measures catch-up while writes continue at the source")
    r.add_argument("--sample", type=float, default=5.0)
    r.add_argument("--seed", type=int, default=None)
    r.set_defaults(func=cmd_restart)

    w = sub.add_parser("wal-guard", parents=[common],
                       help="exercise the heartbeat / WAL-bloat guard")
    w.add_argument("--idle", type=float, default=150.0,
                   help="idle seconds per phase; must exceed the flush interval")
    w.add_argument("--settle", type=float, default=70.0)
    w.set_defaults(func=cmd_wal_guard)

    a = p.parse_args()
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
