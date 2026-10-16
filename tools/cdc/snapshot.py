"""Ad-hoc snapshot: re-emit the current state of every captured table as op=r events.

    python -m tools.cdc.snapshot --domain ecommerce [--table public.users ...] [--timeout 900]

Why this exists (CHANGES-FROM-GR1 A10). Kafka's default retention is 7 days
(`log.retention.hours=168`; nothing in this stack overrides it). M1 took the initial
snapshot on 2026-08-28. When M3 came to build Bronze on 2026-09-12, retention had already
deleted it: `categories`, which never changes after seeding, had *zero* events left on its
topic. A Bronze stream bootstrapped from Kafka at that point can never see a row that
existed only in the snapshot. GR1 section 4.2 promises Bronze "full auditability and
replay capability"; that promise has an unstated precondition -- Bronze must already be
landing before retention expires -- and this tool is the recovery when it was not.

Mechanism. Debezium's `execute-snapshot` signal, type BLOCKING, sent over the Kafka signal
channel each connector declares (`signal.kafka.topic`). Blocking rather than incremental:
an incremental snapshot writes open/close watermark rows into a signalling table at the
source, which on MySQL needs an INSERT grant GR1 Appendix A deliberately withholds from the
cdc account. A blocking snapshot writes nothing at the source; its cost is that streaming
pauses while it reads -- seconds, at this scale.

Proof. For each table, the op=r events that appear on its topic after the signal are
counted and compared with the table's row count at the source. With the source idle the
two must match exactly. If a table's row count moved during the run, the comparison is not
meaningful and the table is reported as CHANGED rather than passed.

Domain-free: tables, topics, databases and the signal topic all come from the connectors'
declared config; the heartbeat relation is excluded by parsing `heartbeat.action.query`.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from mosaic import COMPOSE, docker_bin, docker_env  # noqa: E402
from tools.cdc.restart import (_heartbeat_relation, consume_range, end_offsets_many,  # noqa: E402
                               is_mysql, load_connectors)
from tools.cdc.verify import _kafka  # noqa: E402
from tools.dbsession import MySqlSession, PostgresSession  # noqa: E402

LEDGER = ROOT / "experiments" / "results" / "snapshot_events.jsonl"
POLL_SECONDS = 3.0
FLAT_SAMPLES = 4


class Target:
    """One table to re-snapshot, with the connector that owns it."""

    def __init__(self, connector: str, config: dict, table: str) -> None:
        self.connector = connector
        self.config = config
        self.table = table
        self.topic = f"{config['topic.prefix']}.{table}"
        self.rows_before = 0
        self.rows_after = 0
        self.r_events = 0
        self.other_events = 0


def _session(config: dict):
    if is_mysql(config):
        return MySqlSession(database=config.get("database.include.list", "").split(",")[0].strip())
    return PostgresSession(database=config.get("database.dbname", ""))


def row_count(target: Target) -> int:
    return int(_session(target.config).scalar(f"SELECT COUNT(*) FROM {target.table}"))


def targets(domain: str, only: list[str]) -> list[Target]:
    out = []
    for name, config in load_connectors(domain):
        heartbeat = _heartbeat_relation(config)
        for table in (t.strip() for t in config.get("table.include.list", "").split(",")):
            if not table or table.split(".")[-1] == heartbeat:
                continue
            if only and table not in only:
                continue
            out.append(Target(name, config, table))
    return out


def ensure_topic(topic: str) -> None:
    """The signal channel reads partition 0 of a topic that must exist."""
    _kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
           "--create", "--if-not-exists", "--topic", topic,
           "--partitions", "1", "--replication-factor", "1")


def send_signal(config: dict, tables: list[str]) -> subprocess.CompletedProcess:
    """One execute-snapshot signal. The record key must equal the connector's topic.prefix;
    data-collections are regular expressions, hence the escaping."""
    payload = json.dumps({"type": "execute-snapshot",
                          "data": {"data-collections": [re.escape(t) for t in tables],
                                   "type": "BLOCKING"}})
    return subprocess.run(
        [docker_bin(), "compose", "-f", str(COMPOSE), "exec", "-T", "kafka",
         "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092",
         "--topic", config["signal.kafka.topic"],
         "--property", "parse.key=true", "--property", "key.separator=|"],
        input=f"{config['topic.prefix']}|{payload}\n",
        capture_output=True, text=True, env=docker_env())


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", default="ecommerce")
    p.add_argument("--table", action="append", default=[],
                   help="restrict to this captured table (repeatable), e.g. public.users")
    p.add_argument("--timeout", type=float, default=900.0)
    a = p.parse_args()

    todo = targets(a.domain, a.table)
    if not todo:
        print("nothing to snapshot -- no captured tables matched.")
        return 1
    missing = sorted({t.connector for t in todo if not t.config.get("signal.kafka.topic")})
    if missing:
        print(f"connector(s) without a Kafka signal channel: {', '.join(missing)}\n"
              "  add signal.enabled.channels=kafka and signal.kafka.* to the connector JSON,\n"
              "  then re-register:  python mosaic.py connectors")
        return 1

    print(f"ad-hoc BLOCKING snapshot of {len(todo)} table(s), domain {a.domain}\n")
    for t in todo:
        t.rows_before = row_count(t)
    topics = [t.topic for t in todo]
    start = end_offsets_many(topics)

    t0 = time.time()
    by_connector: dict[str, list[Target]] = {}
    for t in todo:
        by_connector.setdefault(t.connector, []).append(t)
    for name, group in by_connector.items():
        config = group[0].config
        ensure_topic(config["signal.kafka.topic"])
        r = send_signal(config, [t.table for t in group])
        state = "sent" if r.returncode == 0 else f"FAILED rc={r.returncode} {r.stderr[-300:]}"
        print(f"  signal -> {config['signal.kafka.topic']:<28} {len(group)} table(s)  {state}")
        if r.returncode != 0:
            return 1

    # Done when every topic has grown by at least its table's row count and the end
    # offsets have then stayed flat for a few polls. Growth alone is not enough -- the
    # last chunk of a large table can still be in flight when the count is first met.
    print("\nwaiting for the snapshot events to land...")
    flat, last, end = 0, None, start
    while time.time() - t0 < a.timeout:
        time.sleep(POLL_SECONDS)
        end = end_offsets_many(topics)
        grown = all(sum(end[t.topic].values()) - sum(start[t.topic].values()) >= t.rows_before
                    for t in todo)
        flat = flat + 1 if end == last else 0
        last = end
        if grown and flat >= FLAT_SAMPLES:
            break
    else:
        print(f"TIMEOUT after {a.timeout:.0f}s -- the snapshot did not complete. "
              "Check: python mosaic.py logs kafka-connect")
    elapsed = time.time() - t0

    print(f"snapshot events landed in {elapsed:.1f}s; counting op=r per topic...\n")
    for t in todo:
        t.rows_after = row_count(t)
        for msg in consume_range(t.topic, start[t.topic], end[t.topic], timeout_ms=60000):
            env = msg.get("payload", msg) if isinstance(msg, dict) else {}
            if env.get("op") == "r":
                t.r_events += 1
            else:
                t.other_events += 1

    failed = 0
    print(f"  {'table':<28} {'source rows':>11} {'op=r events':>11} {'other':>6}  verdict")
    for t in todo:
        if t.rows_before != t.rows_after:
            verdict = f"CHANGED ({t.rows_before}->{t.rows_after} during the run)"
            failed += 1
        elif t.r_events == t.rows_before:
            verdict = "ok"
        else:
            verdict = f"MISMATCH ({t.r_events - t.rows_before:+d})"
            failed += 1
        print(f"  {t.table:<28} {t.rows_before:>11} {t.r_events:>11} {t.other_events:>6}  {verdict}")

    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "recorded_at": datetime.now(timezone.utc).isoformat(), "domain": a.domain,
            "kind": "blocking", "elapsed_s": round(elapsed, 1),
            "verdict": "pass" if not failed else "fail",
            "tables": {t.table: {"topic": t.topic, "rows": t.rows_before,
                                 "rows_after": t.rows_after, "r_events": t.r_events,
                                 "other_events": t.other_events} for t in todo},
        }) + "\n")

    if failed:
        print(f"\nFAIL: {failed} table(s) did not re-emit exactly their current rows.")
        return 1
    print(f"\nPASS: every table re-emitted exactly its current row count ({elapsed:.1f}s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
