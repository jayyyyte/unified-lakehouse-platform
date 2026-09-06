"""Verify what Debezium actually put on Kafka, against what the connector promised.

    python -m tools.cdc.verify --domain ecommerce [--messages 200] [--require-update]

This is the evidence step for ADR 0004 and for GR1 Table 4.5. The claim being
tested is not "masking is configured" -- that is visible in the JSON -- but:

  1. every column named in `column.mask.hash.*` arrives hashed in the **after**
     image *and in the before image of every UPDATE*. The before image is the
     whole argument of ADR 0004: an SMT chain masks only `after`, so the previous
     value of a PII column leaks on every update. Only an UPDATE event can prove
     this, which is why --require-update exists and why the load generator has to
     have run.
  2. every column in `column.exclude.list` is absent outright -- not null, not
     hashed, absent.
  3. the Debezium envelope survives (op / before / after / source), because GR1
     section 4.2 requires Bronze to store it unflattened.

Expectations are derived from the connector config itself rather than hard-coded,
so this stays domain-agnostic: point it at another domain's connectors and it
checks whatever that domain declared.

One expectation cannot be read off the config, and finding that out is the reason
this tool exists. Debezium truncates the hash to the **declared width of the source
column**, silently: `orders.buyer_phone VARCHAR(32)` yields 32 hex characters, half
a SHA-256 digest, while `orders.buyer_email VARCHAR(255)` yields all 64. So the
expected digest length is min(64, column width), read from information_schema at
verification time -- and a column narrower than the digest is reported as a
weakened mask rather than quietly accepted. See docs/adr/0004.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from mosaic import COMPOSE, docker_bin, docker_env  # noqa: E402
from tools.dbsession import MySqlSession, PostgresSession  # noqa: E402

HEX = re.compile(r"^[0-9a-f]+$")
DIGEST_HEX_LEN = 64          # SHA-256, hex encoded
MASK_KEY = re.compile(r"^column\.mask\.hash\.([A-Za-z0-9-]+)\.with\.salt\.(.+)$")
ENVELOPE_KEYS = {"op", "before", "after", "source"}
CONSOLE_CONSUMER = "/opt/kafka/bin/kafka-console-consumer.sh"


class Expectation:
    """What one captured table's topic should look like."""

    def __init__(self, topic: str, table: str) -> None:
        self.topic = topic
        self.table = table
        self.masked: set[str] = set()
        self.excluded: set[str] = set()
        self.hash_len: dict[str, int] = {}   # column -> expected hex digest length


def column_widths(config: dict) -> dict[tuple[str, str], int | None]:
    """(table, column) -> declared character width, or None for unbounded types.

    Read from the live source rather than parsed out of the DDL: the width that
    decides how much of the digest survives is whatever the running database says
    it is, not whatever a checked-in file hoped for.
    """
    connector = config.get("connector.class", "")
    if "mysql" in connector.lower():
        db = config.get("database.include.list", "").split(",")[0].strip()
        session, schema_col, schema = MySqlSession(database=db), "table_schema", db
    else:
        db = config.get("database.dbname", "")
        schema = config.get("schema.include.list", "public").split(",")[0].strip()
        session, schema_col = PostgresSession(database=db), "table_schema"
    rows = session.query_rows(
        "SELECT table_name, column_name, COALESCE(character_maximum_length, -1) "
        f"FROM information_schema.columns WHERE {schema_col} = '{schema}'"
    )
    out: dict[tuple[str, str], int | None] = {}
    for r in rows:
        if len(r) >= 3:
            width = int(r[2])
            out[(f"{schema}.{r[0]}", r[1])] = None if width < 0 else width
    return out


def expectations(config: dict) -> list[Expectation]:
    prefix = config["topic.prefix"]
    tables = [t.strip() for t in config.get("table.include.list", "").split(",") if t.strip()]
    exps = {t: Expectation(f"{prefix}.{t}", t) for t in tables}

    def attach(spec: str, bucket: str) -> None:
        for fq in (s.strip() for s in spec.split(",")):
            if not fq:
                continue
            table, _, column = fq.rpartition(".")
            if table in exps:
                getattr(exps[table], bucket).add(column)

    for key, value in config.items():
        if MASK_KEY.match(key):
            attach(value, "masked")
    attach(config.get("column.exclude.list", ""), "excluded")

    widths = column_widths(config) if any(e.masked for e in exps.values()) else {}
    for exp in exps.values():
        for column in exp.masked:
            width = widths.get((exp.table, column), None)
            exp.hash_len[column] = (DIGEST_HEX_LEN if width is None
                                    else min(DIGEST_HEX_LEN, width))
    return list(exps.values())


def _kafka(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [docker_bin(), "compose", "-f", str(COMPOSE), "exec", "-T", "kafka", *args],
        capture_output=True, text=True, env=docker_env(),
    )


def end_offsets(topic: str) -> dict[int, int]:
    """partition -> next offset to be written."""
    r = _kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
               "--topic", topic, "--time", "-1")
    out: dict[int, int] = {}
    for line in r.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[1].isdigit() and parts[2].lstrip("-").isdigit():
            out[int(parts[1])] = int(parts[2])
    return out


def consume(topic: str, max_messages: int, timeout_ms: int, tail: bool = True) -> list[dict]:
    """Sample messages from a topic -- from the tail by default.

    Reading from the beginning samples only the initial snapshot: every event is
    op=r with a null before image, so the masking check that actually matters
    (the before image of an UPDATE) can never fire no matter how many messages are
    read. The interesting events are the most recent ones, so the default is to
    seek to (end - max_messages) on each partition.

    The console consumer exits non-zero when it hits --timeout-ms having read
    fewer than --max-messages, which is normal for a quiet topic. The exit code is
    therefore ignored and the parsed messages are the result -- one of the few
    places where ignoring a return code is correct rather than lazy.
    """
    runs: list[subprocess.CompletedProcess] = []
    offsets = end_offsets(topic) if tail else {}
    if tail and offsets:
        per_partition = max(1, max_messages // len(offsets))
        for partition, end in offsets.items():
            start = max(0, end - per_partition)
            if end == 0:
                continue
            runs.append(_kafka(
                CONSOLE_CONSUMER, "--bootstrap-server", "localhost:9092",
                "--topic", topic, "--partition", str(partition), "--offset", str(start),
                "--max-messages", str(per_partition), "--timeout-ms", str(timeout_ms)))
    else:
        runs.append(_kafka(
            CONSOLE_CONSUMER, "--bootstrap-server", "localhost:9092",
            "--topic", topic, "--from-beginning",
            "--max-messages", str(max_messages), "--timeout-ms", str(timeout_ms)))

    out = []
    for r in runs:
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def list_topics() -> set[str]:
    r = subprocess.run(
        [docker_bin(), "compose", "-f", str(COMPOSE), "exec", "-T", "kafka",
         "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"],
        capture_output=True, text=True, env=docker_env(),
    )
    return {t.strip() for t in r.stdout.splitlines() if t.strip()}


def check_topic(exp: Expectation, messages: list[dict]) -> tuple[list[str], dict]:
    """Return (violations, stats) for one topic."""
    problems: list[str] = []
    stats = {"messages": len(messages), "ops": {}, "before_images": 0,
             "masked_checked": 0, "excluded_checked": 0}

    for msg in messages:
        # Debezium wraps the change event in `payload` when schemas are enabled;
        # with schemas.enable=false (ADR 0005) the envelope is at the top level.
        env = msg.get("payload", msg)
        if not isinstance(env, dict):
            continue
        missing = ENVELOPE_KEYS - set(env)
        if missing:
            problems.append(f"envelope incomplete, missing {sorted(missing)} "
                            f"(GR1 4.2 requires the unflattened envelope)")
            continue
        op = env.get("op", "?")
        stats["ops"][op] = stats["ops"].get(op, 0) + 1

        for image_name in ("before", "after"):
            image = env.get(image_name)
            if not isinstance(image, dict):
                continue
            if image_name == "before":
                stats["before_images"] += 1
            for column in exp.masked:
                if column not in image:
                    problems.append(f"{image_name}.{column} missing but declared masked")
                    continue
                value = image[column]
                stats["masked_checked"] += 1
                if value is None:
                    continue
                want = exp.hash_len.get(column, DIGEST_HEX_LEN)
                text = str(value)
                if not (HEX.match(text) and len(text) == want):
                    problems.append(
                        f"op={op} {image_name}.{column} is not a {want}-char hex digest: "
                        f"{text[:40]!r} (len {len(text)})")
            for column in exp.excluded:
                stats["excluded_checked"] += 1
                if column in image:
                    problems.append(
                        f"op={op} {image_name}.{column} present but declared excluded "
                        f"(value {str(image[column])[:20]!r})")
    return problems, stats


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain", default="ecommerce")
    p.add_argument("--messages", type=int, default=200,
                   help="messages to sample per topic")
    p.add_argument("--timeout-ms", type=int, default=15000)
    p.add_argument("--from-beginning", action="store_true",
                   help="sample the oldest messages (the initial snapshot) instead of "
                        "the most recent ones")
    p.add_argument("--require-update", action="store_true",
                   help="fail unless at least one UPDATE with a before image was "
                        "inspected -- the only proof ADR 0004's claim holds")
    a = p.parse_args()

    conn_dir = ROOT / "domains" / a.domain / "connectors"
    files = sorted(conn_dir.glob("*.json"))
    if not files:
        print(f"no connector configs under {conn_dir}")
        return 1

    topics = list_topics()
    if not topics:
        print("could not list Kafka topics -- is the stack up?")
        return 1

    failed = 0
    truncated = 0
    before_images_total = 0
    updates_total = 0

    for f in files:
        cfg = json.loads(f.read_text(encoding="utf-8"))["config"]
        print(f"\n{f.name}  (topic prefix {cfg['topic.prefix']})")
        for exp in expectations(cfg):
            if exp.topic not in topics:
                print(f"  {exp.topic:<44} MISSING -- no such topic")
                failed += 1
                continue
            messages = consume(exp.topic, a.messages, a.timeout_ms,
                               tail=not a.from_beginning)
            problems, stats = check_topic(exp, messages)
            ops = ",".join(f"{k}={v}" for k, v in sorted(stats["ops"].items())) or "none"
            mask_desc = ", ".join(f"{c}({exp.hash_len[c]} hex)" for c in sorted(exp.masked)) or "-"
            declared = f"mask={mask_desc} drop={sorted(exp.excluded) or '-'}"
            verdict = "ok" if not problems and messages else ("EMPTY" if not messages else "FAIL")
            print(f"  {exp.topic:<44} {stats['messages']:>4} msgs  ops[{ops}]  {verdict}")
            print(f"      declared {declared}")
            for column in sorted(exp.masked):
                if exp.hash_len.get(column, DIGEST_HEX_LEN) < DIGEST_HEX_LEN:
                    truncated += 1
                    print(f"      NOTE: {column} is narrower than a SHA-256 digest, so the "
                          f"mask is truncated to {exp.hash_len[column]} hex chars "
                          f"({exp.hash_len[column] * 4} bits)")
            print(f"      checked  {stats['masked_checked']} masked value(s), "
                  f"{stats['excluded_checked']} exclusion(s), "
                  f"{stats['before_images']} before image(s)")
            before_images_total += stats["before_images"]
            updates_total += stats["ops"].get("u", 0)
            for problem in problems[:8]:
                print(f"      VIOLATION: {problem}")
            if len(problems) > 8:
                print(f"      ... {len(problems) - 8} more")
            failed += bool(problems)

    print()
    if updates_total == 0:
        note = ("no UPDATE events were sampled, so masking of the *before* image -- "
                "the entire point of ADR 0004 -- is UNPROVEN by this run.\n"
                "  produce some:  python -m tools.loadgen.run start --rate 10 --duration 60")
        if a.require_update:
            print(f"FAIL: {note}")
            return 1
        print(f"note: {note}")
    else:
        print(f"inspected {updates_total} UPDATE event(s) and "
              f"{before_images_total} before image(s).")

    if truncated:
        print(f"{truncated} masked column(s) are narrower than a SHA-256 digest. The value is\n"
              "  still a one-way hash, but widening the source column to 64 chars is what it\n"
              "  takes to keep the full digest. Recorded in docs/adr/0004.")

    if failed:
        print(f"{failed} topic(s) failed verification.")
        return 1
    print("all sampled topics match what the connectors declared.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
