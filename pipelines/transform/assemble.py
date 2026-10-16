"""Assemble the dbt project from domains/: generate Silver, copy in each domain's Gold.

    python assemble.py            (the dbt image runs this before every dbt command)

Why generate Silver instead of writing it. Silver, per GR1 section 4.2, is the current
state of each captured table: deduplicated, typed, latest-wins. None of that depends on
what the table *means* -- only on its key, its columns and how Debezium encoded them. All
three are derivable, so hand-writing seven near-identical models per domain would put
domain vocabulary into pipelines/ for no gain, and adding the second domain would mean
editing this directory. Generated, adding a domain touches only domains/ (L3).

Derivation, all from declared or live sources -- the same discipline as tools/cdc:

  captured tables  each connector's topic.prefix + table.include.list, minus the
                   connector's own heartbeat relation (parsed out of
                   heartbeat.action.query; it is CDC plumbing, not data)
  columns, types   the live source catalog, read through Trino's federation catalogs
                   (mysql / postgresql information_schema) -- what the database actually
                   has, not what a DDL file once said
  dropped columns  column.exclude.list            (Debezium never sends them)
  masked columns   column.mask.* / column.truncate.*   (the value is a hash, so: text)
  key              the fields of a real Kafka record key in Bronze -- the key Debezium
                   actually uses, including any message.key.columns override

Gold is domain business logic and stays hand-written, in domains/<domain>/gold/. It is
copied (not linked: Docker Desktop bind mounts do not carry symlinks reliably) into
models/gold/_domains/<domain>/, and domains/<domain>/gold/tests/ into tests/_domains/.

Everything written here is gitignored and rebuilt on every run, so the generated SQL can
always be inspected and never drifts from the connectors.

Python 3.11 standard library only: this runs inside the dbt image.
"""
from __future__ import annotations

import glob
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
DOMAINS = Path(os.environ.get("MOSAIC_DOMAINS_DIR", "/opt/mosaic/domains"))
TRINO = (f"http://{os.environ.get('MOSAIC_TRINO_HOST', 'trino')}:"
         f"{os.environ.get('MOSAIC_TRINO_PORT', '8080')}/v1/statement")
SILVER_DIR = HERE / "models" / "silver" / "_generated"
GOLD_DIR = HERE / "models" / "gold" / "_domains"
TESTS_DIR = HERE / "tests" / "_domains"
BRONZE = "iceberg.bronze.cdc_events"

# Debezium connector class -> the Trino catalog that federates that source engine.
ENGINE_CATALOG = {
    "io.debezium.connector.mysql.MySqlConnector": "mysql",
    "io.debezium.connector.postgresql.PostgresConnector": "postgresql",
}


# --------------------------------------------------------------------- trino
def trino(sql: str) -> list[list]:
    headers = {"X-Trino-User": "dbt-assemble"}
    req = urllib.request.Request(TRINO, data=sql.encode(), headers=headers, method="POST")
    rows: list[list] = []
    while True:
        with urllib.request.urlopen(req, timeout=60) as resp:
            doc = json.loads(resp.read().decode())
        if doc.get("error"):
            raise RuntimeError(f"trino: {doc['error'].get('message')}\n  sql: {sql}")
        rows.extend(doc.get("data") or [])
        if not doc.get("nextUri"):
            return rows
        req = urllib.request.Request(doc["nextUri"], headers=headers)


def wait_for_trino(timeout: float = 120.0) -> None:
    deadline = time.time() + timeout
    while True:
        try:
            trino("SELECT 1")
            return
        except (urllib.error.URLError, OSError, RuntimeError) as e:
            if time.time() > deadline:
                raise SystemExit(f"assemble: Trino not reachable at {TRINO}: {e}")
            time.sleep(3)


# ----------------------------------------------------------------- derivation
def _patterns(value: str) -> list[re.Pattern]:
    """Debezium column lists are comma-separated regular expressions."""
    return [re.compile(p.strip()) for p in value.split(",") if p.strip()]


def _matches(patterns: list[re.Pattern], qualified: str) -> bool:
    return any(p.fullmatch(qualified) for p in patterns)


def _heartbeat_relation(config: dict) -> str | None:
    m = re.search(r"insert\s+into\s+([\w.]+)", config.get("heartbeat.action.query", ""), re.I)
    if not m:
        return None
    rel = m.group(1)
    return rel if "." in rel else f"public.{rel}"


def decode_for(data_type: str, masked: bool) -> tuple[str, str]:
    """(Silver column type, cdc_decode kind) for a Trino-reported source type.

    The connectors run decimal.handling.mode=string and time.precision.mode=connect,
    which is what makes these encodings fixed: decimals arrive as text, DATE as days,
    a zone-less timestamp as epoch millis of its wall clock, TIMESTAMPTZ as ISO-8601.
    """
    t = data_type.lower()
    if masked:
        return "varchar", "text"
    if t in ("bigint", "integer", "smallint", "tinyint", "double", "real", "boolean") \
            or t.startswith("decimal"):
        return t, "cast"
    if t.startswith(("varchar", "char")):
        return "varchar", "text"
    if t == "date":
        return "date", "epoch_days"
    if t.startswith("timestamp") and "with time zone" in t:
        return "timestamp(6) with time zone", "iso_ts"
    if t.startswith("timestamp"):
        # Harmonised to an instant (GR1 section 4.4 wanted a TimestampConverter SMT for
        # this; ADR 0004 removed the SMT chain, so the conversion lives here).
        return "timestamp(6) with time zone", "wallclock_ms"
    print(f"  note: source type {data_type!r} kept as text")
    return "varchar", "text"


class Captured:
    def __init__(self, domain: str, config: dict, table: str) -> None:
        self.domain = domain
        self.table = table                          # db.table / schema.table
        self.schema, self.relation = table.split(".", 1)
        self.topic = f"{config['topic.prefix']}.{table}"
        self.catalog = ENGINE_CATALOG.get(config["connector.class"])
        self.excluded = _patterns(config.get("column.exclude.list", ""))
        self.masked = []
        for k, v in config.items():
            if k.startswith(("column.mask.", "column.truncate.")):
                self.masked += _patterns(v)
        self.columns: list[dict] = []
        self.key: list[str] = []

    @property
    def model(self) -> str:
        return f"silver__{self.relation}"


def captured_tables() -> list[Captured]:
    out = []
    for path in sorted(glob.glob(str(DOMAINS / "*" / "connectors" / "*.json"))):
        domain = Path(path).parts[-3]
        config = json.loads(Path(path).read_text(encoding="utf-8"))["config"]
        heartbeat = _heartbeat_relation(config)
        for table in config.get("table.include.list", "").split(","):
            table = table.strip()
            if table and table != heartbeat:
                out.append(Captured(domain, config, table))
    return out


def resolve(cap: Captured) -> str | None:
    """Fill columns and key from the live catalog and Bronze; a reason if impossible."""
    if not cap.catalog:
        return "no Trino catalog for this connector class"
    rows = trino(f"SELECT column_name, data_type FROM {cap.catalog}.information_schema.columns "
                 f"WHERE table_schema = '{cap.schema}' AND table_name = '{cap.relation}' "
                 f"ORDER BY ordinal_position")
    if not rows:
        return f"{cap.catalog}.{cap.table} not visible through Trino"
    for name, data_type in rows:
        qualified = f"{cap.table}.{name}"
        if _matches(cap.excluded, qualified):
            continue
        sql_type, decode = decode_for(data_type, _matches(cap.masked, qualified))
        cap.columns.append({"name": name, "type": sql_type, "decode": decode})
    key = trino(f"SELECT record_key FROM {BRONZE} WHERE topic = '{cap.topic}' "
                f"AND record_key IS NOT NULL LIMIT 1")
    if not key:
        return "no keyed event in Bronze yet (keyless table, or nothing landed)"
    cap.key = list(json.loads(key[0][0]).keys())
    missing = [k for k in cap.key if k not in {c["name"] for c in cap.columns}]
    if missing:
        return f"key column(s) {missing} not among the captured columns"
    return None


# ------------------------------------------------------------------- writing
def silver_sql(cap: Captured) -> str:
    cols = ",\n".join(f"        {json.dumps(c)}" for c in cap.columns)
    return (f"-- GENERATED by assemble.py from domains/{cap.domain}/connectors -- do not edit.\n"
            f"-- {cap.catalog}.{cap.table}  <-  {cap.topic}\n"
            f"{{{{ config(alias={cap.relation!r}, unique_key={cap.key!r}) }}}}\n"
            f"{{{{ cdc_current_state(\n"
            f"    topic={cap.topic!r},\n"
            f"    key={cap.key!r},\n"
            f"    columns=[\n{cols}\n    ]) }}}}\n")


def silver_schema(caps: list[Captured]) -> str:
    lines = ["# GENERATED by assemble.py -- do not edit.", "version: 2", "", "models:"]
    for cap in caps:
        lines += [f"  - name: {cap.model}",
                  f"    description: \"Current state of {cap.catalog}.{cap.table} "
                  f"(domain {cap.domain}), from Bronze topic {cap.topic}.\"",
                  "    columns:"]
        if len(cap.key) == 1:
            lines += [f"      - name: {cap.key[0]}", "        tests: [unique, not_null]"]
        for m in ("_src_ts", "_ingest_ts", "_event_id", "_src_position"):
            lines += [f"      - name: {m}", "        tests: [not_null]"]
        lines += ["      - name: _op", "        tests:",
                  "          - accepted_values: {values: ['c', 'u', 'd', 'r']}"]
        if len(cap.key) > 1:
            lines += ["    tests:", f"      - unique_combination: {{columns: {cap.key}}}"]
    return "\n".join(lines) + "\n"


def _fresh_dir(path: Path) -> None:
    """Empty a generated directory. Only ever called on the gitignored outputs above."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def copy_gold() -> list[str]:
    copied = []
    for gold in sorted(DOMAINS.glob("*/gold")):
        domain = gold.parent.name
        for src in sorted(gold.rglob("*")):
            if not src.is_file() or src.suffix not in (".sql", ".yml", ".yaml"):
                continue
            rel = src.relative_to(gold)
            if rel.parts[0] == "tests":
                dst = TESTS_DIR / domain / Path(*rel.parts[1:])
            else:
                dst = GOLD_DIR / domain / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            copied.append(f"{domain}/{rel.as_posix()}")
    return copied


def main() -> int:
    if not DOMAINS.is_dir():
        print(f"assemble: no domains directory at {DOMAINS}", file=sys.stderr)
        return 2
    wait_for_trino()

    caps, skipped = [], []
    for cap in captured_tables():
        reason = resolve(cap)
        (skipped.append((cap, reason)) if reason else caps.append(cap))

    clash = {}
    for cap in caps:
        clash.setdefault(cap.relation, []).append(cap.table)
    clash = {k: v for k, v in clash.items() if len(v) > 1}
    if clash:
        print(f"assemble: Silver table names collide across sources/domains: {clash}",
              file=sys.stderr)
        return 2

    _fresh_dir(SILVER_DIR)
    for cap in caps:
        (SILVER_DIR / f"{cap.model}.sql").write_text(silver_sql(cap), encoding="utf-8")
    (SILVER_DIR / "schema.yml").write_text(silver_schema(caps), encoding="utf-8")

    _fresh_dir(GOLD_DIR)
    _fresh_dir(TESTS_DIR)
    gold = copy_gold()

    print(f"assemble: {len(caps)} Silver model(s) generated, {len(gold)} Gold file(s) copied")
    for cap in caps:
        print(f"  silver.{cap.relation:<16} key={','.join(cap.key):<16} "
              f"{len(cap.columns)} cols  <- {cap.topic}")
    for cap, reason in skipped:
        print(f"  skipped {cap.topic}: {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
