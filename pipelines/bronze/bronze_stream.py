"""Bronze: every captured Debezium topic -> one Iceberg table, envelope intact.

    spark-submit /opt/mosaic/bronze/bronze_stream.py [--trigger 10] [--max-offsets 50000]

GR1 section 4.2, taken literally: Bronze "stores the unmodified Debezium change envelope
... with no transformation applied", and the write path "applies deduplication using the
source offset as an idempotency key, appends the result to Bronze". Silver and Gold are
dbt-trino (ADR 0003); this is the only Spark job in the system.

Shape. One table, iceberg.bronze.cdc_events, for every topic of every domain. The
envelope is kept as the verbatim Kafka value; beside it sit only the columns needed to
route, deduplicate and date an event. Nothing here names a table, a column or a domain:
the topic list is derived from domains/*/connectors/*.json at start-up, exactly as
tools/cdc derives it, so adding a domain adds topics without touching this file.

Dedup. M2 measured why this is load-bearing (CHANGES-FROM-GR1 A8): Kafka Connect flushes
source offsets on a 60 s timer, so a crashed worker replays up to a minute of work --
110 duplicate events across one 45 s SIGKILL. The key must therefore identify a *source*
change event, and must come out identical however many times that event reaches Kafka.
Each micro-batch is anti-joined against Bronze on it before the append, which absorbs
both replay sources in the system with one mechanism: Connect re-producing from a stale
offset, and Spark re-running a batch whose commit landed before its checkpoint did.

Freshness. Every row carries _src_ts (source commit time, from source.ts_ms) and
_ingest_ts, and every commit records max(_src_ts) -- overall and per topic -- in the
Iceberg snapshot summary (CHANGES-FROM-GR1 C8). N1 reads it from there.

Pairing guard. The checkpoint (spark-state volume) and the table (Nessie) are two halves
of one state. If the table vanishes while the checkpoint survives, a naive restart
creates an empty table and resumes *after* everything it had written: silent loss. The
job refuses to start in that state. The reverse -- table kept, checkpoint lost -- is
harmless by construction: the stream re-reads from the earliest retained offset and the
idempotency key discards whatever Bronze already holds.

Python 3.8: this runs on the interpreter inside apache/spark:3.5.1.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

NAMESPACE = "iceberg.bronze"
TABLE = f"{NAMESPACE}.cdc_events"
DOMAINS_DIR = "/opt/mosaic/domains"
CHECKPOINT = "/opt/mosaic/state/checkpoints/bronze"
STATUS_FILE = "/tmp/mosaic-bronze/status.json"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    event_id        STRING    COMMENT 'idempotency key: one value per source change event',
    topic           STRING    COMMENT '<topic.prefix>.<captured table>',
    source_table    STRING    COMMENT 'schema-or-database.table, from the envelope source block',
    op              STRING    COMMENT 'c | u | d | r',
    record_key      STRING    COMMENT 'Kafka record key, verbatim JSON (the primary key)',
    envelope        STRING    COMMENT 'Kafka record value, verbatim: op/before/after/source/ts_ms',
    kafka_partition INT,
    kafka_offset    BIGINT,
    kafka_ts        TIMESTAMP COMMENT 'when Kafka Connect produced the record',
    _src_ts         TIMESTAMP COMMENT 'source commit time (envelope source.ts_ms)',
    _ingest_ts      TIMESTAMP COMMENT 'when the micro-batch that landed this row ran'
)
USING iceberg
PARTITIONED BY (topic, days(_src_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
"""
COLUMNS = ["event_id", "topic", "source_table", "op", "record_key", "envelope",
           "kafka_partition", "kafka_offset", "kafka_ts", "_src_ts", "_ingest_ts"]


# ------------------------------------------------------------------ topology
def declared_topics(domains_dir: str) -> list:
    """Every table topic any domain's connectors will write, from their declared config.

    Only table topics: `table.include.list` entries under `topic.prefix`. The connector's
    own `__debezium-heartbeat.*` topic carries no row changes and is not subscribed.
    """
    topics = set()
    for path in sorted(glob.glob(os.path.join(domains_dir, "*", "connectors", "*.json"))):
        with open(path, encoding="utf-8") as fh:
            config = json.load(fh)["config"]
        prefix = config["topic.prefix"]
        for table in config.get("table.include.list", "").split(","):
            if table.strip():
                topics.add(f"{prefix}.{table.strip()}")
    return sorted(topics)


# -------------------------------------------------------------------- shaping
def shape(raw: DataFrame) -> DataFrame:
    """Kafka records -> Bronze rows, before the key and the ingest stamp are added.

    Tombstones are dropped. Debezium follows every delete with a null-valued record so
    that log compaction can forget the key; it carries no envelope -- the delete event
    before it *is* the change -- so there is nothing in it for Bronze to keep.

    `source` is the envelope's source block as JSON text. It is not stored as a column
    (it is already inside `envelope`); it is lifted out so the key can be built from it.
    """
    value = F.col("value").cast("string")

    def field(path: str) -> Column:
        return F.get_json_object(value, path)

    return raw.where(F.col("value").isNotNull()).select(
        F.col("topic"),
        F.concat_ws(".", F.coalesce(field("$.source.schema"), field("$.source.db")),
                    field("$.source.table")).alias("source_table"),
        field("$.op").alias("op"),
        F.col("key").cast("string").alias("record_key"),
        value.alias("envelope"),
        field("$.source").alias("source"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.expr("timestamp_millis(cast(get_json_object(cast(value AS STRING), "
               "'$.source.ts_ms') AS BIGINT))").alias("_src_ts"),
    )


def idempotency_key() -> Column:
    """The Bronze idempotency key: one value per *source* change event, however many
    times Kafka Connect delivers that event.

    Built only from columns of shape()'s output (all STRING unless noted):

        topic, source_table, op, record_key, envelope,
        source           the envelope's source block as JSON text
        kafka_partition  INT
        kafka_offset     BIGINT
        kafka_ts         TIMESTAMP  (when Connect produced the record)
        _src_ts          TIMESTAMP  (source commit time)

    Must return a deterministic Column -- no rand(), no current_timestamp() -- because
    the same event arriving in two different micro-batches has to hash identically.

    What goes in, and why each excluded candidate is excluded:

      * Where the change sits in the source's own log: binlog file/pos/row (MySQL) or
        lsn/txId (PostgreSQL), plus source.ts_ms. A replay re-reads the same log record,
        so these come out identical. A connector only fills its own fields; the other
        source's are null, and to_json() keeps that unambiguous.
      * topic, op, record_key: the same log position can carry several rows (every
        snapshot row shares the snapshot's position), so the row's identity is needed.
      * The before/after images, as belt and braces: also reproduced exactly from the
        same log record, and they keep two distinct changes apart even if a position
        field turned out to be shared more widely than documented.

      - NOT the whole envelope: its top-level ts_ms is when Debezium *processed* the
        event, so every redelivery carries a new value.
      - NOT the whole source block: PostgreSQL's source.sequence is
        [lastCommitLsn, lsn], and lastCommitLsn after a restart is seeded from the
        stored offset rather than from the transaction that actually preceded.
      - NOT kafka_partition/offset/ts: a Connect replay produces *new* Kafka records.
    """
    def source(field: str) -> Column:
        return F.get_json_object(F.col("source"), f"$.{field}")

    def image(side: str) -> Column:
        return F.get_json_object(F.col("envelope"), f"$.{side}")

    identity = F.struct(
        F.col("topic").alias("topic"),
        F.col("op").alias("op"),
        F.col("record_key").alias("key"),
        source("file").alias("file"),
        source("pos").alias("pos"),
        source("row").alias("row"),
        source("lsn").alias("lsn"),
        source("txId").alias("tx"),
        source("ts_ms").alias("ts_ms"),
        image("before").alias("before"),
        image("after").alias("after"),
    )
    return F.sha2(F.to_json(identity), 256)


# ---------------------------------------------------------------------- sink
class Lander:
    """foreachBatch sink: key, dedup within the batch, anti-join against Bronze, append.

    Runs on the driver. The anti-join reads only the slice of Bronze the batch could
    collide with -- the batch's own topics and its `_src_ts` range -- because a replayed
    event carries the same source commit time as the original, so a duplicate can never
    sit outside that range. That keeps the dedup read proportional to the batch rather
    than to the table.
    """

    def __init__(self) -> None:
        self.last: dict = {}
        self.totals = {"batches": 0, "rows_in": 0, "written": 0,
                       "dup_in_batch": 0, "dup_in_bronze": 0}

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        keyed = batch.withColumn("event_id", idempotency_key()).persist()
        try:
            self._land(spark, keyed, batch_id)
        finally:
            keyed.unpersist()

    def _land(self, spark: SparkSession, keyed: DataFrame, batch_id: int) -> None:
        rows_in = keyed.count()
        if rows_in == 0:
            return
        unique = keyed.dropDuplicates(["event_id"])
        n_unique = unique.count()

        bounds = keyed.agg(F.min("_src_ts").alias("lo"), F.max("_src_ts").alias("hi")).first()
        topics = [r[0] for r in keyed.select("topic").distinct().collect()]
        existing = spark.table(TABLE).where(F.col("topic").isin(topics))
        if bounds.lo is not None:
            existing = existing.where(F.col("_src_ts").between(bounds.lo, bounds.hi)
                                      | F.col("_src_ts").isNull())

        # A literal, not current_timestamp(): if `fresh` were partly evicted from cache
        # between count() and append(), a recomputed partition would carry a different
        # stamp. One value per batch is also what lets a reader join a row to the
        # snapshot that published it (mosaic.ingest_ts_us) -- the end-to-end latency KPI.
        ingest = datetime.now(timezone.utc)
        fresh = (unique.join(existing.select("event_id"), "event_id", "left_anti")
                 .withColumn("_ingest_ts", F.lit(ingest))
                 .select(*COLUMNS)
                 .persist())
        try:
            written = fresh.count()
            summary = {}
            if written:
                per_topic = (fresh.groupBy("topic")
                             .agg(F.expr("max(unix_millis(_src_ts))").alias("ms"))
                             .collect())
                summary = self._summary(batch_id, rows_in, n_unique, written, per_topic)
                summary["mosaic.ingest_ts_us"] = str(
                    int(ingest.timestamp()) * 1_000_000 + ingest.microsecond)
                writer = fresh.writeTo(TABLE)
                for key, value in summary.items():
                    writer = writer.option(f"snapshot-property.{key}", value)
                writer.append()
        finally:
            fresh.unpersist()

        self.last = {"batch_id": batch_id, "rows_in": rows_in, "written": written,
                     "dup_in_batch": rows_in - n_unique,
                     "dup_in_bronze": n_unique - written,
                     "max_src_ts": summary.get("mosaic.max_src_ts"),
                     "landed_at": datetime.now(timezone.utc).isoformat()}
        self.totals["batches"] += 1
        for k in ("rows_in", "written", "dup_in_batch", "dup_in_bronze"):
            self.totals[k] += self.last[k]
        dups = self.last["dup_in_batch"] + self.last["dup_in_bronze"]
        print(f"bronze: batch {batch_id}: {rows_in} in, {written} written, "
              f"{dups} duplicate(s) dropped", flush=True)

    @staticmethod
    def _summary(batch_id: int, rows_in: int, n_unique: int, written: int,
                 per_topic: list) -> dict:
        """Snapshot-summary properties for this commit. Values must be strings."""
        stamped = [r for r in per_topic if r.ms is not None]
        props = {
            "mosaic.batch_id": str(batch_id),
            "mosaic.rows_in": str(rows_in),
            "mosaic.rows_written": str(written),
            "mosaic.dup_in_batch": str(rows_in - n_unique),
            "mosaic.dup_in_bronze": str(n_unique - written),
        }
        if stamped:
            top = max(r.ms for r in stamped)
            props["mosaic.max_src_ts_ms"] = str(top)
            props["mosaic.max_src_ts"] = datetime.fromtimestamp(
                top / 1000, tz=timezone.utc).isoformat()
            for r in stamped:
                props[f"mosaic.max_src_ts_ms.{r.topic}"] = str(r.ms)
        return props


# -------------------------------------------------------------------- guards
def wait_for_catalog(spark: SparkSession, timeout: float = 90.0) -> None:
    """Nessie has no container healthcheck (CLAUDE.md), so `depends_on` only promises
    that it was started. Probe it until it answers rather than dying on the first try."""
    deadline = time.time() + timeout
    while True:
        try:
            spark.sql("SHOW NAMESPACES IN iceberg").collect()
            return
        except Exception as e:  # noqa: BLE001 -- any failure here means "not yet"
            if time.time() > deadline:
                raise RuntimeError(f"Iceberg catalog (Nessie) not reachable: {e}") from e
            time.sleep(3)


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.table(TABLE).schema
        return True
    except AnalysisException:
        return False


def checkpoint_has_progress(path: str) -> bool:
    offsets = os.path.join(path, "offsets")
    return os.path.isdir(offsets) and any(n.isdigit() for n in os.listdir(offsets))


REFUSAL = f"""
REFUSING TO START: the streaming checkpoint records consumed Kafka offsets, but
{TABLE} does not exist in the catalog.

Starting anyway would create an empty table and resume *after* everything the old one
held -- a silent gap in Bronze. The usual cause is the Nessie catalog being lost while
the spark-state volume survived.

  - If the table should exist, restore the catalog first (nessie-data volume).
  - To rebuild Bronze from whatever Kafka still retains, set the checkpoint aside:
        docker compose run --rm spark /opt/spark/bin/spark-submit \\
            /opt/mosaic/bronze/bronze_stream.py --reset-checkpoint
    and then check what retention has already deleted (CHANGES-FROM-GR1 A10).
"""


# -------------------------------------------------------------------- status
def write_status(query, lander: Lander, topics: list, started_at: str) -> None:
    """Atomically publish stream state for the healthcheck and `mosaic.py bronze`."""
    progress = query.lastProgress or {}
    backlog = None
    for source in progress.get("sources") or []:
        end, latest = source.get("endOffset"), source.get("latestOffset")
        if isinstance(end, dict) and isinstance(latest, dict):
            backlog = (backlog or 0) + sum(
                max(0, int(p_latest) - int(end.get(topic, {}).get(part, p_latest)))
                for topic, parts in latest.items() for part, p_latest in parts.items())
    doc = {
        "written_at": time.time(),
        "active": bool(query.isActive),
        "started_at": started_at,
        "table": TABLE,
        "topics": topics,
        "backlog_records": backlog,
        "last_batch": lander.last,
        "totals": lander.totals,
        "progress": {k: progress.get(k) for k in (
            "batchId", "numInputRows", "inputRowsPerSecond",
            "processedRowsPerSecond", "timestamp")},
    }
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    tmp = STATUS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh)
    os.replace(tmp, STATUS_FILE)


# ---------------------------------------------------------------------- main
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--trigger", type=int,
                   default=int(os.environ.get("MOSAIC_BRONZE_TRIGGER_SECONDS", "10")),
                   help="micro-batch interval, seconds (latency vs. files per day)")
    p.add_argument("--max-offsets", type=int, default=50000,
                   help="records per micro-batch cap (bounds the catch-up batch)")
    p.add_argument("--domains-dir", default=DOMAINS_DIR)
    p.add_argument("--checkpoint", default=CHECKPOINT)
    p.add_argument("--bootstrap", default=os.environ.get("MOSAIC_KAFKA", "kafka:9092"))
    p.add_argument("--reset-checkpoint", action="store_true",
                   help="set the checkpoint aside (renamed, not deleted) and re-read "
                        "every retained offset")
    a = p.parse_args()

    topics = declared_topics(a.domains_dir)
    if not topics:
        print(f"no connector configs under {a.domains_dir}/*/connectors", file=sys.stderr)
        return 2

    spark = SparkSession.builder.appName("mosaic-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    wait_for_catalog(spark)

    if a.reset_checkpoint and os.path.isdir(a.checkpoint):
        # Set aside, never delete: a checkpoint is half of Bronze's state, and the
        # operator may yet need it back (mv it to restore).
        aside = f"{a.checkpoint}.reset-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
        shutil.move(a.checkpoint, aside)
        print(f"bronze: checkpoint set aside as {aside}", flush=True)
    if not table_exists(spark) and checkpoint_has_progress(a.checkpoint):
        print(REFUSAL, file=sys.stderr, flush=True)
        return 3

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    spark.sql(DDL)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", ",".join(topics))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", a.max_offsets)
           .load())

    lander = Lander()
    query = (shape(raw).writeStream
             .queryName("bronze")
             .foreachBatch(lander)
             .option("checkpointLocation", a.checkpoint)
             .trigger(processingTime=f"{a.trigger} seconds")
             .start())

    started_at = datetime.now(timezone.utc).isoformat()
    print(f"bronze: {len(topics)} topic(s) -> {TABLE}, trigger {a.trigger}s", flush=True)
    for topic in topics:
        print(f"  {topic}", flush=True)
    try:
        while not query.awaitTermination(15):
            write_status(query, lander, topics, started_at)
    finally:
        write_status(query, lander, topics, started_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())
