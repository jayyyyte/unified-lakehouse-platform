# Implementation status

**Last updated:** 2026-09-08 | **Current milestone:** M2 **done, verified** | **Next action:** M3 -- Spark Bronze into Iceberg on Nessie (the main technical risk)

Fastest way to see real state: `python mosaic.py doctor`.

Keep this file current. It is the first thing a new session should read after
[CLAUDE.md](../CLAUDE.md), and its job is to prevent re-deriving what is already done.

Update rules: move a milestone to **done** only when its verification actually ran and
passed. Anything written but not executed is **unverified** — say so explicitly. Record
non-obvious findings under *Findings*, not just in commit messages.

---

## Milestones

| | Milestone | State |
|---|---|---|
| **M0** | Problem spec, skeleton, stack definition | **done, verified** |
| **M1** | Sources, seed corpus, load generator | **done, verified** — CDC live on both sources, masking proven, load generator + stock-out lever running |
| **M2** | CDC ingestion + restart-safety demo | **done, verified** -- no gap across a SIGKILL under load; GR1 section 4.4's WAL guard exercised, found inert, fixed, re-measured |
| M3 | Medallion on Iceberg (Spark Bronze, dbt-trino Silver/Gold) | not started |
| M4 | Trino federation + embedding pipeline | catalogs done in M0; embeddings not started |
| M5 | N1 Freshness Registry | schema exists; collectors not started |
| M6 | N2 Retrieval + fusion engine | not started |
| M7 | Experiments E1–E4 | not started |
| M8 | N3 governance, `jobmarket` domain, console | not started |

---

## What is verified working

Confirmed by an actual run, not by inspection.

**Stack** — `python mosaic.py up`, 10/10 services healthy (`doctor` confirms).

| Check | Evidence |
|---|---|
| MySQL CDC prerequisites | `binlog_format=ROW`, `binlog_row_image=FULL`, `gtid_mode=ON` |
| PostgreSQL CDC prerequisites | `wal_level=logical`, publication `mosaic_pub`, pgvector installed |
| Domain schemas | MySQL 3 tables; PostgreSQL 6 tables |
| Platform metadata DB | `mosaic_platform` with all 6 `platform.*` tables |
| Nessie | API v2 responding on 19120 (host probe — no container healthcheck) |
| MinIO | bucket `s3://mosaic` created |
| Trino catalogs | `iceberg` (Nessie), `mysql`, `postgresql`, `platform` — all queryable |
| Federation | Trino → live PostgreSQL returns the domain tables |
| Abstraction gate | `check-abstraction` passes (6 files scanned) |
| Kafka Connect | starts and stays healthy with the JSON converter (ADR 0005) |
| Debezium connectors | both registered first try, `connector=RUNNING tasks=RUNNING` |
| CDC topics | all 7 present; snapshot delivered every seeded row |
| PII masking | `cdc-verify` passes on both sources, before **and** after images |
| Load generator | 25 ops/s for 45 s, row-count delta matched exactly on all 4 tables |
| Stock-out lever | `before.stock_qty=192 -> after.stock_qty=0` observed on the Kafka topic |
| Restart safety (M2) | 45 s SIGKILL of Connect under load: **0 rows lost** of 3,711 across 4 tables, 110 duplicates, caught up 30.3 s after restart |
| Committed offset resumes (M2) | `mysql-bin.000007:1238339 -> :2399951` (GTID `1-61738 -> 1-63546`), PostgreSQL `lsn 70936688 -> 72250344` |
| WAL guard (M2) | flush advance over 130 s idle: **0 bytes broken -> 688,776 bytes fixed**; retained WAL 704,880 -> 4,544 bytes |
| Envelope + masking after the M2 fix | `cdc-verify` re-run: 807 UPDATE events, 807 before images, all topics match what the connectors declared |

**Seed corpus** — generated and bulk-loaded at `small`, all 7 tables matching `manifest.json`.

| | rows |
|---|---|
| categories / users / products / inventory | 48 / 5,000 / 1,000 / 1,000 |
| orders / order_items / payments | 20,000 / 40,266 / 19,000 |
| **total** | **86,314** |

Determinism verified: identical SHA-256 across runs with `--seed` + `--now` pinned.
Semantic clustering spot-checked — "waterproof hiking backpack" retrieves outdoor items only.

## Written but NOT yet executed

Nothing from M1 or M2. Everything either milestone added has been run against the live stack,
and the failures are kept in the ledgers on purpose (see *Run ledgers* below).

Still unexecuted and belonging to later milestones: the `gov`, `orch` and `console` profiles,
every Spark job, and every dbt model. M3 recon identified a candidate Spark/Iceberg/Nessie
jar-and-config set, but **no `spark-submit` has been run and no Iceberg table exists** -- that
is recon, not a result.

## Current runtime state

Stack **up** with the default profile, 10/10 healthy. `gov`, `orch` and `console` profiles
have never been started; their services (OPA, Marquez, Airflow, API) are unproven.

Seeded at `small` scale, plus growth from M1/M2 load-generator runs (the seeded counts below
are the corpus, not the current row counts). `docker/api.Dockerfile` is referenced by the
`console` profile but **does not exist yet** — that profile will fail to build until M6/M8.

`mosaic_pub` now publishes **five** tables: the four domain tables plus `cdc_heartbeat`, which
finding 10 showed must be captured for the WAL guard to work at all.

---

## Findings worth carrying into the report

> Full narrative version, with evidence and suggested framing:
> [CHANGES-FROM-GR1.md](CHANGES-FROM-GR1.md).

1. **GR1 Appendix C's SMT masking chain cannot work as specified.** See
   [ADR 0004](adr/0004-connector-native-masking.md). This is the strongest "implementation
   revealed a design contradiction" result so far and should be written up with the two
   configs side by side.
2. **Two GR1 Appendix B image tags do not resolve** (`debezium/connect:2.7`,
   `projectnessie/nessie:0.90.4` on Docker Hub). Minor, but it means Appendix B was never
   executed as written.
3. **MySQL reports a missing `FILE` privilege as `ERROR 1045 Access denied`** — an error
   that reads as authentication failure. Cost one debugging cycle.
4. **GR1 Appendix B's Avro converter cannot load.** `debezium/connect` does not bundle
   Confluent's Avro converter, so the worker dies at startup. See
   [ADR 0005](adr/0005-json-converter.md). Together with finding 2 and ADR 0004, this is
   the third independent way Appendix B/C could not have been run as written — worth
   stating plainly in the GR2 report, since it is exactly what an implementation phase is
   supposed to surface.
5. **Tooling bug worth remembering:** `docker compose ps` omits exited containers, so a
   health tally built from it silently drops a crashed service and reports success. This
   produced one false "all services healthy". `mosaic.py` now diffs against
   `compose config --services` and reads exit codes from `ps -a`.
6. **PostgreSQL emits no before image under the default replica identity** (M1). GR1 §4.2
   requires Bronze to keep the before/after envelope; Appendix A.2's DDL never sets a replica
   identity, so `before` is `null` on every PostgreSQL UPDATE and the requirement is
   unsatisfiable for half the system. MySQL was fine -- Appendix B sets `binlog_row_image=FULL`
   and there is no PostgreSQL counterpart. Fixed with `REPLICA IDENTITY FULL` on three tables.
   See CHANGES-FROM-GR1 §A6. Strong report material: *a requirement that holds on one source
   and silently does not on another.*
7. **Debezium truncates a mask hash to the source column's width** (M1). `buyer_phone
   VARCHAR(32)` gets 32 hex chars, not 64; nothing warns. Still a one-way digest, but the
   strength of the mask turns out to be a property of the source DDL. See §A7.
8. **A load generator that reports its send rate is lying.** One run reported 549 ops/s while
   MySQL was committing it 152 s later; an earlier one silently lost 5,327 orders when the
   client was killed mid-drain. Both were caught only by re-counting rows afterwards. The
   generator now throttles against committed rows and reports *sent* and *sustained*
   separately. **~100 ops/s sustained** here, bounded by per-commit fsync under Docker Desktop.

9. **"Restart safety" is at-least-once with a replay window, not exactly-once** (M2). GR1
   Table 4.3 states it in one line -- *"offset stored in the Kafka Connect offsets topic"* --
   but the offset is flushed on a **timer** (`offset.flush.interval.ms = 60000`, the Connect
   default), not per record. A crash therefore replays up to a minute of work from a stale
   offset. Measured: a 45 s SIGKILL lost **zero** of 3,711 rows and replayed **110 duplicate
   events**. The replay window is bounded by the flush interval, not by the outage length, so
   a 5-second crash and a 5-minute crash replay the same ~60 s. This is also why GR1 section
   4.2's "deduplicate on the source offset" is load-bearing for M3 rather than decorative.

10. **GR1 section 4.4's WAL-bloat guard was inert as configured** (M2). The section correctly
    names the PostgreSQL risk and correctly names the mitigation -- a heartbeat that advances
    `confirmed_flush_lsn` while the captured tables are idle. The heartbeat was firing (503
    rows written, 503 messages on the heartbeat topic) and **still did nothing**, because
    `heartbeat.action.query` wrote to a table outside `mosaic_pub`. `pgoutput` decodes only
    published tables, so the heartbeat's WAL never became a change event and Debezium never
    gained an LSN to confirm. Over the same 130 s idle window: **0 bytes of flush advance
    before the fix, 688,776 bytes after**; retained WAL 704,880 -> 4,544 bytes. Same *shape*
    as finding 6 -- a mitigation GR1 names in prose that is silently ineffective until
    something measures it. No error, no warning, no log line in either case.

## Open questions

- **Scale.** Everything so far is `small` (86k rows). GR1 §10.1 wants 10M+ for E2 to be
  meaningful. MySQL loaded ~4k rows/s, so `full` projects to roughly 40 minutes. Worth
  running once the pipeline shape stops changing — not before.
- **Trino + Iceberg write path** is unproven. `SHOW SCHEMAS FROM iceberg` works, but no
  table has been created, and dbt-trino `MERGE INTO` against Nessie-catalogued Iceberg
  (ADR 0003) is the main technical risk in M3.
- **Spark → Iceberg on Nessie** needs the right jar set at `spark-submit`; the `spark`
  container currently just runs `sleep infinity`.

---

## Tooling added for future sessions

- `python mosaic.py doctor` — one-shot state report (environment, stack, schemas, catalogs,
  seed volume, connectors, abstraction gate). Use it instead of a dozen probe commands.
- `python mosaic.py check-ports` — host port collisions with the owning container named.
- `python mosaic.py cdc-verify [--require-update]` — checks the Kafka topics against what the
  connectors *declared*, deriving expectations from the connector JSON (so it works for any
  domain). Samples the topic **tail**: reading from the beginning returns only snapshot events,
  where `before` is always null and the masking check that matters can never fire.
- `python mosaic.py loadgen --rate N --duration S` — continuous OLTP write mix across both
  sources. Verifies its own effects by re-counting rows.
- `python mosaic.py stockout --top 3` / `restock --last 3` — the L0 lever. Every event, pulled
  or organic, is appended to `experiments/results/stockout_events.jsonl` with the source commit
  timestamp, which is what E1/E3 read to know when the clock started.
- `python mosaic.py restart-safety [--mode kill|stop|pause]` — kills Connect mid-stream under
  load and proves the resume lost nothing. The no-gap proof is exact rather than statistical:
  every captured table with a generated key writes a contiguous id range, which is bracketed at
  the kill and required in full on the topic afterwards. Domain-free — topics come from the
  connector's declared config, key columns from the live catalog.
- `python mosaic.py wal-guard [--idle N]` — exercises GR1 section 4.4's heartbeat guard in both
  directions (idle with the worker alive, then idle with it dead), because the guard only means
  something as a contrast. Refuses to run if `--idle` does not exceed the 60 s flush interval.
- `docs/BUILD-LOG.md` — append-only record of what was built per milestone and what each run
  measured, including the harness defects caught before a milestone was called done.
- `docs/ADVISOR-BRIEFING.md` — the supervision-meeting script: what to say, what to demo, and
  what is not verified.
- `CLAUDE.md` — durable context and the environment gotchas that each cost a failed run.
- `docs/CHANGES-FROM-GR1.md` — every deviation from the GR1 report, why, and with evidence.

## Immediate next steps

M2 is closed. Both halves ran and passed; the WAL guard passed only *after* M2 found it was
doing nothing (finding 10). M3 is next, and it is where the real technical risk sits:

1. **Spark Bronze into Iceberg on Nessie.** The `spark` container still runs `sleep infinity`
   and ships no Iceberg/Nessie/S3 jars, though Maven Central is reachable from inside it, so
   `--packages` is viable. Pin Spark's Iceberg to **the version Trino reads with** so writer
   and reader stay symmetric. Bronze must keep the unmodified envelope (GR1 section 4.2) and
   carry `_src_ts`/`_ingest_ts`, with `max(_src_ts)` stamped into each Iceberg snapshot
   summary -- N1 cannot be retrofitted if this is skipped.
2. **Implement the dedup key.** Finding 9 makes GR1 section 4.2's "deduplicate using the
   source offset as an idempotency key" a real requirement: ~110 duplicates per crash is what
   Bronze will actually receive.
3. **Then dbt-trino Silver/Gold**, where `MERGE INTO` against Nessie-catalogued Iceberg
   (ADR 0003) is the second unproven path.

Landing Bronze also unblocks the last blocked CDC KPI in GR1 Table 4.4: end-to-end latency
from source commit to availability in Bronze.

### Caveats to carry into M3

- The publication fix for finding 10 was applied to the **running** database with
  `ALTER PUBLICATION`. The edit to `domains/ecommerce/ddl/postgres.sql` only takes effect on a
  fresh volume -- the same retroactivity trap CLAUDE.md records for `REPLICA IDENTITY FULL`.
- `nessie` runs `NESSIE_VERSION_STORE_TYPE: IN_MEMORY`. Iceberg table metadata will not
  survive a Nessie restart even though the MinIO data files will, so tables vanish from the
  catalog. Decide before the demo whether to switch to `ROCKSDB` with a named volume.
- With `loadgen` running, the seed corpus's deliberately empty stock slice is replenished
  within a few minutes. **For an on-demand failure, use `stockout`; do not wait for one.**
