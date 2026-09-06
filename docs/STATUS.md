# Implementation status

**Last updated:** 2026-08-28 | **Current milestone:** M1 **done, verified** | **Next action:** M2 -- restart-safety demo (kill Connect mid-stream, prove offsets resume)

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
| M2 | CDC ingestion + restart-safety demo | not started |
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

**Seed corpus** — generated and bulk-loaded at `small`, all 7 tables matching `manifest.json`.

| | rows |
|---|---|
| categories / users / products / inventory | 48 / 5,000 / 1,000 / 1,000 |
| orders / order_items / payments | 20,000 / 40,266 / 19,000 |
| **total** | **86,314** |

Determinism verified: identical SHA-256 across runs with `--seed` + `--now` pinned.
Semantic clustering spot-checked — "waterproof hiking backpack" retrieves outdoor items only.

## Written but NOT yet executed

Nothing from M1. The items that were listed here on 2026-08-25 -- Debezium registration and the
load generator -- have both been run; results below.

Still unexecuted and belonging to later milestones: the `gov`, `orch` and `console` profiles,
every Spark job, and every dbt model.

## Current runtime state

Stack **up** with the default profile. `gov`, `orch` and `console` profiles have never
been started; their services (OPA, Marquez, Airflow, API) are unproven.

Seeded at `small` scale. `docker/api.Dockerfile` is referenced by the `console` profile
but **does not exist yet** — that profile will fail to build until M6/M8.

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
- `CLAUDE.md` — durable context and the environment gotchas that each cost a failed run.
- `docs/CHANGES-FROM-GR1.md` — every deviation from the GR1 report, why, and with evidence.

## Immediate next steps

M1 is closed. M2 is the restart-safety demo:

1. Kill `kafka-connect` mid-stream while `loadgen` is running, restart it, and show that
   `_connect_offsets` resumes exactly where it stopped -- no gap, no duplicate beyond
   at-least-once. The load generator's row-count check is the ground truth to compare against.
2. Same test for the PostgreSQL replication slot: confirm `confirmed_flush_lsn` advances and
   that the heartbeat (`heartbeat.interval.ms=10000`) keeps it advancing while the captured
   tables are idle -- the WAL-bloat guard from GR1 §4.4 has never been exercised under a real
   stall.
3. Then M3: Spark Bronze into Iceberg on Nessie, which is where the `spark-submit` jar set and
   the `MERGE INTO` write path (the main technical risk) get proven.

### A caveat to carry into M2

With `loadgen` running, the seed corpus's deliberately empty stock slice (3% of products) is
replenished within a few minutes -- replenishment waits `RESTOCK_LEAD_SECONDS` (300 s), then
refills anything low. Organic stock-outs still occur, but at `small` scale draining a head
product from ~250 units takes ~20 minutes at 25 ops/s. **For an on-demand failure, use
`stockout`; do not wait for one to happen.**
