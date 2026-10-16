# Implementation status

**Last updated:** 2026-10-16 | **Current milestone:** M3 **done, verified** -> M4 next | **Next action:** M4 -- the embedding pipeline (`product_embeddings` from Gold/Silver product text), then register datasets for N1

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
| **M3** | Medallion on Iceberg (Spark Bronze, dbt-trino Silver/Gold) | **done, verified** -- Bronze exactly-once across *both* replay sources; end-to-end latency KPI measured; Silver equals the live sources row for row after incremental MERGEs under load; Gold checked against live aggregates |
| M4 | Trino federation + embedding pipeline | catalogs done in M0; embeddings not started |
| M5 | N1 Freshness Registry | schema exists; collectors not started |
| M6 | N2 Retrieval + fusion engine | not started |
| M7 | Experiments E1–E4 | not started |
| M8 | N3 governance, `jobmarket` domain, console | not started |

---

## What is verified working

Confirmed by an actual run, not by inspection.

**Stack** — `python mosaic.py up`, 10/10 services healthy; `doctor` all checks passed
(2026-10-05), including the new `bronze stream` check.

| Check | Evidence |
|---|---|
| MySQL CDC prerequisites | `binlog_format=ROW`, `binlog_row_image=FULL`, `gtid_mode=ON` |
| PostgreSQL CDC prerequisites | `wal_level=logical`, publication `mosaic_pub`, pgvector installed |
| Domain schemas | MySQL 3 tables; PostgreSQL 6 tables |
| Platform metadata DB | `mosaic_platform` with all 6 `platform.*` tables |
| Nessie | API v2 on 19120; `ROCKSDB` on volume `nessie-data`, survives restarts |
| Trino catalogs | `iceberg` (Nessie), `mysql`, `postgresql`, `platform` — all queryable |
| Abstraction gate | `check-abstraction` passes (6 files scanned) |
| Debezium connectors | both `RUNNING`, Kafka signal channel declared |
| PII masking | `cdc-verify` passes on both sources, before **and** after images |
| Load generator | 5,401 ops over 240 s, row-count delta matched exactly on all 4 tables (2026-10-05) |
| Restart safety (M2, re-run M3) | SIGKILL of Connect for 45 s under load: **0 missing**, **183 replayed events** (identity-matched), caught up 30.2 s after restart |
| WAL guard (M2) | flush advance over 130 s idle: 0 bytes broken -> 688,776 bytes fixed |
| **Bronze stream (M3)** | `mosaic-spark` runs `bronze_stream.py` as its main process; healthy = query active + progress < 60 s; 8 topics -> `iceberg.bronze.cdc_events` |
| **Bronze bootstrap (M3)** | after `resnapshot`, Bronze holds exactly each table's source row count as `op=r` (`n = distinct keys = distinct event_id`), 114,709 rows, 0 null `_src_ts` |
| **Bronze exactly-once, Connect side (M3)** | for the restart-safety run's bracketed inserts, Kafka held 1,107 / 2,010 / 724 / 95 insert events for 1,088 / 1,980 / 719 / 95 keys; Bronze holds **exactly one per key** |
| **Bronze exactly-once, Spark side (M3)** | `docker kill` of the stream landed between batch 77's Iceberg commit and its checkpoint commit; on restart batch 77 re-ran: **673 in, 0 written, 673 dropped**, still one Iceberg snapshot for the batch; inserts in the window exact |
| **End-to-end latency (M3, GR1 Table 4.4)** | source commit -> Iceberg Bronze snapshot, 120 s at 20 ops/s: MySQL p50 **7.76 s** / p95 11.66 / p99 12.29 / max 13.21; PostgreSQL p50 **7.99 s** / p95 12.03 / p99 12.91 / max 13.51. Capture (commit -> Kafka) p50 0.26 s MySQL, 0.49 s PostgreSQL; the rest is the 10 s trigger |
| **Silver (M3)** | 7 generated models, incremental `MERGE`; after 4 builds under concurrent load, `reconcile`: **0 missing / 0 extra / 0 differing** rows in all 7 tables vs the live sources (36,678 orders, 70,056 items, 29,827 payments, 6,479 users, 1,000 products, 48 categories, 1,000 inventory) |
| Silver negative control | same check with the wrong source zone: every MySQL row differs, PostgreSQL 0 -- the comparison has teeth |
| **Gold (M3)** | `orders` (incremental MERGE, GR1 section 4.3 partitioning), `users` (`bucket(user_id, 32)`), `product_sales`, `user_category_affinity`; 78/78 dbt nodes pass incl. both cross-source referential tests |
| Gold vs live sources | `gold.orders` vs MySQL aggregates: 36,678 orders, 0 missing / extra / differing; `product_sales.stock_qty` vs live inventory: 0 of 1,000 differ (at quiescence) |
| GR1 Listing 4.2 | the federated `iceberg.gold.orders` x `postgresql.public.users` query runs, 100 rows, 1.61 s |
| ANALYZE post-hook | `SHOW STATS` on `gold.orders`: NDV `order_id` 36,678 |
| Trino memory bound | `-Xmx3G` (`docker/trino/jvm.config`) + `mem_limit: 6g` + `MALLOC_ARENA_MAX=2`, 2026-10-16. Same workload, uncapped -> final: incremental `dbt build` peak 6.20 -> **2.59 GiB**, 2,944-partition write 7.04 -> **3.90 GiB**, kernel peak 7.39 -> **3.93 GiB**; 0 OOM events; 78/78 pass. `MALLOC_ARENA_MAX` alone took off-heap 1.47 -> 0.87 GiB |

## Written but NOT yet executed

- The **withheld-commit branch** of `bronze-verify replay` (moves `commits/N` aside to force a
  re-run). Both runs so far crashed into states Spark handles itself -- one mid-batch, one in
  the exact commit-landed/checkpoint-missing window -- so that branch never fired.
- Iceberg **maintenance** (`optimize`, `expire_snapshots`, `remove_orphan_files`) does not exist
  yet. Every Silver MERGE adds position-delete files (5 on `silver.orders` after 4 builds) and
  every Bronze micro-batch adds a snapshot; nothing compacts them. GR1 gives this to Airflow,
  whose `orch` profile has never been started.
- dbt has **no schedule**: Silver/Gold refresh only when `python mosaic.py dbt` runs. Gold's
  staleness is therefore operator-driven until orchestration exists.
- The `gov`, `orch` and `console` profiles are still unstarted. `docker/api.Dockerfile` does not
  exist; `console` will fail to build until M6/M8.

## Current runtime state

Stack **up**, default profile, 10/10 healthy, Bronze stream active with backlog 0. Silver and Gold
last built at 2026-10-16 03:39 UTC (incremental, no source changes since 2026-10-05); nothing
refreshes them automatically.

Row counts now (seed corpus + load-generator growth): orders 36,678, order_items 70,056, payments
29,827, users 6,479, products 1,000, categories 48, inventory 1,000.

Bronze's checkpoint was **set aside, not deleted**, on 2026-10-05:
`spark-state:/opt/mosaic/state/checkpoints/bronze.stale-2026-09-23` (its offsets pointed at data
retention had already deleted; the table it belonged to was empty). Safe to remove once nobody
needs it.

`mosaic_pub` publishes five tables: the four domain tables plus `cdc_heartbeat` (finding 10).

---

## Findings worth carrying into the report

> Full narrative version, with evidence and suggested framing:
> [CHANGES-FROM-GR1.md](CHANGES-FROM-GR1.md).

1. **GR1 Appendix C's SMT masking chain cannot work as specified.** See
   [ADR 0004](adr/0004-connector-native-masking.md).
2. **Two GR1 Appendix B image tags do not resolve** (`debezium/connect:2.7`,
   `projectnessie/nessie:0.90.4` on Docker Hub).
3. **MySQL reports a missing `FILE` privilege as `ERROR 1045 Access denied`.**
4. **GR1 Appendix B's Avro converter cannot load** ([ADR 0005](adr/0005-json-converter.md)).
5. **`docker compose ps` omits exited containers**, which once produced a false "all healthy".
6. **PostgreSQL emits no before image under the default replica identity** (A6).
7. **Debezium truncates a mask hash to the source column's width** (A7).
8. **A load generator that reports its send rate is lying** -- ~100 ops/s sustained here.
9. **"Restart safety" is at-least-once with a replay window** (A8). *Corrected in M3:* M2's
   "110 duplicates" counted events per key, which includes legitimate updates; the
   identity-matched count for a comparable run is 183 replays. Conclusion unchanged.
10. **GR1 section 4.4's WAL-bloat guard was inert as configured** (A9).
11. **Kafka retention deleted the CDC history before Bronze could land it -- twice** (A10).
    2026-09-12: `categories` topic empty. 2026-10-05, after a 10-day stop: *every* table topic
    empty, and Bronze's checkpoint pointed at deleted offsets (`OffsetOutOfRange`). Recovered
    both times by a BLOCKING ad-hoc snapshot; Bronze landed it exactly.
12. **Trino cannot write custom Iceberg snapshot-summary properties.** Spark can (Bronze stamps
    `max(_src_ts)` and `ingest_ts_us` per commit). Silver/Gold watermarks must be read from the
    data. Also: **Trino 451's `$files` lists no delete files** -- MERGE's position deletes are
    visible only in the snapshot summary (`total-position-deletes`). N1 must use summaries.
13. **Bronze is exactly-once over an at-least-once source** (positive result, M3). One
    idempotency key -- SHA-256 over op, record key, source position, `source.ts_ms` and both
    images -- absorbs Connect replays *and* Spark batch re-runs. Neither the whole envelope
    (its `ts_ms` is processing time) nor the whole source block (PostgreSQL's `sequence` is
    seeded from the stored offset after a restart) would have worked.
14. **MySQL `DATETIME` arrives 7 hours off** (A11) -- the wall clock encoded as if UTC. GR1's
    `TimestampConverter` was load-bearing; with A1's SMTs gone, Silver converts. Negative
    control proves it.
15. **GR1 section 4.3's `gold.orders` partitioning is sized for another scale** (A12): 2,944
    partitions of 12.6 rows; needs Trino's writer cap raised; `ANALYZE` 33x and full scan 14x
    slower than unpartitioned. Kept as written; **decision open** (see open questions).
    Measured 2026-10-13: an incremental build that changes **0 rows** still spends 45-61 s on
    `gold.orders` (one statement scans 27,142 splits for 95 MiB), and was what drove Trino's
    heap to 6+ GiB before its heap was capped.
16. **The seed and the load generator used different region vocabularies** (D15) -- cities vs.
    `north`/`central`/... -- so per-region aggregates split. Fixed in the generator; existing rows
    keep the five macro-region values.
17. **Silver must order by source position, not time** (M3 design). A snapshot read carries its
    *read* time, which can postdate the commit of an UPDATE positioned after it. Verified per
    source: MySQL `source.pos` is per binlog event (orders and items of one GTID have distinct
    positions), PostgreSQL `lsn` is per change.

## Open questions

- **`gold.orders` partition spec (A12).** GR1's `day x bucket(region, 16)` gives ~13-row files
  here. Keep it (spec-faithful) or move to month-level partitioning? Best settled in E2 at
  `full` scale. The user's call.
- **Kafka retention (A10).** Leave the 7-day default (Bronze is the system of record) or set
  `retention.ms=-1` on CDC topics so a long stop cannot erase unconsumed history?
- **Scale.** Everything is still `small` (~36k orders). GR1 section 10.1 wants 10M+ for E2.
  `full` projects to ~40 minutes of loading; now that the pipeline shape is fixed, it is worth
  doing before M7.
- **Silver partitioning (D12).** Unpartitioned current-state tables; revisit only if a consumer
  needs pruning.

---

## Tooling added for future sessions

- `python mosaic.py doctor` — one-shot state report, now including the Bronze stream.
- `python mosaic.py check-ports` / `check-abstraction` / `cdc-verify` / `loadgen` /
  `stockout` / `restock` / `restart-safety` / `wal-guard` — see CLAUDE.md.
- `python mosaic.py resnapshot` — Debezium BLOCKING snapshot of every captured table over the
  Kafka signal channel; verifies op=r counts against source row counts.
- `python mosaic.py bronze` — Bronze stream liveness, backlog, totals, last batch.
- `python mosaic.py bronze-verify exactly-once` — carries the last `restart-safety` run through to
  Bronze: exactly one insert event per bracketed key.
- `python mosaic.py bronze-verify replay` — SIGKILLs the Bronze stream under load, forces the
  in-flight batch to re-run, proves no duplicate and no gap.
- `python mosaic.py bronze-verify latency` — GR1 Table 4.4's end-to-end KPI, exact per row
  (`_ingest_ts` joins each row to the snapshot that published it).
- `python mosaic.py dbt [-- <args>]` — Silver/Gold. The image runs `assemble.py` first, which
  generates Silver from the connectors and copies `domains/*/gold/` in.
- `python -m tools.lakehouse.reconcile` — Silver vs the live sources, row by row. Run idle.
- `tools/trino.py` — stdlib Trino REST client; `query()`, `scalar()`.
- Ledgers in `experiments/results/`: `restart_events`, `snapshot_events`, `bronze_events`,
  `lakehouse_events`, `stockout_events` (`.jsonl`, append-only, failures kept on purpose --
  `lakehouse_events` line 2 is the deliberate wrong-zone negative control).
- `docs/BUILD-LOG.md`, `docs/ADVISOR-BRIEFING.md`, `docs/CHANGES-FROM-GR1.md`, `CLAUDE.md`.

## Immediate next steps

M3 is closed. M4 is next:

1. **Embedding pipeline** (`pipelines/embeddings/`): read product text, embed with
   `all-MiniLM-L6-v2` (384-d), write `product_embeddings` with `refreshed_at`. Its batch cadence
   *is* the slow channel's staleness, so it must be a deliberate, recorded interval.
2. **Register datasets** in `platform.datasets` -- Bronze, the 7 Silver tables, the 4 Gold tables,
   the vector table -- each with its `row_timestamp_column` (`_src_ts` on Silver/Gold,
   `refreshed_at` on the vectors). That is the hand-off to M5 (N1 collectors), which will read
   Bronze watermarks from snapshot summaries and Silver/Gold watermarks from the data (finding 12).
3. Consider the open questions above before E2; scale to `full` before M7.

### Caveats to carry forward

- The publication fix (finding 10) and `REPLICA IDENTITY FULL` were applied to the **running**
  database; the DDL edits only take effect on a fresh volume.
- Kafka holds a re-snapshot (2026-10-05) on top of streaming history; Silver orders by source
  position, so multiple `op=r` per key are harmless -- do not "fix" them.
- With `loadgen` running, the seed's empty stock slice refills within minutes. **For an
  on-demand failure, use `stockout`.**
- Gold is as fresh as the last `dbt` run. During the M3 load test the live source showed product
  68 at stock 0 while Silver/Gold still said 9 -- the L0 asymmetry, observed in this stack.
