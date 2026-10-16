# MOSAIC — Build Log

**A dated, per-milestone record of what was built, how it was verified, and what each run
actually measured.**

Last updated: 2026-10-05 · Covers M0–M3 (M3 complete)

All M2 figures below come from the final run against the corrected configuration and the
corrected harness — ledger entry 7 of 7 in `experiments/results/restart_events.jsonl`.

---

## How to read this document

Three documents describe this project's state and they do **not** overlap:

| Document | Answers | Changes |
|---|---|---|
| [STATUS.md](STATUS.md) | *Where are we right now?* | every session |
| [CHANGES-FROM-GR1.md](CHANGES-FROM-GR1.md) | *How does the build differ from the GR1 design, and why?* | when a deviation is found |
| **this file** | *What was built, in what order, and what did each run measure?* | append-only |

STATUS.md is a snapshot and is overwritten. This file is a **ledger**: entries are appended,
never rewritten, so the trajectory survives even when the snapshot moves on. When defending
GR2, this is where the "we did the work and here is the evidence" material comes from.

**Provenance of entries.** M0 and M1 below are reconstructed from STATUS.md and
CHANGES-FROM-GR1.md, which earlier sessions wrote at the time. **M2 is first-hand**: every
number in it was produced by a command run on 2026-09-07/08 and is reproducible from the
machine-readable ledgers in `experiments/results/`.

**Rule for this file:** a row may say *verified* only if a command was actually run and its
output read. Anything written but not executed is marked **unexecuted**, in the same words
STATUS.md uses. Numbers are quoted from real output, never from expectation.

---

## Contents

1. [Milestone summary](#1-milestone-summary)
2. [Deliverables by milestone](#2-deliverables-by-milestone)
3. [M2 — CDC restart safety and the WAL guard](#3-m2--cdc-restart-safety-and-the-wal-guard)
   - [3.1 What was built](#31-what-was-built)
   - [3.2 The process, step by step](#32-the-process-step-by-step)
   - [3.3 Experiment 1 — restart under load](#33-experiment-1--restart-under-load)
   - [3.4 Experiment 2 — the WAL-accumulation guard](#34-experiment-2--the-wal-accumulation-guard)
   - [3.5 Findings produced by M2](#35-findings-produced-by-m2)
   - [3.6 Defects found in this session's own harness](#36-defects-found-in-this-sessions-own-harness)
4. [Environment facts established](#4-environment-facts-established)
5. [Command reference added so far](#5-command-reference-added-so-far)
6. [GR1 Table 4.4 KPI coverage](#6-gr1-table-44-kpi-coverage)
7. [What is not verified](#7-what-is-not-verified)
8. [Run ledgers](#8-run-ledgers)
9. [M3 — Medallion on Iceberg](#9-m3--medallion-on-iceberg)

---

## 1. Milestone summary

| | Milestone | State | Date closed | Evidence |
|---|---|---|---|---|
| **M0** | Problem spec, skeleton, stack definition | **done, verified** | 2026-08-24 | 10/10 services healthy; `doctor` |
| **M1** | Sources, seed corpus, load generator, CDC live | **done, verified** | 2026-08-28 | `cdc-verify` passes both sources; loadgen row-count delta exact |
| **M2** | CDC ingestion + restart-safety demo | **done, verified** | **2026-09-08** | `restart-safety` PASS, `wal-guard` PASS (after a fix M2 itself found) |
| **M3** | Medallion on Iceberg (Spark Bronze, dbt-trino Silver/Gold) | **done, verified** | **2026-10-05** | `bronze-verify exactly-once` + `replay` PASS; latency KPI measured; `reconcile` exact after MERGEs under load (§9) |
| M4 | Trino federation + embedding pipeline | catalogs done in M0 | — | embeddings not started |
| M5 | N1 Freshness Registry | schema exists, collectors not started | — | — |
| M6 | N2 Retrieval + fusion engine | not started | — | — |
| M7 | Experiments E1–E4 | not started | — | — |
| M8 | N3 governance, `jobmarket` domain, console | not started | — | — |

---

## 2. Deliverables by milestone

Only files that exist in the tree. Line counts are as at 2026-09-08.

| Milestone | Path | Kind | What it does |
|---|---|---|---|
| M0 | `docker/compose.yaml` | stack | 10-service single-node deployment, profiles `gov`/`orch`/`console` |
| M0 | `docker/trino/catalog/*.properties` | config | 4 Trino catalogs: `iceberg` (Nessie), `mysql`, `postgresql`, `platform` |
| M0 | `sql/platform.sql` | schema | `mosaic_platform` — 6 domain-free `platform.*` tables (N1 + N3) |
| M0 | `mosaic.py` | tooling | the task runner; `make` is not installed on this machine |
| M0 | `docs/PROBLEM.md` | spec | the L0–L3 abstraction ladder |
| M1 | `domains/ecommerce/ddl/{mysql,postgres}.sql` | schema | GR1 Appendix A, plus `REPLICA IDENTITY FULL` (finding A6) |
| M1 | `domains/ecommerce/connectors/*.json` | config | Debezium connectors with connector-native masking (ADR 0004) |
| M1 | `tools/seed/` | tooling | deterministic, semantically real corpus generator + bulk loader |
| M1 | `tools/loadgen/` | tooling | continuous OLTP write mix + the stock-out lever (manufactures L0) |
| M1 | `tools/cdc/verify.py` | tooling | masking/envelope proof, expectations derived from connector JSON |
| M1 | `tools/dbsession.py` | tooling | long-lived psql/mysql sessions over `compose exec -T` |
| **M2** | **`tools/cdc/restart.py`** | **tooling** | **restart-safety + WAL-guard harness (this milestone's main artifact)** |
| **M2** | **`experiments/results/restart_events.jsonl`** | **data** | **machine-readable ledger of every M2 run** |
| **M3** | **`pipelines/bronze/bronze_stream.py`** | **pipeline** | **every Debezium topic -> `iceberg.bronze.cdc_events`, envelope intact, idempotency-keyed** |
| **M3** | **`docker/spark/`** | **image** | **`mosaic/spark:3.5.1-iceberg1.5.2`, jars baked, Iceberg pinned to Trino's** |
| **M3** | **`pipelines/transform/`** | **dbt project** | **domain-free: `assemble.py` generates Silver, `cdc_current_state` macro, Bronze source** |
| **M3** | **`domains/ecommerce/gold/`** | **models** | **4 Gold models + tests, incl. GR1 §4.3 cross-source checks** |
| **M3** | **`docker/dbt/`** | **image** | **`mosaic/dbt:1.8` (dbt-core 1.8.8, dbt-trino 1.8.3)** |
| **M3** | **`tools/bronze/verify.py`** | **tooling** | **Bronze exactly-once (both replay sources) + commit->Bronze latency** |
| **M3** | **`tools/lakehouse/reconcile.py`** | **tooling** | **Silver vs the live sources, row by row** |
| **M3** | **`tools/cdc/snapshot.py`, `tools/trino.py`** | **tooling** | **ad-hoc BLOCKING snapshot; stdlib Trino REST client** |

---

## 3. M2 — CDC restart safety and the WAL guard

**Goal, as STATUS.md set it before the work started:**

> 1. Kill `kafka-connect` mid-stream while `loadgen` is running, restart it, and show that
>    `_connect_offsets` resumes exactly where it stopped — no gap, no duplicate beyond
>    at-least-once.
> 2. Same test for the PostgreSQL replication slot: confirm `confirmed_flush_lsn` advances
>    and that the heartbeat keeps it advancing while the captured tables are idle — the
>    WAL-bloat guard from GR1 §4.4 has never been exercised under a real stall.

Both are now done. The second one **failed on first execution**, which is the most valuable
result in this milestone.

### 3.1 What was built

| File | Change | Why |
|---|---|---|
| `tools/cdc/restart.py` | **new** | The harness. Two sub-commands: `restart`, `wal-guard`. |
| `mosaic.py` | +2 commands | `restart-safety`, `wal-guard`, following the existing `_tool()` delegation pattern |
| `domains/ecommerce/ddl/postgres.sql` | `cdc_heartbeat` added to `mosaic_pub` | the fix for finding A9 below |
| `domains/ecommerce/connectors/postgres-cdc.json` | `public.cdc_heartbeat` added to `table.include.list` | same fix; the connector must *capture* the table it heartbeats into |
| `experiments/results/restart_events.jsonl` | **new** | append-only run ledger, same shape as `stockout_events.jsonl` |

**Design commitments carried into the harness** (each exists to stop a specific way of
reporting success while proving nothing):

- **Nothing in it names a domain.** Captured tables and their topics come from the
  connector's own `topic.prefix` + `table.include.list` (reusing `expectations()` from
  `tools/cdc/verify.py`); the generated-key column comes from the live catalog
  (`information_schema` on MySQL, `pg_attrdef` on PostgreSQL). Point it at another domain's
  connectors and it measures that domain.
- **The no-gap proof is exact, not statistical.** Every captured table with a generated key
  writes a *contiguous* id range. The run brackets that range at the kill and again when
  writing stops, then requires every id in it on the topic. One missing id fails the run and
  names itself.
- **Duplicates are evidence, not failure.** See finding A8.
- **"Caught up" is detected mechanically** — topic end-offsets flat for 3 consecutive polls —
  never eyeballed, and measured over the *workload's* topics only. The connector's own
  heartbeat topic is permanent background traffic and is excluded; leaving it in cost one run
  a bogus 214 s "recovery" (defect 6 below).

### 3.2 The process, step by step

| # | Step | Command / action | Result |
|---|---|---|---|
| 1 | Establish the state | `python mosaic.py doctor` | Docker Desktop was down; started it, `up` → **10/10 healthy**, all checks pass |
| 2 | Probe the offsets API | `curl :8083/connectors/<n>/offsets` | works; returns binlog `file`/`pos`/`gtids` and PG `lsn_commit` — better than parsing `_connect_offsets` |
| 3 | **Find the fact the design turns on** | grep worker log for `offset.flush.interval.ms` | **60000** — offsets flush on a timer, not per record |
| 4 | Probe MySQL position syntax | `SHOW MASTER STATUS` vs `SHOW BINARY LOG STATUS` | 8.0.46 → the former; the latter is an 8.4 spelling and errors |
| 5 | Probe slot instrumentation | `pg_replication_slots` + `pg_wal_lsn_diff` | all columns exist on PG 15 |
| 6 | Confirm the cheap poll | `kafka-get-offsets.sh --topic-partitions a,b,c` | one exec returns all topics (`--topic-list` is **not** an option in this build) |
| 7 | Write the harness | `tools/cdc/restart.py` | — |
| 8 | Smoke-test derivation only | import `topology()` | 7 captured tables, 6 with a generated key, all topics correct |
| 9 | Run experiment 1 | `python mosaic.py restart-safety` | **PASS**, but one evidence line proved nothing → fixed, see §3.6 |
| 10 | Re-run experiment 1 | same | **PASS**, every line load-bearing |
| 11 | Run experiment 2 | `python mosaic.py wal-guard --idle 130` | **FAIL** — the guard was inert |
| 12 | Diagnose | inspect `pg_publication_tables` | `cdc_heartbeat` was **not** in `mosaic_pub` |
| 13 | Fix | `ALTER PUBLICATION` + DDL + connector edits + re-register | publication now 5 tables |
| 14 | Re-run experiment 2 | `python mosaic.py wal-guard --idle 130` | **PASS** |
| 15 | Re-check the M1 proof still holds | `python mosaic.py cdc-verify` | **PASS** — 807 UPDATE events, 807 before images; the new `cdc_heartbeat` topic carries 67 `op=c` events, so the fix is visible end-to-end |

### 3.3 Experiment 1 — restart under load

```
python mosaic.py restart-safety --rate 25 --warmup 20 --stall 45 --drain 20
```

Mode `kill` = **SIGKILL**, deliberately. `docker stop` sends SIGTERM, which lets Connect
flush its offsets on the way out — a graceful shutdown proves materially less, because the
stale-offset replay is exactly the behaviour under test.

**Coverage proof** — every id committed at the source during the outage, found on its topic:

| Captured table | id range written | rows | events on topic | missing | duplicates |
|---|---|---|---|---|---|
| `ecommerce_tx.orders` | 27000..28054 | 1,055 | 1,844 | **0** | 36 |
| `ecommerce_tx.order_items` | 52728..54617 | 1,890 | 2,427 | **0** | 48 |
| `ecommerce_tx.payments` | 23612..24282 | 671 | 879 | **0** | 20 |
| `public.users` | 5614..5708 | 95 | 211 | **0** | 6 |
| `public.products` | — | 0 | 222 | — | no inserts in window |
| `public.categories` | — | 0 | 0 | — | no inserts in window |
| `public.inventory` | — | — | 2,716 | — | no generated key (PK is a FK) |

**Timings and offsets:**

| Measurement | Value |
|---|---|
| operations sent while CDC was dead | 1,760 |
| outage length | 45 s (SIGKILL) |
| connectors report `RUNNING` after restart | 8.3 s |
| writes continued into the recovery | 28.4 s |
| **fully caught up after restart** | **30.3 s** (1.9 s after the last write) |
| committed offset passes the kill point | 72.8 s after restart (flush interval is 60 s) |
| MySQL offset, at kill → after | `mysql-bin.000007:1238339` → `:2399951` |
| MySQL GTID, at kill → after | `…:1-61738` → `…:1-63546` (1,808 transactions) |
| PostgreSQL offset, at kill → after | `lsn 70936688` → `lsn 72250344` |
| slot during outage | `confirmed_flush` **FROZEN**; retained WAL 772,480 → 1,327,160 bytes |
| total duplicate events | 110 |

**Verdict: PASS.** 4 tables proved gap-free across a SIGKILL of the worker.

### 3.4 Experiment 2 — the WAL-accumulation guard

```
python mosaic.py wal-guard --idle 130
```

Three phases, because the guard only means anything as a **contrast**: idle with the worker
alive (the flush must advance — the heartbeat is what moves it), idle with the worker dead
(it must freeze), then restart (it must recover). Showing only the first phase would show a
number moving without showing what moves it.

The idle window must exceed the 60 s flush interval, or a frozen flush position demonstrates
nothing but a timer that has not fired. The harness refuses to run if `--idle` is too small.

**Before the fix — FAIL.** 130 s idle with a *healthy* connector:

| Sample | `confirmed_flush_lsn` | `current_wal_lsn` | retained WAL |
|---|---|---|---|
| start | `0/40D3EA8` | `0/4175C28` | 2,216,816 |
| +130 s | `0/40D3EA8` *(unchanged)* | `0/4176308` *(advanced)* | 2,218,576 |

WAL advancing while the confirmed-flush position does not is precisely the unbounded-retention
condition GR1 §4.4 says the heartbeat prevents.

**After the fix — PASS:**

| Phase | Observation | Verdict |
|---|---|---|
| idle, worker alive | `confirmed_flush` advanced **688,776 bytes** | ok |
| idle, worker down | `confirmed_flush` advanced 0 bytes | frozen as expected |
| after restart | `confirmed_flush` advanced 4,488 bytes | ok |
| retained WAL now | **4,544 bytes** (was 704,880 before the fix) | bounded |

| idle, connector **alive** | `confirmed_flush` advance over 130 s |
|---|---|
| heartbeat table outside the publication | **0 bytes** |
| heartbeat table published *(the fix)* | **688,776 bytes** |

**One honest caveat the harness now prints itself.** Retained WAL did *not* grow during the
outage phase, and that is correct rather than a weak result: the heartbeat's `INSERT` is
issued **by the connector**, so killing the worker also stops the only writer and the database
goes genuinely idle. The guard protects an idle capture set beside a *busy* database — which
is what phase 1 measures. It cannot protect anything while the consumer is down.

### 3.5 Findings produced by M2

Both belong in CHANGES-FROM-GR1 Part A and in the GR2 report.

#### A8 — Restart safety is at-least-once with a replay window, not exactly-once

GR1 Table 4.3 states restart safety in one line: *"offset stored in the Kafka Connect offsets
topic"*. True, and not the whole story. **Connect flushes source offsets on a timer** —
`offset.flush.interval.ms = 60000` on this worker, which is the Connect default. The stored
offset therefore trails the last produced record by up to a minute, and a crash replays
everything committed since.

Measured: **110 duplicate events** across one 45 s SIGKILL, and **zero** missing rows. The
replay window is bounded by the flush interval, not by the length of the outage — a 5-second
crash and a 5-minute crash replay the same ~60 s of work.

*Why it matters for the report:* a harness that scored duplicates as failure would report a
false negative on a mechanism working exactly as designed. Downstream, it is also the reason
GR1 §4.2's "deduplicate using the source offset as an idempotency key" is load-bearing rather
than decorative — M3's Bronze layer must actually implement it.

#### A9 — GR1 §4.4's WAL-bloat guard was inert as configured

The heartbeat was firing correctly (503 rows in `cdc_heartbeat`, 503 messages on
`__debezium-heartbeat.mosaic.postgres`) and it still did not advance `confirmed_flush_lsn`,
because **`heartbeat.action.query` wrote to a table outside the publication**. `pgoutput`
decodes only *published* tables, so the heartbeat's WAL was never turned into a change event,
Debezium never gained a new LSN to confirm, and the flush position stood still while WAL
accumulated.

The fix is one table in two places — `mosaic_pub` and `table.include.list`. The effect is
0 bytes → 688,776 bytes of flush advance over the same 130 s idle window.

*Why it matters for the report:* this is the same **shape** as finding A6 (PostgreSQL emits no
before image under the default replica identity) — a mitigation GR1 names in prose, which is
silently ineffective until something measures it. Neither failure produces an error, a warning,
or a log line. Both were found only by building the measurement. That is a defensible answer to
*"what did the implementation phase actually contribute?"*

### 3.6 Defects found in this session's own harness

Recorded because this project has been bitten three times by measurement bugs that reported
success (a false "all healthy" from `docker compose ps`; a load generator quoting its send
rate; a `--from-beginning` topic sample making a before-image check pass vacuously). These are
the M2 additions to that list, all caught before the milestone was called done.

| # | Defect | Symptom | Fix |
|---|---|---|---|
| 1 | Committed offset read **inside its own flush window** | Printed `at kill:` and `after:` as identical, so the headline evidence line proved nothing while looking like a result. A later manual probe showed the offset *had* advanced. | Poll until the offset passes the kill point and **report how long that took** — the wait became the measurement (30.4 s on the passing run) |
| 2 | Fixed sleep of one flush interval (the first attempt at fixing #1) | Still read too early: the flush landed ~97 s after restart on one run, not the ~60 s the interval alone suggests | Replaced with the poll above |
| 3 | `query_rows(sep="|")` for PostgreSQL | `PostgresSession` reads with `-F "\t"`, so every row would have parsed as a single field and both the slot sample and the key-column derivation would have silently returned nothing | Use the default tab separator |
| 4 | Idleness check counted the heartbeat table | Once `cdc_heartbeat` became captured (the A9 fix), its own writes would have been flagged as contamination and failed every `wal-guard` run | Exclude the relation named by `heartbeat.action.query`, parsed from the declared config |
| 5 | Verdict did not require the offset to move | A worker that came back and consumed nothing would still have passed: topics quiet, no id missing from the bracketed window | `_offset_progressed()` added to the pass condition |
| 6 | **The A9 fix broke the recovery metric.** Once `cdc_heartbeat` was captured, its topic gained an event every 10 s *forever*, and the quiescence detector watched every topic | One run reported **214.59 s** of "recovery" — the harness waiting on a heartbeat it had itself put into the capture set. The real figure was ~30 s | Drain only the workload's topics; exclude the relation named by `heartbeat.action.query` |
| 7 | Offset advance timed from the *poll's* start, not the restart | Printed "committed offset caught up **0.03 s** after the restart" — the poll began after a 214 s drain, by which time the offset had long since moved. Meaningless, and flattering | Poll the offset *inside* the drain loop and time both from `t_restart`. Now reports 72.8 s, consistent with a 60 s flush interval |

---

## 4. Environment facts established

Additions to CLAUDE.md's gotcha list, each costing at least one failed or misleading run.

| Fact | Consequence |
|---|---|
| `offset.flush.interval.ms = 60000` on this worker | never read a committed offset within ~60 s of a restart and call it evidence |
| MySQL here is **8.0.46** → `SHOW MASTER STATUS` | `SHOW BINARY LOG STATUS` is the 8.4 spelling and is a syntax error |
| `kafka-get-offsets.sh` has **no** `--topic-list` in this build | use `--topic-partitions a,b,c` — one exec for all topics, which is what makes a 2 s poll affordable |
| `PostgresSession` reads with `-F "\t"` | `query_rows()` on PG must use the default tab separator, not `|` |
| Debezium `heartbeat.action.query` only helps if its table is **captured** | otherwise the WAL guard is inert and silent (finding A9) |
| The heartbeat's writer **is the connector** | killing the worker stops the heartbeat too, so an outage phase shows no WAL growth on an otherwise-idle database |
| Docker Desktop stopping takes the whole stack with it | happened mid-session; named volumes survived, `up` restores |

---

## 5. Command reference added so far

| Command | Milestone | Purpose |
|---|---|---|
| `python mosaic.py doctor` | M0 | one-shot state report |
| `python mosaic.py up / down / clean` | M0 | lifecycle; `up` waits for **healthy** |
| `python mosaic.py check-ports` | M0 | host port collisions, with owners |
| `python mosaic.py check-abstraction` | M0 | the L3 domain-purity gate |
| `python mosaic.py generate` / `python -m tools.seed.load` | M1 | corpus generation and bulk load |
| `python mosaic.py connectors` | M1 | register Debezium connectors for a domain |
| `python mosaic.py cdc-verify [--require-update]` | M1 | masking/envelope proof on the topics |
| `python mosaic.py loadgen --rate N --duration S` | M1 | continuous OLTP writes |
| `python mosaic.py stockout / restock` | M1 | the L0 failure lever |
| **`python mosaic.py restart-safety [--mode kill\|stop\|pause]`** | **M2** | **kill Connect under load, prove no gap** |
| **`python mosaic.py wal-guard [--idle N]`** | **M2** | **heartbeat / WAL-accumulation guard** |
| **`python mosaic.py resnapshot`** | **M3** | **BLOCKING ad-hoc snapshot, verified against source row counts** |
| **`python mosaic.py bronze`** | **M3** | **Bronze stream liveness, backlog, last batch** |
| **`python mosaic.py bronze-verify exactly-once\|replay\|latency`** | **M3** | **the Bronze proofs and the end-to-end KPI** |
| **`python mosaic.py dbt [-- args]`** | **M3** | **Silver + Gold (assemble, then dbt)** |
| **`python -m tools.lakehouse.reconcile`** | **M3** | **Silver == live sources, row by row** |

---

## 6. GR1 Table 4.4 KPI coverage

GR1 defines five CDC key performance indicators to be measured "once the pipeline is
implemented in GR2". Two are now measured.

| KPI | State | Value / blocker |
|---|---|---|
| **End-to-end latency (source commit → Iceberg Bronze)** | **measured (M3)** | p50 **7.8 s** MySQL / **8.0 s** PostgreSQL, p99 12.3 / 12.9 s; capture p50 0.26 / 0.49 s, the rest is the 10 s trigger |
| Throughput under sustained load | partially (M1) | ~100 ops/s sustained, bounded by per-commit fsync under Docker Desktop |
| **Restart recovery time** | **measured (M2)** | **30.3 s** to fully caught up after a 45 s SIGKILL, with writes continuing 28.4 s into recovery |
| DDL handling (`ALTER TABLE` downstream impact) | not started | limitation recorded in ADR 0005 (`schemas.enable=false` hides pure type changes) |
| **WAL lag (PostgreSQL only)** | **measured (M2)** | frozen flush + growing WAL reproduced on demand; guard fixed and re-measured (0 → 688,776 bytes) |

---

## 7. What is not verified

Stated explicitly, per this project's reporting rule.

- **The DDL fix is applied to the running database by hand** (`ALTER PUBLICATION`). The edit
  to `domains/ecommerce/ddl/postgres.sql` only takes effect on a fresh volume — the same
  retroactivity caveat CLAUDE.md records for `REPLICA IDENTITY FULL`.
- ~~M3 is recon-complete but unexecuted.~~ *Superseded 2026-10-05:* M3 ran and is verified;
  what M3 left unverified is listed in §9.7.
- The `gov`, `orch` and `console` profiles have still never been started.
- Everything remains at `small` scale (~86k seeded rows plus load-generator growth).

---

## 8. Run ledgers

Machine-readable, append-only, one JSON object per line. These are what E1–E4 read back.

| File | Written by | Contents |
|---|---|---|
| `experiments/results/stockout_events.jsonl` | `stockout` / `restock` / `loadgen` | every stock-out (organic or manual) with its source commit timestamp |
| `experiments/results/restart_events.jsonl` | `restart-safety` / `wal-guard` | per-run verdict, gap and duplicate counts, all timings, offsets at kill and after, and the full replication-slot sample series; since M3 also per-table key ranges |
| `experiments/results/snapshot_events.jsonl` | `resnapshot` | per-table op=r counts vs source rows |
| `experiments/results/bronze_events.jsonl` | `bronze-verify` | exactly-once, Spark-replay and latency runs |
| `experiments/results/lakehouse_events.jsonl` | `tools.lakehouse.reconcile` | per-table missing / extra / differing counts |

As at 2026-09-08 the restart ledger holds 5 entries: 3 `restart-safety` (pass, inconclusive,
pass) and 2 `wal-guard` (fail, pass). **The failures are kept deliberately.** The
`wal-guard` fail/pass pair *is* the evidence for finding A9, and the `inconclusive` entry is
the run that exposed harness defect #1 in §3.6.


---

## 9. M3 — Medallion on Iceberg

**Closed 2026-10-05. First-hand:** every number below was produced by a command run that day
and is reproducible from `experiments/results/{bronze,lakehouse,restart,snapshot}_events.jsonl`.
Earlier M3 work (2026-09-12..23: the Spark image, Nessie persistence, the two write-path smoke
tests, the Bronze job minus its key) is recorded in STATUS.md's verified table.

### 9.1 What was built

| Piece | What it does |
|---|---|
| `idempotency_key()` in `bronze_stream.py` | SHA-256 over `to_json(struct(topic, op, record_key, source.file/pos/row/lsn/txId/ts_ms, before, after))` -- the last piece the Bronze stream was blocked on |
| `_ingest_ts` as a per-batch literal | replaces `current_timestamp()`: stable even if a cached partition is recomputed, and stamped into the snapshot summary as `mosaic.ingest_ts_us` so each row joins to the snapshot that published it |
| `--reset-checkpoint` renames | the checkpoint is set aside, never deleted |
| `tools/bronze/verify.py` | `exactly-once` (Connect replay), `replay` (Spark replay), `latency` (Table 4.4 KPI) |
| `tools/cdc/restart.py` changes | duplicates = identical source events; per-table key ranges written to the ledger |
| `pipelines/transform/` | domain-free dbt project: `assemble.py` (Silver generator + Gold copier), `cdc_current_state` macro, Bronze source with freshness, schema-name rule |
| `domains/ecommerce/gold/` | `orders`, `users`, `product_sales`, `user_category_affinity` + tests |
| `docker/dbt/`, compose `dbt` service | dbt-core 1.8.8 + dbt-trino 1.8.3, profile `transform` |
| `tools/lakehouse/reconcile.py` | Silver vs the live sources through Trino federation |
| `tools/trino.py` | stdlib Trino REST client |

### 9.2 The process, step by step

1. **State on arrival.** Whole stack `Exited (255)` for 10 days (a Docker Desktop restart).
   `mosaic-spark` had last run on 2026-09-23 and exited 1 on the `NotImplementedError` of the
   unwritten key -- after reaching Nessie, creating the table and subscribing to 8 topics, so the
   plumbing itself was already proven.
2. **Key written; `up`.** Spark failed on its first batch: `Cannot fetch offset 2278 ...
   cdc_heartbeat ... Some data may have been lost`. Kafka: **every table topic had earliest ==
   latest** -- retention had deleted all of it while the stack was down (finding A10, second
   occurrence). Bronze: 0 rows, 0 snapshots, so the checkpoint (`offsets/0..1`, `commits/0`) held
   nothing worth keeping. A delete of it was declined by the session's permission policy; it was
   **renamed** to `bronze.stale-2026-09-23` instead, and `--reset-checkpoint` was changed to do
   the same.
3. **Stream healthy** at 04:20:35 UTC. `resnapshot`: all 7 tables re-emitted exactly their source
   row counts, **114,709 op=r in 40.4 s**. Bronze drained to backlog 0; checked through *Trino*
   (the other engine): per table `n = distinct keys = distinct event_id = source rows`.
4. **Connect replay.** `restart-safety --mode kill`: PASS, 0 missing, **183 replayed events**.
   Then `bronze-verify exactly-once` (9.3).
5. **Spark replay**, twice (9.3). **Latency** (9.4).
6. **dbt, first build:** 4 PostgreSQL Silver tables built and passed every test; the 3 MySQL ones
   failed with `integer overflow` -- Trino's `date_add` narrows its amount to 32 bits, and epoch
   millis do not fit. Rewritten on `from_unixtime_nanos`.
7. **Second build:** all Silver fine, PostgreSQL ones now through the incremental MERGE path;
   `gold.orders` failed `ICEBERG_TOO_MANY_OPEN_PARTITIONS (174 > 100)` under GR1 §4.3's partition
   spec. Writer cap raised to 5000, Trino restarted. **Third build: 78/78.**
8. **Reconcile** on the freshly built Silver: exact. **Negative control** with the wrong source
   zone: every MySQL row differs.
9. **MERGE under load:** `loadgen --rate 25 --duration 240` in the background, three `dbt build`s
   during it, one `stockout`. Then quiesce, final build, reconcile: exact (9.5).

### 9.3 Bronze exactly-once

**Connect side** -- `bronze-verify exactly-once` against the 04:32 UTC restart-safety run:

| Table | keys written in the outage window | insert events on Kafka | Bronze `op=c` rows | distinct |
|---|---|---|---|---|
| `ecommerce_tx.orders` | 28405..29492 (1,088) | 1,107 | 1,088 | 1,088 |
| `ecommerce_tx.order_items` | 55258..57237 (1,980) | 2,010 | 1,980 | 1,980 |
| `ecommerce_tx.payments` | 24491..25209 (719) | 724 | 719 | 719 |
| `public.users` | 5722..5816 (95) | 95 | 95 | 95 |

Independent of Bronze's own key on purpose: an insert happens once at the source, so a second
`op=c` for a key can only be a replay, and a missing one only a gap.

**Spark side** -- `bronze-verify replay` (SIGKILL the stream under 20 ops/s, ~30 s outage):

| Run | Crash landed | Re-run of the in-flight batch | Window check |
|---|---|---|---|
| 1 | mid-batch 70, nothing committed | 2,987 in, 2,987 written | 624 / 1,137 / 440 / 70 inserts, all exact |
| 2 | **after batch 77's Iceberg commit, before its checkpoint commit** | **673 in, 0 written, 673 dropped**; still 1 snapshot for batch 77 | 536 / 1,033 / 371 / 46, all exact |

Run 2 is the case the Bronze docstring claimed to absorb and that a naive append would have
duplicated -- it occurred naturally, so the tool's withheld-commit branch (written to force it)
never fired. Spark's own log: `batch 77: 673 in, 673 written` -> restart -> `batch 77: 673 in,
0 written, 673 duplicate(s) dropped`.

### 9.4 End-to-end latency (GR1 Table 4.4)

`bronze-verify latency`, 120 s at 20 ops/s (2,400 ops), trigger 10 s. Availability = commit time
of the Iceberg snapshot that published the row (joined exactly via `_ingest_ts`); `op=r` excluded.

| Source | events | p50 | p95 | p99 | max | capture p50 / p95 | landing p50 / p95 |
|---|---|---|---|---|---|---|---|
| MySQL | 5,144 | 7.76 s | 11.66 | 12.29 | 13.21 | 0.26 / 0.48 | 7.50 / 11.37 |
| PostgreSQL | 2,791 | 7.99 s | 12.03 | 12.91 | 13.51 | 0.49 / 0.87 | 7.50 / 11.51 |

Capture = source commit -> Kafka record; landing = Kafka -> snapshot commit. Debezium is
sub-second on both; the trigger interval dominates (uniform 0-10 s wait + ~2.5 s batch). The KPI
is therefore tunable by `BRONZE_TRIGGER_SECONDS`, at the cost of more, smaller files.

### 9.5 Silver and Gold

Incremental MERGE row counts during and after the load test:

| Model | build 2 | build 3 | final |
|---|---|---|---|
| `silver.orders` | 577 | 660 | 2,621 |
| `silver.inventory` | 278 | 300 | 724 |
| `silver.users` | 73 | 54 | 349 |
| `silver.order_items` / `payments` / `products` | | | 3,795 / 1,363 / 173 |
| `gold.orders` | 577 | 660 | 2,621 |

Build 1 merged 0 rows: it ran before the generator's first commits reached Bronze.

**Reconcile, final (quiescent):** 0 missing / 0 extra / 0 differing in all 7 tables -- 36,678
orders, 70,056 items, 29,827 payments, 6,479 users, 1,000 products, 48 categories, 1,000
inventory -- on every non-masked column (masked: `buyer_email`, `buyer_phone`, `email`, `phone`).

**Gold vs live aggregates (ad hoc, federated):** `gold.orders` item lines, units and status vs
MySQL: 36,678 orders, 0 missing / extra / differing. `product_sales.stock_qty` vs live
`inventory`: 0 of 1,000 differ. During the load, product 68 hit stock 0 at the source at
05:01:49; Bronze had it 2 s later; Silver/Gold still said 9 until the next build -- L0, observed.

**Iceberg side effects:** `silver.orders` after 4 MERGEs: 4 data files, 5 position-delete files,
785 position deletes (snapshot summary; Trino 451's `$files` does not list delete files).
`gold.product_sales` kept 6 snapshots across builds (`CREATE OR REPLACE`). `SHOW STATS` after the
ANALYZE post-hook: `order_id` NDV 36,678.

**GR1 §4.3's partitioning, measured** (finding A12): `gold.orders` 2,944 partitions, 2,968 files,
12.6 rows/file. `ANALYZE` 8.25 s vs 0.25 s on unpartitioned `silver.orders` (33x); full scan
1.28 s vs 0.09 s (14x). GR1 Listing 4.2: 100 rows in 1.61 s.

### 9.6 Findings produced by M3

| # | Finding | Where |
|---|---|---|
| A10 | Kafka retention erased CDC history before Bronze could land it -- twice | CHANGES-FROM-GR1 |
| A11 | MySQL `DATETIME` arrives 7 h off; harmonisation moves from an SMT to Silver | CHANGES-FROM-GR1 |
| A12 | §4.3's `gold.orders` partitioning is sized for another scale | CHANGES-FROM-GR1 |
| A8 correction | M2's duplicate count mixed replays with legitimate updates | CHANGES-FROM-GR1 A8 |
| D15 | seed and load generator used different region vocabularies | CHANGES-FROM-GR1 |
| -- | Trino cannot write snapshot-summary properties; `$files` hides delete files | STATUS finding 12 |

### 9.7 What M3 leaves unverified

- `bronze-verify replay`'s withheld-commit branch (never needed; see 9.3).
- Iceberg maintenance -- no compaction, snapshot expiry or orphan cleanup exists yet.
- No dbt schedule; Silver/Gold refresh only on `python mosaic.py dbt`.
- Everything at `small` scale.

### 9.8 Defects caught in this session's own work

1. **M2's duplicate metric** (above, A8 correction) -- found while designing the Bronze check,
   because the insert-range proof needed a count that meant *replays*.
2. **A wrong caveat, caught by the data.** The latency tool first printed that MySQL source times
   have one-second resolution, inferred from the snapshot rows. The capture figures contradicted
   it (p50 0.26 s could not survive up to +1 s of truncation); querying Bronze showed 19,029 of
   19,045 MySQL inserts with sub-second times -- only `op=r` rows are whole seconds. Caveat
   corrected before it reached any document.
3. **`decimal` over REST.** The first latency run crashed printing `max()`: Trino's REST protocol
   returns decimals as strings. Cast to `double` in SQL; the run was repeated rather than patched.
4. **Negative control kept in the ledger.** `lakehouse_events.jsonl` line 2 is the deliberate
   wrong-zone run (`MISMATCH`); it is the evidence that the reconcile check can fail.
