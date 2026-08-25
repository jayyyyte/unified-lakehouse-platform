# Implementation status

**Last updated:** 2026-08-25 | **Current milestone:** M1 (partially done) | **Next action:** register Debezium connectors, then build the load generator

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
| **M1** | Sources, seed corpus, load generator | **in progress** — generator + loader done; load generator not started |
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

**Seed corpus** — generated and bulk-loaded at `small`, all 7 tables matching `manifest.json`.

| | rows |
|---|---|
| categories / users / products / inventory | 48 / 5,000 / 1,000 / 1,000 |
| orders / order_items / payments | 20,000 / 40,266 / 19,000 |
| **total** | **86,314** |

Determinism verified: identical SHA-256 across runs with `--seed` + `--now` pinned.
Semantic clustering spot-checked — "waterproof hiking backpack" retrieves outdoor items only.

## Written but NOT yet executed

- `python mosaic.py connectors` — Debezium connector registration has never been run.
  The configs in `domains/ecommerce/connectors/` are valid JSON but unproven against a
  live Debezium 2.7.3. **Expect the first run to surface config issues** — particularly
  the connector-native masking properties from ADR 0004, which replaced the SMT chain and
  have not been exercised.
- `tools/seed/load.py --truncate` path has run; the load generator does not exist yet.

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
- `CLAUDE.md` — durable context and the environment gotchas that each cost a failed run.
- `docs/CHANGES-FROM-GR1.md` — every deviation from the GR1 report, why, and with evidence.

## Immediate next steps

1. `python mosaic.py connectors` — register both Debezium connectors; confirm the initial
   snapshot lands the seeded rows into Kafka topics, and that ADR 0004's masking actually
   masks (check `before` **and** `after` images for hashed email/phone, and that
   `national_id` / `raw_card_number` are absent entirely).
2. Load generator (`tools/loadgen/`) — continuous OLTP writes plus the **stock-out
   injection lever**. This is what manufactures the freshness asymmetry the whole thesis
   measures, so it is a first-class deliverable, not a test fixture.
3. That closes M1 and sets up M2's restart-safety demo.
