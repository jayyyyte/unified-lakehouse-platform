# MOSAIC

A self-hosted unified lakehouse platform for multi-source CDC and hybrid top-*k* analytics.

The design phase deliverable is [`docs/GR1_report.md`](docs/GR1_report.md); this repository is
the system that report specifies, plus the components that carry its novelty.

**Start here:** [`docs/PROBLEM.md`](docs/PROBLEM.md) — the one concrete failure this platform
exists to fix, and the four-rung ladder from that failure to a domain-generic platform.

Working on the code? [`CLAUDE.md`](CLAUDE.md) carries the durable context and the
environment gotchas; [`docs/STATUS.md`](docs/STATUS.md) says what is actually built and
verified right now. `python mosaic.py doctor` reports live state in one command.

---

## The short version

Combine an exact SQL ranking (fresh to the minute) with an approximate vector ranking (stale by
hours) and fuse them with Reciprocal Rank Fusion, and you get a specific, reproducible bug: **a
product that sold out five minutes ago still ranks #1.** RRF scores by rank position alone — it
has no term for how old the data behind a ranking is, so it cannot penalise a stale channel for
being confidently wrong.

MOSAIC fixes that by making freshness a first-class, queryable property of every dataset, and
then making fusion a function of scores *and* watermarks. Then it proves the fix is not
e-commerce-specific by running a second, unrelated domain on the same binaries.

## Quick start

Requires Docker with Compose v2 and Python 3.11+. `make` is optional.

```bash
python mosaic.py up             # core slice: sources + CDC + Iceberg/MinIO + Trino
python mosaic.py connectors     # register the Debezium connectors
python mosaic.py status         # health of every service
```

`up` does not return until every healthcheck passes — Kafka Connect in particular accepts
connections well before it will accept a connector, and everything downstream depends on the
difference.

Optional profiles, each opt-in so the default footprint stays survivable on one machine:

```bash
python mosaic.py up --profile gov       # OPA + Marquez
python mosaic.py up --profile orch      # Airflow
python mosaic.py up --profile console   # MOSAIC API + demo console
```

| Command | |
|---|---|
| `python mosaic.py status` | health of every running service |
| `python mosaic.py logs [service]` | tail logs |
| `python mosaic.py sql` | Trino CLI |
| `python mosaic.py psql [--database ...]` | PostgreSQL shell |
| `python mosaic.py check-abstraction` | the L3 domain-purity gate |
| `python mosaic.py down` / `clean` | stop / stop and destroy volumes |

## Layout

The `platform/` ÷ `domains/` split **is** the central claim, made structural. Anything that
knows what a product is lives under `domains/`. `check-abstraction` enforces this mechanically.

```
docker/            Compose stack, Trino catalogs, OPA config
sql/platform.sql   platform metadata store (Freshness Registry, tags, audit) -- domain-free
pipelines/
  bronze/          Spark Structured Streaming: CDC topics -> Iceberg Bronze
  transform/       dbt-trino models: Silver + Gold
  embeddings/      text -> 384-d vectors, deliberately scheduled
platform/          *** GENERIC CORE -- no domain vocabulary permitted ***
  freshness/         watermark collectors + registry API            (N1)
  retrieval/         channel runners + fusion policies              (N2)
  governance/        tag model, propagation, embedding guard        (N3)
  spec/              schema + validation for <domain>.yaml
domains/
  ecommerce/         ddl/, connectors/, retrieval.yaml, tags.yaml
  jobmarket/         the same four things -- and nothing else       (L3 proof)
services/api/      FastAPI: /retrieve, /freshness, /admin
services/console/  demo UI
experiments/       E1..E4 runners; results/ as CSV + plots
docs/adr/          architecture decision records
```

## What is novel here

The reference architecture — Debezium → Kafka → Spark → Iceberg/MinIO → Trino, plus pgvector —
is a well-known assembly. Reproducing it is necessary but is not the contribution.

| | Component | Gap it closes |
|---|---|---|
| **N1** | Freshness Registry | Iceberg exposes snapshots, Debezium exposes offsets, catalogs expose lineage — nobody joins them into one answer to *"how old is the data behind this result?"* Without it, GR1 Eq. 5.1 cannot be evaluated at all. |
| **N2** | Freshness-aware hybrid fusion | GR1 §5.3's declared research contribution, currently a problem statement with no implementation and no measurement. |
| **N3** | Tag propagation reaching embeddings | GR1 §1.1 names PII leaking through embeddings as a motivating gap; §4.6's two masking layers both stop at the relational boundary. |
| **N4** | Freshness-SLA query routing *(stretch)* | Makes the freshness plane actionable rather than merely observable. |

## Departures from GR1

Each is a **substitution, not an omission**, and each has an ADR. An examiner reading the GR1
report will notice the difference; the reasoning is on paper before they ask.

| GR1 says | Here | Why |
|---|---|---|
| Apache Ranger | OPA + Trino access control | [ADR 0001](docs/adr/0001-opa-instead-of-ranger.md) — same role hierarchy (Table 4.6), policy becomes testable text, one container instead of four |
| DataHub | OpenLineage → Marquez | [ADR 0002](docs/adr/0002-marquez-instead-of-datahub.md) — the emission path GR1 specifies is unchanged; only the receiver is lighter |
| Spark for Silver/Gold + Great Expectations | dbt-trino, Spark retained for Bronze | [ADR 0003](docs/adr/0003-dbt-trino-for-silver-gold.md) — Silver/Gold are batch set-operations Trino can already do |
| Kafka Connect SMT masking (Appendix C) | Debezium connector-native masking | [ADR 0004](docs/adr/0004-connector-native-masking.md) — **the Appendix C chain cannot satisfy §4.2 and §4.6 at the same time**; see below |

ADR 0004 is a finding rather than a preference, and belongs in the GR2 report. GR1 §4.2 requires
Bronze to hold the unmodified change envelope; §4.6 requires masking before Kafka. Kafka Connect's
`MaskField` only addresses top-level fields, which is why Appendix C runs `ExtractNewRecordState`
first — and that flattening destroys the very envelope §4.2 demands. Worse, even where it applies,
the SMT masks only the `after` image, so the **before**-image of any UPDATE carries the unmasked
previous value straight into Bronze. Debezium's connector-native masking runs before the envelope
is built and has neither problem.

Deferred, per GR1's own scope-decision record: MongoDB as a third source, Milvus, and the
Prometheus/Grafana observability stack.

**[`docs/CHANGES-FROM-GR1.md`](docs/CHANGES-FROM-GR1.md) is the complete record** of every
deviation -- findings, substitutions, additions, and defects found inside the GR1 report
itself -- written to be read directly while drafting the GR2 report.

## Status

| Milestone | |
|---|---|
| **M0** Problem spec, skeleton, stack definition | **done** |
| M1 Sources, seed, load generator | next |
| M2 CDC ingestion + restart-safety demo | |
| M3 Medallion on Iceberg | |
| M4 Trino federation + embeddings | |
| M5 N1 Freshness Registry | |
| M6 N2 Retrieval + fusion | |
| M7 Experiments E1–E4 | |
| M8 N3 governance, second domain, console | |
