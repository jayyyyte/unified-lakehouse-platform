# ADR 0003 — dbt-trino for Silver and Gold; Spark retained for Bronze

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §4.1, §4.2, Figure 4.1

## Context

Figure 4.1 places three engines in the processing layer: Spark Structured Streaming, dbt, and
Great Expectations. GR1 §4.2 assigns Spark the whole write path — Bronze append plus merge or
upsert into Silver and Gold.

Those are two different jobs. Bronze landing is genuinely streaming: consume Debezium topics
continuously, normalise the envelope, deduplicate on the source offset, append. Silver and Gold
are batch set-operations over tables that already live in Iceberg, and Trino can already write
Iceberg through `MERGE INTO`.

## Decision

- **Bronze** — Spark Structured Streaming, as designed. Streaming earns its place here.
- **Silver and Gold** — dbt models compiled to Trino SQL, writing Iceberg via `MERGE INTO`.
- **Quality gates** — dbt `tests:` blocks, replacing Great Expectations.

## Consequences

**Kept.** The medallion semantics of GR1 §4.2 exactly: Bronze holds the unmodified change
envelope; Silver is deduplicated, typed, masked, validated; Gold is joined, aggregated and
partitioned per §4.3. Spark stays in the architecture and in the report.

**Gained.** Silver and Gold become declarative SQL rather than imperative Spark jobs — less
code, and the lineage is derivable from the DAG for free. No second heavy runtime for batch.
Great Expectations disappears as a dependency while its function survives: uniqueness on the
dedup key, not-null on `_src_ts`, and the cross-source referential checks that GR1 §4.3 says no
single engine can enforce, all expressed as dbt tests.

**Lost.** Great Expectations' richer expectation vocabulary and its data-docs output. dbt tests
cover the checks §4.3 actually specifies; the surplus was not being used.

**Note.** `ANALYZE` after each batch write (GR1 §4.5) becomes a dbt post-hook rather than a
separately scheduled Airflow task — the operational overhead §4.5 flags is still real, but it
is now attached to the write that causes it instead of to a timer.
