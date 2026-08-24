# ADR 0002 — OpenLineage + Marquez in place of DataHub

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §3.6, Table 4.1

## Context

GR1 selects DataHub for the data catalog and column-level lineage graph, ingesting lineage
emitted by Airflow and Spark in **OpenLineage** format (§3.6). A production DataHub is five
services — GMS, Elasticsearch, the frontend, and the MAE/MCE consumers — plus its own Kafka
topics. Appendix B's own comment concedes the point: *"Kept minimal here for GR1."*

The decisive observation is that GR1's lineage requirement is defined in terms of the
**OpenLineage emission path**, not in terms of DataHub. DataHub is one possible receiver.

## Decision

Keep the OpenLineage emission path exactly as designed, and receive it with **Marquez**, the
OpenLineage reference implementation: one service plus a Postgres schema.

## Consequences

**Kept.** Automatic lineage capture from Spark and dbt with no per-pipeline code changes, a
navigable lineage graph, and dataset-level metadata — the substance of GR1's governance
requirement (ii).

**Gained.** Marquez's dataset-version model records a facet per run, which the Freshness
Registry (N1) consumes directly rather than reconstructing. The lightweight choice is
*better aligned* with the novel component, not merely cheaper.

**Lost.** DataHub's richer catalog surface — glossary, ownership, search UI — and its
column-level lineage. Column-level lineage is the one real loss: dbt's column-level lineage
covers the Silver/Gold hop, but the Bronze hop stays table-level. Recorded as a known
limitation rather than papered over.

**Framing.** Substitution, not omission. Because the emission side is unchanged, swapping the
receiver back to DataHub later is a configuration change, which is the extensibility property
GR1 §2.3 asks for.
