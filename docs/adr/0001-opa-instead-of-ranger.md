# ADR 0001 — Open Policy Agent in place of Apache Ranger

**Status:** Accepted · **Date:** 2026-08-24 · **Relates to:** GR1 §4.6, Table 4.5, Table 4.6

## Context

GR1 Appendix B specifies Apache Ranger as the authorization engine, enforcing the five-role
hierarchy of Table 4.6 and the query-time column masking of Table 4.5 (masking Layer 2).

A working Ranger deployment needs the Ranger Admin service, a backing relational database, Solr
for audit indexing, and the Trino plugin wired into the coordinator. On the single-node
Docker Compose target of Appendix B, that competes for memory with Kafka, Spark, Trino, MinIO
and two databases, and historically costs multiple weeks of integration before the first policy
evaluates.

## Decision

Enforce authorization with **Open Policy Agent** behind Trino's `opa` access-control provider.

The policy model is unchanged. Table 4.6's five roles — `data_admin`, `data_engineer`,
`data_analyst`, `ml_engineer`, `bi_user` — are expressed verbatim as Rego, and column masking
remains query-time and role-dependent exactly as Table 4.5 describes.

## Consequences

**Kept.** Every guarantee GR1 claims: centralised policy, role-based access at catalog / schema
/ table / column level, query-time column masking, and an audit trail of decisions.

**Gained.** Policy becomes a plain-text, version-controlled, unit-testable artifact — `opa test`
runs the role hierarchy as a test suite in CI. That is materially better evidence for a defense
than screenshots of an admin UI. One container instead of four.

**Lost.** Ranger's web UI for policy authoring, and its tag-sync integration with a catalog.
The first is not a project requirement. The second matters for N3 (tag propagation), so the tag
model is owned by `platform/governance/` and handed to OPA as policy data — which is the reason
N3 is a contribution rather than a configuration exercise.

**Framing.** This is a *substitution, not an omission*. Ranger remains the correct choice at
production scale with a dedicated platform team, and the report should say so. An examiner
comparing this repository against GR1 Appendix B will notice the difference; the honest answer
is on paper before they ask.
