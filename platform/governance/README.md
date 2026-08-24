# platform/governance — tag propagation and the embedding guard (N3)

GR1 §1.1 names PII leaking through embeddings as a motivating gap. GR1 §4.6's two masking layers
both stop at the relational boundary, so the vector side is left ungoverned. This package closes
the platform's own open gap.

**Tags, not column names.** A policy that lists column names has to be rewritten for every new
domain, which would make governance domain-specific and break the L3 claim. Policy is therefore
keyed on tags (`sensitive`, `restricted`, `public`) attached to `(dataset, column)` pairs.

**Propagation.** Tags travel along `platform.column_lineage` edges, so a tag declared once at the
source survives every medallion hop without being redeclared.

**The guard.** Before an embedding job reads a text column, it asks whether that column carries a
restricting tag — directly or by propagation. If it does, the job refuses. This is the mechanism
that makes "PII cannot reach the vector index" an enforced property rather than a convention.

`policies/` holds the Rego consumed by OPA (see `docs/adr/0001`), including the five roles of
GR1 Table 4.6. `opa test policies/` runs the role hierarchy as a unit-test suite.
