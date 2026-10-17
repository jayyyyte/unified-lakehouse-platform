# platform/ — the generic core

**Contract: nothing in this tree may know what domain it is running.**

Enforced mechanically by `python mosaic.py check-abstraction`, which fails the build if any
business vocabulary appears in any file here. That gate is the mechanical form of the claim
that MOSAIC is a platform rather than one pipeline. See `docs/PROBLEM.md`, rung L3.

Domain knowledge belongs in `domains/<domain>/` and reaches this code only as *data*: a parsed
`retrieval.yaml`, a row in `platform.datasets`, a tag in `platform.column_tags`. Identifiers
that arrive at runtime are opaque strings here and are never interpreted.

| Package | Responsibility |
|---|---|
| `spec/` | schema and validation for `domains/*/retrieval.yaml` |
| `freshness/` | watermark collectors and the registry API (N1) |
| `retrieval/` | channel runners and fusion policies (N2) |
| `governance/` | tag model, propagation along column lineage, embedding-input guard (N3) |
| `runtime/` | database access for jobs running inside the stack (Trino REST, psycopg) |

Inside the `jobs` container this directory is mounted as the package `mosaic_platform`, never as
`platform`: that name would shadow the standard-library module torch and transformers import.
The gate also scans `pipelines/embeddings/`, the index runner, which is generic by the same
contract (docs/adr/0006-m4-embedding-pipeline.md).
