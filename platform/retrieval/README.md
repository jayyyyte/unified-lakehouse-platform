# platform/retrieval — channels and fusion (N2)

A **channel** is one ranked list: an engine, a query template, the dataset whose watermark
governs it, and a freshness SLA. A **fusion policy** combines channels into one ranking.

Policies:

| Policy | Behaviour |
|---|---|
| `rrf` | the baseline of Cormack et al. (2009). Rank position only; no freshness term. |
| `weighted` | fixed per-channel weights over normalised scores. |
| `freshness_weighted` | GR1 Eq. 5.2 — per-channel score scaled by a decay `g(.)` of that channel's staleness. `g` is pluggable (linear, exponential) because determining its right form is the experimental work, not a settled result. |

Every result carries provenance: which channels contributed, the rank within each, the staleness
observed, and the decay applied. That provenance is what makes the difference between policies
legible in the five minutes an examiner will give a demo — and it is also what experiment E4
reads back.

The runner never names a business entity. It reads a validated `domains/*/retrieval.yaml` and
treats every identifier in it as an opaque string.
