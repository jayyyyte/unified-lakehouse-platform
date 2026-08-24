# platform/freshness — the Freshness Registry (N1)

One queryable watermark plane over every dataset the platform can serve from.

Answers the question no existing component answers: **how old is the data behind this result?**
Iceberg exposes snapshots, Debezium exposes offsets, catalogs expose lineage; none of them joins
those into a single freshness answer, and without one, GR1 Eq. 5.1 cannot be evaluated at all.

Collectors, one per source of truth:

| Collector | Reads | Produces |
|---|---|---|
| CDC | Kafka Connect offsets + the envelope `source.ts_ms` | how far behind each connector is |
| Table | Iceberg snapshot summary (`max(_src_ts)` recorded at commit) | how fresh each materialised relation is |
| Vector | the index's declared `row_timestamp_column` | how fresh the embedding index is |

**Design note.** Per-entity watermarks are *not* materialised — that does not scale past a few
million rows. Each dataset instead declares in `platform.datasets.row_timestamp_column` the
column that already carries its per-row source time, and the channel query reads it inline.
Only dataset-level watermarks are stored.

Storage: `platform.datasets`, `platform.watermarks`, `platform.watermark_history` in
`sql/platform.sql`.
