{#-
  gold.product_catalog -- one row per product: what it is, in words, plus the two fast
  facts the slow channel snapshots. GR1 section 4.1: pgvector serves "embeddings
  generated from the Gold-layer product catalog"; this is that catalog.

  It is the embedding pipeline's source (domains/ecommerce/retrieval.yaml):
    - name, description, category_name  -> embedded text (slow-moving; the load
      generator never touches them, so an embedding can only go stale through the
      facts below, never through its own text -- CHANGES-FROM-GR1 C9)
    - stock_qty, price                    -> copied beside each vector at refresh time
    - stock_qty > 0                       -> eligibility: what enters the index (ADR 0006, M4-A1)

  Kept separate from product_sales on purpose: that table is the SQL channel's raw
  material and has no reason to carry free text; this one has no reason to carry
  sales history. Rebuilt in full each run (1 row per product).
-#}
{{ config(materialized='table') }}

select
    p.id                                   as product_id,
    p.sku,
    p.name,
    p.description,
    p.category_id,
    c.name                                 as category_name,
    p.price,
    inv.stock_qty,
    inv.warehouse,
    greatest(p._src_ts, c._src_ts, coalesce(inv._src_ts, p._src_ts))          as _src_ts,
    greatest(p._ingest_ts, c._ingest_ts, coalesce(inv._ingest_ts, p._ingest_ts)) as _ingest_ts
from {{ ref('silver__products') }} p
join {{ ref('silver__categories') }} c
  on c.id = p.category_id and not c._is_deleted
left join {{ ref('silver__inventory') }} inv
       on inv.product_id = p.id and not inv._is_deleted
where not p._is_deleted
