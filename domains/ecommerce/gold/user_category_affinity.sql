{#-
  gold.user_category_affinity -- how much each shopper buys from each leaf category.

  The "relevant to shopper X" half of the L0 query (docs/PROBLEM.md): the SQL channel
  scores products by the shopper's affinity for their category combined with
  product_sales. One row per (user, category) the user has ever bought from.

  _src_ts covers the orders and items that contribute. A product's own changes (price)
  do not move it -- they do not change affinity. Rebuilt in full each run.
-#}
{{ config(materialized='table') }}

select
    o.user_id,
    p.category_id,
    count(distinct o.order_id)                     as orders,
    sum(i.quantity)                                as units,
    sum(i.quantity * i.unit_price)                 as revenue,
    max(o.created_at)                              as last_purchased_at,
    max(greatest(i._src_ts, o._src_ts))            as _src_ts,
    max(greatest(i._ingest_ts, o._ingest_ts))      as _ingest_ts
from {{ ref('silver__order_items') }} i
join {{ ref('silver__orders') }}   o on o.order_id = i.order_id
join {{ ref('silver__products') }} p on p.id = i.product_id
where not i._is_deleted and not o._is_deleted and not p._is_deleted
  and o.status <> 'cancelled'
group by o.user_id, p.category_id
