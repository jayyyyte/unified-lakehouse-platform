{#-
  gold.product_sales -- one row per product: what it is, what it costs, how much is in
  stock, and how it has been selling.

  This is the SQL channel's raw material (docs/PROBLEM.md L1): purchase volume, revenue
  and recency, minutes fresh. It deliberately also carries stock_qty *as Silver last
  saw it* -- the fact the L0 failure turns on. Whether a channel trusts that copy or
  asks the live source is N2's decision, and _src_ts on the row is what lets it decide
  (per-entity freshness, read inline; never materialised separately -- CLAUDE.md).

  Windows (units_7d / units_30d) are relative to the build. Cancelled orders do not
  count as sales. Rebuilt in full each run.
-#}
{{ config(materialized='table') }}

with lines as (
    select i.product_id,
           i.order_id,
           i.quantity,
           i.unit_price,
           o.created_at,
           greatest(i._src_ts, o._src_ts)          as _src_ts,
           greatest(i._ingest_ts, o._ingest_ts)    as _ingest_ts
    from {{ ref('silver__order_items') }} i
    join {{ ref('silver__orders') }} o on o.order_id = i.order_id
    where not i._is_deleted and not o._is_deleted and o.status <> 'cancelled'
),

sales as (
    select product_id,
           count(distinct order_id)                                               as order_count,
           sum(quantity)                                                          as units_sold,
           sum(quantity * unit_price)                                             as revenue,
           sum(if(created_at >= current_timestamp - interval '7' day, quantity, 0))  as units_7d,
           sum(if(created_at >= current_timestamp - interval '30' day, quantity, 0)) as units_30d,
           max(created_at)                                                        as last_ordered_at,
           max(_src_ts)                                                           as _src_ts,
           max(_ingest_ts)                                                        as _ingest_ts
    from lines
    group by product_id
)

select
    p.id                               as product_id,
    p.sku,
    p.name,
    p.category_id,
    p.price,
    inv.stock_qty,
    inv.warehouse,
    coalesce(s.order_count, 0)         as order_count,
    coalesce(s.units_sold, 0)          as units_sold,
    coalesce(s.revenue, 0)             as revenue,
    coalesce(s.units_7d, 0)            as units_7d,
    coalesce(s.units_30d, 0)           as units_30d,
    s.last_ordered_at,
    greatest(p._src_ts, coalesce(inv._src_ts, p._src_ts),
             coalesce(s._src_ts, p._src_ts))              as _src_ts,
    greatest(p._ingest_ts, coalesce(inv._ingest_ts, p._ingest_ts),
             coalesce(s._ingest_ts, p._ingest_ts))        as _ingest_ts
from {{ ref('silver__products') }} p
left join {{ ref('silver__inventory') }} inv
       on inv.product_id = p.id and not inv._is_deleted
left join sales s on s.product_id = p.id
where not p._is_deleted
