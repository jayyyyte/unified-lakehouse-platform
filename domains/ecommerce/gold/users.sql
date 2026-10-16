{#-
  gold.users -- one row per shopper, with order history rolled up.

  GR1 section 4.3 partitions users by bucket(32, user_id), but the source table has no
  user_id column -- its key is `id` (CHANGES-FROM-GR1 F3). Gold names the key user_id,
  which is what the partition spec, and every join from orders, already assume.

  No PII: email and phone are hashes by the time they reach Silver, and Gold does not
  need them; GR1 Listing 4.2 fetches names from the live source instead. Rebuilt in
  full each run -- a few thousand rows.
-#}
{{ config(
    materialized='table',
    properties={"partitioning": "ARRAY['bucket(user_id, 32)']"}
) }}

with orders as (
    select user_id,
           count(*)                                            as order_count,
           sum(if(status <> 'cancelled', total_amount, 0))     as lifetime_value,
           min(created_at)                                     as first_order_at,
           max(created_at)                                     as last_order_at,
           max(_src_ts)                                        as _src_ts,
           max(_ingest_ts)                                     as _ingest_ts
    from {{ ref('orders') }}
    where not _is_deleted
    group by user_id
)

select
    u.id                                   as user_id,
    u.region,
    u.created_at,
    coalesce(o.order_count, 0)             as order_count,
    coalesce(o.lifetime_value, 0)          as lifetime_value,
    o.first_order_at,
    o.last_order_at,
    greatest(u._src_ts, coalesce(o._src_ts, u._src_ts))          as _src_ts,
    greatest(u._ingest_ts, coalesce(o._ingest_ts, u._ingest_ts)) as _ingest_ts
from {{ ref('silver__users') }} u
left join orders o on o.user_id = u.id
where not u._is_deleted
