{#-
  gold.orders -- one row per order, enriched with its items and payments.

  The table GR1 Listing 4.2 queries, federated against the live users table, and the one
  section 4.3 partitions: days(created_at), bucket(16, region).

  Incremental MERGE (ADR 0003): an order is rebuilt when the order itself, any of its
  items, or any of its payments changed since the last build. All three live in MySQL
  but arrive through three topics, so an order can land a batch before its items; the
  late rows then carry a newer _ingest_ts and pull the order back in on the next run.

  _src_ts / _ingest_ts are those of the newest contributing change (convention C8), so
  a row's freshness is exactly as old as the oldest-possible fact it summarises.
-#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    properties={"partitioning": "ARRAY['day(created_at)', 'bucket(region, 16)']"}
) }}

{%- set since -%}
(select coalesce(max(_ingest_ts), from_unixtime(0)) from {{ this }})
{%- endset %}

with touched as (
    {%- if is_incremental() %}
    select order_id from {{ ref('silver__orders') }}      where _ingest_ts > {{ since }}
    union
    select order_id from {{ ref('silver__order_items') }} where _ingest_ts > {{ since }}
    union
    select order_id from {{ ref('silver__payments') }}    where _ingest_ts > {{ since }}
    {%- else %}
    select order_id from {{ ref('silver__orders') }}
    {%- endif %}
),

o as (
    select * from {{ ref('silver__orders') }}
    where order_id in (select order_id from touched)
),

items as (
    select order_id,
           count(*)                    as item_lines,
           sum(quantity)               as units,
           sum(quantity * unit_price)  as items_amount,
           max(_src_ts)                as _src_ts,
           max(_ingest_ts)             as _ingest_ts
    from {{ ref('silver__order_items') }}
    where not _is_deleted and order_id in (select order_id from touched)
    group by order_id
),

pay as (
    select order_id,
           max_by(status, _src_position)            as payment_status,
           max_by(method, _src_position)            as payment_method,
           sum(if(status = 'captured', amount, 0))  as captured_amount,
           max(_src_ts)                             as _src_ts,
           max(_ingest_ts)                          as _ingest_ts
    from {{ ref('silver__payments') }}
    where not _is_deleted and order_id in (select order_id from touched)
    group by order_id
)

select
    o.order_id,
    o.user_id,
    o.status,
    o.total_amount,
    o.currency,
    o.region,
    o.created_at,
    o.updated_at,
    coalesce(items.item_lines, 0)        as item_lines,
    coalesce(items.units, 0)             as units,
    items.items_amount,
    pay.payment_status,
    pay.payment_method,
    coalesce(pay.captured_amount, 0)     as captured_amount,
    o._is_deleted,
    greatest(o._src_ts, coalesce(items._src_ts, o._src_ts),
             coalesce(pay._src_ts, o._src_ts))          as _src_ts,
    greatest(o._ingest_ts, coalesce(items._ingest_ts, o._ingest_ts),
             coalesce(pay._ingest_ts, o._ingest_ts))    as _ingest_ts
from o
left join items on items.order_id = o.order_id
left join pay   on pay.order_id   = o.order_id
