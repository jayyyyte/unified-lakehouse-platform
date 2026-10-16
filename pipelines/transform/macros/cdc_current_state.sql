{#-
  cdc_current_state: one Bronze topic -> the current state of its table, one row per key.

  GR1 section 4.2's Silver: deduplicated, normalised, type-cast. Every generated Silver
  model is a single call to this macro (assemble.py writes the call); nothing here knows
  which table it is building.

  Latest wins by SOURCE POSITION, never by time. A snapshot read carries the time the
  snapshot *read* the row, which can be later than the commit of an UPDATE positioned
  after the snapshot -- ordering by _src_ts would then resurrect the older state. The
  position is the change's place in the source log, made sortable as text:
      PostgreSQL  lpad(lsn)                     -- per-change WAL position
      MySQL       file ':' lpad(pos) ':' lpad(row)  -- binlog event, row within it
  Per key, both orders equal commit order: row locks serialise writers to one row.

  Incremental runs read only Bronze rows newer than the table's own max(_ingest_ts).
  That is safe because Bronze commits micro-batches in order and stamps each with its
  start time, so a row with a later _ingest_ts becomes visible only after every row
  with an earlier one. The incoming changes are then ranked TOGETHER WITH the current
  target rows for the same keys, and only keys whose winner is an incoming change are
  emitted: MERGE never overwrites a newer state with an older one, and keys whose
  "change" turns out to be stale are not rewritten at all (no needless delete files).

  Deletes are soft: op=d keeps the row with _is_deleted = true, its values taken from
  the before image. Silver stays an audit of what existed; Gold decides what to show.

  columns: list of {name, type, decode} from assemble.py; decode is one of
    cast          CAST(text AS type)                  numbers, decimals (decimal.handling.mode=string), booleans
    text          the JSON text as varchar            strings, enums, chars, masked hashes
    wallclock_ms  epoch millis of a wall-clock value  MySQL DATETIME under time.precision.mode=connect
    epoch_days    days since epoch                    DATE under time.precision.mode=connect
    iso_ts        ISO-8601 instant                    PostgreSQL TIMESTAMPTZ (Debezium ZonedTimestamp)
-#}
{% macro cdc_current_state(topic, key, columns) %}
{%- set meta = ['_op', '_is_deleted', '_src_position', '_src_ts', '_ingest_ts', '_event_id'] -%}
{%- set names = columns | map(attribute='name') | list -%}

with changes as (
    select
        event_id,
        op,
        _src_ts,
        _ingest_ts,
        {{ cdc_source_position('envelope') }} as _src_position,
        case when op = 'd' then json_extract(envelope, '$.before')
             else json_extract(envelope, '$.after') end as _img
    from {{ source('bronze', 'cdc_events') }}
    where topic = '{{ topic }}'
    {%- if is_incremental() %}
      and _ingest_ts > (select coalesce(max(_ingest_ts), from_unixtime(0)) from {{ this }})
    {%- endif %}
),

decoded as (
    select
        {%- for c in columns %}
        {{ cdc_decode('_img', c) }} as {{ adapter.quote(c.name) }},
        {%- endfor %}
        op as _op,
        op = 'd' as _is_deleted,
        _src_position,
        _src_ts,
        _ingest_ts,
        event_id as _event_id,
        1 as _from_change
    from changes
),

candidates as (
    select * from decoded
    {%- if is_incremental() %}
    union all
    select
        {%- for n in names %} t.{{ adapter.quote(n) }},{% endfor %}
        {%- for m in meta %} t.{{ m }},{% endfor %}
        0 as _from_change
    from {{ this }} t
    join (select distinct {{ key | map('string') | join(', ') }} from decoded) k
      on {% for k in key %}t.{{ k }} = k.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
    {%- endif %}
),

ranked as (
    select *,
           row_number() over (
               partition by {{ key | join(', ') }}
               -- ties (the same event read twice) go to the row already in the table
               order by _src_position desc, _from_change asc
           ) as _rn
    from candidates
)

select
    {%- for n in names %}
    {{ adapter.quote(n) }},
    {%- endfor %}
    {{ meta | join(',\n    ') }}
from ranked
where _rn = 1 and _from_change = 1
{% endmacro %}


{#- Sortable text position of a change in its source log (see cdc_current_state). -#}
{% macro cdc_source_position(envelope) -%}
case
    when json_extract_scalar({{ envelope }}, '$.source.lsn') is not null
        then lpad(json_extract_scalar({{ envelope }}, '$.source.lsn'), 20, '0')
    else concat(
        coalesce(json_extract_scalar({{ envelope }}, '$.source.file'), ''), ':',
        lpad(coalesce(json_extract_scalar({{ envelope }}, '$.source.pos'), '0'), 20, '0'), ':',
        lpad(coalesce(json_extract_scalar({{ envelope }}, '$.source.row'), '0'), 10, '0'))
end
{%- endmacro %}


{#- One column out of a Debezium row image, typed. -#}
{% macro cdc_decode(img, c) -%}
{%- set v = "json_extract_scalar(" ~ img ~ ", '$[\"" ~ c.name ~ "\"]')" -%}
{%- if c.decode == 'cast' -%}
    cast({{ v }} as {{ c.type }})
{%- elif c.decode == 'wallclock_ms' -%}
    {#- Epoch millis -> the wall clock they encode (read in UTC) -> that wall clock in the
        source's zone. Not date_add('millisecond', ...): Trino narrows its amount to a
        32-bit int, and present-day epoch millis overflow it. -#}
    cast(with_timezone(
             cast(at_timezone(from_unixtime_nanos(cast({{ v }} as bigint) * 1000000), 'UTC')
                  as timestamp(3)),
             '{{ var("source_time_zone") }}') as timestamp(6) with time zone)
{%- elif c.decode == 'epoch_days' -%}
    date_add('day', cast({{ v }} as integer), date '1970-01-01')
{%- elif c.decode == 'iso_ts' -%}
    cast(from_iso8601_timestamp_nanos({{ v }}) as timestamp(6) with time zone)
{%- else -%}
    cast({{ v }} as varchar)
{%- endif -%}
{%- endmacro %}
