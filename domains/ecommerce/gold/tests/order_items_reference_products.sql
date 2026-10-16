-- GR1 section 4.3: an order item (MySQL) must reference a product (PostgreSQL). Neither
-- engine can enforce it, so the pipeline does. Rows younger than five minutes are
-- exempt -- see the note at the top of schema.yml on eventual cross-source integrity.
select i.order_item_id, i.product_id
from {{ ref('silver__order_items') }} i
left join {{ ref('silver__products') }} p on p.id = i.product_id
where p.id is null
  and not i._is_deleted
  and i._src_ts < current_timestamp - interval '5' minute
