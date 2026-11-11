select b.seller_id,count(*) distinct_order_seller_pairs,
sum(o.late_eligible) eligible_order_seller_pairs,sum(coalesce(o.is_late,0)) late_order_seller_pairs,
avg(o.purchase_to_delivery_days) avg_order_delivery_days
from {{ ref('bridge_order_seller') }} b join {{ ref('fact_orders') }} o using(order_id)
group by b.seller_id
