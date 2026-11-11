with item_totals as (
 select order_id,sum(price) merchandise_brl,sum(freight_value) freight_brl,count(*) item_count
 from {{ ref('fact_order_items') }} group by order_id
), payment_totals as (
 select order_id,sum(payment_value) payment_brl,count(*) payment_count
 from {{ ref('fact_payments') }} group by order_id
), base as (
 select o.*,c.customer_unique_id,c.customer_zip_code_prefix,c.customer_city,c.customer_state,
 to_date(o.order_purchase_timestamp) purchase_date,
 datediff('second',to_timestamp_ntz(o.order_purchase_timestamp),to_timestamp_ntz(o.order_delivered_carrier_date))/86400.0 purchase_to_carrier_days,
 datediff('second',to_timestamp_ntz(o.order_delivered_carrier_date),to_timestamp_ntz(o.order_delivered_customer_date))/86400.0 carrier_to_customer_days,
 datediff('second',to_timestamp_ntz(o.order_purchase_timestamp),to_timestamp_ntz(o.order_delivered_customer_date))/86400.0 purchase_to_delivery_days,
 iff(o.order_status='delivered' and o.order_delivered_customer_date is not null and o.order_estimated_delivery_date is not null,1,0) late_eligible,
 r.review_score,r.review_history_count,
 coalesce(i.merchandise_brl,0)::number(18,2) merchandise_brl,
 coalesce(i.freight_brl,0)::number(18,2) freight_brl,
 coalesce(p.payment_brl,0)::number(18,2) payment_brl,
 coalesce(i.item_count,0) item_count,coalesce(p.payment_count,0) payment_count
 from {{ ref('stg_orders') }} o
 left join {{ ref('dim_order_customer') }} c using(customer_id)
 left join {{ ref('order_review_summary') }} r using(order_id)
 left join item_totals i using(order_id)
 left join payment_totals p using(order_id)
)
select *, iff(late_eligible=1,iff(to_date(order_delivered_customer_date)>to_date(order_estimated_delivery_date),1,0),null) is_late
from base
