select purchase_date,customer_state,count(*) orders,
sum(late_eligible) eligible_delivered,sum(coalesce(is_late,0)) late_orders,
sum(coalesce(is_late,0))/nullif(sum(late_eligible),0) late_delivery_rate,
avg(purchase_to_delivery_days) avg_delivery_days,count(purchase_to_delivery_days) duration_coverage,
sum(merchandise_brl) merchandise_brl,sum(freight_brl) freight_brl,sum(payment_brl) payment_brl
from {{ ref('fact_orders') }} group by purchase_date,customer_state
