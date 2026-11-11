select case when late_eligible=0 then 'unknown' when is_late=1 then 'late' else 'on_time' end delivery_outcome,
review_score,count(*) orders,count(review_score) reviewed_orders
from {{ ref('fact_orders') }} group by delivery_outcome,review_score
