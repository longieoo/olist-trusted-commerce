select order_id,payment_sequential,count(*) n from {{ ref('fact_payments') }} group by order_id,payment_sequential having count(*)>1
