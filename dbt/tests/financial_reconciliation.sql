select 1 failure
where (select sum(merchandise_brl) from {{ ref('fact_orders') }}) != (select sum(price) from {{ ref('fact_order_items') }})
or (select sum(freight_brl) from {{ ref('fact_orders') }}) != (select sum(freight_value) from {{ ref('fact_order_items') }})
or (select sum(payment_brl) from {{ ref('fact_orders') }}) != (select sum(payment_value) from {{ ref('fact_payments') }})
