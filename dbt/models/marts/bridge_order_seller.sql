select distinct order_id,seller_id from {{ ref('fact_order_items') }}
