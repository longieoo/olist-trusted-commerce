select order_id,seller_id,count(*) n from {{ ref('bridge_order_seller') }} group by order_id,seller_id having count(*)>1
