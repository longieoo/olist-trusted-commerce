select
    data:order_id::varchar as order_id,
    data:order_item_id::integer as order_item_id,
    data:product_id::varchar as product_id,
    data:seller_id::varchar as seller_id,
    data:shipping_limit_date::varchar as shipping_limit_date,
    data:price::number(18,2) as price,
    data:freight_value::number(18,2) as freight_value
from {{ source('olist_raw', 'order_items') }}
where dataset_version = '{{ var('dataset_version') }}'
