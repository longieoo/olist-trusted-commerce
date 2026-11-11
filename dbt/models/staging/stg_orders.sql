select
    data:order_id::varchar as order_id,
    data:customer_id::varchar as customer_id,
    data:order_status::varchar as order_status,
    data:order_purchase_timestamp::varchar as order_purchase_timestamp,
    data:order_approved_at::varchar as order_approved_at,
    data:order_delivered_carrier_date::varchar as order_delivered_carrier_date,
    data:order_delivered_customer_date::varchar as order_delivered_customer_date,
    data:order_estimated_delivery_date::varchar as order_estimated_delivery_date
from {{ source('olist_raw', 'orders') }}
where dataset_version = '{{ var('dataset_version') }}'
