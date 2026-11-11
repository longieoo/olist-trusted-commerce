select
    data:order_id::varchar as order_id,
    data:payment_sequential::integer as payment_sequential,
    data:payment_type::varchar as payment_type,
    data:payment_installments::integer as payment_installments,
    data:payment_value::number(18,2) as payment_value
from {{ source('olist_raw', 'order_payments') }}
where dataset_version = '{{ var('dataset_version') }}'
