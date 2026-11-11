select
    data:customer_id::varchar as customer_id,
    data:customer_unique_id::varchar as customer_unique_id,
    data:customer_zip_code_prefix::varchar as customer_zip_code_prefix,
    data:customer_city::varchar as customer_city,
    data:customer_state::varchar as customer_state
from {{ source('olist_raw', 'customers') }}
where dataset_version = '{{ var('dataset_version') }}'
