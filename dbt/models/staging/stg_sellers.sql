select
    data:seller_id::varchar as seller_id,
    data:seller_zip_code_prefix::varchar as seller_zip_code_prefix,
    data:seller_city::varchar as seller_city,
    data:seller_state::varchar as seller_state
from {{ source('olist_raw', 'sellers') }}
where dataset_version = '{{ var('dataset_version') }}'
