select
    data:geolocation_zip_code_prefix::varchar as geolocation_zip_code_prefix,
    data:geolocation_lat::float as geolocation_lat,
    data:geolocation_lng::float as geolocation_lng,
    data:geolocation_city::varchar as geolocation_city,
    data:geolocation_state::varchar as geolocation_state
from {{ source('olist_raw', 'geolocation') }}
where dataset_version = '{{ var('dataset_version') }}'
