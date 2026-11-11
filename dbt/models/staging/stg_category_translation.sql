select
    data:product_category_name::varchar as product_category_name,
    data:product_category_name_english::varchar as product_category_name_english
from {{ source('olist_raw', 'category_translation') }}
where dataset_version = '{{ var('dataset_version') }}'
