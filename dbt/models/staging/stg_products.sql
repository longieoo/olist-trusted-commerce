select
    data:product_id::varchar as product_id,
    data:product_category_name::varchar as product_category_name,
    data:product_name_lenght::integer as product_name_lenght,
    data:product_description_lenght::integer as product_description_lenght,
    data:product_photos_qty::integer as product_photos_qty,
    data:product_weight_g::integer as product_weight_g,
    data:product_length_cm::integer as product_length_cm,
    data:product_height_cm::integer as product_height_cm,
    data:product_width_cm::integer as product_width_cm
from {{ source('olist_raw', 'products') }}
where dataset_version = '{{ var('dataset_version') }}'
