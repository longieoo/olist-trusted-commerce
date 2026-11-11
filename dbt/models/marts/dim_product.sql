select p.*,t.product_category_name_english from {{ ref('stg_products') }} p left join {{ ref('stg_category_translation') }} t using(product_category_name)
