select
    data:review_id::varchar as review_id,
    data:order_id::varchar as order_id,
    data:review_score::integer as review_score,
    data:review_comment_title::varchar as review_comment_title,
    data:review_comment_message::varchar as review_comment_message,
    data:review_creation_date::varchar as review_creation_date,
    data:review_answer_timestamp::varchar as review_answer_timestamp
from {{ source('olist_raw', 'order_reviews') }}
where dataset_version = '{{ var('dataset_version') }}'
