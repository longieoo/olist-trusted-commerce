select order_id, review_score, review_id, review_answer_timestamp,
 count(*) over(partition by order_id) as review_history_count
from {{ ref('review_history') }}
qualify row_number() over(partition by order_id order by review_answer_timestamp desc nulls last,
 review_creation_date desc nulls last,review_id desc)=1
