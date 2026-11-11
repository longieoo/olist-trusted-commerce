select replay_run_id,order_id,count(*) n from {{ ref('replay_order_state') }} group by replay_run_id,order_id having count(*)>1
