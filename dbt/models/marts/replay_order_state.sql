select * from {{ ref('fact_lifecycle_events') }}
qualify row_number() over(partition by replay_run_id,order_id order by milestone_rank desc,event_time desc,event_id desc)=1
