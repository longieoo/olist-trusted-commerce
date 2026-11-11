select replay_run_id,event_id,count(*) n from {{ ref('fact_lifecycle_events') }} group by replay_run_id,event_id having count(*)>1
