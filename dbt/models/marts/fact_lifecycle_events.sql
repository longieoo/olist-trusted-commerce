select data:event_id::varchar event_id,data:replay_run_id::varchar replay_run_id,
data:dataset_version::varchar dataset_version,data:order_id::varchar order_id,data:event_type::varchar event_type,
data:source_event_time::varchar source_event_time,
data:event_time::timestamp_ntz event_time,data:emitted_at::timestamp_ntz emitted_at,
data:ingested_at::timestamp_ntz ingested_at,data:ingestion_lag_seconds::float ingestion_lag_seconds,
case data:event_type::varchar when 'order_created' then 0 when 'order_approved' then 1 when 'handed_to_carrier' then 2 when 'order_delivered' then 3 end milestone_rank
from {{ source('olist_raw','lifecycle_events') }}
qualify row_number() over(partition by data:replay_run_id::varchar,data:event_id::varchar order by data:ingested_at::timestamp_ntz)=1
