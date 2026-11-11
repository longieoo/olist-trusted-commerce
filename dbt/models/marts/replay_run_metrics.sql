select data:replay_run_id::varchar replay_run_id,data:session::varchar session,
data:raw_all_runs::integer raw_all_runs,
data:valid_physical_all_runs::integer valid_physical_all_runs,
data:valid_physical_run::integer valid_physical_run,
data:unique_valid_run::integer unique_valid_run,
data:quarantined_all_runs::integer quarantined_all_runs,
data:duplicate_physical_run::integer duplicate_physical_run,
data:online_physical_run::integer online_physical_run,
data:online_unique_run::integer online_unique_run,
data:recovered_omitted_run::integer recovered_omitted_run,
data:acknowledged_physical::integer acknowledged_physical,
data:ack_missing_from_raw::integer ack_missing_from_raw,
data:state_rows::integer state_rows,
data:mean_ingestion_lag_seconds::float mean_ingestion_lag_seconds,
data:max_ingestion_lag_seconds::float max_ingestion_lag_seconds,
data:reconciled_at::timestamp_tz reconciled_at
from {{ source('olist_raw','replay_metrics') }}
qualify row_number() over(partition by data:replay_run_id::varchar order by data:reconciled_at::timestamp_tz desc nulls last,dataset_version desc)=1
