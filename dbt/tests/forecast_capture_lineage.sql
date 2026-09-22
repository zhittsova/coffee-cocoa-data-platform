with capture_ids as (
    select lag_0_capture_id as capture_id from {{ ref('forecast_origin_features') }}
    union
    select lag_1_capture_id from {{ ref('forecast_origin_features') }}
    union
    select lag_3_capture_id from {{ ref('forecast_origin_features') }}
    union
    select lag_12_capture_id from {{ ref('forecast_origin_features') }}
    union
    select target_capture_id from {{ ref('forecast_targets') }}
),

missing as (
    select capture_id from capture_ids
    where capture_id is not null
    except
    select source_capture_id from {{ ref('forecast_capture_metadata') }}
)

select capture_id from missing
union
select source_capture_id as capture_id
from {{ ref('forecast_capture_metadata') }}
where first_seen_capture_at_utc > retrieved_at_utc
