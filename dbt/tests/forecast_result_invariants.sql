{{ config(enabled=env_var('COFFEE_COCOA_ENABLE_FORECAST_RESULTS', 'false') == 'true') }}

with failures as (
    select 'prediction_key' as failure from {{ ref('forecast_predictions') }}
    group by run_id, origin_month, horizon_months, model_name
    having count(*) != 1
    union all
    select 'metric_key' from {{ ref('forecast_metrics') }}
    group by run_id, split_name, horizon_months, model_name
    having count(*) != 1
    union all
    select 'bad_interval' from {{ ref('forecast_predictions') }}
    where interval_lower > interval_upper
       or (interval_lower is null) != (interval_upper is null)
    union all
    select 'bad_counts' from {{ ref('forecast_metrics') }}
    where scored_count > prediction_count or prediction_count > origin_count
       or interval_scored_count > scored_count
    union all
    select 'selection_count' from {{ ref('forecast_metrics') }}
    where split_name = 'development' and is_selected
    group by run_id, horizon_months
    having count(*) != 1
)
select * from failures
