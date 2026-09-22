{{ config(enabled=env_var('COFFEE_COCOA_ENABLE_FORECAST_RESULTS', 'false') == 'true') }}

with prediction_counts as (
    select
        run_id,
        split_name,
        horizon_months,
        model_name,
        count(*) as origin_count,
        count(prediction_usd_per_kg) as prediction_count,
        count(absolute_error) as scored_count,
        count(*) filter (where target_usd_per_kg is null) as missing_target_count,
        count(*) filter (where fit_status = 'failed_fit') as failed_fit_count,
        count(*) filter (where absolute_error is not null and interval_lower is not null)
            as interval_scored_count
    from {{ ref('forecast_predictions') }}
    where split_name = 'holdout' or target_month <= date '2023-12-01'
    group by run_id, split_name, horizon_months, model_name
),

selection_counts as (
    select
        run_id,
        horizon_months,
        count(*) filter (where is_selected) as selected_count
    from {{ ref('forecast_metrics') }}
    where split_name = 'development'
    group by run_id, horizon_months
),

failures as (
    select 'prediction_key' as failure from {{ ref('forecast_predictions') }}
    group by run_id, origin_month, horizon_months, model_name
    having count(*) != 1
    union all
    select 'metric_key' from {{ ref('forecast_metrics') }}
    group by run_id, split_name, horizon_months, model_name
    having count(*) != 1
    union all
    select 'bad_interval' from {{ ref('forecast_predictions') }}
    where
        interval_lower > interval_upper
        or (interval_lower is null) != (interval_upper is null)
    union all
    select 'bad_counts' from {{ ref('forecast_metrics') }}
    where
        scored_count > prediction_count or prediction_count > origin_count
        or interval_scored_count > scored_count
    union all
    select 'selection_count' from selection_counts
    where selected_count != 1
    union all
    select 'summary_counts' from {{ ref('forecast_metrics') }} as metrics
    full outer join prediction_counts as predictions
        on
            metrics.run_id = predictions.run_id
            and metrics.split_name = predictions.split_name
            and metrics.horizon_months = predictions.horizon_months
            and metrics.model_name = predictions.model_name
    where
        metrics.run_id is null or predictions.run_id is null
        or metrics.origin_count != predictions.origin_count
        or metrics.prediction_count != predictions.prediction_count
        or metrics.scored_count != predictions.scored_count
        or metrics.missing_target_count != predictions.missing_target_count
        or metrics.failed_fit_count != predictions.failed_fit_count
        or metrics.interval_scored_count != predictions.interval_scored_count
)

select * from failures
