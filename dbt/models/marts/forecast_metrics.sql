{{ config(enabled=env_var('COFFEE_COCOA_ENABLE_FORECAST_RESULTS', 'false') == 'true') }}

select
    run_id,
    split_name,
    horizon_months,
    model_name,
    is_selected,
    origin_count,
    prediction_count,
    scored_count,
    missing_target_count,
    failed_fit_count,
    mae,
    rmse,
    smape_percent,
    interval_scored_count,
    interval_coverage,
    mean_interval_width
from {{ source('forecast_results', 'metrics') }}
