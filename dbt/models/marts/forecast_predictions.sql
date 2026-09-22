{{ config(enabled=env_var('COFFEE_COCOA_ENABLE_FORECAST_RESULTS', 'false') == 'true') }}

select
    run_id,
    origin_month,
    target_month,
    issue_at_utc,
    split_name,
    evaluation_mode,
    horizon_months,
    model_name,
    fit_status,
    labeled_training_origins,
    origin_capture_id,
    seasonal_input_month,
    seasonal_input_capture_id,
    target_capture_id,
    prediction_usd_per_kg,
    target_usd_per_kg,
    absolute_error,
    squared_error,
    smape_percent,
    interval_lower,
    interval_upper,
    interval_covered,
    calibration_count
from {{ source('forecast_results', 'predictions') }}
