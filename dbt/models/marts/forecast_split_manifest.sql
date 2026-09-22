{{ assert_forecast_mode() }}

with origins as (
    select cast(month_start as date) as origin_month
    from generate_series(date '2020-01-01', date '2025-12-01', interval 1 month)
        as months (month_start)
)

select
    origin_month,
    'retrospective_current_vintage' as evaluation_mode,
    60 as minimum_observed_history_months,
    48 as minimum_labeled_training_origins,
    cast(origin_month + interval 1 month as timestamp) at time zone 'UTC' as issue_at_utc,
    case
        when origin_month < date '2024-01-01' then 'development'
        else 'holdout'
    end as split_name
from origins
