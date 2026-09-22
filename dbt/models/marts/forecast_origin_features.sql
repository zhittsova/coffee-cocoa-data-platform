{{ assert_forecast_mode() }}

with cocoa as (
    select
        month_key,
        price_usd_per_kg,
        source_capture_id
    from {{ ref('fct_benchmark_prices') }}
    where benchmark_series = 'cocoa'
),

origins as (
    select
        split.origin_month,
        split.issue_at_utc,
        split.split_name,
        split.evaluation_mode,
        count(cocoa.month_key) filter (
            where cocoa.price_usd_per_kg is not null
        ) as observed_history_months_60
    from {{ ref('forecast_split_manifest') }} as split
    left join cocoa
        on
            cocoa.month_key between
            cast(split.origin_month - interval 59 month as date)
            and split.origin_month
    group by 1, 2, 3, 4
)

select
    origins.origin_month,
    origins.issue_at_utc,
    origins.split_name,
    origins.evaluation_mode,
    origins.origin_month as lag_0_month,
    lag_0.price_usd_per_kg as lag_0_usd_per_kg,
    lag_0.source_capture_id as lag_0_capture_id,
    cast(origins.origin_month - interval 1 month as date) as lag_1_month,
    lag_1.price_usd_per_kg as lag_1_usd_per_kg,
    lag_1.source_capture_id as lag_1_capture_id,
    cast(origins.origin_month - interval 3 month as date) as lag_3_month,
    lag_3.price_usd_per_kg as lag_3_usd_per_kg,
    lag_3.source_capture_id as lag_3_capture_id,
    cast(origins.origin_month - interval 12 month as date) as lag_12_month,
    lag_12.price_usd_per_kg as lag_12_usd_per_kg,
    lag_12.source_capture_id as lag_12_capture_id,
    cast(origins.observed_history_months_60 as integer) as observed_history_months_60,
    origins.observed_history_months_60 = 60 as has_minimum_history
from origins
left join cocoa as lag_0 on origins.origin_month = lag_0.month_key
left join cocoa as lag_1 on lag_1.month_key = cast(origins.origin_month - interval 1 month as date)
left join cocoa as lag_3 on lag_3.month_key = cast(origins.origin_month - interval 3 month as date)
left join
    cocoa as lag_12
    on lag_12.month_key = cast(origins.origin_month - interval 12 month as date)
