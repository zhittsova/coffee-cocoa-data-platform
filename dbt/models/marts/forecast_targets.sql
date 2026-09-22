{{ assert_forecast_mode() }}

with cocoa as (
    select
        month_key,
        price_usd_per_kg,
        source_capture_id
    from {{ ref('fct_benchmark_prices') }}
    where
        source_dataset = 'world_bank_pink_sheet_monthly'
        and benchmark_series = 'cocoa'
),

horizons as (
    select 1 as horizon_months
    union all
    select 3
)

select
    split.origin_month,
    split.issue_at_utc,
    split.split_name,
    split.evaluation_mode,
    horizons.horizon_months,
    cast(split.origin_month + horizons.horizon_months * interval 1 month as date)
        as target_month,
    future_price.price_usd_per_kg as target_usd_per_kg,
    future_price.source_capture_id as target_capture_id,
    cast(count(past_target.month_key) filter (
        where past_origin.price_usd_per_kg is not null
        and past_target.price_usd_per_kg is not null
    ) as integer) as labeled_training_origins
from {{ ref('forecast_split_manifest') }} as split
cross join horizons
left join cocoa as future_price
    on
        future_price.month_key
        = cast(split.origin_month + horizons.horizon_months * interval 1 month as date)
left join cocoa as past_origin
    on
        past_origin.month_key >= date '2016-01-01'
        and split.origin_month > past_origin.month_key
left join cocoa as past_target
    on
        past_target.month_key
        = cast(past_origin.month_key + horizons.horizon_months * interval 1 month as date)
        and split.origin_month >= past_target.month_key
group by 1, 2, 3, 4, 5, 6, 7, 8
