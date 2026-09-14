with ordered as (
    select
        source_dataset,
        series_id,
        period_month,
        price_usd_per_kg,
        source_capture_id,
        lag(period_month) over (
            partition by source_dataset, series_id order by period_month
        ) as previous_month,
        lag(price_usd_per_kg) over (
            partition by source_dataset, series_id order by period_month
        ) as previous_price
    from {{ ref('stg_benchmark_prices') }}
)

select
    source_dataset,
    series_id,
    period_month,
    price_usd_per_kg,
    source_capture_id,
    case
        when
            date_diff('month', previous_month, period_month) = 1
            and price_usd_per_kg is not null
            and previous_price is not null
            then price_usd_per_kg - previous_price
    end as change_usd_per_kg,
    case
        when
            date_diff('month', previous_month, period_month) = 1
            and price_usd_per_kg is not null
            and previous_price > 0
            then (price_usd_per_kg - previous_price) / previous_price * 100
    end as change_percent
from ordered
