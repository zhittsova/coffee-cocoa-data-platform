select
    source_dataset,
    series_id as benchmark_series,
    period_month as month_key,
    price_usd_per_kg,
    change_usd_per_kg,
    change_percent,
    'USD' as currency_code,
    'kg' as mass_unit,
    source_capture_id,
    source_dataset || ':' || series_id || ':' || cast(period_month as varchar)
        as benchmark_price_key,
    case when series_id = 'cocoa' then 'cocoa' else 'coffee' end as commodity_key
from {{ ref('monthly_benchmark_prices') }}
