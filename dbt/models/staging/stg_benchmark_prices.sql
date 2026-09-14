select
    source_dataset,
    series_id,
    period_month,
    source_value_text,
    price_usd_per_kg,
    source_capture_id,
    source_status
from {{ source('world_bank_prices', 'monthly_prices') }}
