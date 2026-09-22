{{ assert_forecast_mode() }}

select
    source_capture_id,
    source_update_text,
    source_update_date,
    retrieved_at_utc,
    first_seen_capture_at_utc
from {{ source('world_bank_prices', 'forecast_capture_metadata') }}
