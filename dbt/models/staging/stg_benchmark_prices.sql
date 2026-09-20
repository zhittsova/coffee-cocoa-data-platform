{{ config(materialized='incremental', incremental_strategy='delete+insert',
    on_schema_change='fail', pre_hook="{{ delete_replacement_scope('prices') }}") }}

select
    source_dataset,
    series_id,
    period_month,
    source_value_text,
    price_usd_per_kg,
    source_capture_id,
    source_status
from {{ source('world_bank_prices', 'monthly_prices') }}

{% if is_incremental() %}
where {{ replacement_predicate('prices', '') }}
{% endif %}
