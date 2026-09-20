-- depends_on: {{ ref('stg_trade_observations') }}

with commodities (commodity_key, commodity_name) as (
    values
    ('coffee', 'Coffee'),
    ('cocoa', 'Cocoa')
)

select
    commodity_key,
    commodity_name
from commodities
