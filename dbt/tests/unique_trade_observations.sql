select
    source_dataset,
    frequency_code,
    reporter_code,
    partner_code,
    product_code,
    flow_code,
    indicator_code,
    period_month
from {{ ref('stg_trade_observations') }}
group by 1, 2, 3, 4, 5, 6, 7, 8
having count(*) != 1
