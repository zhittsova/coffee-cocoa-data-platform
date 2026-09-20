select 1
from {{ ref('fct_trade_observations') }}
where
    product_code = '18069090'
    and (
        (classification_year <= 2021 and comparability_segment != '2017-2021')
        or (classification_year >= 2022 and comparability_segment != '2022-2026')
    )
