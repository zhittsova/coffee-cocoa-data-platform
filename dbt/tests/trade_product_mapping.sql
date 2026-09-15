select 1
from {{ ref('trade_product_groups') }}
having
    count(*) != 33
    or min(valid_from_year) != 2017
    or max(valid_to_year) != 2026
