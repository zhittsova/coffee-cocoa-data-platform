-- depends_on: {{ ref('stg_trade_observations') }}

with annual_products as (
    select
        product_code,
        product_group,
        unnest(range(valid_from_year, valid_to_year + 1)) as classification_year
    from {{ ref('trade_product_groups') }}
)

select
    product_code,
    classification_year,
    'CN' as classification_code,
    product_group,
    product_code || ':' || cast(classification_year as varchar) as trade_product_key,
    case
        when product_group like 'coffee_%' then 'coffee'
        when product_group like 'cocoa_%' or product_group = 'chocolate_cocoa_preparations'
            then 'cocoa'
    end as commodity_key,
    product_code = '18069090' as has_2022_scope_break,
    case
        when product_code = '18069090' and classification_year <= 2021
            then '2017-2021'
        when product_code = '18069090' and classification_year >= 2022
            then '2022-2026'
        else 'no identified CN8 leaf break'
    end as comparability_segment
from annual_products
