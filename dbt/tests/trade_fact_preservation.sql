with staged as (
    select
        count(distinct concat_ws(
            ':',
            source_dataset,
            reporter_code,
            partner_code,
            product_code,
            flow_code,
            cast(period_month as varchar)
        )) as expected_fact_count,
        sum(source_value) filter (
            where indicator_code = 'VALUE_IN_EUROS'
        ) as staged_value_eur,
        sum(normalized_value) filter (
            where indicator_code = 'QUANTITY_IN_100KG'
        ) as staged_mass_kg
    from {{ ref('stg_trade_observations') }}
),

facts as (
    select
        count(*) as fact_count,
        sum(trade_value_eur) as fact_value_eur,
        sum(net_mass_kg) as fact_mass_kg
    from {{ ref('fct_trade_observations') }}
),

joined as (
    select
        count(*) as joined_count,
        sum(facts.trade_value_eur) as joined_value_eur,
        sum(facts.net_mass_kg) as joined_mass_kg
    from {{ ref('fct_trade_observations') }} as facts
    inner join {{ ref('dim_trade_product') }} as products
        on facts.trade_product_key = products.trade_product_key
    inner join {{ ref('dim_trade_partner') }} as partners
        on facts.trade_partner_key = partners.trade_partner_key
)

select 1
from staged
cross join facts
cross join joined
where
    staged.expected_fact_count != facts.fact_count
    or facts.fact_count != joined.joined_count
    or staged.staged_value_eur != facts.fact_value_eur
    or facts.fact_value_eur != joined.joined_value_eur
    or staged.staged_mass_kg != facts.fact_mass_kg
    or facts.fact_mass_kg != joined.joined_mass_kg
