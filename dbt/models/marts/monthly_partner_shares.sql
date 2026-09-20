with named_values as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        flow_code,
        flow_name,
        value_basis,
        trade_partner_key,
        partner_code,
        partner_label,
        sum(trade_value_eur) as partner_value_eur,
        count(distinct product_code) as observed_product_count
    from {{ ref('fct_trade_observations') }}
    where partner_class = 'named' and has_numeric_value
    group by all
),

with_denominator as (
    select
        *,
        sum(partner_value_eur) over (
            partition by
                source_dataset,
                reporter_code,
                month_key,
                product_group,
                flow_code
        ) as named_partner_denominator_eur
    from named_values
)

select
    *,
    'observed named partners with numeric VALUE_IN_EUROS cells'
        as concentration_universe,
    case
        when named_partner_denominator_eur > 0
            then partner_value_eur / named_partner_denominator_eur
    end as named_partner_value_share,
    named_partner_denominator_eur > 0 as has_positive_named_denominator,
    row_number() over (
        partition by
            source_dataset,
            reporter_code,
            month_key,
            product_group,
            flow_code
        order by partner_value_eur desc, partner_code asc
    ) as named_partner_rank
from with_denominator
