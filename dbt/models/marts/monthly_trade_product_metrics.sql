with world_observations as (
    select * from {{ ref('fct_trade_observations') }}
    where partner_code = 'WORLD'
),

expected_products as (
    select
        classification_year,
        product_group,
        count(*) as expected_product_count,
        bool_or(has_2022_scope_break) as contains_2022_scope_break
    from {{ ref('dim_trade_product') }}
    group by all
),

expected_universe as (
    select
        classification_year,
        count(*) as universe_expected_product_count
    from {{ ref('dim_trade_product') }}
    group by all
),

grouped as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        flow_code,
        flow_name,
        value_basis,
        count(distinct product_code) filter (
            where value_cell_present
        ) as represented_value_product_count,
        count(distinct product_code) filter (
            where has_numeric_value
        ) as observed_value_product_count,
        count(distinct product_code) filter (
            where quantity_cell_present
        ) as represented_mass_product_count,
        count(distinct product_code) filter (
            where has_numeric_mass
        ) as observed_mass_product_count,
        count(distinct product_code) filter (
            where has_numeric_value and has_numeric_mass and net_mass_kg > 0
        ) as paired_product_count,
        sum(trade_value_eur) filter (
            where has_numeric_value
        ) as world_value_eur,
        sum(net_mass_kg) filter (
            where has_numeric_mass
        ) as world_net_mass_kg,
        sum(trade_value_eur) filter (
            where has_numeric_value and has_numeric_mass and net_mass_kg > 0
        ) as paired_value_eur,
        sum(net_mass_kg) filter (
            where has_numeric_value and has_numeric_mass and net_mass_kg > 0
        ) as paired_net_mass_kg
    from world_observations
    group by all
),

with_coverage as (
    select
        grouped.*,
        expected_products.expected_product_count,
        expected_products.contains_2022_scope_break,
        expected_universe.universe_expected_product_count,
        grouped.observed_value_product_count
        = expected_products.expected_product_count as has_complete_value_coverage,
        grouped.observed_mass_product_count
        = expected_products.expected_product_count as has_complete_mass_coverage,
        grouped.paired_product_count
        = expected_products.expected_product_count as has_complete_paired_coverage
    from grouped
    inner join expected_products
        on
            grouped.classification_year = expected_products.classification_year
            and grouped.product_group = expected_products.product_group
    inner join expected_universe
        on grouped.classification_year = expected_universe.classification_year
),

with_denominators as (
    select
        *,
        sum(world_value_eur) over (
            partition by source_dataset, reporter_code, month_key, flow_code
        ) as observed_product_mix_denominator_eur,
        sum(observed_value_product_count) over (
            partition by source_dataset, reporter_code, month_key, flow_code
        ) as universe_observed_value_product_count,
        max(universe_expected_product_count) over (
            partition by source_dataset, reporter_code, month_key, flow_code
        ) as declared_universe_expected_product_count
    from with_coverage
)

select
    *,
    'WORLD rows for observed selected CN8 leaves' as coverage_basis,
    case
        when paired_net_mass_kg > 0 then paired_value_eur / paired_net_mass_kg
    end as observed_unit_value_eur_per_kg,
    case
        when observed_product_mix_denominator_eur > 0
            then world_value_eur / observed_product_mix_denominator_eur
    end as observed_product_value_share,
    universe_observed_value_product_count = declared_universe_expected_product_count
        as has_complete_product_mix_universe
from with_denominators
