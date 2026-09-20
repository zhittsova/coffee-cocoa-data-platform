with named_concentration as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        flow_code,
        flow_name,
        value_basis,
        max(named_partner_denominator_eur) as named_partner_value_eur,
        count(*) as named_partner_count,
        max(named_partner_value_share) as largest_named_partner_share,
        sum(named_partner_value_share * named_partner_value_share)
            as named_partner_hhi
    from {{ ref('monthly_partner_shares') }}
    group by all
),

context_by_leaf as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        product_code,
        flow_code,
        sum(trade_value_eur) filter (
            where partner_class = 'named' and has_numeric_value
        ) as named_value_eur,
        sum(trade_value_eur) filter (
            where partner_class = 'special' and has_numeric_value
        ) as observed_special_value_eur,
        sum(trade_value_eur) filter (
            where partner_code = 'WORLD' and has_numeric_value
        ) as world_value_eur
    from {{ ref('fct_trade_observations') }}
    where partner_class != 'aggregate' or partner_code = 'WORLD'
    group by all
),

context_values as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        flow_code,
        sum(observed_special_value_eur) as observed_special_value_eur,
        sum(world_value_eur) as world_value_eur,
        sum(
            case
                when world_value_eur is not null
                    then
                        world_value_eur
                        - coalesce(named_value_eur, 0)
                        - coalesce(observed_special_value_eur, 0)
            end
        ) as reconciliation_residual_eur,
        count(*) filter (where named_value_eur is not null)
            as named_observed_product_count,
        count(*) filter (where observed_special_value_eur is not null)
            as special_observed_product_count,
        count(*) filter (where world_value_eur is not null)
            as world_observed_product_count,
        string_agg(product_code, ',' order by product_code) filter (
            where named_value_eur is not null
        ) as named_product_coverage,
        string_agg(product_code, ',' order by product_code) filter (
            where observed_special_value_eur is not null
        ) as special_product_coverage,
        string_agg(product_code, ',' order by product_code) filter (
            where world_value_eur is not null
        ) as world_product_coverage
    from context_by_leaf
    group by all
)

select
    context_values.source_dataset,
    context_values.reporter_code,
    context_values.month_key,
    context_values.classification_year,
    context_values.product_group,
    context_values.flow_code,
    named_concentration.named_partner_value_eur,
    named_concentration.largest_named_partner_share,
    named_concentration.named_partner_hhi,
    context_values.observed_special_value_eur,
    context_values.world_value_eur,
    context_values.reconciliation_residual_eur,
    context_values.named_observed_product_count,
    context_values.special_observed_product_count,
    context_values.world_observed_product_count,
    context_values.named_product_coverage,
    context_values.special_product_coverage,
    context_values.world_product_coverage,
    'observed named partners with numeric VALUE_IN_EUROS cells'
        as concentration_universe,
    case context_values.flow_code when '1' then 'imports' when '2' then 'exports' end
        as flow_name,
    case context_values.flow_code when '1' then 'CIF' when '2' then 'FOB' end
        as value_basis,
    coalesce(named_concentration.named_partner_count, 0) as named_partner_count,
    coalesce(named_concentration.named_partner_value_eur > 0, false)
        as has_positive_named_denominator,
    context_values.observed_special_value_eur
    / nullif(context_values.world_value_eur, 0) as observed_special_share_of_world,
    (
        context_values.reconciliation_residual_eur
    ) / nullif(context_values.world_value_eur, 0) as residual_share_of_world,
    case
        when context_values.world_value_eur is null then 'WORLD unavailable'
        when
            coalesce(context_values.named_product_coverage, '__NULL__')
            != coalesce(context_values.world_product_coverage, '__NULL__')
            then 'partial WORLD coverage'
        when
            context_values.reconciliation_residual_eur = 0
            then 'exact'
        when
            context_values.reconciliation_residual_eur > 0
            then 'positive residual'
        else 'named plus special exceeds WORLD'
    end as reconciliation_status
from context_values
left join named_concentration
    on
        context_values.source_dataset = named_concentration.source_dataset
        and context_values.reporter_code = named_concentration.reporter_code
        and context_values.month_key = named_concentration.month_key
        and context_values.classification_year
        = named_concentration.classification_year
        and context_values.product_group = named_concentration.product_group
        and context_values.flow_code = named_concentration.flow_code
