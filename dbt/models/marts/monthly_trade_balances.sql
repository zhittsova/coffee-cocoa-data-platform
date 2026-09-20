with value_cells as (
    select * from {{ ref('fct_trade_observations') }}
    where partner_code = 'WORLD' and has_numeric_value
),

by_flow as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        flow_code,
        sum(trade_value_eur) as flow_value_eur,
        count(distinct product_code) as observed_product_count,
        string_agg(distinct product_code, ',' order by product_code)
            as product_coverage_signature
    from value_cells
    group by all
),

paired_flows as (
    select
        source_dataset,
        reporter_code,
        month_key,
        classification_year,
        product_group,
        max(flow_value_eur) filter (
            where flow_code = '1'
        ) as import_value_cif_eur,
        max(flow_value_eur) filter (
            where flow_code = '2'
        ) as export_value_fob_eur,
        max(observed_product_count) filter (
            where flow_code = '1'
        ) as import_product_count,
        max(observed_product_count) filter (
            where flow_code = '2'
        ) as export_product_count,
        max(product_coverage_signature) filter (
            where flow_code = '1'
        ) as import_product_coverage,
        max(product_coverage_signature) filter (
            where flow_code = '2'
        ) as export_product_coverage
    from by_flow
    group by all
)

select
    *,
    'WORLD rows with identical observed value-leaf sets for both flows'
        as coverage_basis,
    coalesce(
        import_product_coverage is not null
        and import_product_coverage = export_product_coverage,
        false
    ) as has_compatible_flow_coverage,
    case
        when
            import_product_coverage is not null
            and import_product_coverage = export_product_coverage
            then export_value_fob_eur - import_value_cif_eur
    end as trade_balance_eur
from paired_flows
