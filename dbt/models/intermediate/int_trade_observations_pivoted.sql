with observations as (
    select * from {{ ref('stg_trade_observations') }}
),

pivoted as (
    select
        source_dataset,
        frequency_code,
        reporter_code,
        partner_code,
        partner_label,
        partner_class,
        is_partner_detail,
        product_code,
        product_group,
        classification_code,
        classification_year,
        comparability_segment,
        flow_code,
        flow_name,
        period_month,
        source_capture_id,
        source_update_time,
        count(*) filter (
            where indicator_code = 'VALUE_IN_EUROS'
        ) = 1 as value_cell_present,
        max(source_value) filter (
            where indicator_code = 'VALUE_IN_EUROS'
        ) as trade_value_eur,
        max(source_status) filter (
            where indicator_code = 'VALUE_IN_EUROS'
        ) as value_status,
        max(status_available) filter (
            where indicator_code = 'VALUE_IN_EUROS'
        ) as value_status_available,
        count(*) filter (
            where indicator_code = 'QUANTITY_IN_100KG'
        ) = 1 as quantity_cell_present,
        max(normalized_value) filter (
            where indicator_code = 'QUANTITY_IN_100KG'
        ) as net_mass_kg,
        max(source_status) filter (
            where indicator_code = 'QUANTITY_IN_100KG'
        ) as quantity_status,
        max(status_available) filter (
            where indicator_code = 'QUANTITY_IN_100KG'
        ) as quantity_status_available
    from observations
    group by all
)

select
    *,
    trade_value_eur is not null as has_numeric_value,
    net_mass_kg is not null as has_numeric_mass,
    trade_value_eur is not null and net_mass_kg is not null as has_paired_measures,
    case
        when trade_value_eur is not null and net_mass_kg > 0
            then trade_value_eur / net_mass_kg
    end as unit_value_eur_per_kg,
    case flow_code when '1' then 'CIF' when '2' then 'FOB' end as value_basis
from pivoted
