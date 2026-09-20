select
    period_month as month_key,
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
    value_basis,
    trade_value_eur,
    net_mass_kg,
    unit_value_eur_per_kg,
    value_cell_present,
    has_numeric_value,
    value_status,
    value_status_available,
    quantity_cell_present,
    has_numeric_mass,
    quantity_status,
    quantity_status_available,
    has_paired_measures,
    source_capture_id,
    source_update_time,
    concat_ws(
        ':',
        source_dataset,
        reporter_code,
        partner_code,
        product_code,
        cast(classification_year as varchar),
        flow_code,
        cast(period_month as varchar)
    ) as trade_observation_key,
    product_code || ':' || cast(classification_year as varchar) as trade_product_key,
    source_dataset || ':' || reporter_code || ':' || partner_code
        as trade_partner_key,
    case
        when product_group like 'coffee_%' then 'coffee'
        else 'cocoa'
    end as commodity_key
from {{ ref('int_trade_observations_pivoted') }}
