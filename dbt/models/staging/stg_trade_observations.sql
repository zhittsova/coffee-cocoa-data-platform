{{ config(materialized='incremental', incremental_strategy='delete+insert',
    on_schema_change='fail', pre_hook="{{ delete_replacement_scope('trade') }}") }}

with source_data as (
    select * from {{ source('eurostat_trade', 'monthly_trade') }}
),

product_mapping as (
    select * from {{ ref('trade_product_groups') }}
)

select
    source_data.source_dataset,
    source_data.frequency_code,
    source_data.reporter_code,
    source_data.partner_code,
    source_data.partner_label,
    source_data.product_code,
    product_mapping.product_group,
    source_data.classification_code,
    source_data.classification_year,
    source_data.flow_code,
    source_data.indicator_code,
    source_data.source_value_text,
    source_data.source_value,
    source_data.source_status,
    source_data.status_available,
    source_data.period_month,
    source_data.source_capture_id,
    source_data.source_update_time,
    case
        when source_data.partner_code in (
            'WORLD', 'INT_EU', 'EXT_EU', 'INT_EU27_2020', 'EXT_EU27_2020',
            'INT_EA', 'EXT_EA', 'INT_EA21', 'EXT_EA21'
        ) then 'aggregate'
        when source_data.partner_code in (
            'QP', 'QQ', 'QR', 'QS', 'QU', 'QV', 'QW', 'QX', 'QY', 'QZ'
        ) then 'special'
        else 'named'
    end as partner_class,
    source_data.partner_code not in (
        'WORLD', 'INT_EU', 'EXT_EU', 'INT_EU27_2020', 'EXT_EU27_2020',
        'INT_EA', 'EXT_EA', 'INT_EA21', 'EXT_EA21'
    ) as is_partner_detail,
    case
        when source_data.product_code = '18069090' and source_data.classification_year <= 2021
            then '2017-2021'
        when source_data.product_code = '18069090' and source_data.classification_year >= 2022
            then '2022-2026'
        else 'no identified CN8 leaf break'
    end as comparability_segment,
    case source_data.flow_code when '1' then 'imports' when '2' then 'exports' end as flow_name,
    case
        when source_data.indicator_code = 'VALUE_IN_EUROS' then 'EUR'
        when source_data.indicator_code = 'QUANTITY_IN_100KG' then '100kg'
    end as source_unit,
    case
        when source_data.indicator_code = 'VALUE_IN_EUROS' then 'EUR'
        when source_data.indicator_code = 'QUANTITY_IN_100KG' then 'kg'
    end as normalized_unit,
    case
        when source_data.indicator_code = 'VALUE_IN_EUROS' then source_data.source_value
        when source_data.indicator_code = 'QUANTITY_IN_100KG' then source_data.source_value * 100
    end as normalized_value
from source_data
left join product_mapping
    on
        source_data.product_code = product_mapping.product_code
        and source_data.classification_year
        between product_mapping.valid_from_year and product_mapping.valid_to_year

{% if is_incremental() %}
where {{ replacement_predicate('trade', 'source_data') }}
{% endif %}
