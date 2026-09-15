select *
from {{ ref('stg_trade_observations') }}
where
    (source_value is null and normalized_value is not null)
    or (source_value is not null and normalized_value is null)
    or (
        indicator_code = 'VALUE_IN_EUROS'
        and (
            source_unit != 'EUR'
            or normalized_unit != 'EUR'
            or normalized_value != source_value
        )
    )
    or (
        indicator_code = 'QUANTITY_IN_100KG'
        and (
            source_unit != '100kg'
            or normalized_unit != 'kg'
            or normalized_value != source_value * 100
        )
    )
    or (partner_class = 'aggregate' and is_partner_detail)
    or (partner_class != 'aggregate' and not is_partner_detail)
    or (not status_available and source_status is not null)
