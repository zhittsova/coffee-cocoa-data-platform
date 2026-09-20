with partner_labels as (
    select
        source_dataset,
        reporter_code,
        partner_code,
        partner_class,
        is_partner_detail,
        min(partner_label) as partner_label,
        count(distinct partner_label) as label_count
    from {{ ref('stg_trade_observations') }}
    group by all
)

select
    source_dataset,
    reporter_code,
    partner_code,
    partner_label,
    partner_class,
    is_partner_detail,
    label_count,
    source_dataset || ':' || reporter_code || ':' || partner_code as trade_partner_key
from partner_labels
