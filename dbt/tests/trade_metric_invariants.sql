with fact_unit_values as (
    select 1 as failure
    from {{ ref('fct_trade_observations') }}
    where
        (
            net_mass_kg <= 0
            and unit_value_eur_per_kg is not null
        )
        or (
            net_mass_kg > 0
            and trade_value_eur is not null
            and abs(unit_value_eur_per_kg - trade_value_eur / net_mass_kg) > 1e-12
        )
),

named_share_sums as (
    select 1 as failure
    from {{ ref('monthly_partner_shares') }}
    where has_positive_named_denominator
    group by
        source_dataset,
        reporter_code,
        month_key,
        product_group,
        flow_code
    having abs(sum(named_partner_value_share) - 1) > 1e-12
),

product_share_sums as (
    select 1 as failure
    from {{ ref('monthly_trade_product_metrics') }}
    where observed_product_mix_denominator_eur > 0
    group by source_dataset, reporter_code, month_key, flow_code
    having abs(sum(observed_product_value_share) - 1) > 1e-12
),

balance_gates as (
    select 1 as failure
    from {{ ref('monthly_trade_balances') }}
    where
        (not has_compatible_flow_coverage and trade_balance_eur is not null)
        or (
            has_compatible_flow_coverage
            and trade_balance_eur != export_value_fob_eur - import_value_cif_eur
        )
),

concentration_bounds as (
    select 1 as failure
    from {{ ref('monthly_partner_concentration') }}
    where
        (not has_positive_named_denominator and named_partner_hhi is not null)
        or named_partner_hhi < 0
        or named_partner_hhi > 1
)

select * from fact_unit_values
union all
select * from named_share_sums
union all
select * from product_share_sums
union all
select * from balance_gates
union all
select * from concentration_bounds
