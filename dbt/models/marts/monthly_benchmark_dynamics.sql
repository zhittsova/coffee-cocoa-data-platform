with priced as (
    select * from {{ ref('fct_benchmark_prices') }}
    where price_usd_per_kg is not null
),

base_months as (
    select
        source_dataset,
        benchmark_series,
        min(month_key) as base_month
    from priced
    group by all
),

bases as (
    select
        priced.source_dataset,
        priced.benchmark_series,
        priced.price_usd_per_kg as base_price_usd_per_kg
    from priced
    inner join base_months
        on
            priced.source_dataset = base_months.source_dataset
            and priced.benchmark_series = base_months.benchmark_series
            and priced.month_key = base_months.base_month
)

select
    prices.benchmark_price_key,
    prices.source_dataset,
    prices.commodity_key,
    prices.benchmark_series,
    prices.month_key,
    prices.price_usd_per_kg,
    prices.change_usd_per_kg,
    prices.change_percent,
    prices.currency_code,
    prices.mass_unit,
    false as is_trade_product_identity,
    prices.source_capture_id,
    prices.price_usd_per_kg / nullif(bases.base_price_usd_per_kg, 0) * 100
        as benchmark_index
from {{ ref('fct_benchmark_prices') }} as prices
left join bases
    on
        prices.source_dataset = bases.source_dataset
        and prices.benchmark_series = bases.benchmark_series
