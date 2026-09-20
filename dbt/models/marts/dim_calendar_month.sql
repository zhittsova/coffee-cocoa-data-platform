-- depends_on: {{ ref('stg_trade_observations') }}

with calendar_spine (month_start) as (
    select * from range(
        date '2015-01-01',
        date '2026-09-01',
        interval 1 month
    )
)

select
    cast(month_start as date) as month_key,
    year(month_start) as calendar_year,
    quarter(month_start) as calendar_quarter,
    month(month_start) as calendar_month,
    strftime(month_start, '%Y-%m') as month_label
from calendar_spine
