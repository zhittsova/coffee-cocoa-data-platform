select
    source_dataset,
    series_id,
    period_month
from {{ ref('monthly_benchmark_prices') }}
group by 1, 2, 3
having count(*) != 1
