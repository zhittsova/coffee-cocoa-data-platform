with bad_splits as (
    select origin_month
    from {{ ref('forecast_split_manifest') }}
    where
        (origin_month < date '2024-01-01' and split_name <> 'development')
        or (origin_month >= date '2024-01-01' and split_name <> 'holdout')
        or evaluation_mode <> 'retrospective_current_vintage'
        or minimum_observed_history_months <> 60
        or minimum_labeled_training_origins <> 48
        or issue_at_utc <> cast(origin_month + interval 1 month as timestamp) at time zone 'UTC'
),

bad_features as (
    select origin_month
    from {{ ref('forecast_origin_features') }}
    where
        lag_0_month <> origin_month
        or lag_1_month <> cast(origin_month - interval 1 month as date)
        or lag_3_month <> cast(origin_month - interval 3 month as date)
        or lag_12_month <> cast(origin_month - interval 12 month as date)
        or has_minimum_history <> (observed_history_months_60 = 60)
        or observed_history_months_60 > 60
),

bad_targets as (
    select origin_month
    from {{ ref('forecast_targets') }}
    where
        horizon_months not in (1, 3)
        or target_month <> cast(origin_month + horizon_months * interval 1 month as date)
        or target_month <= origin_month
        or labeled_training_origins < 0
)

select origin_month from bad_splits
union all
select origin_month from bad_features
union all
select origin_month from bad_targets
