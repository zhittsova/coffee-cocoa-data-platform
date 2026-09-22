# Cocoa forecast evaluation

Scope: [issue #24](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/24).

The optional `forecast` extra runs a local retrospective experiment on the
governed World Bank cocoa USD/kg dataset. Dagster reads one immutable input
snapshot, publishes versioned Parquet predictions and metrics, builds the dbt
result marts, runs the result invariants and publishes a new read snapshot.
The core runtime can build its source and domain tables without this extra.

## Evaluation contract

The fixed origins and one- and three-month targets are in the
[forecast dataset contract](009-forecast-datasets.md). A scored origin needs
60 consecutive observed months through its issue cutoff and 48 labeled prior
origins for its horizon. Every model uses the same eligible origins. Missing
labels remain unscored; fit failures remain visible. The evaluation uses
the selected current source vintage and does not claim historical as-of
availability.

Persistence repeats the price at the origin. Seasonal naive uses the price
at the target month one year earlier, which is origin minus 11 months for
the one-month horizon and origin minus nine months for the three-month
horizon. The candidate is an AR(1) with an intercept, fitted by ordinary
least squares on all consecutive observed months through each development
origin, after the 60-month history gate is met. A
rank-deficient or nonfinite fit fails explicitly. Its one- or three-step
forecast starts from the observed origin price.

Development uses expanding rolling origins through December 2023. For
model selection, only development forecasts whose target is observed by
December 2023 count. Development origins whose target falls in 2024 keep
their predictions but expose no target or error. The lowest development
MAE wins separately for each horizon; exact ties prefer persistence, then
seasonal naive, then AR(1). The AR(1) coefficients used in the holdout are
fitted once on the consecutive history through December 2023. They remain
fixed across the 2024-2025 holdout.
An origin's observed price may serve as its input at issue time, but no
holdout label enters model fitting or selection.

Predictions include point error and a symmetric 90% empirical interval.
The radius is the finite-sample rank of absolute prior development errors
for the same model and horizon, with at least 12 available residuals.
Development calibration uses only earlier predictions whose targets have
been observed by the scored origin. Holdout calibration is frozen at
December 2023. Intervals are null until enough residuals exist. Reports
show MAE, RMSE, sMAPE (zero when both prediction and target are zero),
coverage and mean interval width by split, horizon and model, with explicit
scored and interval denominators.

## Publication and limits

`data/forecasts/runs/<run_id>/` retains the config, package and code/lock
hashes, input snapshot and capture identities, predictions and summaries.
The current Parquet copies feed the dbt `forecast_predictions` and
`forecast_metrics` marts. Their dbt source and marts are enabled only for
an explicit forecast run. Dagster's forecast assets show the input snapshot,
result files, dbt tables and result snapshot in order.

Run `uv run --locked --extra forecast python -m coffee_cocoa_platform.forecast_cli --root <local-root>`
after a complete tested input snapshot with sufficient
price history has been published at that root. The command does not fetch a
source. It refuses to evaluate an already published forecast snapshot again.

The experiment is descriptive and retrospective. Current downloads may
contain revisions that were unavailable at historical issue times. The
empirical intervals are a diagnostic; they do not guarantee prospective
coverage. Model selection may favor a baseline, and the holdout can perform
worse than development. Real captures and prediction rows stay local while
redistribution rights remain unresolved.
