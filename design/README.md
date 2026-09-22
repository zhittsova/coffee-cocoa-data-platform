# Coffee & Cocoa Data Platform

Design status: benchmark prices, bounded trade staging, dbt domain models and
source replacements run locally. Forecasts and later operational controls remain planned. See [data
contracts](data-contracts.md) for the bounded source scope.

Build a local data platform for coffee and cocoa prices and trade, with dbt at
the center of the transformation layer. Dagster coordinates ingestion, data
checks and materialization. DuckDB and Parquet keep execution local. Forecasting
and notebooks consume governed datasets produced by the platform.

## Scope

- Monthly global cocoa, Arabica and Robusta benchmarks.
- Germany's imports and exports across relevant partner countries, with separate
  totals and defined product groups spanning beans and processed products.
- Trade history from January 2017 through observed current coverage; benchmark
  history from January 2015. Annual definitions include a documented 2022 break.
- Reproducible captures, revision handling, typed schemas, tested models and a
  catalog generated from project metadata.
- Forecast evaluation with temporal splits, baseline comparisons and explicit
  limits on historical information availability.

A small fixture makes checks fast. The analytical profile covers the wider trade
model; a small initial source sample does not establish that coverage.

The platform should explain benchmark changes, trade value and mass, product mix,
partner concentration and forecast performance. Unit values are not retail prices.
Trade statistics do not establish processing margins or causal price transmission.

## Delivery

Native local execution comes first, followed by Docker Compose. Recovery and
validated AWS/Hetzner infrastructure code follow local acceptance. Live cloud
deployment is a separate choice. This project does not claim production scale,
forecast superiority or operational readiness before those outcomes are tested.

Issues define a change's scope and acceptance before implementation. A normal
feature PR contains its code, tests and any durable specification updates.
Material architectural changes may need a design PR first. Specifications evolve
when source evidence or implementation tests invalidate an assumption.

AI assists development. The maintainer owns requirements, technical decisions,
review and acceptance. Source evidence and independently derived expected results
support verification; generated code or tests alone do not establish correctness.

- [Architecture](architecture.md)
- [Data contracts](data-contracts.md)
- [First feature: local foundation](features/001-local-foundation.md)
- [Benchmark price pipeline](features/002-benchmark-price-pipeline.md)
- [Trade ingestion pipeline](features/003-trade-ingestion-pipeline.md)
- [dbt domain model](features/004-dbt-domain-models.md)
- [Local metadata catalog](features/006-governance-catalog.md)

Offline fixtures are the confirmed default. The first forecast targets monthly
cocoa USD/kg at one- and three-month horizons. Freeze its evaluation splits and
availability rules before implementation. The 2015-2016 trade extension remains
deferred until annual classifications are validated. Additional reporters and an FX source require their own scoped changes.
