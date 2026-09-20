# dbt, DuckDB, and Dagster

A local data project built around dbt, with Python ingestion, Dagster orchestration,
Parquet storage, and DuckDB analytics.

The Python project installs with uv on Python 3.12. Local pipelines read World
Bank monthly cocoa, Arabica and Robusta benchmarks and bounded German trade data
through Dagster. They publish validated Parquet and build DuckDB models with dbt.

Run `uv sync --locked` to install the package and development tools. Use
`uv sync --locked --no-dev` for runtime dependencies alone, or
`uv sync --locked --group notebooks` to add notebook tools.

Run both offline synthetic fixtures from the repository root:

```sh
uv run --locked python -m coffee_cocoa_platform.pipeline_cli --root .
```

The command publishes benchmark and trade Parquet files with capture manifests,
then runs the related dbt models and tests in `warehouse/coffee_cocoa.duckdb`.
The trade fixture spans the 2021/2022 CN boundary and keeps sparse cells, source
status, partner totals and special partners distinct.

dbt publishes separate benchmark and trade facts, conformed calendar, commodity,
product and partner dimensions, and monthly analytical models. Trade models
declare their observed CN8 and partner coverage before calculating unit values,
product mix, balances, shares or concentration. `metric_dictionary` exposes each
governed metric's expression, unit, denominator and coverage from model metadata.

To fetch the World Bank workbook explicitly, choose an inclusive range:

```sh
uv run --locked python -m coffee_cocoa_platform.price_cli --mode live --start 2015-01 --end 2026-08 --root .
```

The live adapter downloads the workbook once with a 2 MiB response limit and
retains its original bytes under `data/raw/`. Both commands use
`COFFEE_COCOA_HOME` as the data root. Real captures and derived files stay local.
The source is [World Bank Commodity Price Data (Pink Sheet)](https://www.worldbank.org/en/research/commodity-markets);
the selected series have ICCO and ICO inputs. The pipeline selects those series,
reshapes monthly rows and rounds to decimal(20,8). See the
[dataset terms](https://www.worldbank.org/ext/en/legal/terms-conditions/datasets)
before redistributing real observations.

Eurostat trade downloads are also explicit. The smoke profile keeps the original
two-product, five-partner sample:

```sh
uv run --locked python -m coffee_cocoa_platform.trade_cli --mode live --profile smoke --start 2024-01 --end 2024-01 --root .
```

The full profile accepts bounds from January 2017 through August 2026. It splits
each year into five product batches of at most eight CN8 leaves and requests both
flows, both measures and the complete supported partner dimension. A failed or
incomplete slice stops publication; rerunning the same command resumes only
checksum-verified captures. Real trade captures stay local under Eurostat's
[reuse policy](https://ec.europa.eu/eurostat/help/copyright-notice).

Read the [project design](design/README.md) for the architecture and data
contracts, including the [dbt domain model](design/features/004-dbt-domain-models.md).

Pull requests run the locked local checks, package smoke test and conventional
title check. Main requires those checks, a PR and signed commits; it rejects force
pushes and deletion. There is no required self-approval for the solo maintainer.
