# dbt, DuckDB, and Dagster

A local data project built around dbt, with Python ingestion, Dagster orchestration,
Parquet storage, and DuckDB analytics.

The Python project installs with uv on Python 3.12. The first local pipeline reads
World Bank monthly cocoa, Arabica and Robusta benchmarks through Dagster, writes
validated Parquet, and builds a monthly result in DuckDB with dbt.

Run `uv sync --locked` to install the package and development tools. Use
`uv sync --locked --no-dev` for runtime dependencies alone, or
`uv sync --locked --group notebooks` to add notebook tools.

Run the offline synthetic fixture from the repository root:

```sh
uv run --locked python -m coffee_cocoa_platform.price_cli --root .
```

The command writes `data/parquet/benchmark_prices.parquet`, a capture manifest
beside it, and `warehouse/coffee_cocoa.duckdb`. It runs dbt models and tests.
To fetch the publisher workbook explicitly, choose an inclusive range:

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

Read the [project design](design/README.md) for the architecture and data
contracts.

Pull requests run the locked local checks, package smoke test and conventional
title check. Main requires those checks, a PR and signed commits; it rejects force
pushes and deletion. There is no required self-approval for the solo maintainer.
