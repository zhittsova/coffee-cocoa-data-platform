# dbt, DuckDB, and Dagster

A local data project built around dbt, with Python ingestion, Dagster orchestration,
Parquet storage, and DuckDB analytics.

The Python project foundation installs with uv on Python 3.12. The pipeline is
not runnable yet; ingestion, dbt models and Dagster assets follow in later changes.

Run `uv sync --locked` to install the package and development tools. Use
`uv sync --locked --no-dev` for runtime dependencies alone, or
`uv sync --locked --group notebooks` to add notebook tools. The project uses
`COFFEE_COCOA_HOME` (default: current directory) as the root for future `data/`,
`warehouse/` and `.state/` paths. These directories are created only by future
pipeline steps.

Read the [project design](design/README.md) for the planned architecture, data
contracts and first implementation feature.

Pull requests run the locked local checks, package smoke test and conventional
title check. Main requires those checks, a PR and signed commits; it rejects force
pushes and deletion. There is no required self-approval for the solo maintainer.
