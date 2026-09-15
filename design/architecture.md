# Architecture

Status: the World Bank price path and bounded Eurostat trade staging path are
implemented locally. Trade marts, forecasting, read snapshots, recovery and
deployment controls below remain planned.

## Responsibilities

| Component | Responsibility |
| --- | --- |
| Python | Bounded source access, format parsing and typed Parquet publication |
| Dagster | Asset dependencies, refreshes, backfills, retry policy and run metadata |
| dbt | Classification mapping, transformations, facts, dimensions, business tests and forecast feature tables |
| Parquet | Local standardized observations with explicit schemas and capture identity |
| DuckDB | SQL execution and a rebuildable analytical warehouse |
| Forecast package | Training, temporal evaluation and versioned prediction outputs |
| Notebooks | Read-only exploration and presentation of published results |

## Data flow

Retain original source captures and their request/checksum manifests. Parse them
into validated observations with source codes, missingness and provenance. The
trade capture completes a fixed set of year/product slices before replacing the
current Parquet file. dbt assigns product groups, partner classes, units and the
documented CN comparability segment. Later models build marts and forecast
datasets. Publish a consistent read snapshot only after the required checks pass.

These boundaries correspond to raw, standardized and business layers, often
called bronze, silver and gold. They do not require three physical copies of
every table. Choose materializations for their purpose; avoid empty pass-through
models and partition sizes smaller than the workload justifies.

Keep source-level facts at declared grains. Product dimensions carry classification
and validity periods. Geography dimensions distinguish countries, regions and
totals. Price benchmarks remain separate from trade unit values. Import/export
balances compare compatible coverage and retain their valuation differences.

The catalog starts with dbt and Dagster metadata: purpose, owner, grain, columns,
units, source attribution, lineage, availability and quality results. Add another
catalog service only if these capabilities prove insufficient.

## Why dbt and Dagster

The broader trade model needs reusable definitions and tests for product
hierarchies, partner totals, units, revisions and analytical joins. dbt makes those
rules reviewable alongside their dependencies. Dagster manages recurring sources
with different reporting lags and coordinates safe refreshes and backfills.

This is a local analytical workload. The design is justified by modeled behavior
and maintenance needs; it makes no claim that a small source sample requires
distributed processing. Full refresh remains a valid baseline. Incremental work
must prove equivalent results and have a concrete revision or operational benefit.

## Publication and recovery

Prepare each capture in isolation. A failed download or validation cannot replace
the last valid publication. Keep immutable capture identity separate from the
current observation key so revised records do not accumulate as duplicate facts.
Bound backfills and retain a manifest of complete slices.

Coordinate every supported warehouse writer. Notebooks read a published immutable
snapshot, avoiding concurrent access to the mutable DuckDB file. Retention must
preserve inputs needed to reproduce published results and active readers.

## Forecasting boundary

dbt prepares features with declared observation periods, availability assumptions
and source versions. Python performs fitting and evaluation; Dagster coordinates
the resulting assets. Use time-ordered evaluation and compare simple baselines.
Report errors and interval coverage by forecast horizon.

Current downloads may contain revisions unavailable historically. A backtest using
those downloads is retrospective unless historical vintages and availability
are independently established. Future features must never enter an earlier
training cutoff. A source's newest requested month is not proof of an observation.

## Local execution and later deployment

Use one installable Python package, a root uv lockfile and an explicit supported
Python version. Lock developer tools with the project. Keep notebook and forecast
dependencies optional where they are not required by the core pipeline.

Docker Compose runs the same application with explicit persistent storage and
localhost access. AWS and Hetzner have independent Terraform roots and state;
Ansible configures Hetzner. Deployment choices follow measured runtime needs and
an explicitly selected account, budget and access policy.
