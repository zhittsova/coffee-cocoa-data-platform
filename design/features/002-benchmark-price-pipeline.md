# Benchmark price pipeline

Status: implemented locally. Scope and acceptance were defined in
[issue #8](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/8).

One command runs a synthetic World Bank-shaped workbook through Dagster, a typed
Arrow/Parquet source and dbt's DuckDB models. A separate explicit live command
downloads one bounded monthly workbook for a named month range. Both use the same
parser and publication path. The source workbook is saved by SHA-256; current
Parquet replaces the previous file only after full validation and staged-file
verification. The manifest records source identity and observed coverage.

The supported source rows are cocoa, Coffee Arabica and Coffee Robusta in nominal
USD/kg. The workbook's numeric cell text is retained before decimal(20,8)
normalization. Source status is null because the selected fields provide no
observation status. Missing cells stay null, and absent months are not created.
The monthly dbt result reports a price change only when the preceding calendar
month has a usable price. It does not sum unlike benchmark series.

The offline fixture has three represented months, one absent month, eight prices
and one blank cell. dbt models and tests run in the same Dagster asset graph.
Source captures, manifests and DuckDB files are local data. Trade ingestion,
forecasting, revision backfills and read snapshots are separate features.
