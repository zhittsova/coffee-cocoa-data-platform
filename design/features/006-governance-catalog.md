# Local metadata catalog

Scope and acceptance are defined in [issue #16](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/16).

The dbt source, seed and model YAML files own purpose, grain, typed columns, units,
origin, reuse terms, classification, refresh expectations and provenance. dbt
model contracts enforce names and types for all SQL models. Source Parquet and
seed schemas are checked against those declarations by the catalog validator.
Existing dbt data tests cover semantic constraints that DuckDB contracts do not
enforce. Classification is descriptive metadata, not access control.

Dagster reads the dbt source declarations for its two raw asset descriptions,
owners, schemas and discovery fields. A dbt translator carries model metadata
into Dagster's lineage graph. Materializations record the local source manifest
path and capture identity.
The two capture manifests record the source version, retrieval and source URL.
Replacement warehouses retain selected capture identities in `_revision_state`
and archived manifests under `.state/capture_manifests/`.

Run the offline fixture pipeline, then generate and validate the catalog against
its root:

```sh
uv run --locked python -m coffee_cocoa_platform.pipeline_cli --root data/catalog-fixture
uv run --locked python -m coffee_cocoa_platform.catalog_cli --root data/catalog-fixture
uv run --locked dbt docs serve --project-dir dbt --profiles-dir dbt
```

The generator writes ignored `dbt/target/index.html`, `manifest.json`,
`catalog.json` and `provenance.json`. The provenance index resolves fact capture
IDs to local manifests and records the latest replacement run ID when available.
It adds physical external Parquet column types to the generated catalog because
dbt-duckdb does not introspect those sources into `catalog.json`.
It validates declarations against the selected warehouse and checks that fact
capture IDs resolve to source manifests or the replacement registry. No separate
catalog service or automatic schedule is required.

World Bank dataset terms include third-party conditions; redistribution rights
for real observations remain unresolved. Eurostat reuse requires attribution,
access date and identification of transformations. Real captures and generated
catalog artifacts stay local.
