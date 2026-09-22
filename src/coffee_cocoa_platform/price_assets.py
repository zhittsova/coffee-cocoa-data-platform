"""Dagster's source-to-dbt benchmark asset graph."""

import os
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
    in_process_executor,
)
from dagster_dbt import DbtCliResource, dbt_assets

from coffee_cocoa_platform.catalog_metadata import (
    DBT_DIR,
    DBT_PROJECT,
    GovernedDbtTranslator,
    ensure_manifest,
    source_asset_description,
    source_asset_metadata,
    source_asset_owners,
)
from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.price_fixture import synthetic_workbook
from coffee_cocoa_platform.prices import SOURCE_URL, fetch_workbook, publish_workbook
from coffee_cocoa_platform.refresh import refresh_status
from coffee_cocoa_platform.runtime import (
    coordinated_run,
    local_writer,
    require_in_process,
    run_instance,
    stream_dbt,
)

BENCHMARK_DBT_SELECTION = (
    "+stg_benchmark_prices+ forecast_split_manifest+ forecast_capture_metadata+"
)


@asset(
    required_resource_keys={"writer"},
    check_specs=[
        AssetCheckSpec(
            "source_validated", asset=AssetKey(["world_bank_prices", "monthly_prices"])
        )
    ],
    key=AssetKey(["world_bank_prices", "monthly_prices"]),
    config_schema={"mode": str, "start": str, "end": str},
    description=source_asset_description("world_bank_prices", "monthly_prices"),
    owners=source_asset_owners("world_bank_prices", "monthly_prices"),
    metadata=source_asset_metadata("world_bank_prices", "monthly_prices"),
)
def monthly_prices(context: AssetExecutionContext):
    try:
        result = _capture(context)
    except Exception:
        yield AssetCheckResult(passed=False, check_name="source_validated")
        raise
    yield result


def _capture(context):
    require_in_process(context)
    config = context.op_config
    paths = ProjectPaths.from_root()
    paths.require_capture_root()
    if config["mode"] == "fixture":
        data = synthetic_workbook()
        effective_url = "synthetic://world-bank-monthly-price-fixture-v1"
        retrieved = datetime.now(UTC)
        content_type = None
    elif config["mode"] == "live":
        data, effective_url, retrieved, content_type = fetch_workbook()
    else:
        raise ValueError("mode must be fixture or live")
    manifest = publish_workbook(
        data,
        start=config["start"],
        end=config["end"],
        paths=paths,
        source_url=effective_url if config["mode"] == "fixture" else SOURCE_URL,
        effective_url=effective_url,
        retrieved_at=retrieved,
        fixture=config["mode"] == "fixture",
        content_type=content_type,
    )
    return MaterializeResult(
        check_results=[AssetCheckResult(passed=True, check_name="source_validated")],
        metadata={
            "refresh_status": MetadataValue.json(refresh_status(manifest)),
            "row_count": manifest["row_count"],
            "observed_row_count": manifest["observed_row_count"],
            "sha256": manifest["sha256"],
            "latest_observed_by_series": str(manifest["latest_observed_by_series"]),
            "parquet_path": manifest["parquet_path"],
            "manifest_path": str(paths.parquet / "benchmark_prices.json"),
            "source_capture_id": manifest["source_capture_id"],
            "source_url": manifest["source_url"],
            "retrieved_at_utc": manifest["retrieved_at_utc"],
        },
    )


@dbt_assets(
    required_resource_keys={"writer"},
    manifest=ensure_manifest(),
    project=DBT_PROJECT,
    select=BENCHMARK_DBT_SELECTION,
    dagster_dbt_translator=GovernedDbtTranslator(),
)
def benchmark_dbt(context: AssetExecutionContext):
    yield from stream_dbt(context, BENCHMARK_DBT_SELECTION)


defs = Definitions(
    executor=in_process_executor,
    assets=[monthly_prices, benchmark_dbt],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
        "writer": local_writer,
    },
)


@coordinated_run
def run_assets(mode: str, start: str, end: str, root: Path) -> bool:
    """Run the same Dagster graph used by the local development UI."""
    from dagster import materialize

    os.environ["COFFEE_COCOA_HOME"] = str(root.resolve())
    paths = ProjectPaths.from_root(root)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        paths.warehouse / "coffee_cocoa.duckdb"
    )
    with run_instance(root) as instance:
        result = materialize(
            instance=instance,
            assets=[monthly_prices, benchmark_dbt],
            resources={
                "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
                "writer": local_writer,
            },
            run_config={
                "ops": {
                    "world_bank_prices__monthly_prices": {
                        "config": {
                            "mode": mode,
                            "start": start,
                            "end": end,
                        }
                    }
                }
            },
        )
    return result.success
