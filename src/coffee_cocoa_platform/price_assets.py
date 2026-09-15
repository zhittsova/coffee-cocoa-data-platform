"""Dagster's source-to-dbt benchmark asset graph."""

import os
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    MaterializeResult,
    asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.price_fixture import synthetic_workbook
from coffee_cocoa_platform.prices import SOURCE_URL, fetch_workbook, publish_workbook

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
MANIFEST = DBT_DIR / "manifest.json"
DBT_PROJECT = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)


@asset(
    key=AssetKey(["world_bank_prices", "monthly_prices"]),
    config_schema={"mode": str, "start": str, "end": str},
    description="Validate one bounded workbook capture and publish typed monthly Parquet.",
)
def monthly_prices(context: AssetExecutionContext) -> MaterializeResult:
    config = context.op_config
    paths = ProjectPaths.from_root()
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
        metadata={
            "row_count": manifest["row_count"],
            "observed_row_count": manifest["observed_row_count"],
            "sha256": manifest["sha256"],
            "latest_observed_by_series": str(manifest["latest_observed_by_series"]),
            "parquet_path": manifest["parquet_path"],
        }
    )


@dbt_assets(manifest=MANIFEST, project=DBT_PROJECT, select="+stg_benchmark_prices+")
def benchmark_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[monthly_prices, benchmark_dbt],
    resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
)


def run_assets(mode: str, start: str, end: str, root: Path) -> bool:
    """Run the same Dagster graph used by the local development UI."""
    from dagster import materialize

    os.environ["COFFEE_COCOA_HOME"] = str(root.resolve())
    paths = ProjectPaths.from_root(root)
    paths.warehouse.mkdir(parents=True, exist_ok=True)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        paths.warehouse / "coffee_cocoa.duckdb"
    )
    result = materialize(
        [monthly_prices, benchmark_dbt],
        resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
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
