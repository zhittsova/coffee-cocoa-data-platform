"""Dagster evaluation and dbt result publication assets."""

import json
import os
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Definitions,
    Output,
    SourceAsset,
    asset,
    in_process_executor,
    materialize,
    multi_asset,
)
from dagster_dbt import DbtCliResource, dbt_assets

from coffee_cocoa_platform.catalog_metadata import (
    DBT_DIR,
    DBT_PROJECT,
    GovernedDbtTranslator,
    ensure_manifest,
)
from coffee_cocoa_platform.forecast import publish
from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.runtime import (
    coordinated_run,
    local_writer,
    require_in_process,
    run_dbt,
    run_instance,
    stream_dbt,
)
from coffee_cocoa_platform.snapshots import publish_snapshot

RESULT_SELECTION = "forecast_predictions+ forecast_metrics+"


@multi_asset(
    outs={
        "predictions": AssetOut(key=AssetKey(["forecast_results", "predictions"])),
        "metrics": AssetOut(key=AssetKey(["forecast_results", "metrics"])),
    },
    deps=[AssetKey("warehouse_snapshot")],
    required_resource_keys={"writer"},
    description="Versioned retrospective cocoa evaluation from one immutable input snapshot.",
)
def forecast_results(context: AssetExecutionContext):
    require_in_process(context)
    result = publish(ProjectPaths.from_root().root)
    for name in ("predictions", "metrics"):
        yield Output(
            None,
            output_name=name,
            metadata={
                "run_id": result["run_id"],
                "input_snapshot_id": result["input_snapshot_id"],
                "selected_models": str(result["selected_models"]),
            },
        )


@dbt_assets(
    required_resource_keys={"writer"},
    manifest=ensure_manifest(),
    project=DBT_PROJECT,
    select=RESULT_SELECTION,
    dagster_dbt_translator=GovernedDbtTranslator(),
)
def forecast_dbt(context: AssetExecutionContext):
    yield from stream_dbt(context, RESULT_SELECTION)


@asset(
    deps=[AssetKey("forecast_predictions"), AssetKey("forecast_metrics")],
    required_resource_keys={"writer"},
    description="Consistent read snapshot after forecast result marts and tests pass.",
)
def forecast_read_snapshot(context: AssetExecutionContext):
    root = ProjectPaths.from_root().root
    check = run_dbt(
        root,
        [
            "test",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--select",
            "forecast_result_invariants",
            "--target-path",
            str(root / ".state" / "forecast-dbt-target"),
            "--log-path",
            str(root / ".state" / "forecast-dbt-log"),
        ],
    )
    if check.returncode:
        raise RuntimeError(
            f"Forecast result invariant test failed: {check.stdout[-4000:]}"
        )
    context.log.info("Forecast result invariant test passed")
    forecast_run_id = json.loads(
        (root / "data" / "parquet" / "forecast_results.json").read_text()
    )["run_id"]
    result = publish_snapshot(
        root,
        {
            "kind": "forecast_evaluation",
            "run_id": context.run.run_id,
            "forecast_run_id": forecast_run_id,
        },
    )
    return {"snapshot_id": result["snapshot_id"], "generation": result["generation"]}


defs = Definitions(
    executor=in_process_executor,
    assets=[
        forecast_results,
        forecast_dbt,
        forecast_read_snapshot,
        SourceAsset(AssetKey("warehouse_snapshot")),
    ],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
        "writer": local_writer,
    },
)


@coordinated_run
def run_forecast_assets(root: Path) -> bool:
    """Run forecast publication against a previously tested complete snapshot."""
    root = ProjectPaths.from_root(root).root
    os.environ["COFFEE_COCOA_HOME"] = str(root)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        root / "warehouse" / "coffee_cocoa.duckdb"
    )
    with run_instance(root) as instance:
        result = materialize(
            assets=[
                forecast_results,
                forecast_dbt,
                forecast_read_snapshot,
                SourceAsset(AssetKey("warehouse_snapshot")),
            ],
            instance=instance,
            resources={
                "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
                "writer": local_writer,
            },
        )
    return result.success
