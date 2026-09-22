"""Dagster publication boundary for a complete tested local warehouse."""

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.snapshots import publish_snapshot


@asset(
    required_resource_keys={"writer"},
    deps=[
        AssetKey("monthly_benchmark_dynamics"),
        AssetKey("monthly_trade_product_metrics"),
    ],
    description="Immutable read snapshot published after the complete fixture dbt build and tests pass.",
)
def warehouse_snapshot(context: AssetExecutionContext):
    paths = ProjectPaths.from_root()
    result = publish_snapshot(
        paths.root,
        {"kind": "dagster_fixture_pipeline", "run_id": context.run.run_id},
    )
    return MaterializeResult(
        metadata={
            "snapshot_id": result["snapshot_id"],
            "generation": result["generation"],
            "warehouse_sha256": result["warehouse"]["sha256"],
            "source_versions": result["source_versions"],
            "retention": result["retention"],
        }
    )
