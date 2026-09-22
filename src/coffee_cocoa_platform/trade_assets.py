"""Dagster's source-to-dbt Eurostat trade asset graph."""

import os
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    Definitions,
    Field,
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
from coffee_cocoa_platform.refresh import refresh_status
from coffee_cocoa_platform.runtime import (
    coordinated_run,
    local_writer,
    require_in_process,
    run_instance,
    stream_dbt,
)
from coffee_cocoa_platform.trade_fixture import fixture_profile, fixture_transport
from coffee_cocoa_platform.trades import make_profile, publish_trade_profile


@asset(
    required_resource_keys={"writer"},
    check_specs=[
        AssetCheckSpec(
            "source_validated", asset=AssetKey(["eurostat_trade", "monthly_trade"])
        )
    ],
    key=AssetKey(["eurostat_trade", "monthly_trade"]),
    config_schema={
        "mode": str,
        "profile": str,
        "start": str,
        "end": str,
        "capture_vintage": Field(str, default_value="initial"),
    },
    description=source_asset_description("eurostat_trade", "monthly_trade"),
    owners=source_asset_owners("eurostat_trade", "monthly_trade"),
    metadata=source_asset_metadata("eurostat_trade", "monthly_trade"),
)
def monthly_trade(context: AssetExecutionContext):
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
        profile = fixture_profile()
        manifest = publish_trade_profile(
            profile,
            paths,
            fixture_transport,
            fixture=True,
            capture_vintage=config["capture_vintage"],
        )
    elif config["mode"] == "live":
        profile = make_profile(config["profile"], config["start"], config["end"])
        manifest = publish_trade_profile(
            profile, paths, capture_vintage=config["capture_vintage"]
        )
    else:
        raise ValueError("mode must be fixture or live")
    return MaterializeResult(
        check_results=[AssetCheckResult(passed=True, check_name="source_validated")],
        metadata={
            "refresh_status": MetadataValue.json(refresh_status(manifest)),
            "profile": manifest["profile"],
            "row_count": manifest["row_count"],
            "slice_count": manifest["slice_count"],
            "total_payload_bytes": manifest["total_payload_bytes"],
            "transport_complete": manifest["transport_complete"],
            "parquet_path": manifest["parquet_path"],
            "manifest_path": str(paths.parquet / "trade_observations.json"),
            "plan_hash": manifest["plan_hash"],
            "capture_vintage": manifest["capture_vintage"],
            "source_capture_ids": [
                item["capture"]["sha256"] for item in manifest["slices"]
            ],
        },
    )


@dbt_assets(
    required_resource_keys={"writer"},
    manifest=ensure_manifest(),
    project=DBT_PROJECT,
    select="+stg_trade_observations+",
    dagster_dbt_translator=GovernedDbtTranslator(),
)
def trade_dbt(context: AssetExecutionContext):
    yield from stream_dbt(context, "+stg_trade_observations+")


defs = Definitions(
    executor=in_process_executor,
    assets=[monthly_trade, trade_dbt],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
        "writer": local_writer,
    },
)


@coordinated_run
def run_trade_assets(
    mode: str,
    profile_name: str,
    start: str | None,
    end: str | None,
    root: Path,
    capture_vintage: str = "initial",
) -> bool:
    """Run the Eurostat source and its dbt seed/staging dependencies."""
    from dagster import materialize

    if mode == "fixture":
        start, end = "2021-12", "2022-01"
    elif start is None or end is None:
        raise ValueError("Live trade assets require explicit start and end months")
    os.environ["COFFEE_COCOA_HOME"] = str(root.resolve())
    paths = ProjectPaths.from_root(root)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        paths.warehouse / "coffee_cocoa.duckdb"
    )
    with run_instance(root) as instance:
        result = materialize(
            instance=instance,
            assets=[monthly_trade, trade_dbt],
            resources={
                "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
                "writer": local_writer,
            },
            run_config={
                "ops": {
                    "eurostat_trade__monthly_trade": {
                        "config": {
                            "mode": mode,
                            "profile": profile_name,
                            "start": start,
                            "end": end,
                            "capture_vintage": capture_vintage,
                        }
                    }
                }
            },
        )
    return result.success
