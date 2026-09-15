"""Dagster's source-to-dbt Eurostat trade asset graph."""

import os
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
from coffee_cocoa_platform.trade_fixture import fixture_profile, fixture_transport
from coffee_cocoa_platform.trades import make_profile, publish_trade_profile

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
MANIFEST = DBT_DIR / "manifest.json"
DBT_PROJECT = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)


@asset(
    key=AssetKey(["eurostat_trade", "monthly_trade"]),
    config_schema={"mode": str, "profile": str, "start": str, "end": str},
    description="Complete a bounded Eurostat plan and publish typed trade Parquet.",
)
def monthly_trade(context: AssetExecutionContext) -> MaterializeResult:
    config = context.op_config
    paths = ProjectPaths.from_root()
    if config["mode"] == "fixture":
        profile = fixture_profile()
        manifest = publish_trade_profile(
            profile, paths, fixture_transport, fixture=True
        )
    elif config["mode"] == "live":
        profile = make_profile(config["profile"], config["start"], config["end"])
        manifest = publish_trade_profile(profile, paths)
    else:
        raise ValueError("mode must be fixture or live")
    return MaterializeResult(
        metadata={
            "profile": manifest["profile"],
            "row_count": manifest["row_count"],
            "slice_count": manifest["slice_count"],
            "total_payload_bytes": manifest["total_payload_bytes"],
            "transport_complete": manifest["transport_complete"],
            "parquet_path": manifest["parquet_path"],
        }
    )


@dbt_assets(manifest=MANIFEST, project=DBT_PROJECT, select="+stg_trade_observations")
def trade_dbt(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[monthly_trade, trade_dbt],
    resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
)


def run_trade_assets(
    mode: str,
    profile_name: str,
    start: str | None,
    end: str | None,
    root: Path,
) -> bool:
    """Run the Eurostat source and its dbt seed/staging dependencies."""
    from dagster import materialize

    if mode == "fixture":
        start, end = "2021-12", "2022-01"
    elif start is None or end is None:
        raise ValueError("Live trade assets require explicit start and end months")
    os.environ["COFFEE_COCOA_HOME"] = str(root.resolve())
    paths = ProjectPaths.from_root(root)
    paths.warehouse.mkdir(parents=True, exist_ok=True)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        paths.warehouse / "coffee_cocoa.duckdb"
    )
    result = materialize(
        [monthly_trade, trade_dbt],
        resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
        run_config={
            "ops": {
                "eurostat_trade__monthly_trade": {
                    "config": {
                        "mode": mode,
                        "profile": profile_name,
                        "start": start,
                        "end": end,
                    }
                }
            }
        },
    )
    return result.success
