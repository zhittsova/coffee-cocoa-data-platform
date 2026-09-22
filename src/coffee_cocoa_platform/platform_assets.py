"""Combined local price and trade Dagster definitions."""

import os
from pathlib import Path

from dagster import Definitions, in_process_executor, materialize
from dagster_dbt import DbtCliResource

from coffee_cocoa_platform.backfill_assets import selected_warehouse
from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.price_assets import (
    DBT_DIR,
    benchmark_dbt,
    monthly_prices,
)
from coffee_cocoa_platform.revision_job import source_replacement_job
from coffee_cocoa_platform.runtime import coordinated_run, local_writer, run_instance
from coffee_cocoa_platform.schedules import fixture_job, fixture_refresh_schedule
from coffee_cocoa_platform.trade_assets import monthly_trade, trade_dbt

defs = Definitions(
    executor=in_process_executor,
    jobs=[source_replacement_job, fixture_job],
    schedules=[fixture_refresh_schedule],
    assets=[
        monthly_prices,
        monthly_trade,
        benchmark_dbt,
        trade_dbt,
        selected_warehouse,
    ],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
        "writer": local_writer,
    },
)


@coordinated_run
def run_fixture_assets(root: Path) -> bool:
    """Run both synthetic source assets and all current dbt descendants."""
    os.environ["COFFEE_COCOA_HOME"] = str(root.resolve())
    paths = ProjectPaths.from_root(root)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        paths.warehouse / "coffee_cocoa.duckdb"
    )
    with run_instance(root) as instance:
        result = materialize(
            instance=instance,
            assets=[monthly_prices, monthly_trade, benchmark_dbt, trade_dbt],
            resources={
                "dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR),
                "writer": local_writer,
            },
            run_config={
                "ops": {
                    "world_bank_prices__monthly_prices": {
                        "config": {
                            "mode": "fixture",
                            "start": "2024-01",
                            "end": "2024-04",
                        }
                    },
                    "eurostat_trade__monthly_trade": {
                        "config": {
                            "mode": "fixture",
                            "profile": "smoke",
                            "start": "2021-12",
                            "end": "2022-01",
                        }
                    },
                }
            },
        )
    return result.success
