"""Combined local price and trade Dagster definitions."""

import os
from pathlib import Path

from dagster import Definitions, materialize
from dagster_dbt import DbtCliResource

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.price_assets import (
    DBT_DIR,
    benchmark_dbt,
    monthly_prices,
)
from coffee_cocoa_platform.trade_assets import monthly_trade, trade_dbt

defs = Definitions(
    assets=[monthly_prices, monthly_trade, benchmark_dbt, trade_dbt],
    resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
)


def run_fixture_assets(root: Path) -> bool:
    """Run both synthetic source assets and all current dbt descendants."""
    os.environ["COFFEE_COCOA_HOME"] = str(root.resolve())
    paths = ProjectPaths.from_root(root)
    paths.warehouse.mkdir(parents=True, exist_ok=True)
    os.environ["COFFEE_COCOA_DUCKDB_PATH"] = str(
        paths.warehouse / "coffee_cocoa.duckdb"
    )
    result = materialize(
        [monthly_prices, monthly_trade, benchmark_dbt, trade_dbt],
        resources={"dbt": DbtCliResource(project_dir=DBT_DIR, profiles_dir=DBT_DIR)},
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
