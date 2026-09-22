"""Explicitly enabled offline scheduling rehearsal; no unattended live access."""

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    SkipReason,
    define_asset_job,
    in_process_executor,
    schedule,
)

fixture_job = define_asset_job(
    "fixture_refresh",
    selection=AssetSelection.all() - AssetSelection.assets("selected_warehouse"),
    executor_def=in_process_executor,
    config={
        "ops": {
            "world_bank_prices__monthly_prices": {
                "config": {"mode": "fixture", "start": "2024-01", "end": "2024-04"}
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


@schedule(
    job=fixture_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
)
def fixture_refresh_schedule(context):
    if os.environ.get("COFFEE_COCOA_ENABLE_FIXTURE_SCHEDULE") != "1":
        return SkipReason(
            "Set COFFEE_COCOA_ENABLE_FIXTURE_SCHEDULE=1 and explicitly start the schedule"
        )
    return RunRequest(
        run_key=context.scheduled_execution_time.isoformat(),
        tags={"source_mode": "fixture"},
    )
