"""Batched monthly selections into the checked replacement warehouse."""

import copy
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetDep,
    AssetKey,
    BackfillPolicy,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
)

from coffee_cocoa_platform.revisions import run_replacement

MONTHS = MonthlyPartitionsDefinition(start_date="2015-01", fmt="%Y-%m")


def select_months(request, start, end):
    """Intersect explicit source selections with one contiguous backfill range."""
    if request.get("schema_version") != "1":
        raise ValueError("A version 1 selection request is required")
    selected = {"schema_version": "1"}
    for source in ("prices", "trade"):
        if source not in request:
            continue
        scope = copy.deepcopy(request[source])
        scope["start"], scope["end"] = (
            max(start, scope["start"]),
            min(end, scope["end"]),
        )
        if scope["start"] <= scope["end"]:
            selected[source] = scope
    # Every claimed partition must be in an explicitly selected source scope.
    from coffee_cocoa_platform.trades import _months

    for month in _months(start, end):
        if not any(
            scope["start"] <= month <= scope["end"]
            for name, scope in selected.items()
            if name != "schema_version"
        ):
            raise ValueError(f"No explicit capture selection for partition {month}")
    return selected


@asset(
    partitions_def=MONTHS,
    backfill_policy=BackfillPolicy.single_run(),
    deps=[
        AssetDep(AssetKey(["world_bank_prices", "monthly_prices"])),
        AssetDep(AssetKey(["eurostat_trade", "monthly_trade"])),
    ],
    config_schema={"root": str, "request": dict},
    check_specs=[AssetCheckSpec("dbt_build_passed", asset="selected_warehouse")],
    description="Monthly source selections; pinned captures feed incremental staging and a complete checked dbt descendant rebuild.",
)
def selected_warehouse(context):
    bounds = context.partition_key_range
    request = select_months(context.op_config["request"], bounds.start, bounds.end)
    try:
        result = run_replacement(Path(context.op_config["root"]), request)
    except Exception:
        yield AssetCheckResult(passed=False, check_name="dbt_build_passed")
        raise
    yield MaterializeResult(
        metadata={
            "partition_start": bounds.start,
            "partition_end": bounds.end,
            "selected_sources": list(request.keys() - {"schema_version"}),
            "replacement_run_id": result["run_id"],
            "warehouse": result["warehouse"],
            "snapshot_id": result["snapshot"]["snapshot_id"],
            "dbt_build_log": str(Path(result["evidence"]) / "dbt-build.log"),
            "dependency_policy": "all dbt descendants rebuild from the complete selected union",
        },
        check_results=[AssetCheckResult(passed=True, check_name="dbt_build_passed")],
    )
