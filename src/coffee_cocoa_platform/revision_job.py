"""Manual Dagster entry point for one explicitly bounded replacement request."""

from pathlib import Path

from dagster import Field, OpExecutionContext, job, op

from coffee_cocoa_platform.revisions import run_replacement


@op(
    config_schema={
        "root": str,
        "request": dict,
        "full_refresh": Field(bool, default_value=False),
    }
)
def replace_source_partitions(context: OpExecutionContext) -> dict:
    config = context.op_config
    result = run_replacement(
        Path(config["root"]), config["request"], full_refresh=config["full_refresh"]
    )
    context.add_output_metadata(
        {
            "run_id": result["run_id"],
            "warehouse": result["warehouse"],
            "snapshot_id": result["snapshot"]["snapshot_id"],
        }
    )
    return result


@job
def source_replacement_job():
    replace_source_partitions()
