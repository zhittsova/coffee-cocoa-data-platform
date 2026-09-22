"""Project-owned metadata shared by dbt docs and Dagster assets."""

import os
import subprocess
import sys
import tempfile
from functools import lru_cache
from pathlib import Path

import yaml
from dagster import TableColumn, TableSchema
from dagster_dbt import DagsterDbtTranslator, DbtProject

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
DBT_PROJECT = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
SOURCE_DEFINITIONS = DBT_DIR / "models" / "staging" / "_sources.yml"


@lru_cache(maxsize=1)
def ensure_manifest() -> Path:
    """Parse current versioned definitions before Dagster loads the asset graph."""
    target = Path(tempfile.mkdtemp(prefix="coffee-cocoa-definitions-"))
    command = [
        str(Path(sys.executable).with_name("dbt")),
        "parse",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--no-partial-parse",
        "--target-path",
        str(target),
        "--log-path",
        str(target),
    ]
    subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
        env=dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false"),
    )
    return target / "manifest.json"


def source_table_definition(source_name: str, table_name: str) -> dict:
    definitions = yaml.safe_load(SOURCE_DEFINITIONS.read_text())
    for source in definitions["sources"]:
        if source["name"] == source_name:
            for table in source["tables"]:
                if table["name"] == table_name:
                    return table
    raise ValueError(f"Unknown dbt source: {source_name}.{table_name}")


def source_asset_owners(source_name: str, table_name: str) -> list[str]:
    table = source_table_definition(source_name, table_name)
    return [f"team:{table['config']['meta']['owner']}"]


def source_asset_description(source_name: str, table_name: str) -> str:
    return source_table_definition(source_name, table_name)["description"]


def source_asset_metadata(source_name: str, table_name: str) -> dict:
    table = source_table_definition(source_name, table_name)
    meta = table["config"]["meta"]
    schema = TableSchema(
        columns=[
            TableColumn(
                name=column["name"],
                type=column["data_type"],
                description=column["description"],
                tags={"unit": column["meta"]["unit"]},
            )
            for column in table["columns"]
        ]
    )
    return {**meta, "dagster/column_schema": schema}


class GovernedDbtTranslator(DagsterDbtTranslator):
    """Expose the dbt-owned metadata on Dagster's dbt asset pages."""

    def get_metadata(self, dbt_resource_props):
        metadata = dict(super().get_metadata(dbt_resource_props))
        metadata.update(dbt_resource_props.get("config", {}).get("meta", {}))
        columns = dbt_resource_props.get("columns", {})
        if columns:
            metadata["dagster/column_schema"] = TableSchema(
                columns=[
                    TableColumn(
                        name=name,
                        type=column["data_type"],
                        description=column.get("description"),
                        tags={"unit": column.get("meta", {}).get("unit", "none")},
                    )
                    for name, column in columns.items()
                ]
            )
        return metadata

    def get_owners(self, dbt_resource_props):
        owner = dbt_resource_props.get("config", {}).get("meta", {}).get("owner")
        return [f"team:{owner}"] if owner else None
