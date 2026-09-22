"""Generate and validate the local dbt catalog against a selected warehouse."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

from coffee_cocoa_platform.catalog_metadata import DBT_DIR

REQUIRED_META = (
    "owner",
    "grain",
    "origin",
    "license",
    "classification",
    "refresh_expectation",
    "provenance",
    "origin_url",
    "license_url",
)
SOURCE_FILES = {
    "monthly_prices": ("benchmark_prices.parquet", "_selected_prices"),
    "monthly_trade": ("trade_observations.parquet", "_selected_trade"),
}


def _schema(connection: duckdb.DuckDBPyConnection, query: str, args=()) -> dict:
    return {
        row[0].lower(): row[1].lower().replace(" ", "")
        for row in connection.execute(f"describe {query}", args).fetchall()
    }


def definition_errors(manifest: dict) -> list[str]:
    """Check versioned discovery declarations without opening a warehouse."""
    errors = []
    resources = {
        **{
            key: value
            for key, value in manifest["nodes"].items()
            if value["resource_type"] in {"model", "seed"}
        },
        **manifest["sources"],
    }
    for node in resources.values():
        name = node["name"]
        if not node.get("description"):
            errors.append(f"{name}: missing purpose")
        meta = node.get("config", {}).get("meta", {})
        for field in REQUIRED_META:
            if not meta.get(field):
                errors.append(f"{name}: missing {field}")
        if not node.get("columns"):
            errors.append(f"{name}: missing column schema")
        for column_name, column in node.get("columns", {}).items():
            if not column.get("description"):
                errors.append(f"{name}.{column_name}: missing description")
            if not column.get("data_type"):
                errors.append(f"{name}.{column_name}: missing data_type")
            if not column.get("meta", {}).get("unit"):
                errors.append(f"{name}.{column_name}: missing unit")
        if (
            node["resource_type"] == "model"
            and not node["config"]["contract"]["enforced"]
        ):
            errors.append(f"{name}: model contract is not enforced")
    return errors


def metadata_errors(manifest: dict, catalog: dict, root: Path) -> list[str]:
    """Check discovery fields and exact declared/physical names and types."""
    errors = definition_errors(manifest)
    warehouse = root / "warehouse" / "coffee_cocoa.duckdb"
    if not warehouse.is_file():
        return [f"missing selected warehouse: {warehouse}"]
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        relations = {
            row[0]
            for row in connection.execute(
                "select table_name from information_schema.tables where table_schema = 'main'"
            ).fetchall()
        }
        resources = {
            **{
                key: value
                for key, value in manifest["nodes"].items()
                if value["resource_type"] in {"model", "seed"}
            },
            **manifest["sources"],
        }
        for unique_id, node in resources.items():
            name = node["name"]
            declared = {}
            for column_name, column in node.get("columns", {}).items():
                declared[column_name.lower()] = (
                    (column.get("data_type") or "").lower().replace(" ", "")
                )
            if node["resource_type"] == "source":
                filename, selected_table = SOURCE_FILES[name]
                parquet = root / "data" / "parquet" / filename
                if parquet.is_file():
                    actual = _schema(
                        connection, "select * from read_parquet(?)", [str(parquet)]
                    )
                elif selected_table in relations:
                    actual = _schema(connection, f'select * from "{selected_table}"')
                else:
                    errors.append(
                        f"{name}: no selected source Parquet or revision table"
                    )
                    continue
                catalog_node = catalog.get("sources", {}).get(unique_id)
                if not catalog_node:
                    errors.append(f"{name}: missing from generated catalog")
                elif {
                    key.lower(): value["type"].lower().replace(" ", "")
                    for key, value in catalog_node.get("columns", {}).items()
                } != actual:
                    errors.append(
                        f"{name}: generated catalog differs from source schema"
                    )
            elif name in relations:
                actual = _schema(connection, f'select * from "{name}"')
                catalog_node = catalog.get("nodes", {}).get(unique_id)
                if not catalog_node:
                    errors.append(f"{name}: missing from generated catalog")
                else:
                    catalog_schema = {
                        column_name.lower(): column["type"].lower().replace(" ", "")
                        for column_name, column in catalog_node.get(
                            "columns", {}
                        ).items()
                    }
                    if catalog_schema != actual:
                        errors.append(
                            f"{name}: generated catalog differs from warehouse"
                        )
            else:
                errors.append(f"{name}: missing from selected warehouse")
                continue
            if declared != actual:
                missing = sorted(actual.keys() - declared.keys())
                extra = sorted(declared.keys() - actual.keys())
                wrong = sorted(
                    col
                    for col in actual.keys() & declared.keys()
                    if actual[col] != declared[col]
                )
                errors.append(
                    f"{name}: schema mismatch: undocumented={missing}, absent={extra}, wrong_types={wrong}"
                )
    return errors


def add_external_source_columns(manifest: dict, catalog: dict, root: Path) -> None:
    """dbt-duckdb omits Parquet external sources from catalog.json introspection."""
    warehouse = root / "warehouse" / "coffee_cocoa.duckdb"
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        relations = {row[0] for row in connection.execute("show tables").fetchall()}
        for unique_id, node in manifest["sources"].items():
            filename, selected_table = SOURCE_FILES[node["name"]]
            parquet = root / "data" / "parquet" / filename
            if parquet.is_file():
                physical = _schema(
                    connection, "select * from read_parquet(?)", [str(parquet)]
                )
            elif selected_table in relations:
                physical = _schema(connection, f'select * from "{selected_table}"')
            else:
                continue
            catalog["sources"][unique_id] = {
                "metadata": {
                    "type": "EXTERNAL",
                    "schema": node["source_name"],
                    "name": node["name"],
                    "database": node["database"],
                    "comment": None,
                    "owner": node["config"]["meta"]["owner"],
                },
                "columns": {
                    name: {
                        "type": data_type.upper(),
                        "index": index,
                        "name": name,
                        "comment": None,
                    }
                    for index, (name, data_type) in enumerate(physical.items(), 1)
                },
                "stats": {},
                "unique_id": unique_id,
            }


def provenance_index(root: Path) -> tuple[dict, list[str]]:
    """Resolve fact capture identities to their local source and run versions."""
    warehouse = root / "warehouse" / "coffee_cocoa.duckdb"
    errors = []
    index = {
        "selected_warehouse": str(warehouse),
        "latest_run_id": None,
        "captures": {},
    }
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        relations = {row[0] for row in connection.execute("show tables").fetchall()}
        fact_ids = {
            "prices": {
                row[0]
                for row in connection.execute(
                    "select distinct source_capture_id from fct_benchmark_prices"
                ).fetchall()
            },
            "trade": {
                row[0]
                for row in connection.execute(
                    "select distinct source_capture_id from fct_trade_observations"
                ).fetchall()
            },
        }
        if "_revision_state" in relations:
            registry = json.loads(
                connection.execute("select captures from _revision_state").fetchone()[0]
            )
            latest = connection.execute(
                "select event from _revision_history order by sequence desc limit 1"
            ).fetchone()
            index["latest_run_id"] = json.loads(latest[0])["run_id"] if latest else None
            for dataset, identities in fact_ids.items():
                for capture_id in sorted(identities):
                    record = registry.get(capture_id)
                    if not record:
                        errors.append(
                            f"{dataset}: capture {capture_id} absent from revision registry"
                        )
                        continue
                    manifest_path = (
                        root
                        / ".state"
                        / "capture_manifests"
                        / f"{record['manifest_id']}.json"
                    )
                    if not manifest_path.is_file():
                        errors.append(
                            f"{dataset}: missing archived manifest {record['manifest_id']}"
                        )
                        continue
                    manifest_bytes = manifest_path.read_bytes()
                    if (
                        hashlib.sha256(manifest_bytes).hexdigest()
                        != record["manifest_id"]
                    ):
                        errors.append(f"{dataset}: archived manifest checksum mismatch")
                        continue
                    source = json.loads(manifest_bytes)
                    if dataset == "prices":
                        matches = [source] if source.get("sha256") == capture_id else []
                    else:
                        matches = [
                            item["capture"]
                            for item in source["slices"]
                            if item["capture"]["sha256"] == capture_id
                        ]
                    if not matches:
                        errors.append(
                            f"{dataset}: capture absent from archived manifest"
                        )
                        continue
                    index["captures"][capture_id] = {
                        "dataset": dataset,
                        "manifest_path": str(manifest_path),
                        "manifest_sha256": record["manifest_id"],
                        "retrieved_at_utc": record["retrieved_at_utc"],
                        "source_update": record["source_update"],
                        "source_url": matches[0].get("effective_url")
                        or source.get("effective_url"),
                    }
            return index, errors
        price_path = root / "data" / "parquet" / "benchmark_prices.json"
        trade_path = root / "data" / "parquet" / "trade_observations.json"
        if not price_path.is_file() or not trade_path.is_file():
            return index, ["missing source capture manifests"]
        price = json.loads(price_path.read_text())
        trade = json.loads(trade_path.read_text())
        known = {
            "prices": {price["sha256"]: (price, price_path)},
            "trade": {
                item["capture"]["sha256"]: (item["capture"], trade_path)
                for item in trade["slices"]
            },
        }
        for dataset, identities in fact_ids.items():
            for capture_id in sorted(identities):
                if capture_id not in known[dataset]:
                    errors.append(
                        f"{dataset}: capture {capture_id} absent from source manifest"
                    )
                    continue
                capture, path = known[dataset][capture_id]
                index["captures"][capture_id] = {
                    "dataset": dataset,
                    "manifest_path": str(path),
                    "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "retrieved_at_utc": capture["retrieved_at_utc"],
                    "source_update": capture.get("source_update_time")
                    or capture.get("source_update")
                    or (
                        price.get("source_update_text")
                        if dataset == "prices"
                        else trade["source_updates"][0]
                    ),
                    "source_url": capture.get("effective_url"),
                }
    return index, errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, required=True, help="Selected local pipeline root"
    )
    parser.add_argument("--target-dir", type=Path, default=DBT_DIR / "target")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    target = args.target_dir.resolve()
    warehouse = root / "warehouse" / "coffee_cocoa.duckdb"
    if not warehouse.is_file():
        parser.error(f"Selected warehouse does not exist: {warehouse}")
    if not args.check_only:
        command = [
            str(Path(sys.executable).with_name("dbt")),
            "docs",
            "generate",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target-path",
            str(target),
            "--no-partial-parse",
        ]
        environment = dict(
            os.environ,
            COFFEE_COCOA_HOME=str(root),
            COFFEE_COCOA_DUCKDB_PATH=str(warehouse),
        )
        subprocess.run(command, env=environment, check=True)
    manifest = json.loads((target / "manifest.json").read_text())
    catalog = json.loads((target / "catalog.json").read_text())
    if not args.check_only:
        add_external_source_columns(manifest, catalog, root)
        (target / "catalog.json").write_text(json.dumps(catalog, indent=2) + "\n")
    provenance, provenance_issues = provenance_index(root)
    errors = metadata_errors(manifest, catalog, root) + provenance_issues
    for artifact in ("index.html", "manifest.json", "catalog.json"):
        if not (target / artifact).is_file():
            errors.append(f"missing generated {artifact}")
    provenance_path = target / "provenance.json"
    if args.check_only:
        if not provenance_path.is_file():
            errors.append("missing generated provenance.json")
        elif json.loads(provenance_path.read_text()) != provenance:
            errors.append("generated provenance index differs from selected warehouse")
    for text in (
        "specs/",
        "docs/working-agreements",
        ".agents/",
        ".codex/",
        "CLAUDE.md",
    ):
        if text in json.dumps(manifest) or text in json.dumps(catalog):
            errors.append(f"private path copied into generated catalog: {text}")
    if errors:
        raise SystemExit("\n".join(errors))
    if not args.check_only:
        provenance_path.write_text(json.dumps(provenance, indent=2) + "\n")
    print(f"Catalog valid: {target / 'index.html'}")
    print(
        f"Resources: {sum(n['resource_type'] in {'model', 'seed'} for n in manifest['nodes'].values())} models/seeds, {len(manifest['sources'])} sources"
    )
    print("Price and trade capture identities resolve to local source versions")
    print(f"Run provenance: {provenance_path}")


if __name__ == "__main__":
    main()
