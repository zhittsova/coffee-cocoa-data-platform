"""Explicit source selections, isolated dbt builds and atomic warehouse replacement."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.prices import PRICE_SCHEMA, SERIES, parse_workbook
from coffee_cocoa_platform.prices import _month as price_month
from coffee_cocoa_platform.runtime import run_dbt, writer_lock
from coffee_cocoa_platform.trades import (
    FLOWS,
    PRODUCT_GROUPS,
    PRODUCTS,
    SUPPORTED_PARTNERS,
    TRADE_SCHEMA,
    TradeSlice,
    _months,
    combine_trade_tables,
    parse_trade_response,
)

SCHEMAS = {"prices": PRICE_SCHEMA, "trade": TRADE_SCHEMA}
FILENAMES = {"prices": "benchmark_prices", "trade": "trade_observations"}
DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"


class RevisionError(ValueError):
    """A selection or candidate cannot be published."""


def _retain_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != data:
            raise RevisionError("Retained capture checksum mismatch")
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(data)
        staged = Path(handle.name)
    try:
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)


def _checked_raw(path: Path, checksum: str, paths: ProjectPaths) -> bytes:
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != checksum:
        raise RevisionError("Raw capture checksum mismatch")
    destination = paths.raw / (checksum + path.suffix)
    _retain_bytes(destination, data)
    return data


def _choices(values, supported, label):
    if not isinstance(values, list) or not values or len(set(values)) != len(values):
        raise RevisionError(f"Explicit nonempty unique {label} required")
    if set(values) - set(supported):
        raise RevisionError(f"Unsupported {label}")
    return sorted(values)


def _matches(row: dict, scope: dict) -> bool:
    month = row["period_month"].isoformat()[:7]
    return scope["start"] <= month <= scope["end"] and all(
        row[key] in values for key, values in scope["keys"].items()
    )


def read_selection(dataset: str, selection: dict, paths: ProjectPaths):
    """Reparse immutable raw bytes; never trust a mutable normalized source file."""
    manifest_path = Path(selection["manifest"]).expanduser().resolve()
    manifest_bytes = manifest_path.read_bytes()
    manifest_id = hashlib.sha256(manifest_bytes).hexdigest()
    if selection.get("manifest_sha256") != manifest_id:
        raise RevisionError(
            "Manifest checksum mismatch; explicitly select the capture version"
        )
    manifest = json.loads(manifest_bytes)
    if manifest.get("schema_version") != "1":
        raise RevisionError("Incompatible source schema version")
    archive = paths.state / "capture_manifests"
    _retain_bytes(archive / f"{manifest_id}.json", manifest_bytes)
    start, end = selection["start"], selection["end"]
    if price_month(start) > price_month(end):
        raise RevisionError("Replacement bounds must be ordered")
    events = []
    if dataset == "prices":
        series = _choices(selection["series"], SERIES.values(), "benchmark series")
        if start < manifest["input_start"] or end > manifest["input_end"]:
            raise RevisionError("Price selection exceeds captured bounds")
        checksum = manifest["sha256"]
        data = _checked_raw(Path(manifest["raw_path"]), checksum, paths)
        table, coverage = parse_workbook(
            data, manifest["input_start"], manifest["input_end"], checksum
        )
        scopes = [{"start": start, "end": end, "keys": {"series_id": series}}]
        events.append(
            {
                "capture_id": checksum,
                "retrieved_at_utc": manifest["retrieved_at_utc"],
                "source_update": coverage["source_update_text"],
            }
        )
    else:
        months = _months(start, end)
        groups = selection.get("product_groups", [])
        if groups:
            groups = _choices(groups, PRODUCT_GROUPS, "product groups")
        products = selection.get("products", []) + [
            product for group in groups for product in PRODUCT_GROUPS[group]
        ]
        products = _choices(products, PRODUCTS, "products")
        flows = _choices(selection["flows"], FLOWS, "flows")
        if selection.get("reporter") != "DE":
            raise RevisionError("Only reporter DE is supported")
        if manifest.get("transport_complete") is not True:
            raise RevisionError("Incomplete capture cannot replace a partition")
        records = manifest["slices"]
        if not records or len(records) != manifest["slice_count"]:
            raise RevisionError("Missing capture slices")
        tables, scopes, covered = [], [], set()
        for record in records:
            request = TradeSlice(**record["request"])
            capture = record["capture"]
            if not capture.get("accepted"):
                raise RevisionError("Unaccepted capture slice")
            checksum = capture["sha256"]
            raw = (
                manifest_path.parent.parent / "raw" / f"eurostat_trade_{checksum}.json"
            )
            data = _checked_raw(raw, checksum, paths)
            table, coverage = parse_trade_response(data, request)
            selected_products = sorted(set(products) & set(request.products))
            selected_months = sorted(
                set(months) & set(_months(request.start, request.end))
            )
            if not selected_products or not selected_months:
                continue
            partners = sorted(request.partners or SUPPORTED_PARTNERS)
            if set(partners) - SUPPORTED_PARTNERS:
                raise RevisionError("Unsupported captured partner universe")
            scope = {
                "start": selected_months[0],
                "end": selected_months[-1],
                "keys": {
                    "reporter_code": ["DE"],
                    "product_code": selected_products,
                    "flow_code": flows,
                    "partner_code": partners,
                },
            }
            for month in selected_months:
                for product in selected_products:
                    key = (month, product)
                    if key in covered:
                        raise RevisionError("Overlapping capture slices")
                    covered.add(key)
            scopes.append(scope)
            tables.append(table)
            events.append(
                {
                    "capture_id": checksum,
                    "retrieved_at_utc": capture["retrieved_at_utc"],
                    "source_update": coverage["source_update_time"],
                }
            )
        if covered != {(month, product) for month in months for product in products}:
            raise RevisionError(
                "Selection exceeds complete captured product/month scope"
            )
        if len({event["source_update"] for event in events}) != 1:
            raise RevisionError("Selected slices have mixed source updates")
        table = combine_trade_tables(tables)
    for event in events:
        event["manifest_id"] = manifest_id
        retrieved = datetime.fromisoformat(event["retrieved_at_utc"])
        if retrieved.tzinfo is None:
            raise RevisionError("Capture retrieval time requires a timezone")
        event["retrieved_at_utc"] = retrieved.astimezone(UTC).isoformat()
    selected = [
        row for row in table.to_pylist() if any(_matches(row, s) for s in scopes)
    ]
    return pa.Table.from_pylist(selected, SCHEMAS[dataset]), scopes, events


def replace_rows(
    previous: pa.Table, incoming: pa.Table, scopes: list[dict]
) -> pa.Table:
    """Replace the declared scope, including keys absent from the new response."""
    if not previous.schema.equals(incoming.schema, check_metadata=False):
        raise RevisionError("Incompatible normalized schema")
    retained = [
        row
        for row in previous.to_pylist()
        if not any(_matches(row, scope) for scope in scopes)
    ]
    combined = pa.concat_tables(
        [pa.Table.from_pylist(retained, previous.schema), incoming]
    )
    if incoming.schema == TRADE_SCHEMA:
        combine_trade_tables([combined])
    return combined


def _record_capture(captures: dict, event: dict, selected_at: str) -> None:
    capture_id = event["capture_id"]
    if capture_id not in captures:
        captures[capture_id] = {
            **event,
            "first_seen_capture_at_utc": event["retrieved_at_utc"],
            "first_selected_at_utc": selected_at,
        }
    else:
        record = captures[capture_id]
        record["first_seen_capture_at_utc"] = min(
            record["first_seen_capture_at_utc"], event["retrieved_at_utc"]
        )


def _dbt_build(candidate: ProjectPaths, scopes: dict, full_refresh: bool) -> None:
    command = [
        "build",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--target-path",
        str(candidate.state / "dbt-target"),
        "--log-path",
        str(candidate.state / "dbt-logs"),
        "--vars",
        json.dumps({"replacement_scopes": scopes}),
    ]
    if full_refresh:
        command.append("--full-refresh")
    result = run_dbt(candidate.root, command)
    (candidate.root / "dbt-build.log").write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RevisionError(f"dbt build failed; see {candidate.root / 'dbt-build.log'}")


def run_replacement(root: Path, request: dict, *, full_refresh: bool = False) -> dict:
    """Publish one checked warehouse generation; callers must not use other writers."""
    paths = ProjectPaths.from_root(root)
    if request.get("schema_version") != "1" or not (
        {"prices", "trade"} & request.keys()
    ):
        raise RevisionError("A version 1 request with a source selection is required")
    with writer_lock(paths.root):
        return _run_locked(paths, request, full_refresh)


def _run_locked(paths: ProjectPaths, request: dict, full_refresh: bool) -> dict:
    run_id = uuid.uuid4().hex
    candidate = ProjectPaths.from_root(paths.state / "replacements" / run_id)
    candidate.parquet.mkdir(parents=True)
    candidate.warehouse.mkdir()
    published = paths.warehouse / "coffee_cocoa.duckdb"
    database = candidate.warehouse / "coffee_cocoa.duckdb"
    journal = candidate.root / "run.json"
    state = {
        "run_id": run_id,
        "status": "preparing",
        "request": request,
        "selected_at_utc": datetime.now(UTC).isoformat(),
        "historical_as_of_availability": "unknown",
    }
    journal.write_text(json.dumps(state, indent=2))
    try:
        previous, history, captures = {}, [], {}
        if published.exists():
            with duckdb.connect(str(published), read_only=True) as connection:
                names = {row[0] for row in connection.execute("show tables").fetchall()}
                if "_revision_history" not in names:
                    raise RevisionError(
                        "Use a separate replacement root; bootstrap requires both selections"
                    )
                history = [
                    json.loads(row[0])
                    for row in connection.execute(
                        "select event from _revision_history order by sequence"
                    ).fetchall()
                ]
                for dataset, schema in SCHEMAS.items():
                    table = connection.execute(
                        f"select * from _selected_{dataset}"
                    ).to_arrow_table()
                    if table.schema.names != schema.names or [
                        f.type for f in table.schema
                    ] != [f.type for f in schema]:
                        raise RevisionError("Incompatible stored source schema")
                    previous[dataset] = table.cast(schema)
                captures = json.loads(
                    connection.execute(
                        "select captures from _revision_state"
                    ).fetchone()[0]
                )
            if not full_refresh:
                shutil.copyfile(published, database)
        scopes, selected, events = {}, {}, {}
        for dataset, schema in SCHEMAS.items():
            old = previous.get(dataset, pa.Table.from_pylist([], schema))
            if dataset in request:
                incoming, scopes[dataset], events[dataset] = read_selection(
                    dataset, request[dataset], paths
                )
                selected[dataset] = replace_rows(old, incoming, scopes[dataset])
                for event in events[dataset]:
                    _record_capture(captures, event, state["selected_at_utc"])
            elif dataset not in previous:
                raise RevisionError(
                    "Initial replacement requires prices and trade selections"
                )
            else:
                selected[dataset], scopes[dataset] = old, []
            pq.write_table(
                selected[dataset],
                candidate.parquet / f"{FILENAMES[dataset]}.parquet",
                compression="zstd",
            )
            if not pq.read_table(
                candidate.parquet / f"{FILENAMES[dataset]}.parquet"
            ).equals(selected[dataset]):
                raise RevisionError("Staged source verification failed")
        state.update(scopes=scopes, selected_captures=events, status="building")
        journal.write_text(json.dumps(state, indent=2))
        _dbt_build(candidate, scopes, full_refresh)
        state["status"] = "validated"
        history.append(state.copy())
        with duckdb.connect(str(database)) as connection:
            connection.execute("begin")
            for dataset, table in selected.items():
                connection.register("selected_input", table)
                connection.execute(
                    f"create or replace table _selected_{dataset} as select * from selected_input"
                )
                connection.unregister("selected_input")
            connection.execute(
                "create or replace table _revision_history(sequence bigint, event varchar)"
            )
            connection.executemany(
                "insert into _revision_history values (?, ?)",
                [(i, json.dumps(event)) for i, event in enumerate(history)],
            )
            connection.execute(
                "create or replace table _revision_state(captures varchar)"
            )
            connection.execute(
                "insert into _revision_state values (?)", [json.dumps(captures)]
            )
            connection.execute("commit")
            connection.execute("checkpoint")
        journal.write_text(json.dumps(state, indent=2))
        paths.warehouse.mkdir(parents=True, exist_ok=True)
        # No fallible work after this single publication boundary. Metadata travels in the DB.
        os.replace(database, published)
        return {
            **state,
            "status": "published",
            "warehouse": str(published),
            "evidence": str(candidate.root),
        }
    except Exception as exc:
        state.update(status="failed_unpublished", error=f"{type(exc).__name__}: {exc}")
        journal.write_text(json.dumps(state, indent=2))
        raise
