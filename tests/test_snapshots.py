"""Read snapshot publication, failure recovery and active-reader contracts."""

import json
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from coffee_cocoa_platform.snapshots import (
    SnapshotError,
    open_snapshot,
    prune_snapshots,
    publish_snapshot,
)


def write_warehouse(root: Path, value: int) -> None:
    database = root / "warehouse/coffee_cocoa.duckdb"
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "create or replace table result as select ? as value", [value]
        )
        connection.execute("checkpoint")


def write_source_manifests(root: Path) -> None:
    parquet = root / "data/parquet"
    parquet.mkdir(parents=True)
    (parquet / "benchmark_prices.json").write_text(
        json.dumps(
            {
                "schema_version": "1",
                "source_capture_id": "price-capture",
                "retrieved_at_utc": "2026-09-22T10:00:00+00:00",
                "source_update_text": "Aug 2026",
                "input_start": "2024-01",
                "input_end": "2024-04",
            }
        )
    )
    (parquet / "trade_observations.json").write_text(
        json.dumps(
            {
                "schema_version": "1",
                "capture_vintage": "fixture",
                "plan_hash": "trade-plan",
                "source_updates": ["2026-08-14"],
                "slices": [
                    {"capture": {"sha256": "trade-a"}},
                    {"capture": {"sha256": "trade-b"}},
                ],
            }
        )
    )


def publish(root: Path, run_id: str):
    return publish_snapshot(root, {"kind": "test_build", "run_id": run_id})


def test_snapshot_records_build_sources_and_warehouse_identity(tmp_path):
    write_warehouse(tmp_path, 1)
    write_source_manifests(tmp_path)
    result = publish(tmp_path, "run-1")

    assert result["generation"] == 1
    assert result["build"] == {"kind": "test_build", "run_id": "run-1"}
    assert result["warehouse"]["bytes"] > 0
    assert result["warehouse"]["table_names"] == ["result"]
    source_ids = {
        item["dataset"]: item["source_capture_ids"]
        for item in result["source_versions"]["manifests"]
    }
    assert source_ids == {
        "benchmark_prices": ["price-capture"],
        "trade": ["trade-a", "trade-b"],
    }
    with open_snapshot(tmp_path) as reader:
        assert reader.snapshot_id == result["snapshot_id"]
        assert reader.connection.execute("select * from result").fetchall() == [(1,)]
        assert reader.manifest["warehouse"]["sha256"] == result["warehouse"]["sha256"]


def test_copy_and_pointer_failures_preserve_prior_snapshot(tmp_path, monkeypatch):
    from coffee_cocoa_platform import snapshots

    write_warehouse(tmp_path, 1)
    first = publish(tmp_path, "run-1")
    pointer = tmp_path / "snapshots/current.json"
    original_pointer = pointer.read_bytes()
    write_warehouse(tmp_path, 2)

    def partial_copy(source, destination):
        destination.write_bytes(source.read_bytes()[:128])
        raise OSError("injected partial copy")

    with monkeypatch.context() as patch:
        patch.setattr(snapshots, "_copy_database", partial_copy)
        with pytest.raises(OSError, match="partial copy"):
            publish(tmp_path, "run-2-copy")
    assert pointer.read_bytes() == original_pointer
    with open_snapshot(tmp_path) as reader:
        assert reader.snapshot_id == first["snapshot_id"]
        assert reader.connection.execute("select * from result").fetchall() == [(1,)]

    def fail_pointer(_staged, _pointer):
        raise OSError("injected pointer failure")

    with monkeypatch.context() as patch:
        patch.setattr(snapshots, "_replace_pointer", fail_pointer)
        with pytest.raises(OSError, match="pointer failure"):
            publish(tmp_path, "run-2-pointer")
    assert pointer.read_bytes() == original_pointer
    with open_snapshot(tmp_path) as reader:
        assert reader.connection.execute("select * from result").fetchall() == [(1,)]

    second = publish(tmp_path, "run-2")
    assert second["generation"] == 2
    assert not any(
        path.name.startswith(".staging-")
        for path in (tmp_path / "snapshots/versions").iterdir()
    )
    with open_snapshot(tmp_path) as reader:
        assert reader.snapshot_id == second["snapshot_id"]
        assert reader.connection.execute("select * from result").fetchall() == [(2,)]


def test_retention_failure_does_not_undo_published_pointer(tmp_path, monkeypatch):
    from coffee_cocoa_platform import snapshots

    write_warehouse(tmp_path, 1)
    publish(tmp_path, "run-1")
    write_warehouse(tmp_path, 2)

    def fail_prune(*_args, **_kwargs):
        raise OSError("injected prune")

    with monkeypatch.context() as patch:
        patch.setattr(snapshots, "_prune_locked", fail_prune)
        result = publish(tmp_path, "run-2")

    assert result["retention"]["errors"] == [
        "retention failed: OSError: injected prune"
    ]
    with open_snapshot(tmp_path) as reader:
        assert reader.snapshot_id == result["snapshot_id"]
        assert reader.connection.execute("select * from result").fetchall() == [(2,)]


def test_version_finalization_holds_registry_exclusive_lock(tmp_path, monkeypatch):
    from coffee_cocoa_platform import snapshots

    write_warehouse(tmp_path, 1)
    original_registry_lock = snapshots._registry_lock
    original_replace = snapshots.os.replace
    state = {"exclusive": 0}

    @contextmanager
    def observed_registry_lock(base, *, exclusive):
        with original_registry_lock(base, exclusive=exclusive):
            state["exclusive"] += int(exclusive)
            try:
                yield
            finally:
                state["exclusive"] -= int(exclusive)

    def checked_replace(source, destination):
        if Path(source).name.startswith(".staging-"):
            assert state["exclusive"] == 1
        return original_replace(source, destination)

    monkeypatch.setattr(snapshots, "_registry_lock", observed_registry_lock)
    monkeypatch.setattr(snapshots.os, "replace", checked_replace)
    publish(tmp_path, "run-1")


def test_reader_stays_pinned_refreshes_explicitly_and_defers_retention(tmp_path):
    write_warehouse(tmp_path, 1)
    first = publish(tmp_path, "run-1")
    reader = open_snapshot(tmp_path)
    first_version = tmp_path / "snapshots/versions" / first["snapshot_id"]

    for value in (2, 3, 4):
        write_warehouse(tmp_path, value)
        latest = publish(tmp_path, f"run-{value}")

    assert first_version.exists()
    assert reader.snapshot_id == first["snapshot_id"]
    assert reader.connection.execute("select * from result").fetchall() == [(1,)]
    with open_snapshot(tmp_path) as current:
        assert current.snapshot_id == latest["snapshot_id"]
        assert current.connection.execute("select * from result").fetchall() == [(4,)]

    assert reader.refresh() is True
    assert reader.snapshot_id == latest["snapshot_id"]
    assert reader.connection.execute("select * from result").fetchall() == [(4,)]
    reader.close()
    result = prune_snapshots(tmp_path)
    assert first["snapshot_id"] in result["removed"]
    assert not first_version.exists()
    versions = [
        path for path in (tmp_path / "snapshots/versions").iterdir() if path.is_dir()
    ]
    assert len(versions) == 3


def test_cross_process_reader_lease_defers_retention(tmp_path):
    write_warehouse(tmp_path, 1)
    first = publish(tmp_path, "run-1")
    first_version = tmp_path / "snapshots/versions" / first["snapshot_id"]
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import sys; from pathlib import Path; "
                "from coffee_cocoa_platform.snapshots import open_snapshot; "
                "reader=open_snapshot(Path(sys.argv[1])); "
                "print(reader.snapshot_id, flush=True); "
                "sys.stdin.readline(); "
                "print(reader.connection.execute('select * from result').fetchall(), flush=True); "
                "reader.close()"
            ),
            str(tmp_path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout.readline().strip() == first["snapshot_id"]
    for value in (2, 3, 4):
        write_warehouse(tmp_path, value)
        publish(tmp_path, f"run-{value}")
    assert first_version.exists()
    output, errors = child.communicate("\n", timeout=30)
    assert child.returncode == 0, errors
    assert output.strip() == "[(1,)]"
    assert first["snapshot_id"] in prune_snapshots(tmp_path)["removed"]
    assert not first_version.exists()


def test_reader_fails_closed_on_corrupt_pointer_and_manifest(tmp_path):
    write_warehouse(tmp_path, 1)
    result = publish(tmp_path, "run-1")
    pointer = tmp_path / "snapshots/current.json"
    pointer.write_text("not json")
    with pytest.raises(SnapshotError, match="pointer"):
        open_snapshot(tmp_path)

    pointer.write_text(json.dumps(result["pointer"]))
    manifest = tmp_path / "snapshots/versions" / result["snapshot_id"] / "manifest.json"
    manifest.write_text("{}")
    with pytest.raises(SnapshotError, match="manifest"):
        open_snapshot(tmp_path)


@pytest.mark.integration
def test_complete_fixture_build_publishes_while_prior_reader_stays_open(
    tmp_path, monkeypatch
):
    from coffee_cocoa_platform import trade_assets
    from coffee_cocoa_platform.platform_assets import run_fixture_assets

    assert run_fixture_assets(tmp_path)
    mutable = tmp_path / "warehouse/coffee_cocoa.duckdb"
    with duckdb.connect(str(mutable), read_only=True) as connection:
        expected = connection.execute(
            "select count(*) from fct_trade_observations"
        ).fetchone()
        source_facts = connection.execute(
            "select month_key, price_usd_per_kg, source_capture_id "
            "from fct_benchmark_prices where benchmark_series = 'cocoa' "
            "order by month_key"
        ).fetchall()
        features = connection.execute(
            "select origin_month, lag_0_usd_per_kg, lag_1_usd_per_kg, "
            "lag_3_usd_per_kg, lag_12_usd_per_kg "
            "from forecast_origin_features order by origin_month"
        ).fetchall()

    with open_snapshot(tmp_path) as first_reader:
        first_id = first_reader.snapshot_id
        assert (
            "forecast_origin_features"
            in first_reader.manifest["warehouse"]["table_names"]
        )
        assert (
            first_reader.connection.execute(
                "select origin_month, lag_0_usd_per_kg, lag_1_usd_per_kg, "
                "lag_3_usd_per_kg, lag_12_usd_per_kg "
                "from forecast_origin_features order by origin_month"
            ).fetchall()
            == features
        )
        assert (
            first_reader.connection.execute(
                "select count(*) from fct_trade_observations"
            ).fetchone()
            == expected
        )
        assert run_fixture_assets(tmp_path)
        assert first_reader.snapshot_id == first_id
        assert (
            first_reader.connection.execute(
                "select count(*) from fct_trade_observations"
            ).fetchone()
            == expected
        )
        with open_snapshot(tmp_path) as second_reader:
            assert second_reader.snapshot_id != first_id
            assert (
                second_reader.connection.execute(
                    "select origin_month, lag_0_usd_per_kg, lag_1_usd_per_kg, "
                    "lag_3_usd_per_kg, lag_12_usd_per_kg "
                    "from forecast_origin_features order by origin_month"
                ).fetchall()
                == features
            )
            assert (
                second_reader.connection.execute(
                    "select month_key, price_usd_per_kg, source_capture_id "
                    "from fct_benchmark_prices where benchmark_series = 'cocoa' "
                    "order by month_key"
                ).fetchall()
                == source_facts
            )
            assert (
                second_reader.connection.execute(
                    "select count(*) from fct_trade_observations"
                ).fetchone()
                == expected
            )
        pointer = tmp_path / "snapshots/current.json"
        successful_pointer = pointer.read_bytes()

        def fail_source(*_args, **_kwargs):
            raise ValueError("injected invalid source")

        monkeypatch.setattr(trade_assets, "publish_trade_profile", fail_source)
        with pytest.raises(Exception, match="injected invalid source"):
            run_fixture_assets(tmp_path)
        assert pointer.read_bytes() == successful_pointer
        assert (
            first_reader.connection.execute(
                "select count(*) from fct_trade_observations"
            ).fetchone()
            == expected
        )
