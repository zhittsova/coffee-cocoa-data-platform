"""Runtime oracles: competing processes, bounded failures and selected backfills."""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dagster import (
    AssetKey,
    AssetSpec,
    DefaultScheduleStatus,
    build_schedule_context,
    materialize,
)

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.runtime import WriterBusyError, writer_lock


def test_inherited_writer_lock_survives_killed_parent(tmp_path):
    child_code = """
import sys, time
from pathlib import Path
import duckdb
root = Path(sys.argv[1])
(root / 'warehouse').mkdir()
connection = duckdb.connect(str(root / 'warehouse/coffee_cocoa.duckdb'))
connection.execute('create table sentinel as select 42 as value')
(root / 'ready').touch()
time.sleep(60)
"""
    parent_code = """
import subprocess, sys, time
from pathlib import Path
from coffee_cocoa_platform.runtime import writer_lock
with writer_lock(Path(sys.argv[1])) as fd:
    child = subprocess.Popen([sys.executable, '-c', sys.argv[2], sys.argv[1]], pass_fds=(fd,))
    print(child.pid, flush=True)
    child.wait()
"""
    parent = subprocess.Popen(
        [sys.executable, "-c", parent_code, str(tmp_path), child_code],
        stdout=subprocess.PIPE,
        text=True,
    )
    child_pid = int(parent.stdout.readline())
    try:
        deadline = time.monotonic() + 20
        while not (tmp_path / "ready").exists():
            assert time.monotonic() < deadline
            time.sleep(0.05)
        with pytest.raises(WriterBusyError), writer_lock(tmp_path):
            pytest.fail("competing writer entered")
        parent.kill()
        parent.wait(timeout=5)
        with pytest.raises(WriterBusyError), writer_lock(tmp_path):
            pytest.fail("orphaned writer lost coordination")
        # A real supported entry point fails without replacing source files.
        rejected = subprocess.run(
            [
                sys.executable,
                "-m",
                "coffee_cocoa_platform.price_cli",
                "--root",
                str(tmp_path),
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert rejected.returncode != 0
        assert "Writer busy" in rejected.stderr
        assert not (tmp_path / "data/parquet").exists()
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        os.kill(child_pid, signal.SIGTERM)
    deadline = time.monotonic() + 10
    while True:
        try:
            with writer_lock(tmp_path):
                break
        except WriterBusyError:
            assert time.monotonic() < deadline
            time.sleep(0.05)
    assert (tmp_path / ".state/writer.lock").exists()
    import duckdb

    with duckdb.connect(str(tmp_path / "warehouse/coffee_cocoa.duckdb")) as connection:
        assert connection.execute("select * from sentinel").fetchall() == [(42,)]


def test_direct_dbt_rejects_uncoordinated_warehouse(tmp_path):
    project = Path(__file__).resolve().parents[1] / "dbt"
    database = tmp_path / "coffee_cocoa.duckdb"
    result = subprocess.run(
        [
            str(Path(sys.executable).with_name("dbt")),
            "seed",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
        env=dict(
            os.environ,
            COFFEE_COCOA_DUCKDB_PATH=str(database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        ),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0
    assert "coordinated dbt_cli" in result.stdout + result.stderr
    assert not database.exists()


def test_transport_retry_bounds_and_invalid_data(tmp_path, monkeypatch):
    from coffee_cocoa_platform import prices
    from coffee_cocoa_platform.trade_fixture import fixture_profile, fixture_transport
    from coffee_cocoa_platform.trades import (
        FetchResult,
        TradeSourceError,
        publish_trade_profile,
    )

    monkeypatch.setattr("time.sleep", lambda _: None)
    attempts = []

    def price_fetch(url, budget):
        attempts.append(url)
        if len(attempts) < 3:
            raise TimeoutError("injected")
        return b"ok", url, datetime.now(UTC), None

    monkeypatch.setattr(prices, "_fetch_workbook", price_fetch)
    assert prices.fetch_workbook()[0] == b"ok"
    assert len(attempts) == 3
    calls = []

    def trade_fetch(request, byte_limit, deadline):
        calls.append(request.slice_id)
        if len(calls) == 1:
            raise TimeoutError("injected")
        return fixture_transport(request, byte_limit, deadline)

    paths = ProjectPaths.from_root(tmp_path / "recover")
    result = publish_trade_profile(fixture_profile(), paths, trade_fetch, fixture=True)
    assert result["transport_complete"]
    assert len(calls) == 3  # One failed attempt plus two accepted slices.
    failed_calls = []

    def always_timeout(*args):
        failed_calls.append(1)
        raise TimeoutError("injected permanent timeout")

    with pytest.raises(TradeSourceError):
        publish_trade_profile(
            fixture_profile(),
            ProjectPaths.from_root(tmp_path / "fail"),
            always_timeout,
            fixture=True,
        )
    assert len(failed_calls) == 3
    invalid_calls = []

    def invalid(request, *args):
        invalid_calls.append(1)
        return FetchResult(
            b"{}", request.url, datetime.now(UTC), "application/json", 0.001
        )

    invalid_paths = ProjectPaths.from_root(tmp_path / "invalid")
    with pytest.raises(TradeSourceError):
        publish_trade_profile(fixture_profile(), invalid_paths, invalid, fixture=True)
    assert len(invalid_calls) == 1
    assert not (invalid_paths.parquet / "trade_observations.json").exists()


def test_refresh_covers_absent_series_and_different_layers(tmp_path):
    from test_revisions import captures

    from coffee_cocoa_platform.refresh import refresh_status

    request = captures(tmp_path)
    manifest = json.loads(Path(request["trade"]["manifest"]).read_text())
    status = refresh_status(manifest, datetime(2022, 3, 1, tzinfo=UTC))
    assert not status["complete_recent_observations"]
    assert {row["layer"] for row in status["coverage"]} == {
        "world",
        "detail",
        "special",
    }
    assert any(
        row["series_with_paired_observations"] < row["requested_series"]
        for row in status["coverage"]
    )
    assert {row["flow"] for row in status["coverage"]} == {"1", "2"}
    assert any(row["observed_age_months"] == 2 for row in status["coverage"])
    manifest["slices"][0]["request"]["partners"].append("INT_EU")
    regional = refresh_status(manifest)["coverage"]
    assert any(
        row["layer"] == "aggregate" and row["series_with_paired_observations"] == 0
        for row in regional
    )


def test_schedule_tick_is_stopped_and_requires_opt_in(monkeypatch):
    from coffee_cocoa_platform.platform_assets import defs
    from coffee_cocoa_platform.schedules import fixture_refresh_schedule

    assert fixture_refresh_schedule.default_status == DefaultScheduleStatus.STOPPED
    context = build_schedule_context(
        scheduled_execution_time=datetime(2026, 9, 21, 9, tzinfo=UTC),
        repository_def=defs.get_repository_def(),
    )
    monkeypatch.delenv("COFFEE_COCOA_ENABLE_FIXTURE_SCHEDULE", raising=False)
    assert fixture_refresh_schedule.evaluate_tick(context).skip_message
    monkeypatch.setenv("COFFEE_COCOA_ENABLE_FIXTURE_SCHEDULE", "1")
    requests = fixture_refresh_schedule.evaluate_tick(context).run_requests
    assert len(requests) == 1
    assert requests[0].tags["source_mode"] == "fixture"
    assert defs.resolve_job_def("fixture_refresh").executor_def.name == "in_process"


@pytest.mark.integration
def test_partitioned_backfill_preserves_unrelated_months(tmp_path):
    from test_revisions import captures, snapshot

    from coffee_cocoa_platform.backfill_assets import selected_warehouse
    from coffee_cocoa_platform.revisions import run_replacement
    from coffee_cocoa_platform.runtime import run_instance

    root = tmp_path / "selected"
    run_replacement(root, captures(tmp_path / "capture-a"))
    before = snapshot(root)
    request = captures(tmp_path / "capture-b", "B")
    request.pop("prices")
    with run_instance(root) as instance:
        result = materialize(
            [
                AssetSpec(AssetKey(["world_bank_prices", "monthly_prices"])),
                AssetSpec(AssetKey(["eurostat_trade", "monthly_trade"])),
                selected_warehouse,
            ],
            instance=instance,
            partition_key="2021-12",
            run_config={
                "ops": {
                    "selected_warehouse": {
                        "config": {"root": str(root), "request": request}
                    }
                }
            },
        )
        assert result.success
        assert instance.get_run_by_id(result.run_id).is_success
        assert len(result.get_asset_materialization_events()) == 1
        assert result.get_asset_check_evaluations()[0].passed
    after = snapshot(root)
    assert before["fct_benchmark_prices"] == after["fct_benchmark_prices"]
    assert before["fct_trade_observations"] != after["fct_trade_observations"]
    assert [
        row
        for row in before["fct_trade_observations"]
        if row[0].isoformat() == "2022-01-01"
    ] == [
        row
        for row in after["fct_trade_observations"]
        if row[0].isoformat() == "2022-01-01"
    ]
    import duckdb

    with duckdb.connect(
        str(root / "warehouse/coffee_cocoa.duckdb"), read_only=True
    ) as connection:
        assert (
            connection.execute(
                "select trade_value_eur from fct_trade_observations where month_key='2021-12-01' and partner_code='BR' and product_code='09011100' and flow_code='1'"
            ).fetchone()[0]
            == 1200
        )
    # Full-refresh equality verifies lagged/aggregate downstream dependencies.
    run_replacement(
        root,
        {
            **request,
            "trade": {**request["trade"], "start": "2021-12", "end": "2021-12"},
        },
        full_refresh=True,
    )
    assert snapshot(root) == after


def test_invalid_source_records_failed_dagster_check(tmp_path, monkeypatch):
    from coffee_cocoa_platform import trade_assets
    from coffee_cocoa_platform.runtime import local_writer, run_instance
    from coffee_cocoa_platform.trades import TradeSourceError

    monkeypatch.setenv("COFFEE_COCOA_HOME", str(tmp_path))
    calls = []

    def invalid(*args, **kwargs):
        calls.append(1)
        raise TradeSourceError("injected incompatible schema")

    monkeypatch.setattr(trade_assets, "publish_trade_profile", invalid)
    with run_instance(tmp_path) as instance:
        result = materialize(
            [trade_assets.monthly_trade],
            instance=instance,
            resources={"writer": local_writer},
            run_config={
                "ops": {
                    "eurostat_trade__monthly_trade": {
                        "config": {
                            "mode": "fixture",
                            "profile": "smoke",
                            "start": "2021-12",
                            "end": "2022-01",
                        }
                    }
                }
            },
            raise_on_error=False,
        )
        assert not result.success
        assert instance.get_run_by_id(result.run_id).is_failure
        assert len(calls) == 1
        assert not result.get_asset_materialization_events()
        assert not result.get_asset_check_evaluations()[0].passed


def test_backfill_range_requires_explicit_source_coverage():
    from coffee_cocoa_platform.backfill_assets import select_months

    request = {"schema_version": "1", "trade": {"start": "2021-12", "end": "2022-02"}}
    assert select_months(request, "2021-12", "2022-01")["trade"] == {
        "start": "2021-12",
        "end": "2022-01",
    }
    assert request["trade"]["end"] == "2022-02"
    with pytest.raises(ValueError, match="No explicit capture"):
        select_months(request, "2021-11", "2022-01")


@pytest.mark.parametrize(
    "directory", ["warehouse", "data/raw", "data/parquet", ".state/trade"]
)
def test_runtime_rejects_shared_warehouse_symlink(tmp_path, directory):
    shared = tmp_path / "shared"
    shared.mkdir()
    root = tmp_path / "root"
    root.mkdir()
    destination = root / directory
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.symlink_to(shared, target_is_directory=True)
    with pytest.raises(ValueError, match="without symlinks"), writer_lock(root):
        pytest.fail("aliased warehouse must not bypass coordination")


def test_dbt_deadline_releases_child_lock(tmp_path):
    from coffee_cocoa_platform.runtime import run_dbt

    with pytest.raises(subprocess.TimeoutExpired):
        run_dbt(tmp_path, ["--version"], timeout=0.01)
    with writer_lock(tmp_path):
        assert not (tmp_path / "warehouse/coffee_cocoa.duckdb").exists()


def test_price_retry_counts_failed_response_bytes(monkeypatch):
    from coffee_cocoa_platform import prices

    monkeypatch.setattr(prices, "MAX_BYTES", 6)
    monkeypatch.setattr("time.sleep", lambda _: None)
    attempts = []

    class Response:
        status = 200

        def __init__(self, chunks):
            self.headers = {"Content-Type": "application/octet-stream"}
            self.chunks = iter(chunks)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def geturl(self):
            return "https://example.invalid/workbook"

        def read(self, count):
            chunk = next(self.chunks, b"")
            if isinstance(chunk, Exception):
                raise chunk
            assert len(chunk) <= count
            return chunk

    def open_response(*args, **kwargs):
        attempts.append(1)
        return Response(
            [b"123", TimeoutError("partial timeout")]
            if len(attempts) == 1
            else [b"4567"]
        )

    monkeypatch.setattr(prices.urllib.request, "urlopen", open_response)
    with pytest.raises(prices.PriceSourceError, match="cumulative byte"):
        prices.fetch_workbook()
    assert len(attempts) == 2
