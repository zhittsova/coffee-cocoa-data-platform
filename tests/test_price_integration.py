"""Hand-checked Dagster/dbt result for the synthetic benchmark workbook."""

import json
import os
import subprocess
import sys
from decimal import Decimal

import duckdb
import pytest


@pytest.mark.integration
def test_fixture_pipeline_and_repeat_are_stable(tmp_path):
    command = [
        sys.executable,
        "-m",
        "coffee_cocoa_platform.price_cli",
        "--root",
        str(tmp_path),
    ]
    environment = dict(os.environ, DAGSTER_DISABLE_TELEMETRY="1")

    def run():
        result = subprocess.run(
            command,
            env=environment,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        manifest = json.loads(
            (tmp_path / "data/parquet/benchmark_prices.json").read_text()
        )
        with duckdb.connect(
            str(tmp_path / "warehouse/coffee_cocoa.duckdb"), read_only=True
        ) as conn:
            rows = conn.execute(
                "select series_id, period_month, price_usd_per_kg, change_usd_per_kg, "
                "change_percent from monthly_benchmark_prices "
                "order by period_month, series_id"
            ).fetchall()
        return manifest, rows

    first_manifest, first_rows = run()
    second_manifest, second_rows = run()
    assert first_manifest["sha256"] == second_manifest["sha256"]
    assert first_rows == second_rows
    assert len(first_rows) == 9
    assert first_manifest["observed_row_count"] == 8
    assert first_manifest["missing_month_count"] == 1
    by_key = {
        (series, month.isoformat()): (price, change, percent)
        for series, month, price, change, percent in first_rows
    }
    assert by_key[("cocoa", "2024-01-01")] == (Decimal("4.40000000"), None, None)
    assert by_key[("cocoa", "2024-02-01")][:2] == (
        Decimal("4.84000000"),
        Decimal("0.44000000"),
    )
    assert by_key[("cocoa", "2024-02-01")][2] == pytest.approx(10.0)
    assert by_key[("coffee_arabica", "2024-02-01")][:2] == (
        Decimal("5.50000000"),
        Decimal("0.50000000"),
    )
    assert by_key[("coffee_robusta", "2024-02-01")] == (None, None, None)
    assert by_key[("coffee_robusta", "2024-04-01")][0] == Decimal("2.12345679")
    assert all(
        change is None
        for (series, month), (_, change, _) in by_key.items()
        if month == "2024-04-01"
    )
    assert not any(month == "2024-03-01" for _, month in by_key)
