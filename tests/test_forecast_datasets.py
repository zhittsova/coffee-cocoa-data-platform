"""Adversarial calendar and vintage oracles for dbt forecast inputs."""

from datetime import UTC, date, datetime
from decimal import Decimal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coffee_cocoa_platform.catalog_metadata import DBT_DIR
from coffee_cocoa_platform.prices import PRICE_SCHEMA, write_forecast_capture_metadata
from coffee_cocoa_platform.runtime import run_dbt


def synthetic_prices(root):
    directory = root / "data/parquet"
    directory.mkdir(parents=True)
    (root / "warehouse").mkdir()
    rows = []
    for year in range(2015, 2027):
        for month in range(1, 13):
            period = date(year, month, 1)
            if period > date(2026, 4, 1) or period == date(2023, 12, 1):
                continue
            index = (year - 2015) * 12 + month
            capture = "capture-a"
            price = Decimal(index)
            if period == date(2024, 2, 1):
                capture, price = "capture-b", Decimal(999)
            if period == date(2024, 4, 1):
                capture, price = "capture-c", Decimal(777)
            rows.append(
                {
                    "source_dataset": "world_bank_pink_sheet_monthly",
                    "series_id": "cocoa",
                    "period_month": period,
                    "source_value_text": str(price),
                    "price_usd_per_kg": price,
                    "source_capture_id": capture,
                    "source_status": None,
                }
            )
    rows.append(
        {
            "source_dataset": "other_price_source",
            "series_id": "cocoa",
            "period_month": date(2024, 1, 1),
            "source_value_text": "1234",
            "price_usd_per_kg": Decimal(1234),
            "source_capture_id": "unrelated-capture",
            "source_status": None,
        }
    )
    path = directory / "benchmark_prices.parquet"
    pq.write_table(pa.Table.from_pylist(rows, PRICE_SCHEMA), path)
    captures = {
        name: {
            "source_update": "Updated on August 14, 2026",
            "retrieved_at_utc": datetime(2026, 9, 22, tzinfo=UTC).isoformat(),
            "first_seen_capture_at_utc": datetime(2026, 9, 22, tzinfo=UTC).isoformat(),
        }
        for name in ("capture-a", "capture-b", "capture-c")
    }
    write_forecast_capture_metadata(directory, captures)
    return path


def dbt(root, *args):
    return run_dbt(
        root,
        [
            *args,
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target-path",
            str(root / "dbt-target"),
            "--log-path",
            str(root / "dbt-logs"),
            "--no-partial-parse",
        ],
    )


def test_repeated_capture_keeps_first_local_seen_time(tmp_path):
    first = datetime(2026, 9, 22, tzinfo=UTC).isoformat()
    later = datetime(2026, 9, 23, tzinfo=UTC).isoformat()
    record = {
        "capture-a": {
            "source_update": "Updated on August 14, 2026",
            "retrieved_at_utc": first,
            "first_seen_capture_at_utc": first,
        }
    }
    write_forecast_capture_metadata(tmp_path, record)
    record["capture-a"]["retrieved_at_utc"] = later
    record["capture-a"]["first_seen_capture_at_utc"] = later
    write_forecast_capture_metadata(tmp_path, record)
    row = pq.read_table(tmp_path / "forecast_capture_metadata.parquet").to_pylist()[0]
    assert row["retrieved_at_utc"].day == 23
    assert row["first_seen_capture_at_utc"].day == 22


@pytest.mark.integration
def test_forecast_lags_labels_splits_and_vintages(tmp_path):
    source = synthetic_prices(tmp_path)
    original = pq.read_table(source)
    result = dbt(
        tmp_path,
        "build",
        "--select",
        "+forecast_origin_features +forecast_targets forecast_capture_metadata",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    database = tmp_path / "warehouse/coffee_cocoa.duckdb"
    with duckdb.connect(str(database), read_only=True) as connection:
        split = connection.execute(
            "select split_name, count(*) from forecast_split_manifest group by 1 order by 1"
        ).fetchall()
        feature = connection.execute(
            "select lag_0_usd_per_kg, lag_1_usd_per_kg, lag_3_usd_per_kg, "
            "lag_12_usd_per_kg, lag_0_capture_id, observed_history_months_60, "
            "has_minimum_history from forecast_origin_features "
            "where origin_month = date '2024-01-01'"
        ).fetchone()
        targets = connection.execute(
            "select horizon_months, target_month, target_usd_per_kg, target_capture_id "
            "from forecast_targets where origin_month = date '2024-01-01' "
            "order by horizon_months"
        ).fetchall()
        columns = {
            row[0]
            for row in connection.execute(
                "describe forecast_origin_features"
            ).fetchall()
        }
        capture = connection.execute(
            "select source_update_date, first_seen_capture_at_utc "
            "from forecast_capture_metadata where source_capture_id = 'capture-b'"
        ).fetchone()
    assert split == [("development", 48), ("holdout", 24), ("training", 48)]
    assert feature == (
        Decimal(109),
        None,
        Decimal(106),
        Decimal(97),
        "capture-a",
        59,
        False,
    )
    assert targets == [
        (1, date(2024, 2, 1), Decimal(999), "capture-b"),
        (3, date(2024, 4, 1), Decimal(777), "capture-c"),
    ]
    with duckdb.connect(str(database), read_only=True) as connection:
        early = connection.execute(
            "select horizon_months, labeled_training_origins from forecast_targets "
            "where origin_month = date '2020-01-01' order by horizon_months"
        ).fetchall()
    assert early == [(1, 48), (3, 46)]
    assert all("target" not in column for column in columns)
    assert capture[0] == date(2026, 8, 14)
    assert capture[1].year == 2026
    assert pq.read_table(source).equals(original)
    repeat = dbt(
        tmp_path,
        "build",
        "--select",
        "+forecast_origin_features +forecast_targets forecast_capture_metadata",
    )
    assert repeat.returncode == 0, repeat.stdout + repeat.stderr
    assert pq.read_table(source).equals(original)


@pytest.mark.integration
def test_as_of_mode_rejects_missing_historical_release_evidence(tmp_path):
    synthetic_prices(tmp_path)
    result = dbt(
        tmp_path,
        "compile",
        "--select",
        "forecast_origin_features",
        "--vars",
        '{"forecast_availability_mode": "as_of"}',
    )
    assert result.returncode != 0
    assert "requires verified historical release and capture evidence" in (
        result.stdout + result.stderr
    )
