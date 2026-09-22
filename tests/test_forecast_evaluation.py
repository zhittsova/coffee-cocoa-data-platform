"""Independent calendar and arithmetic oracles for S10B evaluation."""

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coffee_cocoa_platform.forecast import _metrics, _radius, evaluate, month_shift
from coffee_cocoa_platform.prices import PRICE_SCHEMA, write_forecast_capture_metadata
from coffee_cocoa_platform.runtime import run_dbt
from coffee_cocoa_platform.snapshots import SnapshotReader, publish_snapshot


def _series():
    prices = []
    month = date(2015, 1, 1)
    while month <= date(2026, 3, 1):
        # Uneven movement gives both baselines and AR(1) nontrivial errors.
        value = 10 + 0.03 * len(prices) + (month.month % 4) * 0.2
        prices.append(
            {
                "month_key": month,
                "price_usd_per_kg": value,
                "source_capture_id": "synthetic-a",
            }
        )
        month = month_shift(month, 1)
    features = []
    targets = []
    origin = date(2016, 1, 1)
    while origin <= date(2025, 12, 1):
        split = (
            "training"
            if origin.year < 2020
            else "development"
            if origin.year < 2024
            else "holdout"
        )
        features.append(
            {
                "origin_month": origin,
                "split_name": split,
                "evaluation_mode": "retrospective_current_vintage",
                "has_minimum_history": True,
            }
        )
        for horizon in (1, 3):
            target_month = month_shift(origin, horizon)
            observed = next(row for row in prices if row["month_key"] == target_month)
            prior_count = sum(
                1
                for row in features
                if row["origin_month"] < origin
                and month_shift(row["origin_month"], horizon) <= origin
            )
            targets.append(
                {
                    "origin_month": origin,
                    "horizon_months": horizon,
                    "target_month": target_month,
                    "target_usd_per_kg": observed["price_usd_per_kg"],
                    "target_capture_id": "synthetic-a",
                    "split_name": split,
                    "issue_at_utc": datetime(origin.year, origin.month, 1, tzinfo=UTC),
                    "evaluation_mode": "retrospective_current_vintage",
                    "labeled_training_origins": prior_count,
                }
            )
        origin = month_shift(origin, 1)
    return features, targets, prices


def test_baseline_calendar_gate_and_holdout_isolation(monkeypatch):
    features, targets, prices = _series()
    fits = []
    from coffee_cocoa_platform import forecast

    actual_fit = forecast._fit_ar1

    def recording_fit(history):
        fits.append(tuple(history))
        return actual_fit(history)

    monkeypatch.setattr(forecast, "_fit_ar1", recording_fit)
    rows, _, selection = evaluate(features, targets, prices)
    january = [row for row in rows if row["origin_month"] == date(2020, 1, 1)]
    assert all(
        row["fit_status"] == "insufficient_history"
        for row in january
        if row["horizon_months"] == 3
    )
    persistence = next(
        row
        for row in january
        if row["horizon_months"] == 1 and row["model_name"] == "persistence"
    )
    seasonal = next(
        row
        for row in january
        if row["horizon_months"] == 1 and row["model_name"] == "seasonal_naive"
    )
    assert persistence["prediction_usd_per_kg"] == next(
        row["price_usd_per_kg"]
        for row in prices
        if row["month_key"] == date(2020, 1, 1)
    )
    assert seasonal["seasonal_input_month"] == date(2019, 2, 1)
    assert seasonal["prediction_usd_per_kg"] == next(
        row["price_usd_per_kg"]
        for row in prices
        if row["month_key"] == date(2019, 2, 1)
    )
    march_three = next(
        row
        for row in rows
        if row["origin_month"] == date(2020, 3, 1)
        and row["horizon_months"] == 3
        and row["model_name"] == "seasonal_naive"
    )
    assert march_three["fit_status"] == "ok"
    assert march_three["seasonal_input_month"] == date(2019, 6, 1)
    # One frozen holdout fit, plus a separate development fit per eligible horizon.
    eligible_development = sum(
        row["model_name"] == "ar1"
        and row["split_name"] == "development"
        and row["fit_status"] == "ok"
        for row in rows
    )
    assert len(fits) == eligible_development + 1
    assert len(fits[0]) == 108
    assert len(fits[1]) == 61
    assert len(fits[-1]) == 108
    assert fits[0][-1] == next(
        row["price_usd_per_kg"]
        for row in prices
        if row["month_key"] == date(2023, 12, 1)
    )
    assert set(selection["selected_models"]) == {1, 3}
    assert all(
        row["target_usd_per_kg"] is None and row["target_capture_id"] is None
        for row in rows
        if row["split_name"] == "development"
        and row["target_month"] > date(2023, 12, 1)
    )
    for row in rows:
        if row["split_name"] == "development" and row["interval_lower"] is not None:
            assert row["calibration_count"] >= 12


def test_holdout_labels_cannot_change_selection_or_intervals():
    features, targets, prices = _series()
    original, _, original_selection = evaluate(features, targets, prices)
    for target in targets:
        if target["split_name"] == "holdout":
            target["target_usd_per_kg"] = 999.0
    altered, _, altered_selection = evaluate(features, targets, prices)
    assert original_selection == altered_selection
    original_holdout = [
        (row["prediction_usd_per_kg"], row["interval_lower"], row["interval_upper"])
        for row in original
        if row["split_name"] == "holdout"
    ]
    altered_holdout = [
        (row["prediction_usd_per_kg"], row["interval_lower"], row["interval_upper"])
        for row in altered
        if row["split_name"] == "holdout"
    ]
    assert original_holdout == altered_holdout


def test_missing_target_and_failed_candidate_fit_are_reported():
    features, targets, prices = _series()
    missing = next(
        row
        for row in targets
        if row["origin_month"] == date(2025, 12, 1) and row["horizon_months"] == 3
    )
    missing["target_usd_per_kg"] = None
    rows, summaries, _ = evaluate(features, targets, prices)
    late = [
        row
        for row in rows
        if row["origin_month"] == date(2025, 12, 1) and row["horizon_months"] == 3
    ]
    assert all(
        row["prediction_usd_per_kg"] is not None and row["absolute_error"] is None
        for row in late
    )
    holdout_three = [
        row
        for row in summaries
        if row["split_name"] == "holdout" and row["horizon_months"] == 3
    ]
    assert all(
        row["missing_target_count"] == 1 and row["scored_count"] == 23
        for row in holdout_three
    )

    # Constant prices make intercept plus lag rank deficient; baseline rows survive.
    for row in prices:
        row["price_usd_per_kg"] = 1.0
    failed, _, chosen = evaluate(features, targets, prices)
    assert chosen["holdout_fit_error"] == "rank_deficient_fit"
    assert all(
        row["fit_status"] == "failed_fit"
        for row in failed
        if row["split_name"] == "holdout" and row["model_name"] == "ar1"
    )
    assert all(
        row["prediction_usd_per_kg"] == 1.0
        for row in failed
        if row["split_name"] == "holdout" and row["model_name"] == "persistence"
    )


def test_metric_and_interval_oracle():
    assert _radius([1.0] * 11) is None
    assert _radius(list(range(1, 13))) == 12.0
    rows = [
        {
            "prediction_usd_per_kg": 0.0,
            "target_usd_per_kg": 0.0,
            "absolute_error": 0.0,
            "squared_error": 0.0,
            "smape_percent": 0.0,
            "interval_lower": -1.0,
            "interval_upper": 1.0,
            "interval_covered": True,
            "fit_status": "ok",
        },
        {
            "prediction_usd_per_kg": 3.0,
            "target_usd_per_kg": 1.0,
            "absolute_error": 2.0,
            "squared_error": 4.0,
            "smape_percent": 100.0,
            "interval_lower": 2.0,
            "interval_upper": 4.0,
            "interval_covered": False,
            "fit_status": "ok",
        },
        {
            "prediction_usd_per_kg": 2.0,
            "target_usd_per_kg": None,
            "absolute_error": None,
            "squared_error": None,
            "smape_percent": None,
            "interval_lower": None,
            "interval_upper": None,
            "interval_covered": None,
            "fit_status": "ok",
        },
        {
            "prediction_usd_per_kg": None,
            "target_usd_per_kg": 1.0,
            "absolute_error": None,
            "squared_error": None,
            "smape_percent": None,
            "interval_lower": None,
            "interval_upper": None,
            "interval_covered": None,
            "fit_status": "failed_fit",
        },
    ]
    result = _metrics(rows)
    assert result["origin_count"] == 4
    assert result["prediction_count"] == 3
    assert result["scored_count"] == 2
    assert result["missing_target_count"] == 1
    assert result["failed_fit_count"] == 1
    assert result["mae"] == 1.0
    assert result["rmse"] == pytest.approx(2**0.5)
    assert result["smape_percent"] == 50.0
    assert result["interval_coverage"] == 0.5
    assert result["mean_interval_width"] == 2.0


@pytest.mark.integration
def test_offline_forecast_assets_publish_tested_snapshot(
    tmp_path, monkeypatch, request
):
    from coffee_cocoa_platform.catalog_metadata import DBT_DIR, ensure_manifest

    request.addfinalizer(ensure_manifest.cache_clear)

    _, _, prices = _series()
    parquet = tmp_path / "data" / "parquet"
    parquet.mkdir(parents=True)
    (tmp_path / "warehouse").mkdir()
    rows = [
        {
            "source_dataset": "world_bank_pink_sheet_monthly",
            "series_id": "cocoa",
            "period_month": item["month_key"],
            "source_value_text": str(item["price_usd_per_kg"]),
            "price_usd_per_kg": Decimal(str(item["price_usd_per_kg"])).quantize(
                Decimal("0.00000001")
            ),
            "source_capture_id": "synthetic-a",
            "source_status": None,
        }
        for item in prices
    ]
    source = parquet / "benchmark_prices.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=PRICE_SCHEMA), source)
    write_forecast_capture_metadata(
        parquet,
        {
            "synthetic-a": {
                "source_update": "Synthetic evaluation fixture",
                "retrieved_at_utc": datetime(2026, 9, 22, tzinfo=UTC).isoformat(),
                "first_seen_capture_at_utc": datetime(
                    2026, 9, 22, tzinfo=UTC
                ).isoformat(),
            }
        },
    )
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    built = run_dbt(
        tmp_path,
        [
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--select",
            "+forecast_origin_features +forecast_targets forecast_capture_metadata",
            "--target-path",
            str(tmp_path / "input-dbt-target"),
            "--log-path",
            str(tmp_path / "input-dbt-log"),
        ],
    )
    assert built.returncode == 0, built.stdout + built.stderr
    input_snapshot = publish_snapshot(
        tmp_path, {"kind": "synthetic_forecast_input", "run_id": "synthetic"}
    )
    monkeypatch.setenv("COFFEE_COCOA_ENABLE_FORECAST_RESULTS", "true")
    ensure_manifest.cache_clear()
    from coffee_cocoa_platform.forecast_assets import run_forecast_assets

    assert run_forecast_assets(tmp_path)
    with SnapshotReader(tmp_path) as reader:
        assert reader.snapshot_id != input_snapshot["snapshot_id"]
        count = reader.connection.execute(
            "select count(*) from forecast_predictions where split_name = 'holdout'"
        ).fetchone()[0]
        assert count == 24 * 2 * 3
        assert (
            reader.connection.execute(
                "select count(*) from forecast_metrics"
            ).fetchone()[0]
            == 12
        )
        leaked = reader.connection.execute(
            "select count(*) from forecast_predictions where split_name = 'development' "
            "and target_month > date '2023-12-01' and "
            "(target_usd_per_kg is not null or absolute_error is not null)"
        ).fetchone()[0]
        assert leaked == 0
        assert reader.manifest["build"]["forecast_run_id"]
    pointer = json.loads((parquet / "forecast_results.json").read_text())
    assert pointer["input_snapshot_id"] == input_snapshot["snapshot_id"]
    assert (
        tmp_path / "data" / "forecasts" / "runs" / pointer["run_id"] / "manifest.json"
    ).is_file()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
