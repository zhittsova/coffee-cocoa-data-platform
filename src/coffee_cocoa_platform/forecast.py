"""Retrospective rolling-origin cocoa forecasts from a governed read snapshot."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.snapshots import SnapshotReader

MODELS = ("persistence", "seasonal_naive", "ar1")
HORIZONS = (1, 3)
HISTORY_START = date(2015, 1, 1)
HOLDOUT_START = date(2024, 1, 1)
DEVELOPMENT_END = date(2023, 12, 1)
MIN_CALIBRATION = 12
INTERVAL_LEVEL = 0.9
CONFIG = {
    "schema_version": 1,
    "target": "world_bank_pink_sheet_monthly:cocoa:USD/kg",
    "mode": "retrospective_current_vintage",
    "training_origins": "2016-01..2019-12",
    "history_start": "2015-01",
    "development_origins": "2020-01..2023-12",
    "holdout_origins": "2024-01..2025-12",
    "minimum_observed_history_months": 60,
    "minimum_labeled_training_origins": 48,
    "horizons_months": list(HORIZONS),
    "models": list(MODELS),
    "candidate": "AR(1) with intercept, ordinary least squares on all contiguous observed months through each cutoff",
    "selection": "lowest development MAE by horizon on common scored origins; ties follow model order",
    "holdout_fit_cutoff": "2023-12-01",
    "interval": "symmetric 90% empirical absolute-error radius from eligible development residuals; minimum 12",
    "metrics": ["MAE", "RMSE", "sMAPE", "interval_coverage", "mean_interval_width"],
}


def month_shift(month: date, offset: int) -> date:
    index = month.year * 12 + month.month - 1 + offset
    return date(index // 12, index % 12 + 1, 1)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fit_ar1(history: list[float]) -> tuple[float, float]:
    """Fit y[t] = intercept + slope*y[t-1], rejecting degenerate fits."""
    import numpy as np

    values = np.asarray(history, dtype=float)
    if (
        len(values) < CONFIG["minimum_observed_history_months"]
        or not np.isfinite(values).all()
    ):
        raise ValueError("incomplete_history")
    design = np.column_stack((np.ones(len(values) - 1), values[:-1]))
    coefficients, _, rank, _ = np.linalg.lstsq(design, values[1:], rcond=None)
    if rank != 2 or not np.isfinite(coefficients).all():
        raise ValueError("rank_deficient_fit")
    return float(coefficients[0]), float(coefficients[1])


def _predict_ar1(
    parameters: tuple[float, float], origin_value: float, horizon: int
) -> float:
    value = origin_value
    for _ in range(horizon):
        value = parameters[0] + parameters[1] * value
    if not math.isfinite(value):
        raise ValueError("nonfinite_prediction")
    return value


def _history_through(prices: dict[date, float], origin: date) -> list[float]:
    """Use the full contiguous observed history available by one cutoff."""
    history = []
    month = origin
    while month >= HISTORY_START and month in prices:
        history.append(prices[month])
        month = month_shift(month, -1)
    history.reverse()
    return history


def _radius(errors: list[float]) -> float | None:
    if len(errors) < MIN_CALIBRATION:
        return None
    ordered = sorted(errors)
    position = min(len(ordered), math.ceil((len(ordered) + 1) * INTERVAL_LEVEL))
    return ordered[position - 1]


def _metrics(rows: list[dict]) -> dict:
    scored = [row for row in rows if row["absolute_error"] is not None]
    intervals = [row for row in scored if row["interval_lower"] is not None]
    count = len(scored)
    return {
        "origin_count": len(rows),
        "prediction_count": sum(
            row["prediction_usd_per_kg"] is not None for row in rows
        ),
        "scored_count": count,
        "missing_target_count": sum(row["target_usd_per_kg"] is None for row in rows),
        "failed_fit_count": sum(row["fit_status"] == "failed_fit" for row in rows),
        "mae": sum(row["absolute_error"] for row in scored) / count if count else None,
        "rmse": math.sqrt(sum(row["squared_error"] for row in scored) / count)
        if count
        else None,
        "smape_percent": sum(row["smape_percent"] for row in scored) / count
        if count
        else None,
        "interval_scored_count": len(intervals),
        "interval_coverage": (
            sum(row["interval_covered"] for row in intervals) / len(intervals)
            if intervals
            else None
        ),
        "mean_interval_width": (
            sum(row["interval_upper"] - row["interval_lower"] for row in intervals)
            / len(intervals)
            if intervals
            else None
        ),
    }


def evaluate(
    features: list[dict], targets: list[dict], prices: list[dict]
) -> tuple[list[dict], list[dict], dict]:
    """Evaluate fixed folds, select on development, then score holdout once."""
    price_by_month = {
        row["month_key"]: float(row["price_usd_per_kg"])
        for row in prices
        if row["price_usd_per_kg"] is not None
    }
    capture_by_month = {row["month_key"]: row["source_capture_id"] for row in prices}
    feature_by_month = {row["origin_month"]: row for row in features}
    if len(feature_by_month) != len(features) or len(
        {(r["origin_month"], r["horizon_months"]) for r in targets}
    ) != len(targets):
        raise ValueError("Forecast dataset keys are not unique")
    if (
        not features
        or not targets
        or any(row["evaluation_mode"] != CONFIG["mode"] for row in features + targets)
    ):
        raise ValueError("Only governed retrospective forecast datasets are supported")
    holdout_history = _history_through(price_by_month, DEVELOPMENT_END)
    try:
        holdout_parameters = _fit_ar1(holdout_history)
        holdout_fit_error = None
    except (ValueError, ArithmeticError) as exc:
        holdout_parameters = None
        holdout_fit_error = str(exc)
    rows: list[dict] = []
    for target in sorted(
        targets, key=lambda row: (row["origin_month"], row["horizon_months"])
    ):
        origin = target["origin_month"]
        if (
            target["horizon_months"] not in HORIZONS
            or target["split_name"] == "training"
        ):
            continue
        feature = feature_by_month[origin]
        horizon = target["horizon_months"]
        eligible = bool(
            feature["has_minimum_history"] and target["labeled_training_origins"] >= 48
        )
        origin_price = price_by_month.get(origin)
        seasonal_month = month_shift(target["target_month"], -12)
        seasonal_price = price_by_month.get(seasonal_month)
        history = _history_through(price_by_month, origin)
        for model in MODELS:
            status = "ok"
            prediction = None
            if not eligible:
                status = "insufficient_history"
            elif model == "persistence":
                prediction = origin_price
            elif model == "seasonal_naive":
                prediction = seasonal_price
                if prediction is None:
                    status = "missing_seasonal_input"
            else:
                try:
                    if origin >= HOLDOUT_START:
                        if holdout_parameters is None:
                            raise ValueError(holdout_fit_error or "failed_fit")
                        parameters = holdout_parameters
                    else:
                        parameters = _fit_ar1(history)
                    prediction = _predict_ar1(parameters, origin_price, horizon)
                except (ValueError, ArithmeticError):
                    status = "failed_fit"
            label_allowed = (
                target["split_name"] != "development"
                or target["target_month"] <= DEVELOPMENT_END
            )
            actual = (
                float(target["target_usd_per_kg"])
                if label_allowed and target["target_usd_per_kg"] is not None
                else None
            )
            if prediction is None and status == "ok":
                status = "missing_origin_input"
            error = (
                prediction - actual
                if prediction is not None and actual is not None
                else None
            )
            scale = (abs(prediction) + abs(actual)) / 2 if error is not None else None
            rows.append(
                {
                    "origin_month": origin,
                    "target_month": target["target_month"],
                    "issue_at_utc": target["issue_at_utc"],
                    "split_name": target["split_name"],
                    "evaluation_mode": target["evaluation_mode"],
                    "horizon_months": horizon,
                    "model_name": model,
                    "fit_status": status,
                    "labeled_training_origins": target["labeled_training_origins"],
                    "origin_capture_id": capture_by_month.get(origin),
                    "seasonal_input_month": seasonal_month
                    if model == "seasonal_naive"
                    else None,
                    "seasonal_input_capture_id": capture_by_month.get(seasonal_month)
                    if model == "seasonal_naive"
                    else None,
                    "target_capture_id": target["target_capture_id"]
                    if label_allowed
                    else None,
                    "prediction_usd_per_kg": prediction,
                    "target_usd_per_kg": actual,
                    "absolute_error": abs(error) if error is not None else None,
                    "squared_error": error * error if error is not None else None,
                    "smape_percent": (0.0 if scale == 0 else 100 * abs(error) / scale)
                    if error is not None
                    else None,
                    "interval_lower": None,
                    "interval_upper": None,
                    "interval_covered": None,
                    "calibration_count": 0,
                }
            )
    # A development residual is available only once its target month has passed.
    for row in rows:
        if row["prediction_usd_per_kg"] is None:
            continue
        cutoff = (
            DEVELOPMENT_END if row["split_name"] == "holdout" else row["origin_month"]
        )
        prior = [
            candidate["absolute_error"]
            for candidate in rows
            if candidate["split_name"] == "development"
            and candidate["model_name"] == row["model_name"]
            and candidate["horizon_months"] == row["horizon_months"]
            and candidate["origin_month"] < row["origin_month"]
            and candidate["target_month"] <= cutoff
            and candidate["absolute_error"] is not None
        ]
        row["calibration_count"] = len(prior)
        radius = _radius(prior)
        if radius is not None:
            row["interval_lower"] = row["prediction_usd_per_kg"] - radius
            row["interval_upper"] = row["prediction_usd_per_kg"] + radius
            if row["target_usd_per_kg"] is not None:
                row["interval_covered"] = (
                    row["interval_lower"]
                    <= row["target_usd_per_kg"]
                    <= row["interval_upper"]
                )
    selected = {}
    for horizon in HORIZONS:
        development = [
            row
            for row in rows
            if row["split_name"] == "development"
            and row["horizon_months"] == horizon
            and row["target_month"] <= DEVELOPMENT_END
        ]
        origins = {
            row["origin_month"]
            for row in development
            if row["model_name"] == "persistence" and row["absolute_error"] is not None
        }
        candidates = []
        for model in MODELS:
            model_rows = [
                row
                for row in development
                if row["model_name"] == model
                and row["origin_month"] in origins
                and row["absolute_error"] is not None
            ]
            if origins and len(model_rows) == len(origins):
                candidates.append(
                    (
                        sum(row["absolute_error"] for row in model_rows)
                        / len(model_rows),
                        MODELS.index(model),
                        model,
                    )
                )
        if not candidates:
            raise ValueError(
                f"No complete development comparison for horizon {horizon}"
            )
        selected[horizon] = min(candidates)[2]
    groups = defaultdict(list)
    for row in rows:
        groups[(row["split_name"], row["horizon_months"], row["model_name"])].append(
            row
        )
    summaries = []
    for (split, horizon, model), group in sorted(groups.items()):
        evaluation_rows = [
            row
            for row in group
            if split != "development" or row["target_month"] <= DEVELOPMENT_END
        ]
        summaries.append(
            {
                "split_name": split,
                "horizon_months": horizon,
                "model_name": model,
                "is_selected": selected[horizon] == model,
                **_metrics(evaluation_rows),
            }
        )
    return (
        rows,
        summaries,
        {"selected_models": selected, "holdout_fit_error": holdout_fit_error},
    )


def _table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def _manifest_value(value):
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def publish(root: Path) -> dict:
    """Read one immutable input generation and publish versioned local results."""
    paths = ProjectPaths.from_root(root)
    with SnapshotReader(paths.root) as reader:
        connection = reader.connection
        reader_manifest = reader.manifest
        if reader_manifest.get("build", {}).get("kind") == "forecast_evaluation":
            raise ValueError(
                "Publish a new tested input snapshot before another forecast run"
            )
        features = (
            connection.execute(
                "select * from forecast_origin_features order by origin_month"
            )
            .to_arrow_table()
            .to_pylist()
        )
        targets = (
            connection.execute(
                "select * from forecast_targets order by origin_month, horizon_months"
            )
            .to_arrow_table()
            .to_pylist()
        )
        prices = (
            connection.execute(
                "select month_key, price_usd_per_kg, source_capture_id from fct_benchmark_prices where source_dataset = 'world_bank_pink_sheet_monthly' and benchmark_series = 'cocoa' order by month_key"
            )
            .to_arrow_table()
            .to_pylist()
        )
        captures = (
            connection.execute(
                "select source_capture_id, source_update_text, source_update_date, retrieved_at_utc, first_seen_capture_at_utc from forecast_capture_metadata order by source_capture_id"
            )
            .to_arrow_table()
            .to_pylist()
        )
        input_snapshot_id = reader.snapshot_id
        input_sha = reader_manifest["warehouse"]["sha256"]
    source_path = Path(__file__)
    lock_path = source_path.parents[2] / "uv.lock"
    identity = {
        "config": CONFIG,
        "input_snapshot_id": input_snapshot_id,
        "input_warehouse_sha256": input_sha,
        "code_sha256": _hash_file(source_path),
        "lock_sha256": _hash_file(lock_path),
        "numpy_version": version("numpy"),
        "package_version": version("coffee-cocoa-platform"),
    }
    run_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[
        :24
    ]
    run_dir = paths.data / "forecasts" / "runs" / run_id
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    if run_dir.exists():
        manifest = json.loads((run_dir / "manifest.json").read_text())
        if any(manifest.get(key) != value for key, value in identity.items()):
            raise ValueError("Existing forecast run identity does not match")
        for name, checksum in manifest["artifact_sha256"].items():
            if _hash_file(run_dir / name) != checksum:
                raise ValueError("Existing forecast artifact checksum changed")
    else:
        predictions, summaries, selection = evaluate(features, targets, prices)
        staged = Path(
            tempfile.mkdtemp(prefix=".forecast-", dir=paths.data / "forecasts")
        )
        try:
            for row in predictions:
                row["run_id"] = run_id
            for row in summaries:
                row["run_id"] = run_id
            pq.write_table(
                _table(predictions), staged / "predictions.parquet", compression="zstd"
            )
            pq.write_table(
                _table(summaries), staged / "metrics.parquet", compression="zstd"
            )
            manifest = {
                **identity,
                **selection,
                "run_id": run_id,
                "created_at_utc": datetime.now(UTC).isoformat(),
                "python_version": sys.version.split()[0],
                "capture_metadata": [
                    {key: _manifest_value(value) for key, value in capture.items()}
                    for capture in captures
                ],
                "prediction_rows": len(predictions),
                "metric_rows": len(summaries),
                "artifact_sha256": {
                    name: _hash_file(staged / name)
                    for name in ("predictions.parquet", "metrics.parquet")
                },
            }
            (staged / "manifest.json").write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n"
            )
            os.replace(staged, run_dir)
        finally:
            if staged.exists():
                shutil.rmtree(staged)
    paths.parquet.mkdir(parents=True, exist_ok=True)
    for source, current in (
        ("predictions.parquet", "forecast_predictions.parquet"),
        ("metrics.parquet", "forecast_metrics.parquet"),
    ):
        temp = paths.parquet / (current + ".tmp")
        shutil.copyfile(run_dir / source, temp)
        os.replace(temp, paths.parquet / current)
    (paths.parquet / "forecast_results.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "input_snapshot_id": input_snapshot_id,
                "input_warehouse_sha256": input_sha,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return manifest
