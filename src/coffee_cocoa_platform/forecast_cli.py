"""Evaluate and publish cocoa forecasts from an existing local snapshot."""

import argparse
import os
from pathlib import Path

from coffee_cocoa_platform.paths import ProjectPaths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    os.environ.setdefault("DAGSTER_DISABLE_TELEMETRY", "1")
    os.environ["COFFEE_COCOA_ENABLE_FORECAST_RESULTS"] = "true"
    from coffee_cocoa_platform.forecast_assets import run_forecast_assets

    if not run_forecast_assets(ProjectPaths.from_root(args.root).root):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
