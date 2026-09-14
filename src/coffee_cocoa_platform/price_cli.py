"""Local benchmark pipeline command."""

import argparse
import json
import os
from pathlib import Path

from coffee_cocoa_platform.paths import ProjectPaths


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Dagster + dbt price pipeline")
    parser.add_argument("--mode", choices=("fixture", "live"), default="fixture")
    parser.add_argument(
        "--start", default=None, help="Inclusive YYYY-MM reference month"
    )
    parser.add_argument("--end", default=None, help="Inclusive YYYY-MM reference month")
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    if args.mode == "live" and (args.start is None or args.end is None):
        parser.error("Live mode requires explicit --start and --end")
    start = args.start or "2024-01"
    end = args.end or "2024-04"
    root = ProjectPaths.from_root(args.root).root
    os.environ.setdefault("DAGSTER_DISABLE_TELEMETRY", "1")
    from coffee_cocoa_platform.price_assets import run_assets

    if not run_assets(args.mode, start, end, root):
        raise SystemExit(1)
    manifest = json.loads((root / "data/parquet/benchmark_prices.json").read_text())
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
