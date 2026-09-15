"""Local Eurostat trade pipeline command."""

import argparse
import json
import os
from pathlib import Path

from coffee_cocoa_platform.paths import ProjectPaths


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Dagster + dbt trade pipeline")
    parser.add_argument("--mode", choices=("fixture", "live"), default="fixture")
    parser.add_argument("--profile", choices=("smoke", "full"), default="smoke")
    parser.add_argument("--start", help="Inclusive YYYY-MM reference month")
    parser.add_argument("--end", help="Inclusive YYYY-MM reference month")
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    if args.mode == "live" and (args.start is None or args.end is None):
        parser.error("Live mode requires explicit --start and --end")
    root = ProjectPaths.from_root(args.root).root
    os.environ.setdefault("DAGSTER_DISABLE_TELEMETRY", "1")
    from coffee_cocoa_platform.trade_assets import run_trade_assets

    if not run_trade_assets(
        mode=args.mode,
        profile_name=args.profile,
        start=args.start,
        end=args.end,
        root=root,
    ):
        raise SystemExit(1)
    manifest = json.loads((root / "data/parquet/trade_observations.json").read_text())
    print(
        json.dumps(
            {
                key: manifest[key]
                for key in (
                    "profile",
                    "slice_count",
                    "row_count",
                    "total_payload_bytes",
                    "reserved_payload_bytes",
                    "source_updates",
                    "transport_complete",
                    "parquet_path",
                )
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
