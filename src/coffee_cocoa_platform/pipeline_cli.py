"""Offline two-source Dagster and dbt pipeline command."""

import argparse
import os
from pathlib import Path

from coffee_cocoa_platform.paths import ProjectPaths


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the offline two-source pipeline")
    parser.add_argument("--root", type=Path, default=None)
    args = parser.parse_args()
    root = ProjectPaths.from_root(args.root).root
    os.environ.setdefault("DAGSTER_DISABLE_TELEMETRY", "1")
    from coffee_cocoa_platform.platform_assets import run_fixture_assets

    if not run_fixture_assets(root):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
