"""Apply one contiguous monthly backfill from explicitly pinned local captures."""

import argparse
import json
from pathlib import Path

from dagster import AssetKey, AssetSpec, materialize

from coffee_cocoa_platform.backfill_assets import selected_warehouse
from coffee_cocoa_platform.runtime import run_instance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    with run_instance(args.root) as instance:
        result = materialize(
            [
                AssetSpec(AssetKey(["world_bank_prices", "monthly_prices"])),
                AssetSpec(AssetKey(["eurostat_trade", "monthly_trade"])),
                selected_warehouse,
            ],
            instance=instance,
            tags={
                "dagster/asset_partition_range_start": args.start,
                "dagster/asset_partition_range_end": args.end,
            },
            run_config={
                "ops": {
                    "selected_warehouse": {
                        "config": {
                            "root": str(args.root.resolve()),
                            "request": json.loads(args.request.read_text()),
                        }
                    }
                }
            },
        )
    if not result.success:
        raise SystemExit(1)
    print(f"Backfill run: {result.run_id}")


if __name__ == "__main__":
    main()
