"""Execute a saved, explicit source-version selection through Dagster."""

import argparse
import json
from pathlib import Path

from coffee_cocoa_platform.revision_job import source_replacement_job


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()
    result = source_replacement_job.execute_in_process(
        run_config={
            "ops": {
                "replace_source_partitions": {
                    "config": {
                        "root": str(args.root.resolve()),
                        "request": json.loads(args.request.read_text()),
                        "full_refresh": args.full_refresh,
                    }
                }
            }
        }
    )
    print(json.dumps(result.output_for_node("replace_source_partitions"), indent=2))


if __name__ == "__main__":
    main()
