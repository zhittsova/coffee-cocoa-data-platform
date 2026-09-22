"""Coordinated direct dbt access to a standalone capture warehouse."""

import argparse
from pathlib import Path

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.runtime import run_dbt, writer_lock


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    args = options.args
    if args[:1] == ["--"]:
        args = args[1:]
    if not args or args[0] not in {"build", "run", "test", "seed"}:
        parser.error("Choose build, run, test or seed")
    # Only model selection and a rebuild flag are supported here. Connection,
    # state and artifact overrides belong to explicit application entry points.
    index = 1
    while index < len(args):
        if args[index] == "--full-refresh":
            index += 1
        elif (
            args[index] in {"--select", "--exclude"}
            and index + 1 < len(args)
            and not args[index + 1].startswith("-")
        ):
            index += 2
        else:
            parser.error(
                "Supported options: --select EXPRESSION, --exclude EXPRESSION, --full-refresh"
            )
    paths = ProjectPaths.from_root(options.root)
    project = Path(__file__).resolve().parents[2] / "dbt"
    with writer_lock(paths.root):
        paths.require_capture_root()
        paths.warehouse.mkdir(parents=True, exist_ok=True)
        result = run_dbt(
            paths.root,
            [
                *args,
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(project),
                "--target-path",
                str(paths.state / "dbt-direct"),
                "--log-path",
                str(paths.state / "dbt-direct"),
            ],
        )
    print(result.stdout, end="")
    print(result.stderr, end="")
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
