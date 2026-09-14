# Local project foundation

Status: foundation implemented; pipeline assets follow later.

## Goal

A clean checkout installs one compatible, uv-managed Python project and exposes
the core toolchain. This feature establishes the environment for the first source
pipeline; it does not claim to implement ingestion, models or forecasts.

## Scope

Create a root `pyproject.toml`, committed `uv.lock`, explicit Python pin, and an
installable `src/coffee_cocoa_platform` package using the `uv_build` backend
from `uv init --package`. Resolve dbt Core, dbt-duckdb,
DuckDB, Dagster and dagster-dbt as one compatible dependency graph.

Keep Ruff, pre-commit and test tooling in a project-locked development group.
Notebook dependencies are optional. Add forecast and Ansible groups when their
features introduce actual requirements. Do not install speculative dependencies
or publish empty directories to imply functionality.

Define configuration/data paths and ignore generated data, state and credentials
in Git. Keep local working notes and assistant files out of container contexts;
track AGENTS.md and CLAUDE.md as public workflow guidance.
The README describes actual installed capabilities and links to the design.
Use Pythonic Dagster `Definitions` and `@dbt_assets` with `DbtCliResource` for
the first pipeline assets. The locked versions expose these APIs. Components
would add a configuration layer without a current need.

Python 3.12 is the supported interpreter line for this foundation. The local
pin is 3.12.14; other interpreter lines are outside this feature's support claim.

## Acceptance and validation

| ID | Acceptance criterion | Test and evidence |
| --- | --- | --- |
| LF-01 | A clean local checkout installs with `uv sync --locked`; the package and core integrations import | Clean environment install plus import/CLI version output |
| LF-02 | The supported Python range matches the environment actually validated | Compare the declared range/pin with recorded interpreter and smoke results |
| LF-03 | Runtime-only installation works; developer tools use the lockfile; notebooks remain optional | Install runtime and development selections separately; run locked Ruff/pre-commit checks |
| LF-04 | Built packages and the future container context exclude generated data, state, credentials and local working notes | Inspect ignore rules, built distribution contents and staged changes; verify internal documentation links |

Resolve incompatible dependencies rather than bypassing uv. A listed command is
an acceptance procedure until its successful execution is recorded. Report the
tested environment and material limitations in the linked implementation PR.

## Workflow

Open a feature issue with these acceptance IDs before implementation. Use one
branch and one PR for the coherent foundation change, including tests and any
necessary specification corrections. Keep the PR body brief, with an issue link
and concise validation. Sign commits and use unscoped conventional messages.

The maintainer reviews and merges. The next feature defines a bounded benchmark
source-to-dbt pipeline; expanded trade modeling follows after its source contract
is validated. No source download, cloud deployment or forecast claim belongs in
this foundation feature.
