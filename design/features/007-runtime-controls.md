# Local runtime controls

Scope: [issue #18](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/18).

## Writer coordination and recovery

All supported writers use a nonblocking POSIX lock at `<root>/.state/writer.lock`.
Resolve the root before locking. Source publication, Dagster asset jobs, source
replacement, direct dbt commands and catalog generation share this policy. Asset
jobs use the in-process executor and retain the lock across sources, dbt and
checks. Another writer fails with `Writer busy` before source or warehouse changes.
Runtime data, warehouse and coordination directories must not be symlinked outside
the canonical layout. The supported platform is local macOS/Linux storage, not
network filesystems.

A dbt subprocess inherits the lock descriptor. Killing its parent does not release
the lock while that child still runs. A competing process must wait or fail until
all owners exit. To investigate a busy root, use `lsof <root>/.state/writer.lock`,
then wait for the identified run or terminate that specific process after checking
its command. Never remove the lock file. Its continued presence is normal; the
kernel releases ownership when the final descriptor closes. No PID-file deletion
or lease expiry is needed. Interrupted replacement candidates stay under
`.state/replacements/`; the published database's `_revision_history` identifies the
last successful generation. Retry the pinned request after all writers exit.

Direct dbt connections to a file require the inherited coordination descriptor.
For a standalone capture root, use:

```sh
uv run --locked python -m coffee_cocoa_platform.dbt_cli --root . -- build
```

The wrapper owns profile, warehouse, target and log paths. Use the replacement
command for replacement-managed roots. Raw `dbt build/run/seed` against the mutable
warehouse is rejected by the project adapter plugin. Arbitrary Python connections
cannot be intercepted: writable notebook access is unsupported. Until read
snapshots are available, open the mutable warehouse read-only only inside
`writer_lock(root)` and close the connection before releasing it. A read-only
connection by itself does not coordinate with writers.

CLI runs retain Dagster's local SQLite run/event history in `.state/dagster`.
Use that absolute directory as `DAGSTER_HOME` for the UI. Definition parsing uses
an isolated temporary target and performs no source download. Runtime dbt outputs
live beneath each root's `.state/`; catalog generation defaults to `.state/catalog`
and locks an explicitly shared output directory too. There is no metadata service.

## Selected partitions and dependencies

`selected_warehouse` has monthly partitions from January 2015 and a single-run
backfill policy. Its upstream source assets represent complete bounded captures,
not scalar cells. A selection pins manifest hashes and intersects their explicit
bounds with the requested contiguous partition range. Every claimed month must
belong to a selected source scope. Different source ranges remain distinct.

Bootstrap a separate replacement root with both sources using the
[source replacement command](005-source-revisions.md). Then select months from a
pinned request in one run:

```sh
uv run --locked python -m coffee_cocoa_platform.backfill_cli --root data/reprocessed --request data/selection.json --start 2021-12 --end 2022-01
```

The job reparses retained captures, replaces only the selected source scopes,
rebuilds all dbt descendants from the complete selected union, and publishes only
after dbt tests pass. Rebuilding descendants preserves lagged-price and aggregate
semantics across partition boundaries. Unrelated source rows remain selected.
The asset records the replacement run ID, range, source selections, build log and
blocking build outcome. Captures must already exist; backfills do not download.

Standalone source refresh commands retain their bounded capture-rebuild semantics.
Use the replacement/backfill path when unrelated months must be preserved.

## Retry, quality and refresh status

World Bank transport gets at most three attempts sharing the existing 2 MiB and
60-second allowance, with a 15-second socket timeout. Failed response bytes count
toward the same allowance, preserving the full-profile workbook reservation. Eurostat retries transient
transport failures within the existing three-attempt slice limit and persisted
batch/global byte, attempt and active-time budgets. Retryable failures are timeouts,
connection errors and HTTP 408/429/500/502/503/504. Parse, schema, checksum and budget
failures do not automatically retry. Dagster does not add another retry layer.
Each dbt build has a 480-second deadline; definition parsing has 120 seconds.

Source validation emits a Dagster asset check. Failed validation fails the run
without a successful source materialization. dbt tests emit their existing asset
checks; replacement backfills expose their complete build result as a check.
Source materializations show monthly cadence, publisher update metadata and age
of actual non-null observations. Price series and trade product/flow/layer groups
retain separate coverage. Trade coverage includes requested series with no cells,
paired value/quantity availability and the oldest/latest series endpoints.

Calendar-month observation age is not a measured publication delay or an SLA.
World Bank and Eurostat publish monthly observations with different release and
revision patterns. Trade partner detail and aggregates may end in different
months. Download completion, a shared update timestamp and the requested last
month never assert complete recent observations. No lateness threshold is assumed.

## Optional schedule

`fixture_refresh_schedule` is stopped by default and only rehearses offline fixture
refreshes, Mondays at 09:00 UTC. After a successful manual fixture run, set
`COFFEE_COCOA_ENABLE_FIXTURE_SCHEDULE=1` in the code-location environment, start the
schedule explicitly and run the Dagster daemon with the same `DAGSTER_HOME` and
`COFFEE_COCOA_HOME`. Without the flag, a tick skips. Without a daemon, no scheduled
runs launch. Live source scheduling still requires a separately chosen refresh
policy; enabling this fixture schedule never enables live downloads.

## Acceptance

| ID | Required behavior |
| --- | --- |
| RC-01 | Batched selected months preserve unrelated source rows and rebuild tested descendants. |
| RC-02 | Competing writers fail before mutation, including after a parent dies while its child writes. |
| RC-03 | Transient transport recovers within finite budgets; invalid data fails without publication. |
| RC-04 | Dagster exposes coverage, quality and durable run history; definition loading and scheduling stay offline by default. |

`tests/test_runtime.py` covers the process race, direct dbt guard, transport faults,
coverage, schedule tick and selected warehouse backfill. Revision tests retain the
independent replacement and full-refresh equivalence oracles.
