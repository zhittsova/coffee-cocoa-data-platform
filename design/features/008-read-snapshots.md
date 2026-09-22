# Consistent read snapshots

Scope: [issue #20](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/20).

The complete two-source fixture pipeline and source replacement jobs publish an
immutable DuckDB read snapshot only after dbt build and tests succeed. Partial
price or trade jobs and arbitrary direct dbt selections do not publish a global
snapshot. The publisher holds the root writer lock, checkpoints and closes the
mutable database, copies it into a new version directory, verifies the copy, then
atomically replaces `snapshots/current.json`. The pointer is the visibility
boundary. An interrupted copy or pointer replacement leaves the prior pointer
unchanged and readable.

Each manifest records the snapshot generation, build kind and run ID, publication
time, database checksum and size, table names, source manifest hashes and capture
identities. Replacement snapshots also retain the selected capture and revision
metadata stored in the warehouse. Readers validate the pointer, manifest and
database checksum before opening DuckDB in read-only mode. They never fall back
to the mutable warehouse or scan version directories for a newer file.

Use the reader as a context manager:

```python
from pathlib import Path

from coffee_cocoa_platform.snapshots import open_snapshot

with open_snapshot(Path(".")) as reader:
    rows = reader.connection.execute(
        "select * from monthly_benchmark_prices order by period_month"
    ).fetchall()
```

An open reader stays on its selected version. `reader.refresh()` opens and
validates the current version before it closes the prior connection. The helper
holds a shared per-version lease until the DuckDB connection closes, so retention
does not unlink files still needed by a supported reader.

Retention keeps the current snapshot and two prior successful snapshots by
default. Older active versions remain until their readers close, so the temporary
upper bound is three retained versions plus actively leased versions. A later
publication or an explicit `prune_snapshots` call removes released versions.
Cleanup errors are reported in publication metadata but do not roll back an
already replaced pointer.

This protocol supports local macOS and Linux filesystems. It does not claim
network-filesystem safety, power-loss durability beyond the filesystem's atomic
replace and sync behavior, or historical source availability that the captures
do not establish.

## Acceptance

| ID | Required behavior |
| --- | --- |
| RS-01 | One snapshot identifies one successful full build and its source versions. |
| RS-02 | Build or publication failure preserves the prior readable pointer. |
| RS-03 | An open reader remains on its immutable version while a later build publishes. |
| RS-04 | Retention preserves current, prior and actively leased versions; refresh is explicit. |

`tests/test_snapshots.py` covers publication metadata, injected copy and pointer
failures, pinned readers, refresh, retention, corrupt metadata and repeated full
fixture builds.
