# Source revisions and backfills

Scope is defined in [issue #14](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/14).

A replacement request explicitly selects captured source versions and inclusive
reference-month bounds. Trade selections name Germany, flows, and supported CN8
leaves or product groups. The capture's validated partner universe bounds the
replacement: a smoke capture cannot remove partners it did not request. Both
measures are replaced together. An explicit empty JSON-stat value collection
with complete dimensions removes all observations in its selected scope; missing
value fields, incomplete captures, malformed schemas and checksum mismatches fail.
Benchmark selections name the series and retain absent months and null prices.

Captures remain immutable raw evidence. Use a new `--capture-vintage` label on
the trade command to fetch again; repeat the same label to resume its bounded
plan. Keep each input capture root and manifest when selecting historical
versions. Replacement also archives manifest contents and raw checksums locally.
The selected version is explicit, so an older capture can be restored without
pretending it is the latest publisher update. Reference month, source update,
retrieval time, first recorded capture and selection time remain separate.
Historical as-of availability is unknown.

## Running a replacement

Run the source commands into capture roots, then write a local selection file:

```json
{
  "schema_version": "1",
  "prices": {
    "manifest": "/absolute/capture/data/parquet/benchmark_prices.json",
    "manifest_sha256": "replace with the manifest file SHA-256",
    "start": "2024-01",
    "end": "2024-04",
    "series": ["cocoa", "coffee_arabica", "coffee_robusta"]
  },
  "trade": {
    "manifest": "/absolute/capture/data/parquet/trade_observations.json",
    "manifest_sha256": "replace with the manifest file SHA-256",
    "start": "2021-12",
    "end": "2022-01",
    "reporter": "DE",
    "products": ["09011100", "18069090"],
    "flows": ["1", "2"]
  }
}
```

Pin each manifest with the digest from `shasum -a 256 /absolute/path/to/manifest.json`.
If that manifest changes, the request fails until a new version is explicitly
selected. First-seen capture time uses the earliest evidenced retrieval of those
bytes; first selection time is recorded independently.

Use the actual bounds and products of the captured profile. `product_groups` may
replace or extend `products`; every leaf of each requested group must be covered.
The first request requires both sources. Later requests may select either one.
Use a separate replacement root with no existing standalone pipeline warehouse:

```sh
uv run --locked python -m coffee_cocoa_platform.revision_cli --root data/reprocessed --request data/selection.json
```

The manual Dagster `source_replacement_job` accepts the same request and a
`full_refresh` flag. The CLI's `--full-refresh` builds an empty warehouse from the
same complete selected source union, preserving untouched partitions. No live
request is made during replacement.

## Publication boundary

A run validates raw captures, builds a candidate source union and copies the
previous warehouse for incremental work. dbt materializes both staging models as
incremental tables using the locked adapter's `delete+insert` strategy. A
transactional pre-hook deletes the entire declared scope independently of the
surviving input keys. The insert reads only that scope. Schema changes fail.
Without replacement parameters, standalone capture commands rebuild all staging
rows. Downstream tables rebuild so changes propagate through lagged benchmark
metrics, balances, dimensions and partner denominators.

Only a successful complete dbt build, including its tests, can publish. The
candidate stores the complete selected source tables, capture registry and ordered
selection history alongside its business tables. Closing and checkpointing the
candidate precedes one same-filesystem atomic replacement of
`warehouse/coffee_cocoa.duckdb`. Readers opening that file see either the previous
or new complete database. Failed candidates remain unpublished with a failure
record under `.state/replacements/`; retrying the request starts from the last
published state. The published `_revision_history` identifies the authoritative
run even if a process stops immediately after publication.

Replacement calls use an exclusive local lock. Run them serially and do not point
another warehouse writer at the replacement root. Standalone source assets reject
replacement-managed roots.
Cross-command writer coordination, reader snapshot management, retention and
power-loss recovery remain separate operational work. This boundary handles
process/build failures; it does not claim power-loss durability or reduced total
warehouse I/O. Its benefit is bounded staging transformation with correct source
withdrawals; candidate copies and downstream rebuilds are deliberate local costs.

## Acceptance

| ID | Required behavior |
| --- | --- |
| SR-01 | Identical replay preserves business rows while retaining selection events. |
| SR-02 | Changed, withdrawn, new and empty scopes preserve untouched partitions. |
| SR-03 | Incremental tables and downstream marts equal a clean full refresh, including CN-year boundaries and older-version selections. |
| SR-04 | Schema/checksum/build/publication failure preserves the previous database and supports deterministic retry. |

`tests/test_revisions.py` exercises synthetic transitions, independent expected
facts and metrics, publication failure, capture replay and full-refresh equality.
