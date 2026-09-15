# Trade ingestion pipeline

Status: implemented locally. Scope and acceptance were defined in
[issue #10](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/10).

The Eurostat Comext adapter supports Germany, imports and exports, both contracted
measures, all returned partner categories and 33 CN8 leaves from January 2017
through August 2026. The full profile uses five product slices per year. The smoke
profile keeps the earlier two-product, five-partner query. Both require explicit
date bounds in live mode; synthetic fixtures remain the default.

Each response must match its reporter, products, flows, indicators, months and
partner scope. Full responses must contain the frozen 278-code partner dimension.
The adapter checks streaming byte and active-time budgets, records failed attempts,
and stops after any failed slice. Repeating the identical command resumes only
checksum-verified responses. Current Parquet changes only after every planned
slice validates and duplicate natural keys are rejected.

The typed source table retains source dimensions, annual CN identity, decimal
values, nullable status, status-field availability, response checksum and source
update time. A missing JSON-stat coordinate stays absent. A supplied status can
represent an unavailable value without turning it into zero.

dbt maps the disjoint product groups, partner classes, flow names and units. It
converts quantities from 100 kg to kg and marks aggregate partner rows as
non-detail. The `18069090` leaf has separate comparability segments on either side
of the 2022 classification break. Dagster links the Eurostat source asset to the
mapping seed and staging model. The combined fixture command runs this path and
the benchmark pipeline in one local graph.

Analytical totals, balances, concentration, revision handling and full-history
acceptance remain separate features. Real captures and derived local data are not
published.
