# Cocoa forecast datasets

Scope: [issue #22](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/22).

dbt builds a monthly World Bank cocoa USD/kg dataset for one- and three-month
horizons. `forecast_split_manifest` fixes development origins at January
2020 through December 2023 and an untouched holdout at January 2024 through
December 2025. An origin is a reference month; its forecast issue time is
00:00 UTC on the first day of the next month. The target month is the origin
plus the horizon. Historical training uses an expanding window with at least
60 consecutive observed target months and 48 labeled origins per horizon.
The split dates are fixed in code and do not move with the latest download.
S10B must keep holdout labels out of candidate selection and transformation
fitting, even though the governed target table contains them.

`forecast_origin_features` has one row per origin. It carries the current
vintage's cocoa value at the origin and at one, three and twelve calendar
months earlier, with each input's reference month and capture ID. Missing
months remain null; a nearby observation never substitutes for a missing lag.
The table has no future target or label columns. `forecast_targets` has the
one- and three-month labels in a separate table, with target reference month,
price and capture ID. Consumers join the tables by origin month only when
training or evaluating the declared horizon.

`forecast_capture_metadata` records the capture ID, publisher update label
and parsed date, retrieval timestamp and first seen capture timestamp. The
publisher's update date is a workbook-level label, not a cell release date.
The first seen timestamp is the earliest recorded local capture of those
bytes, not historical publication evidence. Capture ID identifies the source
vintage; current selections can mix capture IDs after a bounded replacement.
Feature and target rows retain their own capture IDs. The read snapshot
identifies the warehouse generation containing the selected facts and these
tables.

The only supported evaluation mode is `retrospective_current_vintage`.
Its lag values use the selected current vintage, so their presence does not
assert they were known at historical issue times. An `as_of` dbt request
fails until a separately specified historical release/capture registry can
prove each observation was available by the issue time. Trade features are
excluded because the current Eurostat update timestamp is dataset-wide and
does not prove per-cell availability. No source fact is changed by these
derived tables.

dbt tests enforce unique keys, exact calendar offsets and capture lineage.
Offline synthetic cases cover revised captures, missing months and labels
after the origin. S10B owns fitting, metrics and uncertainty intervals.
