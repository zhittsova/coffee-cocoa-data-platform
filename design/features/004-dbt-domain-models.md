# dbt domain model

Status: implemented locally. Scope and acceptance were defined in
[issue #12](https://github.com/zhittsova/coffee-cocoa-data-platform/issues/12).

dbt is the authoritative layer for analytical identities and metrics. Conformed
dimensions cover the coffee and cocoa domains, calendar months, annual CN8 leaf
validity and observed trade partners. Benchmark and trade observations remain in
separate facts because a source benchmark series is not a CN8 product and nominal
USD benchmark prices are not EUR trade values.

The trade fact pairs value and mass only at the same source coordinate. It keeps
WORLD, other aggregate areas, named partners and special partner codes separate.
A represented unavailable cell remains distinct from a numeric zero. Unit values
require positive paired mass and describe border value per kilogram, not retail
price, profit, processing margin or bean-equivalent mass.

Monthly product metrics use WORLD rows and disclose observed and declared CN8
coverage. Product mix uses the observed selected-leaf value denominator and shows
whether all 33 annual leaves are present. A balance is published only when import
and export values cover the identical leaf set; imports retain their CIF basis and
exports their FOB basis.

Partner shares and concentration use numeric named-partner values as their stated
universe. Special categories remain visible outside that denominator. WORLD
reconciliation is calculated per CN8 leaf before aggregation so a missing leaf
cannot be offset by another leaf. The output reports WORLD coverage, residuals and
coverage mismatches rather than silently assigning unexplained value.

Benchmark dynamics stay within each World Bank series in nominal USD per kilogram.
Indexes start at the first usable observation in that series and make no CN8 or
coffee-variety identity claim. Public models declare an owner, grain and every
published column. Governed metric metadata supplies the metric dictionary with
the expression, unit, denominator and coverage for each measure.

The default offline command builds both source branches and all descendant dbt
models. Generic relationship tests, singular preservation and metric invariants,
dbt unit tests, and an independent exact fixture oracle cover keys, fan-out,
denominators, sparse cells and reconciliation behavior. Forecast models, source
revision handling and immutable read snapshots remain separate features.
