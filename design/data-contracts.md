# Data contracts

Status: source contract version 2, inspected on 2026-09-14. The benchmark adapter,
monthly benchmark result, bounded trade adapter and trade staging model are
implemented. The supported trade scope is Germany, 33 annual CN8 leaves and both
flows from January 2017 through the latest observed periods below. A complete
trade history download and analytical trade marts are not implemented.

## Benchmarks

Source: [World Bank Commodity Markets](https://www.worldbank.org/en/research/commodity-markets),
using its [monthly workbook](https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx).

| Field | Contract |
| --- | --- |
| Sheet | `Monthly Prices`, selected by name and validated headers |
| Series | Cocoa, Coffee Arabica and Coffee Robusta, each with its own definition |
| Period | Monthly `YYYYMmm`, normalized to a reference month |
| Measure | Nominal USD/kg; preserve source value text and use explicit decimal rounding |
| Key | Source dataset, series and reference month |
| Capture | Retrieval time, effective URL, source update label and checksum |

The inspected workbook reaches August 2026 and contains all three selected series
through the analytical window beginning January 2015. History is revisable. One
capture supplies all requested months; blanks remain missing and incompatible
headers or units fail validation. Trade prices remain in EUR and benchmarks in
USD. Currency conversion requires a separately specified FX source.

The benchmark capture writes one row per selected series and represented month
to `data/parquet/benchmark_prices.parquet`, with nullable `decimal(20,8)` prices
rounded half-even and the original worksheet numeric XML text retained. Blank
cells and recognized `..`, `n.a.` and `N/A` markers stay null; other text fails
validation. Wholly absent months produce no rows. The current file is
replaced only after workbook validation and staged Parquet verification.
`data/parquet/benchmark_prices.json` records the requested range, actual latest
observed month by series, source update label and parsed date where available,
source/effective URLs, UTC retrieval time, SHA-256, byte and row counts,
missingness, schema version and source descriptions. Original workbook bytes
remain under `data/raw/` by checksum. The fixture uses the same adapter with
synthetic source values and a synthetic URI.

dbt reads the Parquet source, exposes a staging view and builds
`monthly_benchmark_prices` in DuckDB. Its change columns compare only adjacent
observed reference months; missing prices or a skipped month yield null changes.
Dagster links the workbook asset to both dbt models. Importing its definitions
reads the checked-in dbt manifest but does not fetch or materialize data.

The [dataset terms](https://www.worldbank.org/ext/en/legal/terms-conditions/datasets)
include additional conditions and exceptions for third-party data. Keep World
Bank and underlying provider attribution. Rights to redistribute the selected
real observations remain unresolved; real captures stay local.

## Annual product mapping

Use string CN8 codes and the classification valid for each reference year.
These 33 leaves occur in every inspected annual scheme from 2017 through 2026,
valid January 1 through December 31 of the named year. Each leaf belongs to one
group. Source headings and intermediate codes never enter the additive leaf set.
[Eurostat classifications](https://ec.europa.eu/eurostat/web/metadata/classifications),
[CN 2026 scheme](http://data.europa.eu/xsp/cn2026/cn2026).

| Group | CN8 leaves |
| --- | --- |
| Unroasted coffee | `09011100`, `09011200` |
| Roasted coffee | `09012100`, `09012200` |
| Coffee extracts | `21011100` |
| Coffee preparations | `21011292`, `21011298` |
| Cocoa beans | `18010000` |
| Cocoa paste | `18031000`, `18032000` |
| Cocoa butter, fat and oil | `18040000` |
| Unsweetened cocoa powder | `18050000` |
| Sweetened cocoa powder | `18061015`, `18061020`, `18061030`, `18061090` |
| Chocolate and other cocoa preparations | `18062010`, `18062030`, `18062050`, `18062070`, `18062080`, `18062095`, `18063100`, `18063210`, `18063290`, `18069011`, `18069019`, `18069031`, `18069039`, `18069050`, `18069060`, `18069070`, `18069090` |

The mapping excludes coffee husks and substitutes `09019010`/`09019090`, cocoa
waste `18020000`, tea/maté `21012020`/`21012092`/`21012098`, and chicory/substitutes
`21013011`/`21013019`/`21013091`/`21013099`. It also excludes white chocolate
`17049030`, bakery goods, other flour/milk mixtures, ice cream and ready beverages
outside the listed leaves. Mixed preparations enter only under their actual
selected source code. Coffee trade has no Arabica/Robusta split; cocoa beans
include raw or roasted beans. Product mass is not bean-equivalent mass.

Unchanged codes do not guarantee unchanged scope. Annual correspondence identifies
a 2022 break in `18069090`: part of the 2021 scope maps to `16021000` and
`16029099`, with the rest retaining `18069090`. Keep separate 2017-2021 and
2022-2026 comparability segments for this leaf and totals containing it. Suppress
unqualified growth comparisons crossing the break; a separately labeled 32-leaf
comparison can exclude it throughout. The effect on German values is unmeasured,
so no adjustment is inferred. [Official correspondence](https://www.idescat.cat/classificacions/?id=nc-2021-ca&lang=en&tc=6&v0=3&v3=18069090).

The named **2015-2016 trade classification extension** is deferred because the
annual leaf definitions were not verified. Benchmark history remains independent.
Years after 2026 require another classification and correspondence check.

## Trade observations and geography

Source: [Eurostat Comext DS-045409](https://ec.europa.eu/eurostat/databrowser/view/ds-045409/default/table?lang=en),
using its [JSON-stat endpoint](https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data/DS-045409).

| Dimension / measure | Contract |
| --- | --- |
| Grain | Dataset, frequency, reporter, partner, product, flow, indicator and month |
| Reporter / frequency | Germany `DE`, monthly `M` |
| Flows | `1` imports; `2` exports |
| Value | `VALUE_IN_EUROS`, statistical border value; imports CIF, exports FOB |
| Mass | `QUANTITY_IN_100KG`, net mass; multiply by 100 for kilograms |
| Partner meaning | Export destination; extra-EU import origin; intra-EU import consignment |
| Missingness / status | Sparse absence differs from zero; inspected responses provide no observation status |

The trade adapter writes represented value or status cells to
`data/parquet/trade_observations.parquet` at the grain above. It retains the
source numeric text, decimal value, nullable source status, status-field
availability, annual CN identity, response checksum and source update timestamp.
Sparse coordinates produce no row. A supplied status with no numeric value does
produce a row, so unavailable values do not become zero or disappear.

dbt joins the 33-leaf validity mapping and exposes `stg_trade_observations`. The
model labels imports and exports, converts source quantity units from 100 kg to
kg, and classifies partners as named, special or aggregate. Aggregate rows are
marked as non-detail and remain separate from named and special partners. The
`18069090` rows use distinct `2017-2021` and `2022-2026` comparability segments.
No trade total or concentration measure is built in this staging feature.

These meanings follow [publisher methodology](https://ec.europa.eu/eurostat/cache/metadata/en/ext_go_detail_sims.htm).
Trade balances and unit values do not measure profits, processing margins or
crop origin on every route. Preserve the 2020/2021 UK reporting transition and
2022 Intrastat framework change in comparability metadata. Stable product codes
alone do not prove stable collection methods.

Omit the partner filter deliberately to retrieve every category offered by the
dataset for each bounded slice. Freeze the returned codes and source codelist
with the capture. The full 2026 sample returns 278 categories: 259 named country
or territory codes, including historical codes, ten special codes and nine
aggregates. Only 171 categories have any populated observations in that sample.
A listed category does not establish current geographic validity or observed trade.

Keep `QP` high seas; `QQ`/`QR`/`QS` stores and provisions; `QU`/`QV`/`QW`
unspecified territories; and `QX`/`QY`/`QZ` territories withheld for commercial
or military reasons in a separate special-partner class. The suffix variants
separate total, intra-EU and extra-EU concepts; never assume they are additive.
`QA` is Qatar. Preserve `XK`, `XS` and other territorial codes rather than treating
all nonstandard codes as unknown. Unrecognized codes stop analytical publication
pending classification. [Partner codelist](https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/codelist/ESTAT/CXT_FREE_ISO/10.0).

Store `WORLD`, `INT_EU`, `EXT_EU`, `INT_EU27_2020`, `EXT_EU27_2020`, `INT_EA`,
`EXT_EA`, `INT_EA21` and `EXT_EA21` as separate aggregate observations in the
inspected response. Other years can return different area versions. Do not add
regional aggregates, their variants and countries together. This dataset returns
`GB` for the whole UK; do not synthesize or add `XI`/`XU` components from another
dataset. [Eurostat trade manual](https://ec.europa.eu/eurostat/documents/3859598/23178546/KS-01-26-012-EN-N.pdf/a502437f-c3d2-b1c8-df8b-95ed12e992a0?download=true&t=1772723967663&version=1.0).

For each product/flow/month, report named-partner sum, observed special amounts,
WORLD and a residual separately. May 2026 value sums reconcile exactly to WORLD
for all 66 inspected product/flow combinations. That observation does not guarantee
future reconciliation or identify sparse cells as zeros. Retain residuals and
block a complete-universe concentration claim when coverage, special-code overlap
or reconciliation is unresolved. A named-country-only concentration must declare
its observed-country denominator and excluded special share.

## Observed availability

The full 33-leaf sample requested January through August 2026. All WORLD
product/flow series have paired value/mass through May. Named-country observations
reach June in 64 of 66 product/flow combinations; exports of `18032000` and
`18062070` reach May. Individual partner series can stop earlier. No selected
trade observations were found in July or August. The source update timestamp is
August 14, 2026; it is not a cell publication date.

Record requested dates, last observed value, last observed mass and last paired
month separately. Never fill recent absence with zero or forward-fill it. Use
common observed coverage for comparisons and expose partial periods. January
2017 and annual 2024 samples validate historical access; the entire 2017-2026
history and its missingness have not been downloaded or measured.

## Bounded retrieval and completeness

Selected engineering recipe: sort the 33 leaves, split into batches of at most
eight, and request one calendar year per slice with both flows and measures,
Germany and all partners. The 2026 end bound is August; future runs must name
an explicit end bound. This produces 50 trade requests for the supported profile.
Capture the World Bank workbook once separately and include it in the profile's
payload accounting.

Keep the existing smoke limits: 2 MiB per response, 10 MiB per batch, at most
20 attempts and five active download minutes. Use sequential requests, a
15-second connection limit and a 60-second response deadline. The full profile
has an explicit 64 MiB cumulative payload ceiling, at most 150 trade attempts
and 30 active download minutes across five checkpointed batches. At most three
attempts may address one slice; the audit recipe stops on failure and requires
an explicit continuation. These are engineering caps, not publisher quotas.

Measured responses: all 33 leaves for January-August 2026 used 546,401 bytes;
January 2017 used 103,662 bytes; a dense eight-leaf, twelve-month 2024 sample
used 374,787 bytes. Extrapolating the dense sample across 50 slices gives about
17.9 MiB before the workbook. This is a sizing estimate, not a measured full run
or an upper bound. Runtime, normalized storage and memory remain unmeasured.

Check limits while streaming and retain failed attempts separately. A checkpoint
records the immutable plan, URL, timestamps, HTTP result, checksum, byte count,
source update, dimensions and decoded coverage for every slice. Resume only
checksum-verified, scope-validated captures; count failed attempts against budgets.
Changed plans, cap exhaustion, malformed JSON, unknown dimensions/codes, pending
jobs and partial responses cannot become authoritative empty data. Require all
50 slices before claiming transport completion, then evaluate sparse coverage and
reconciliation separately. Mixed source-update timestamps require review; one
shared timestamp still does not prove an atomic upstream snapshot.

The implemented `full` command applies this year/product slicing recipe to an
explicit subset of the supported bounds; the complete bounds produce 50 slices.
It omits the partner filter. The `smoke` command keeps
the original two leaves and five partner codes. It resumes only validated response
checksums for the identical plan. Publication requires every planned slice, one
known 278-code partner universe for full slices, exact requested dimensions,
unique natural keys and one reported source update timestamp. The manifest records
per-series latest value, quantity and paired months. The full trade budget reserves
the benchmark adapter's 2 MiB response ceiling, so both sources remain within the
64 MiB profile cap. These transport checks do not turn sparse source coverage into
complete observations.

Apply [Eurostat reuse terms](https://ec.europa.eu/eurostat/help/copyright-notice).
Attribute the dataset and access date, identify transformations and state that
Eurostat is not responsible for derived calculations. Reporter-specific exceptions
must be reviewed before adding reporters. Public fixtures are synthetic; source
captures stay local.

## Run and forecast decisions

Confirmed: offline synthetic fixtures are the default; live downloads require an
explicit command. The first forecast target is the monthly World Bank cocoa
benchmark in nominal USD/kg at one- and three-month horizons.

Before forecast implementation, freeze minimum training history, development
origins, untouched holdout, availability assumptions, metrics and interval policy.
Compare persistence and seasonal-naive baselines with one specified candidate.
Fit transformations and choose the candidate inside development folds only.
Current-vintage backtests are retrospective; historical release availability was
not reconstructed. Trade features remain excluded from as-of forecasts until their
availability policy is justified and tested. A candidate losing to a baseline is
a valid outcome. dbt owns governed inputs/results, Python fits and evaluates,
Dagster coordinates execution, and notebooks present the results.
