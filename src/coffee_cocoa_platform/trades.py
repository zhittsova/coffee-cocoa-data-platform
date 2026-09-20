"""Bounded Eurostat Comext capture and typed trade publication."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

from coffee_cocoa_platform.paths import ProjectPaths

SOURCE_URL = (
    "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/"
    "data/DS-045409"
)
DATASET = "eurostat_comext_ds_045409"
SCHEMA_VERSION = "1"
DIMENSIONS = (
    "freq",
    "reporter",
    "partner",
    "product",
    "flow",
    "indicators",
    "time",
)
FLOWS = ("1", "2")
INDICATORS = ("VALUE_IN_EUROS", "QUANTITY_IN_100KG")
SMOKE_PRODUCTS = ("09011100", "18010000")
SMOKE_PARTNERS = ("BR", "CI", "GH", "VN", "WORLD")
PRODUCT_GROUPS = {
    "coffee_unroasted": ("09011100", "09011200"),
    "coffee_roasted": ("09012100", "09012200"),
    "coffee_extracts": ("21011100",),
    "coffee_preparations": ("21011292", "21011298"),
    "cocoa_beans": ("18010000",),
    "cocoa_paste": ("18031000", "18032000"),
    "cocoa_butter": ("18040000",),
    "cocoa_powder_unsweetened": ("18050000",),
    "cocoa_powder_sweetened": (
        "18061015",
        "18061020",
        "18061030",
        "18061090",
    ),
    "chocolate_cocoa_preparations": (
        "18062010",
        "18062030",
        "18062050",
        "18062070",
        "18062080",
        "18062095",
        "18063100",
        "18063210",
        "18063290",
        "18069011",
        "18069019",
        "18069031",
        "18069039",
        "18069050",
        "18069060",
        "18069070",
        "18069090",
    ),
}
PRODUCTS = tuple(sorted(code for codes in PRODUCT_GROUPS.values() for code in codes))

# Frozen from the complete partner dimension returned by the validated 2017 and
# 2026 DS-045409 samples. Historical codes remain valid source categories.
SUPPORTED_PARTNERS = frozenset(
    """
    AD AE AF AG AI AL AM AN AO AQ AR AS AT AU AW AZ BA BB BD BE BF BG BH BI BJ
    BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR
    CS CU CV CW CX CY CZ DD DE DJ DK DM DO DZ EC EE EG EH ER ES ET EXT_EA
    EXT_EA21 EXT_EU EXT_EU27_2020 FI FJ FK FM FO FR GA GB GD GE GF GH GI GL GM
    GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IN INT_EA INT_EA21
    INT_EU INT_EU27_2020 IO IQ IR IS IT JM JO JP KE KG KH KI KM KN KP KR KW KY
    KZ LA LB LC LI LK LR LS LT LU LV LY MA MD ME MG MH MK ML MM MN MO MP MQ MR
    MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG
    PH PK PL PM PN PS PT PW PY QA QP QQ QR QS QU QV QW QX QY QZ RE RO RU RW SA
    SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SU SV SX SY SZ TC TD TF TG
    TH TJ TK TL TM TN TO TP TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN
    VU WF WORLD WS XA XB XC XK XL XM XO XP XR XS XZ YD YE YT YU ZA ZM ZW
    """.split()  # noqa: SIM905 - grouped source codes are easier to audit here
)

TRADE_SCHEMA = pa.schema(
    [
        pa.field("source_dataset", pa.string(), nullable=False),
        pa.field("frequency_code", pa.string(), nullable=False),
        pa.field("reporter_code", pa.string(), nullable=False),
        pa.field("partner_code", pa.string(), nullable=False),
        pa.field("partner_label", pa.string(), nullable=False),
        pa.field("product_code", pa.string(), nullable=False),
        pa.field("flow_code", pa.string(), nullable=False),
        pa.field("indicator_code", pa.string(), nullable=False),
        pa.field("period_month", pa.date32(), nullable=False),
        pa.field("source_value_text", pa.string()),
        pa.field("source_value", pa.decimal128(24, 4)),
        pa.field("source_status", pa.string()),
        pa.field("status_available", pa.bool_(), nullable=False),
        pa.field("classification_code", pa.string(), nullable=False),
        pa.field("classification_year", pa.int16(), nullable=False),
        pa.field("source_capture_id", pa.string(), nullable=False),
        pa.field("source_update_time", pa.string(), nullable=False),
    ]
)


class TradeSourceError(ValueError):
    """A trade capture cannot be trusted for publication."""


@dataclass(frozen=True)
class TradeSlice:
    slice_id: str
    batch: int
    products: tuple[str, ...]
    partners: tuple[str, ...] | None
    start: str
    end: str
    url: str


@dataclass(frozen=True)
class TradeProfile:
    name: str
    slices: tuple[TradeSlice, ...]
    max_response_bytes: int
    max_batch_bytes: int
    max_batch_attempts: int
    max_batch_seconds: float
    max_total_bytes: int
    max_total_attempts: int
    max_total_seconds: float
    max_slice_attempts: int = 3
    reserved_payload_bytes: int = 0


@dataclass(frozen=True)
class FetchResult:
    data: bytes
    effective_url: str
    retrieved_at: datetime
    content_type: str | None
    elapsed_seconds: float


class TradeTransportError(TradeSourceError):
    """A response failed after bytes became available for retention."""

    def __init__(self, message: str, result: FetchResult) -> None:
        super().__init__(message)
        self.result = result


Transport = Callable[[TradeSlice, int, float], FetchResult]


def _month(value: str) -> date:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise TradeSourceError(f"Invalid month bound: {value}")
    parsed = date.fromisoformat(value + "-01")
    if not date(2017, 1, 1) <= parsed <= date(2026, 8, 1):
        raise TradeSourceError(
            "Trade bounds must use the supported 2017-01 to 2026-08 scope"
        )
    return parsed


def _months(start: str, end: str) -> tuple[str, ...]:
    first, last = _month(start), _month(end)
    if first > last:
        raise TradeSourceError("Trade month bounds must be ordered")
    result = []
    cursor = first
    while cursor <= last:
        result.append(cursor.strftime("%Y-%m"))
        cursor = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)
    return tuple(result)


def _slice_url(
    products: tuple[str, ...],
    partners: tuple[str, ...] | None,
    start: str,
    end: str,
) -> str:
    params: list[tuple[str, str]] = [
        ("lang", "en"),
        ("freq", "M"),
        ("reporter", "DE"),
    ]
    params.extend(("partner", partner) for partner in partners or ())
    params.extend(("product", product) for product in products)
    params.extend(("flow", flow) for flow in FLOWS)
    params.extend(("indicators", indicator) for indicator in INDICATORS)
    params.extend((("sinceTimePeriod", start), ("untilTimePeriod", end)))
    return SOURCE_URL + "?" + urlencode(params)


def make_profile(name: str, start: str, end: str) -> TradeProfile:
    """Build a deterministic smoke or analytical request plan."""
    months = _months(start, end)
    by_year: dict[int, list[str]] = {}
    for month in months:
        by_year.setdefault(int(month[:4]), []).append(month)
    if name == "smoke":
        groups = (SMOKE_PRODUCTS,)
        partners: tuple[str, ...] | None = SMOKE_PARTNERS
        budgets = (10 * 1024 * 1024, 20, 300.0)
        reserved_payload_bytes = 0
    elif name == "full":
        groups = tuple(tuple(PRODUCTS[i : i + 8]) for i in range(0, len(PRODUCTS), 8))
        partners = None
        budgets = (64 * 1024 * 1024, 150, 1800.0)
        # The confirmed 64 MiB profile ceiling includes the separately captured
        # World Bank workbook, whose own transport cap is 2 MiB.
        reserved_payload_bytes = 2 * 1024 * 1024
    else:
        raise TradeSourceError("profile must be smoke or full")
    slices = []
    for year, year_months in sorted(by_year.items()):
        for group_index, products in enumerate(groups):
            sequence = len(slices)
            slices.append(
                TradeSlice(
                    slice_id=f"{year}-{group_index}",
                    batch=sequence // 10,
                    products=products,
                    partners=partners,
                    start=year_months[0],
                    end=year_months[-1],
                    url=_slice_url(products, partners, year_months[0], year_months[-1]),
                )
            )
    return TradeProfile(
        name=name,
        slices=tuple(slices),
        max_response_bytes=2 * 1024 * 1024,
        max_batch_bytes=10 * 1024 * 1024,
        max_batch_attempts=20,
        max_batch_seconds=300.0,
        max_total_bytes=budgets[0],
        max_total_attempts=budgets[1],
        max_total_seconds=budgets[2],
        reserved_payload_bytes=reserved_payload_bytes,
    )


def _category_positions(dimension: Mapping) -> tuple[dict[str, int], dict[str, str]]:
    category = dimension.get("category")
    if not isinstance(category, Mapping):
        raise TradeSourceError("Missing JSON-stat category")
    raw_index = category.get("index")
    if isinstance(raw_index, list):
        positions = {str(code): position for position, code in enumerate(raw_index)}
    elif isinstance(raw_index, Mapping):
        if any(
            isinstance(position, bool) or not isinstance(position, int)
            for position in raw_index.values()
        ):
            raise TradeSourceError("Invalid JSON-stat category position")
        positions = {str(code): position for code, position in raw_index.items()}
    else:
        raise TradeSourceError("Invalid JSON-stat category index")
    if sorted(positions.values()) != list(range(len(positions))):
        raise TradeSourceError("JSON-stat category positions are not contiguous")
    raw_labels = category.get("label", {})
    if not isinstance(raw_labels, Mapping):
        raise TradeSourceError("Invalid JSON-stat category labels")
    labels = {str(code): str(label) for code, label in raw_labels.items()}
    return positions, labels


def _sparse_values(value: object, cell_count: int, field: str) -> dict[int, object]:
    if value is None:
        return {}
    if isinstance(value, list):
        if len(value) != cell_count:
            raise TradeSourceError(f"Invalid {field} array length")
        return {index: item for index, item in enumerate(value) if item is not None}
    if isinstance(value, Mapping):
        result = {}
        for raw_index, item in value.items():
            try:
                index = int(raw_index)
            except (TypeError, ValueError) as exc:
                raise TradeSourceError(f"Invalid {field} position") from exc
            if not 0 <= index < cell_count or item is None:
                raise TradeSourceError(f"Invalid {field} entry")
            result[index] = item
        return result
    raise TradeSourceError(f"Invalid JSON-stat {field}")


def _decimal(value: object) -> tuple[str, Decimal]:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TradeSourceError("Trade values must be numeric")
    text = str(value)
    try:
        parsed = Decimal(text)
        if not parsed.is_finite() or abs(parsed) >= Decimal(100000000000000000000):
            raise InvalidOperation
        normalized = parsed.quantize(Decimal("0.0001"))
    except InvalidOperation as exc:
        raise TradeSourceError("Trade value exceeds decimal(24,4)") from exc
    return text, normalized


def parse_trade_response(data: bytes, request: TradeSlice) -> tuple[pa.Table, dict]:
    """Validate one JSON-stat slice and retain only represented source cells."""
    if len(data) > 2 * 1024 * 1024:
        raise TradeSourceError("Trade response exceeds the per-response limit")
    try:
        document = json.loads(data, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise TradeSourceError("Invalid JSON response") from exc
    if not isinstance(document, Mapping):
        raise TradeSourceError("Trade response must be a JSON object")
    if document.get("class") != "dataset" or document.get("version") != "2.0":
        raise TradeSourceError("Unsupported JSON-stat response")
    if not isinstance(document.get("value"), (Mapping, list)):
        raise TradeSourceError(
            "Missing explicit value collection; not authoritative empty"
        )
    if tuple(document.get("id", ())) != DIMENSIONS:
        raise TradeSourceError("Unexpected JSON-stat dimensions")
    size = document.get("size")
    if not isinstance(size, list) or len(size) != len(DIMENSIONS):
        raise TradeSourceError("Invalid JSON-stat dimension sizes")
    if any(
        isinstance(dimension_size, bool)
        or not isinstance(dimension_size, int)
        or dimension_size <= 0
        for dimension_size in size
    ):
        raise TradeSourceError("Invalid JSON-stat dimension size")
    dimensions = document.get("dimension")
    if not isinstance(dimensions, Mapping):
        raise TradeSourceError("Missing JSON-stat dimensions")
    positions = {}
    labels = {}
    for dimension, expected_size in zip(DIMENSIONS, size):
        if dimension not in dimensions:
            raise TradeSourceError(f"Missing dimension: {dimension}")
        positions[dimension], labels[dimension] = _category_positions(
            dimensions[dimension]
        )
        if len(positions[dimension]) != expected_size:
            raise TradeSourceError(f"Dimension size mismatch: {dimension}")
    expected = {
        "freq": {"M"},
        "reporter": {"DE"},
        "product": set(request.products),
        "flow": set(FLOWS),
        "indicators": set(INDICATORS),
        "time": set(_months(request.start, request.end)),
    }
    for dimension, values in expected.items():
        if set(positions[dimension]) != values:
            raise TradeSourceError(f"Unexpected {dimension} scope")
    partner_codes = set(positions["partner"])
    expected_partners = (
        set(request.partners) if request.partners else SUPPORTED_PARTNERS
    )
    if partner_codes != expected_partners:
        raise TradeSourceError("Unexpected partner universe")
    if partner_codes - set(labels["partner"]):
        raise TradeSourceError("Missing partner labels")
    update = document.get("updated")
    if not isinstance(update, str) or not update:
        raise TradeSourceError("Missing source update timestamp")
    cell_count = 1
    for dimension_size in size:
        cell_count *= dimension_size
    values = _sparse_values(document.get("value"), cell_count, "value")
    status_payload = document.get("status")
    status_present = status_payload is not None
    statuses = _sparse_values(status_payload, cell_count, "status")
    reverse = {
        dimension: {position: code for code, position in mapping.items()}
        for dimension, mapping in positions.items()
    }
    capture_id = hashlib.sha256(data).hexdigest()
    records = []
    for flat_index in sorted(set(values) | set(statuses)):
        cursor = flat_index
        coordinate = {}
        for dimension, dimension_size in reversed(tuple(zip(DIMENSIONS, size))):
            coordinate[dimension] = reverse[dimension][cursor % dimension_size]
            cursor //= dimension_size
        if cursor:
            raise TradeSourceError("Invalid flattened JSON-stat position")
        source_value_text = None
        source_value = None
        if flat_index in values:
            source_value_text, source_value = _decimal(values[flat_index])
        status = statuses.get(flat_index)
        if status is not None and not isinstance(status, str):
            raise TradeSourceError("Trade status must be text")
        period = date.fromisoformat(coordinate["time"] + "-01")
        partner = coordinate["partner"]
        records.append(
            (
                DATASET,
                coordinate["freq"],
                coordinate["reporter"],
                partner,
                labels["partner"].get(partner, partner),
                coordinate["product"],
                coordinate["flow"],
                coordinate["indicators"],
                period,
                source_value_text,
                source_value,
                status,
                status_present,
                "CN",
                period.year,
                capture_id,
                update,
            )
        )
    columns = list(zip(*records)) if records else [[] for _ in TRADE_SCHEMA]
    table = pa.Table.from_arrays(
        [
            pa.array(values, type=field.type)
            for values, field in zip(columns, TRADE_SCHEMA)
        ],
        schema=TRADE_SCHEMA,
    )
    observed_months = sorted(
        {row[8].isoformat()[:7] for row in records if row[10] is not None}
    )
    series: dict[tuple[str, str, str], dict[str, set[str]]] = {}
    for row in records:
        key = (row[3], row[5], row[6])
        observations = series.setdefault(
            key,
            {"VALUE_IN_EUROS": set(), "QUANTITY_IN_100KG": set()},
        )
        if row[10] is not None:
            observations[row[7]].add(row[8].isoformat()[:7])
    series_coverage = []
    for (partner, product, flow), observations in sorted(series.items()):
        value_months = observations["VALUE_IN_EUROS"]
        quantity_months = observations["QUANTITY_IN_100KG"]
        paired_months = value_months & quantity_months
        series_coverage.append(
            {
                "partner_code": partner,
                "product_code": product,
                "flow_code": flow,
                "last_value_month": max(value_months) if value_months else None,
                "last_quantity_month": (
                    max(quantity_months) if quantity_months else None
                ),
                "last_paired_month": max(paired_months) if paired_months else None,
            }
        )
    return table, {
        "source_update_time": update,
        "partner_count": len(partner_codes),
        "represented_cell_count": len(records),
        "value_cell_count": len(values),
        "status_cell_count": len(statuses),
        "explicit_zero_count": sum(value == 0 for value in values.values()),
        "status_available": status_present,
        "first_observed_month": observed_months[0] if observed_months else None,
        "last_observed_month": observed_months[-1] if observed_months else None,
        "series_coverage": series_coverage,
    }


def fetch_trade_slice(
    request: TradeSlice, max_bytes: int, deadline_seconds: float
) -> FetchResult:
    """Fetch one response with connection, elapsed-time and streaming byte caps."""
    started = time.monotonic()
    retrieved = datetime.now(UTC)
    chunks: list[bytes] = []
    effective_url = request.url
    content_type = None
    http_request = urllib.request.Request(
        request.url, headers={"User-Agent": "coffee-cocoa-platform/0.0.0"}
    )
    timeout = min(60.0, max(0.1, deadline_seconds))
    try:
        with urllib.request.urlopen(
            http_request, timeout=min(15.0, timeout)
        ) as response:
            effective_url = response.geturl()
            content_type = response.headers.get("Content-Type")
            if response.status != 200:
                raise TradeSourceError(f"HTTP {response.status}")
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > max_bytes:
                raise TradeSourceError(
                    "Trade response exceeds the remaining byte budget"
                )
            byte_count = 0
            while chunk := response.read(min(64 * 1024, max_bytes - byte_count + 1)):
                chunks.append(chunk)
                byte_count += len(chunk)
                if byte_count > max_bytes:
                    raise TradeSourceError(
                        "Trade response exceeds the remaining byte budget"
                    )
                if time.monotonic() - started > deadline_seconds:
                    raise TradeSourceError(
                        "Trade response exceeded the active-time budget"
                    )
    except Exception as exc:
        partial = getattr(exc, "partial", b"")
        if isinstance(partial, bytes) and partial:
            chunks.append(partial)
        if not chunks:
            raise
        failed_result = FetchResult(
            data=b"".join(chunks),
            effective_url=effective_url,
            retrieved_at=retrieved,
            content_type=content_type,
            elapsed_seconds=time.monotonic() - started,
        )
        raise TradeTransportError(str(exc), failed_result) from exc
    else:
        elapsed = time.monotonic() - started
        return FetchResult(
            data=b"".join(chunks),
            effective_url=effective_url,
            retrieved_at=retrieved,
            content_type=content_type,
            elapsed_seconds=elapsed,
        )


def _plan_payload(profile: TradeProfile) -> dict:
    return {
        "profile": profile.name,
        "schema_version": SCHEMA_VERSION,
        "slices": [asdict(request) for request in profile.slices],
        "limits": {
            key: value
            for key, value in asdict(profile).items()
            if key.startswith("max_") or key == "reserved_payload_bytes"
        },
    }


def _read_attempts(state: Path) -> list[dict]:
    attempts = []
    for path in sorted(state.glob("attempt-*.json")):
        try:
            attempts.append(json.loads(path.read_text()))
        except (json.JSONDecodeError, OSError) as exc:
            raise TradeSourceError(f"Invalid checkpoint metadata: {path.name}") from exc
    return attempts


def _atomic_json(path: Path, value: Mapping) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _write_attempt(state: Path, record: Mapping) -> None:
    sequence = len(list(state.glob("attempt-*.json"))) + 1
    _atomic_json(state / f"attempt-{sequence:03d}.json", record)


def _accepted_slice(
    state: Path, request: TradeSlice, attempts: Iterable[Mapping]
) -> tuple[pa.Table, dict, dict] | None:
    for attempt in reversed(list(attempts)):
        if attempt.get("slice_id") != request.slice_id or not attempt.get("accepted"):
            continue
        response_path = state / str(attempt["response_file"])
        try:
            data = response_path.read_bytes()
        except OSError as exc:
            raise TradeSourceError("Accepted checkpoint response is missing") from exc
        if hashlib.sha256(data).hexdigest() != attempt.get("sha256"):
            raise TradeSourceError("Accepted checkpoint checksum mismatch")
        table, coverage = parse_trade_response(data, request)
        return table, coverage, dict(attempt)
    return None


def _budget(
    profile: TradeProfile, attempts: list[dict], batch: int
) -> tuple[int, float]:
    batch_attempts = [attempt for attempt in attempts if attempt.get("batch") == batch]
    if len(attempts) >= profile.max_total_attempts:
        raise TradeSourceError("Global trade attempt budget exhausted")
    if len(batch_attempts) >= profile.max_batch_attempts:
        raise TradeSourceError("Trade batch attempt budget exhausted")
    total_bytes = profile.reserved_payload_bytes + sum(
        int(attempt.get("byte_count", 0)) for attempt in attempts
    )
    batch_bytes = sum(int(attempt.get("byte_count", 0)) for attempt in batch_attempts)
    byte_limit = min(
        profile.max_response_bytes,
        profile.max_total_bytes - total_bytes,
        profile.max_batch_bytes - batch_bytes,
    )
    total_time = sum(float(attempt.get("elapsed_seconds", 0)) for attempt in attempts)
    batch_time = sum(
        float(attempt.get("elapsed_seconds", 0)) for attempt in batch_attempts
    )
    time_limit = min(
        60.0,
        profile.max_total_seconds - total_time,
        profile.max_batch_seconds - batch_time,
    )
    if byte_limit <= 0 or time_limit <= 0:
        raise TradeSourceError("Trade byte or active-time budget exhausted")
    return byte_limit, time_limit


def combine_trade_tables(tables: list[pa.Table]) -> pa.Table:
    """Combine disjoint slices and reject every repeated natural key."""
    combined = (
        pa.concat_tables(tables) if tables else pa.Table.from_pylist([], TRADE_SCHEMA)
    )
    key_fields = (
        "source_dataset",
        "frequency_code",
        "reporter_code",
        "partner_code",
        "product_code",
        "flow_code",
        "indicator_code",
        "period_month",
    )
    seen = {}
    for row in combined.to_pylist():
        key = tuple(row[field] for field in key_fields)
        if key in seen:
            detail = "conflicting" if seen[key] != row else "duplicate"
            raise TradeSourceError(f"{detail.capitalize()} trade observation: {key}")
        seen[key] = row
    return combined


def publish_trade_profile(
    profile: TradeProfile,
    paths: ProjectPaths,
    transport: Transport = fetch_trade_slice,
    *,
    fixture: bool = False,
    capture_vintage: str = "initial",
) -> dict:
    """Resume verified slices and replace current Parquet only after full validation."""
    if not profile.slices:
        raise TradeSourceError("Trade profile has no slices")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", capture_vintage):
        raise TradeSourceError("Invalid capture vintage")
    plan = json.loads(json.dumps(_plan_payload(profile)))
    if capture_vintage != "initial":
        plan["capture_vintage"] = capture_vintage
    plan_bytes = json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    plan_hash = hashlib.sha256(plan_bytes).hexdigest()
    state = paths.state / "trade" / plan_hash
    state.mkdir(parents=True, exist_ok=True)
    plan_path = state / "plan.json"
    if plan_path.exists():
        if json.loads(plan_path.read_text()) != plan:
            raise TradeSourceError("Checkpoint plan changed")
    else:
        _atomic_json(plan_path, plan)
    tables = []
    slices = []
    for request in profile.slices:
        attempts = _read_attempts(state)
        accepted = _accepted_slice(state, request, attempts)
        if accepted is None:
            slice_attempts = [
                attempt
                for attempt in attempts
                if attempt.get("slice_id") == request.slice_id
            ]
            if len(slice_attempts) >= profile.max_slice_attempts:
                raise TradeSourceError(
                    f"Attempt budget exhausted for {request.slice_id}"
                )
            byte_limit, time_limit = _budget(profile, attempts, request.batch)
            started = datetime.now(UTC)
            result = None
            try:
                result = transport(request, byte_limit, time_limit)
                if len(result.data) > byte_limit:
                    raise TradeSourceError(
                        "Trade response exceeds the remaining byte budget"
                    )
                table, coverage = parse_trade_response(result.data, request)
            except Exception as exc:
                elapsed = max(0.0, (datetime.now(UTC) - started).total_seconds())
                failed: dict[str, object] = {}
                failed_result = (
                    result
                    if isinstance(result, FetchResult)
                    else getattr(exc, "result", None)
                )
                if isinstance(failed_result, FetchResult):
                    digest = hashlib.sha256(failed_result.data).hexdigest()
                    failed_file = f"failed-{request.slice_id}-{digest}.response"
                    (state / failed_file).write_bytes(failed_result.data)
                    failed = {
                        "byte_count": len(failed_result.data),
                        "content_type": failed_result.content_type,
                        "effective_url": failed_result.effective_url,
                        "elapsed_seconds": failed_result.elapsed_seconds,
                        "response_file": failed_file,
                        "retrieved_at_utc": failed_result.retrieved_at.astimezone(
                            UTC
                        ).isoformat(),
                        "sha256": digest,
                    }
                _write_attempt(
                    state,
                    {
                        "accepted": False,
                        "batch": request.batch,
                        "byte_count": 0,
                        "elapsed_seconds": elapsed,
                        "error": f"{type(exc).__name__}: {exc}",
                        "slice_id": request.slice_id,
                        "started_at_utc": started.isoformat(),
                        **failed,
                    },
                )
                raise TradeSourceError(
                    f"Slice {request.slice_id} failed; run the same plan to continue"
                ) from exc
            sha256 = hashlib.sha256(result.data).hexdigest()
            response_file = f"response-{request.slice_id}-{sha256}.json"
            response_path = state / response_file
            if response_path.exists() and response_path.read_bytes() != result.data:
                raise TradeSourceError("Checkpoint response path collision")
            response_path.write_bytes(result.data)
            attempt = {
                "accepted": True,
                "batch": request.batch,
                "byte_count": len(result.data),
                "content_type": result.content_type,
                "effective_url": result.effective_url,
                "elapsed_seconds": result.elapsed_seconds,
                "http_status": None if fixture else 200,
                "response_file": response_file,
                "retrieved_at_utc": result.retrieved_at.astimezone(UTC).isoformat(),
                "sha256": sha256,
                "slice_id": request.slice_id,
            }
            _write_attempt(state, attempt)
            accepted = table, coverage, attempt
        table, coverage, attempt = accepted
        tables.append(table)
        capture = dict(attempt)
        capture.setdefault("http_status", None if fixture else 200)
        slices.append(
            {"request": asdict(request), "capture": capture, "coverage": coverage}
        )
    table = combine_trade_tables(tables)
    source_updates = {record["coverage"]["source_update_time"] for record in slices}
    if len(source_updates) != 1:
        raise TradeSourceError("Trade slices report mixed source update timestamps")
    paths.parquet.mkdir(parents=True, exist_ok=True)
    paths.raw.mkdir(parents=True, exist_ok=True)
    for slice_record in slices:
        capture = slice_record["capture"]
        checkpoint = state / capture["response_file"]
        raw = paths.raw / f"eurostat_trade_{capture['sha256']}.json"
        if (
            raw.exists()
            and hashlib.sha256(raw.read_bytes()).hexdigest() != capture["sha256"]
        ):
            raise TradeSourceError("Existing raw trade capture checksum mismatch")
        if not raw.exists():
            raw.write_bytes(checkpoint.read_bytes())
    output = paths.parquet / "trade_observations.parquet"
    manifest_path = paths.parquet / "trade_observations.json"
    with tempfile.NamedTemporaryFile(
        dir=paths.parquet,
        prefix=".trade_observations.",
        suffix=".parquet",
        delete=False,
    ) as handle:
        staged = Path(handle.name)
    try:
        pq.write_table(table, staged, compression="zstd")
        verified = pq.read_table(staged)
        if verified.schema != TRADE_SCHEMA or verified.num_rows != table.num_rows:
            raise TradeSourceError("Staged trade Parquet verification failed")
        manifest = {
            "dataset": DATASET,
            "fixture": fixture,
            "plan_hash": plan_hash,
            "capture_vintage": capture_vintage,
            "profile": profile.name,
            "row_count": table.num_rows,
            "reserved_payload_bytes": profile.reserved_payload_bytes,
            "schema_version": SCHEMA_VERSION,
            "slice_count": len(profile.slices),
            "source_updates": sorted(source_updates),
            "status_available_in_any_slice": any(
                record["coverage"]["status_available"] for record in slices
            ),
            "total_payload_bytes": sum(
                record["capture"]["byte_count"] for record in slices
            ),
            "transport_complete": True,
            "parquet_path": str(output),
            "slices": slices,
        }
        os.replace(staged, output)
        _atomic_json(manifest_path, manifest)
        return manifest
    finally:
        staged.unlink(missing_ok=True)
