"""S05 trade grain, completeness, budget and failure checks."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.trade_fixture import fixture_profile, fixture_transport
from coffee_cocoa_platform.trades import (
    PRODUCTS,
    SUPPORTED_PARTNERS,
    FetchResult,
    TradeSourceError,
    TradeTransportError,
    combine_trade_tables,
    fetch_trade_slice,
    make_profile,
    parse_trade_response,
    publish_trade_profile,
)


def fixture_result(index: int = 0) -> tuple:
    request = fixture_profile().slices[index]
    result = fixture_transport(request, 64 * 1024, 10)
    return request, result


def test_full_profile_is_the_validated_50_slice_plan():
    profile = make_profile("full", "2017-01", "2026-08")

    assert len(PRODUCTS) == len(set(PRODUCTS)) == 33
    assert len(SUPPORTED_PARTNERS) == 278
    assert len(profile.slices) == 50
    assert {request.batch for request in profile.slices} == set(range(5))
    assert all(len(request.products) <= 8 for request in profile.slices)
    assert all(request.partners is None for request in profile.slices)
    assert profile.max_total_bytes == 64 * 1024 * 1024
    assert profile.reserved_payload_bytes == 2 * 1024 * 1024
    assert profile.max_total_attempts == 150
    assert profile.max_total_seconds == 1800
    for year in range(2017, 2027):
        slices = [
            request for request in profile.slices if request.start[:4] == str(year)
        ]
        assert len(slices) == 5
        assert {product for request in slices for product in request.products} == set(
            PRODUCTS
        )
        assert sum(len(request.products) for request in slices) == len(PRODUCTS)
    assert profile.slices[-1].end == "2026-08"


def test_smoke_profile_keeps_original_narrow_filters():
    profile = make_profile("smoke", "2024-01", "2024-01")

    assert len(profile.slices) == 1
    request = profile.slices[0]
    assert request.products == ("09011100", "18010000")
    assert request.partners == ("BR", "CI", "GH", "VN", "WORLD")
    assert "partner=WORLD" in request.url
    assert request.url.count("flow=") == 2
    assert request.url.count("indicators=") == 2


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2016-12", "2017-01"),
        ("2026-08", "2026-09"),
        ("2024-02", "2024-01"),
    ],
)
def test_profile_rejects_unsupported_bounds(start, end):
    with pytest.raises(TradeSourceError):
        make_profile("full", start, end)


def test_fixture_rows_preserve_sparse_status_units_and_boundary_identity():
    request, result = fixture_result()
    table, coverage = parse_trade_response(result.data, request)
    rows = {
        (
            row["partner_code"],
            row["product_code"],
            row["flow_code"],
            row["indicator_code"],
        ): row
        for row in table.to_pylist()
    }

    assert table.num_rows == 10
    assert coverage["source_update_time"] == "2026-08-14T11:00:00+0200"
    assert coverage["partner_count"] == 3
    assert coverage["represented_cell_count"] == 10
    assert coverage["value_cell_count"] == 9
    assert coverage["status_cell_count"] == 1
    assert coverage["explicit_zero_count"] == 1
    assert coverage["status_available"] is True
    assert coverage["first_observed_month"] == "2021-12"
    assert coverage["last_observed_month"] == "2021-12"
    cocoa_coverage = next(
        item
        for item in coverage["series_coverage"]
        if item["partner_code"] == "BR"
        and item["product_code"] == "18069090"
        and item["flow_code"] == "1"
    )
    assert cocoa_coverage["last_value_month"] == "2021-12"
    assert cocoa_coverage["last_quantity_month"] is None
    assert cocoa_coverage["last_paired_month"] is None
    assert rows[("BR", "09011100", "1", "VALUE_IN_EUROS")]["source_value"] == Decimal(
        "1000.0000"
    )
    assert rows[("QS", "09011100", "2", "QUANTITY_IN_100KG")][
        "source_value"
    ] == Decimal("0.0000")
    unavailable = rows[("BR", "18069090", "1", "QUANTITY_IN_100KG")]
    assert unavailable["source_value"] is None
    assert unavailable["source_status"] == "u"
    assert unavailable["status_available"] is True
    assert unavailable["classification_year"] == 2021
    assert ("BR", "18069090", "2", "VALUE_IN_EUROS") not in rows


def test_response_without_status_records_capability_as_unavailable():
    request, result = fixture_result(1)
    table, coverage = parse_trade_response(result.data, request)

    assert coverage["status_available"] is False
    assert all(row["status_available"] is False for row in table.to_pylist())
    assert all(row["source_status"] is None for row in table.to_pylist())
    assert {row["classification_year"] for row in table.to_pylist()} == {2022}


@pytest.mark.parametrize("dimension", ["partner", "product"])
def test_response_rejects_incomplete_partner_or_parent_product_scope(dimension):
    request, result = fixture_result()
    document = json.loads(result.data)
    index = document["dimension"][dimension]["category"]["index"]
    if dimension == "partner":
        index.pop("WORLD")
        document["size"][2] -= 1
    else:
        position = index.pop("09011100")
        index["0901"] = position
    data = json.dumps(document).encode()

    with pytest.raises(TradeSourceError, match=f"Unexpected {dimension}"):
        parse_trade_response(data, request)


def test_conflicting_duplicate_key_is_rejected():
    request, result = fixture_result()
    table, _ = parse_trade_response(result.data, request)
    changed_rows = table.to_pylist()
    changed_rows[0]["source_value"] = Decimal("9999.0000")
    changed = pa.Table.from_pylist(changed_rows, schema=table.schema)

    with pytest.raises(TradeSourceError, match="Conflicting trade observation"):
        combine_trade_tables([table, changed])


def test_failed_slice_requires_explicit_resume_before_publication(tmp_path):
    paths = ProjectPaths.from_root(tmp_path)
    profile = fixture_profile()
    calls = []
    failed = False

    def flaky(request, max_bytes, deadline_seconds):
        nonlocal failed
        calls.append(request.slice_id)
        if request.slice_id == "fixture-2022" and not failed:
            failed = True
            raise TimeoutError("simulated retryable timeout")
        return fixture_transport(request, max_bytes, deadline_seconds)

    with pytest.raises(TradeSourceError, match="run the same plan to continue"):
        publish_trade_profile(profile, paths, flaky, fixture=True)
    assert not (paths.parquet / "trade_observations.parquet").exists()

    manifest = publish_trade_profile(profile, paths, flaky, fixture=True)

    assert calls == ["fixture-2021", "fixture-2022", "fixture-2022"]
    assert manifest["transport_complete"] is True
    assert manifest["slice_count"] == 2
    assert manifest["row_count"] == 18
    attempts = [
        json.loads(path.read_text())
        for path in sorted(
            (paths.state / "trade" / manifest["plan_hash"]).glob("attempt-*.json")
        )
    ]
    assert [attempt["accepted"] for attempt in attempts] == [True, False, True]


def test_invalid_continuation_retains_failed_bytes_and_previous_publication(tmp_path):
    paths = ProjectPaths.from_root(tmp_path)
    good_manifest = publish_trade_profile(
        fixture_profile(), paths, fixture_transport, fixture=True
    )
    output = paths.parquet / "trade_observations.parquet"
    before = output.read_bytes()
    bad_profile = replace(fixture_profile(), name="invalid-fixture")

    def malformed(request, max_bytes, deadline_seconds):
        return FetchResult(
            data=b'{"class":"dataset"}',
            effective_url=request.url,
            retrieved_at=datetime(2026, 8, 15, tzinfo=UTC),
            content_type="application/json",
            elapsed_seconds=0.01,
        )

    with pytest.raises(TradeSourceError):
        publish_trade_profile(bad_profile, paths, malformed, fixture=True)
    assert output.read_bytes() == before
    bad_plan = hashlib.sha256(
        json.dumps(
            {
                "profile": bad_profile.name,
                "schema_version": "1",
                "slices": [
                    {
                        "slice_id": request.slice_id,
                        "batch": request.batch,
                        "products": list(request.products),
                        "partners": list(request.partners),
                        "start": request.start,
                        "end": request.end,
                        "url": request.url,
                    }
                    for request in bad_profile.slices
                ],
                "limits": {
                    key: value
                    for key, value in bad_profile.__dict__.items()
                    if key.startswith("max_") or key == "reserved_payload_bytes"
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    attempts = list((paths.state / "trade" / bad_plan).glob("attempt-*.json"))
    assert len(attempts) == 1
    attempt = json.loads(attempts[0].read_text())
    assert attempt["byte_count"] == len(b'{"class":"dataset"}')
    assert (paths.state / "trade" / bad_plan / attempt["response_file"]).exists()
    assert pq.read_table(output).num_rows == good_manifest["row_count"]


def test_interrupted_transport_retains_and_budgets_partial_bytes(tmp_path):
    paths = ProjectPaths.from_root(tmp_path)
    partial = b'{"class":"dataset"'

    def interrupted(request, max_bytes, deadline_seconds):
        result = FetchResult(
            data=partial,
            effective_url=request.url,
            retrieved_at=datetime(2026, 8, 15, tzinfo=UTC),
            content_type="application/json",
            elapsed_seconds=0.25,
        )
        raise TradeTransportError("simulated partial download", result)

    with pytest.raises(TradeSourceError, match="run the same plan to continue"):
        publish_trade_profile(fixture_profile(), paths, interrupted, fixture=True)

    plan_dirs = list((paths.state / "trade").iterdir())
    assert len(plan_dirs) == 1
    attempts = list(plan_dirs[0].glob("attempt-*.json"))
    assert len(attempts) == 1
    attempt = json.loads(attempts[0].read_text())
    assert attempt["byte_count"] == len(partial)
    assert attempt["elapsed_seconds"] == 0.25
    assert (plan_dirs[0] / attempt["response_file"]).read_bytes() == partial
    assert not (paths.parquet / "trade_observations.parquet").exists()


def test_live_transport_exposes_bytes_read_before_interruption(monkeypatch):
    request = make_profile("smoke", "2024-01", "2024-01").slices[0]

    class InterruptedResponse:
        status = 200

        def __init__(self):
            self.headers = {"Content-Type": "application/json"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def geturl(self):
            return request.url

        def read(self, size):
            if not hasattr(self, "read_once"):
                self.read_once = True
                return b'{"class"'
            raise ConnectionError("simulated interrupted response")

    monkeypatch.setattr(
        "coffee_cocoa_platform.trades.urllib.request.urlopen",
        lambda http_request, timeout: InterruptedResponse(),
    )

    with pytest.raises(TradeTransportError) as raised:
        fetch_trade_slice(request, max_bytes=64 * 1024, deadline_seconds=10)

    assert raised.value.result.data == b'{"class"'
    assert raised.value.result.effective_url == request.url
    assert raised.value.result.content_type == "application/json"


def test_global_attempt_budget_blocks_incomplete_publication(tmp_path):
    paths = ProjectPaths.from_root(tmp_path)
    profile = replace(fixture_profile(), max_total_attempts=1)
    calls = []

    def counted(request, max_bytes, deadline_seconds):
        calls.append(request.slice_id)
        return fixture_transport(request, max_bytes, deadline_seconds)

    with pytest.raises(TradeSourceError, match="attempt budget exhausted"):
        publish_trade_profile(profile, paths, counted, fixture=True)

    assert calls == ["fixture-2021"]
    assert not (paths.parquet / "trade_observations.parquet").exists()


def test_mixed_source_updates_block_publication(tmp_path):
    paths = ProjectPaths.from_root(tmp_path)

    def mixed_updates(request, max_bytes, deadline_seconds):
        result = fixture_transport(request, max_bytes, deadline_seconds)
        if request.slice_id == "fixture-2022":
            document = json.loads(result.data)
            document["updated"] = "2026-08-15T11:00:00+0200"
            return replace(result, data=json.dumps(document).encode())
        return result

    with pytest.raises(TradeSourceError, match="mixed source update timestamps"):
        publish_trade_profile(fixture_profile(), paths, mixed_updates, fixture=True)

    assert not (paths.parquet / "trade_observations.parquet").exists()


def test_raw_checksum_conflict_blocks_republication(tmp_path):
    paths = ProjectPaths.from_root(tmp_path)
    manifest = publish_trade_profile(
        fixture_profile(), paths, fixture_transport, fixture=True
    )
    capture = manifest["slices"][0]["capture"]
    raw = paths.raw / f"eurostat_trade_{capture['sha256']}.json"
    raw.write_bytes(b"corrupt")

    with pytest.raises(TradeSourceError, match="checksum mismatch"):
        publish_trade_profile(fixture_profile(), paths, fixture_transport, fixture=True)
