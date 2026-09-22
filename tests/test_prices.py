"""S04 source-contract and failed-publication checks."""

import hashlib
import os
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.price_fixture import synthetic_workbook
from coffee_cocoa_platform.prices import (
    PriceSourceError,
    parse_workbook,
    publish_workbook,
)


def altered_workbook(data: bytes, old: bytes, new: bytes) -> bytes:
    output = BytesIO()
    with ZipFile(BytesIO(data)) as original, ZipFile(output, "w") as rewritten:
        for item in original.infolist():
            contents = original.read(item.filename)
            if item.filename == "xl/worksheets/sheet2.xml":
                assert old in contents
                contents = contents.replace(old, new, 1)
            rewritten.writestr(item, contents)
    return output.getvalue()


def test_synthetic_rows_have_independent_expected_values():
    data = synthetic_workbook()
    table, coverage = parse_workbook(
        data, "2024-01", "2024-04", hashlib.sha256(data).hexdigest()
    )
    rows = {
        (row["series_id"], row["period_month"].isoformat()): row
        for row in table.to_pylist()
    }
    assert table.num_rows == 9
    assert coverage["represented_month_count"] == 3
    assert coverage["missing_month_count"] == 1
    assert coverage["null_price_count"] == 1
    assert coverage["latest_observed_by_series"] == {
        "cocoa": "2024-04-01",
        "coffee_arabica": "2024-04-01",
        "coffee_robusta": "2024-04-01",
    }
    assert rows[("cocoa", "2024-01-01")]["source_value_text"] == "4.4000000000000004"
    assert rows[("cocoa", "2024-01-01")]["price_usd_per_kg"] == Decimal("4.40000000")
    assert rows[("coffee_robusta", "2024-02-01")]["price_usd_per_kg"] is None
    assert rows[("coffee_robusta", "2024-04-01")]["price_usd_per_kg"] == Decimal(
        "2.12345679"
    )
    assert not any(period == "2024-03-01" for _, period in rows)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b"Synthetic fixture; not World Bank observations", b""),
        (b">Cocoa<", b">Cooca<"),
        (b"($/kg)", b"EUR/kg"),
        (b"2024M02", b"2024M01"),
        (b"2024M02", b"2024M13"),
        (b"2024M02", b""),
        (
            b'<c r="L7" t="n"><v>4.4000000000000004</v></c>',
            b'<c r="L7" t="n"><v>NaN</v></c>',
        ),
        (
            b'<c r="L7" t="n"><v>4.4000000000000004</v></c>',
            b'<c r="L7" t="inlineStr"><is><t>unknown</t></is></c>',
        ),
        (
            b'<c r="L7" t="n"><v>4.4000000000000004</v></c>',
            b'<c r="L7"><f>1+3.4</f><v>4.4</v></c>',
        ),
    ],
)
def test_invalid_source_fails(old, new):
    data = altered_workbook(synthetic_workbook(), old, new)
    with pytest.raises(PriceSourceError):
        parse_workbook(data, "2024-01", "2024-04", "capture")


def test_truncated_capture_fails():
    with pytest.raises(PriceSourceError):
        parse_workbook(synthetic_workbook()[:100], "2024-01", "2024-04", "capture")


def test_explicit_missing_marker_stays_null():
    data = altered_workbook(
        synthetic_workbook(),
        b'<c r="L7" t="n"><v>4.4000000000000004</v></c>',
        b'<c r="L7" t="inlineStr"><is><t>..</t></is></c>',
    )
    table, coverage = parse_workbook(data, "2024-01", "2024-04", "capture")
    row = next(
        row
        for row in table.to_pylist()
        if row["series_id"] == "cocoa"
        and row["period_month"].isoformat() == "2024-01-01"
    )
    assert row["source_value_text"] == ".."
    assert row["price_usd_per_kg"] is None
    assert coverage["null_price_count"] == 2


def test_repeated_and_failed_publication_keep_valid_parquet(tmp_path, monkeypatch):
    paths = ProjectPaths.from_root(tmp_path)
    data = synthetic_workbook()

    def publish(value):
        return publish_workbook(
            value,
            start="2024-01",
            end="2024-04",
            paths=paths,
            source_url="synthetic://fixture-v1",
            effective_url="synthetic://fixture-v1",
            retrieved_at=datetime(2024, 5, 1, tzinfo=UTC),
            fixture=True,
        )

    first = publish(data)
    output = paths.parquet / "benchmark_prices.parquet"
    before = output.read_bytes()
    assert first["row_count"] == 9
    assert pq.read_table(output).num_rows == 9
    second = publish(data)
    assert second["sha256"] == first["sha256"]
    assert output.read_bytes() == before
    with pytest.raises(PriceSourceError):
        publish(data[:100])
    assert output.read_bytes() == before

    raw_path = paths.raw / f"world_bank_monthly_{first['sha256']}.xlsx"
    raw_path.write_bytes(b"corrupt")
    with pytest.raises(PriceSourceError, match="checksum mismatch"):
        publish(data)
    assert output.read_bytes() == before
    raw_path.write_bytes(data)

    def interrupted_write(*args, **kwargs):
        raise OSError("simulated interrupted write")

    monkeypatch.setattr(
        "coffee_cocoa_platform.prices.pq.write_table", interrupted_write
    )
    with pytest.raises(OSError, match="interrupted write"):
        publish(data)
    assert output.read_bytes() == before


def test_definitions_import_does_not_download_or_materialize(tmp_path):
    environment = dict(os.environ, COFFEE_COCOA_HOME=str(tmp_path))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from coffee_cocoa_platform.price_assets import defs; "
                "g=defs.resolve_asset_graph(); "
                "assert len(g.get_all_asset_keys()) == 10; "
                "assert any(str(k) == \"AssetKey(['world_bank_prices', 'monthly_prices'])\" "
                "for k in g.get_all_asset_keys()); "
                "assert any(str(k) == \"AssetKey(['monthly_benchmark_dynamics'])\" "
                "for k in g.get_all_asset_keys())"
            ),
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert not list(tmp_path.iterdir())
