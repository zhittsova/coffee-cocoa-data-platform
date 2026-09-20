"""Independent revision state oracles and publication failure checks."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from coffee_cocoa_platform.paths import ProjectPaths
from coffee_cocoa_platform.price_fixture import synthetic_workbook
from coffee_cocoa_platform.prices import publish_workbook
from coffee_cocoa_platform.revisions import (
    RevisionError,
    read_selection,
    run_replacement,
)
from coffee_cocoa_platform.trade_fixture import (
    FIXTURE_PRODUCTS,
    _response,
    fixture_profile,
    fixture_transport,
)
from coffee_cocoa_platform.trades import (
    FetchResult,
    TradeSourceError,
    parse_trade_response,
    publish_trade_profile,
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def revision_transport(version):
    def transport(request, max_bytes, deadline_seconds):
        document = json.loads(_response(request))
        if version != "A":
            document["updated"] = "2026-09-01T09:00:00+0000"
        if request.start == "2021-12":
            if version == "B":
                # BR coffee imports 1200; WORLD 1500; withdraw BR chocolate;
                # add QS coffee imports 100 EUR / 100 kg. Flat keys are independent
                # of adapter decoding: 3 partners x 2 products x 2 flows x 2 measures.
                document["value"]["0"] = 1200
                document["value"]["16"] = 1500
                del document["value"]["4"]
                document["status"] = {}
                document["value"]["8"] = 100
                document["value"]["9"] = 1
            elif version == "empty":
                document["value"] = {}
                document["status"] = {}
        data = json.dumps(document, sort_keys=True).encode()
        return FetchResult(
            data,
            request.url,
            datetime(2026, 9, 2, tzinfo=UTC),
            "application/json",
            0.001,
        )

    return transport


def captures(root, version="A", profile=None):
    paths = ProjectPaths.from_root(root)
    publish_workbook(
        synthetic_workbook(),
        start="2024-01",
        end="2024-04",
        paths=paths,
        source_url="synthetic://prices",
        effective_url="synthetic://prices",
        retrieved_at=datetime(2026, 8, 15, tzinfo=UTC),
        fixture=True,
    )
    publish_trade_profile(
        profile or fixture_profile(),
        paths,
        revision_transport(version),
        fixture=True,
        capture_vintage=version,
    )
    return {
        "schema_version": "1",
        "prices": {
            "manifest": str(paths.parquet / "benchmark_prices.json"),
            "manifest_sha256": digest(paths.parquet / "benchmark_prices.json"),
            "start": "2024-01",
            "end": "2024-04",
            "series": ["cocoa", "coffee_arabica", "coffee_robusta"],
        },
        "trade": {
            "manifest": str(paths.parquet / "trade_observations.json"),
            "manifest_sha256": digest(paths.parquet / "trade_observations.json"),
            "start": "2021-12",
            "end": "2022-01",
            "reporter": "DE",
            "products": list(FIXTURE_PRODUCTS),
            "flows": ["1", "2"],
        },
    }


def snapshot(root):
    with duckdb.connect(
        str(root / "warehouse/coffee_cocoa.duckdb"), read_only=True
    ) as con:
        names = [
            row[0]
            for row in con.execute("show tables").fetchall()
            if not row[0].startswith("_")
        ]
        return {
            name: con.execute(f'select * from "{name}" order by all').fetchall()
            for name in names
        }


def query(root, sql):
    with duckdb.connect(
        str(root / "warehouse/coffee_cocoa.duckdb"), read_only=True
    ) as con:
        return con.execute(sql).fetchall()


def test_empty_requires_explicit_complete_envelope():
    request = fixture_profile().slices[0]
    document = json.loads(_response(request))
    document.pop("value")
    with pytest.raises(TradeSourceError, match="explicit value"):
        parse_trade_response(json.dumps(document).encode(), request)
    document["value"], document["status"] = {}, {}
    assert parse_trade_response(json.dumps(document).encode(), request)[0].num_rows == 0
    document["dimension"].pop("time")
    with pytest.raises(TradeSourceError, match="Missing dimension"):
        parse_trade_response(json.dumps(document).encode(), request)


def test_selection_rejects_incomplete_scope_schema_and_checksum(tmp_path):
    request = captures(tmp_path / "capture")
    paths = ProjectPaths.from_root(tmp_path / "selected")
    selection = dict(request["trade"], product_groups=["coffee_unroasted"], products=[])
    with pytest.raises(RevisionError, match="exceeds complete"):
        read_selection("trade", selection, paths)
    manifest_path = Path(request["trade"]["manifest"])
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = "2"
    manifest_path.write_text(json.dumps(manifest))
    request["trade"]["manifest_sha256"] = digest(manifest_path)
    with pytest.raises(RevisionError, match="schema"):
        read_selection("trade", request["trade"], paths)
    manifest["schema_version"] = "1"
    manifest["transport_complete"] = False
    manifest_path.write_text(json.dumps(manifest))
    request["trade"]["manifest_sha256"] = digest(manifest_path)
    with pytest.raises(RevisionError, match="Incomplete"):
        read_selection("trade", request["trade"], paths)
    manifest["transport_complete"] = True
    manifest_path.write_text(json.dumps(manifest))
    request["trade"]["manifest_sha256"] = digest(manifest_path)
    checksum = manifest["slices"][0]["capture"]["sha256"]
    (
        manifest_path.parent.parent / "raw" / f"eurostat_trade_{checksum}.json"
    ).write_bytes(b"bad")
    with pytest.raises(RevisionError, match="checksum"):
        read_selection("trade", request["trade"], paths)


def test_new_capture_vintage_fetches_again(tmp_path):
    calls = []

    def transport(*args):
        calls.append(args[0].slice_id)
        return fixture_transport(*args)

    paths = ProjectPaths.from_root(tmp_path)
    for vintage in ("A", "A", "B"):
        publish_trade_profile(
            fixture_profile(), paths, transport, fixture=True, capture_vintage=vintage
        )
    assert len(calls) == 4
    assert len(list(paths.state.glob("trade/*/plan.json"))) == 2


@pytest.mark.integration
def test_replacement_transitions_and_full_refresh(tmp_path, monkeypatch):
    root = tmp_path / "published"
    a = captures(tmp_path / "A")
    b = captures(tmp_path / "B", "B")
    empty = captures(tmp_path / "empty", "empty")
    run_replacement(root, a)
    original = snapshot(root)
    assert len(original["fct_trade_observations"]) == 9
    run_replacement(root, a)
    assert snapshot(root) == original
    assert query(root, "select count(*) from _revision_history") == [(2,)]
    assert query(
        root, "select json_array_length(json_keys(captures)) from _revision_state"
    ) == [(3,)]
    b_selection = {
        "schema_version": "1",
        "trade": dict(b["trade"], end="2021-12", flows=["1"]),
    }
    untouched_sql = "select * from fct_trade_observations where month_key = '2022-01-01' or flow_code = '2' order by all"
    untouched = query(root, untouched_sql)
    run_replacement(root, b_selection)
    assert query(root, untouched_sql) == untouched
    expected = [
        ("BR", "09011100", "1", 1200, 550),
        ("QS", "09011100", "1", 100, 100),
        ("QS", "09011100", "2", 200, 0),
        ("WORLD", "09011100", "1", 1500, 700),
        ("WORLD", "09011100", "2", 250, 100),
    ]
    assert (
        query(
            root,
            "select partner_code, product_code, flow_code, trade_value_eur, net_mass_kg from fct_trade_observations where month_key='2021-12-01' order by 1,2,3",
        )
        == expected
    )
    assert query(
        root,
        "select trade_balance_eur from monthly_trade_balances where month_key='2021-12-01'",
    ) == [(-1250,)]
    assert query(
        root,
        "select named_partner_value_eur, observed_special_value_eur, reconciliation_residual_eur from monthly_partner_concentration where month_key='2021-12-01' and flow_code='1'",
    ) == [(1200, 100, 200)]
    revised = snapshot(root)
    # Full refresh uses the identical stored source union and selected scope, so
    # differences in any staging/dimension/fact/mart result are observable.
    run_replacement(root, b_selection, full_refresh=True)
    assert snapshot(root) == revised
    empty_selection = {
        "schema_version": "1",
        "trade": dict(empty["trade"], end="2021-12", flows=["1"]),
    }
    run_replacement(root, empty_selection)
    emptied = snapshot(root)
    assert len(emptied["fct_trade_observations"]) == 6
    assert len(emptied["stg_trade_observations"]) == 12
    assert query(root, untouched_sql) == untouched
    assert query(
        root,
        "select import_value_cif_eur, export_value_fob_eur, trade_balance_eur from monthly_trade_balances where month_key='2021-12-01'",
    ) == [(None, 250, None)]
    run_replacement(root, empty_selection, full_refresh=True)
    assert snapshot(root) == emptied
    # Explicit older vintage restores withdrawals and respects both CN years.
    run_replacement(root, a)
    assert snapshot(root) == original
    assert query(
        root,
        "select distinct classification_year, comparability_segment from fct_trade_observations where product_code='18069090' order by 1",
    ) == [(2021, "2017-2021"), (2022, "2022-2026")]
    # A validated candidate that fails its final publication cannot leak tables,
    # lineage, selection history or metadata into the published database.
    database = root / "warehouse/coffee_cocoa.duckdb"
    before_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    with monkeypatch.context() as patch:

        def fail_publish(*args):
            raise OSError("injected publication failure")

        patch.setattr("coffee_cocoa_platform.revisions.os.replace", fail_publish)
        with pytest.raises(OSError, match="injected"):
            run_replacement(root, b_selection)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before_hash
    assert snapshot(root) == original
    failed = [
        json.loads(p.read_text())
        for p in (root / ".state/replacements").glob("*/run.json")
    ]
    assert any(event["status"] == "failed_unpublished" for event in failed)
    run_replacement(root, b_selection)
    assert snapshot(root) == revised
    # A new month is additive even when selected after a later source timestamp.
    profile = fixture_profile()
    february = replace(
        profile.slices[1], slice_id="fixture-february", start="2022-02", end="2022-02"
    )
    new = captures(tmp_path / "new", profile=replace(profile, slices=(february,)))
    new_request = {
        "schema_version": "1",
        "trade": dict(new["trade"], start="2022-02", end="2022-02"),
    }
    run_replacement(root, new_request)
    added = snapshot(root)
    assert len(added["fct_trade_observations"]) == 13
    run_replacement(root, new_request, full_refresh=True)
    assert snapshot(root) == added


@pytest.mark.integration
def test_benchmark_revision_and_failed_candidate(tmp_path, monkeypatch):
    from decimal import Decimal
    from io import BytesIO
    from zipfile import ZipFile

    root = tmp_path / "published"
    a = captures(tmp_path / "A")
    run_replacement(root, a)
    original = snapshot(root)
    database = root / "warehouse/coffee_cocoa.duckdb"
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    # Failure after a staging mutation leaves every published table and metadata
    # at A, even though the candidate itself has become inconsistent.
    with monkeypatch.context() as patch:

        def fail_after_staging(candidate, scopes, full_refresh):
            with duckdb.connect(
                str(candidate.warehouse / "coffee_cocoa.duckdb")
            ) as con:
                con.execute("delete from stg_trade_observations")
            raise RevisionError("injected failure after staging")

        patch.setattr("coffee_cocoa_platform.revisions._dbt_build", fail_after_staging)
        with pytest.raises(RevisionError, match="after staging"):
            run_replacement(root, a)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    assert snapshot(root) == original
    # The locked dbt adapter must reject a staging schema difference explicitly.
    from coffee_cocoa_platform.revisions import _dbt_build

    with monkeypatch.context() as patch:

        def incompatible_staging(candidate, scopes, full_refresh):
            with duckdb.connect(
                str(candidate.warehouse / "coffee_cocoa.duckdb")
            ) as con:
                con.execute(
                    "alter table stg_benchmark_prices add column unexpected integer"
                )
            _dbt_build(candidate, scopes, full_refresh)

        patch.setattr(
            "coffee_cocoa_platform.revisions._dbt_build", incompatible_staging
        )
        with pytest.raises(RevisionError, match="dbt build failed"):
            run_replacement(root, a)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    assert any(
        "source and target schemas on this incremental model are out of sync"
        in p.read_text()
        for p in (root / ".state/replacements").glob("*/dbt-build.log")
    )
    broken = dict(a, trade=dict(a["trade"], end="2022-02"))
    with pytest.raises(RevisionError, match="exceeds complete"):
        run_replacement(root, broken)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    manifest_path = Path(a["trade"]["manifest"])
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = "99"
    manifest_path.write_text(json.dumps(manifest))
    a["trade"]["manifest_sha256"] = digest(manifest_path)
    with pytest.raises(RevisionError, match="schema"):
        run_replacement(root, a)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    manifest["schema_version"] = "1"
    manifest_path.write_text(json.dumps(manifest))
    a["trade"]["manifest_sha256"] = digest(manifest_path)
    output = BytesIO()
    with (
        ZipFile(BytesIO(synthetic_workbook())) as source,
        ZipFile(output, "w") as target,
    ):
        for item in source.infolist():
            content = source.read(item.filename)
            if item.filename == "xl/worksheets/sheet2.xml":
                content = content.replace(b"4.4000000000000004", b"4.0", 1)
            target.writestr(item, content)
    price_paths = ProjectPaths.from_root(tmp_path / "price_B")
    publish_workbook(
        output.getvalue(),
        start="2024-01",
        end="2024-04",
        paths=price_paths,
        source_url="synthetic://prices",
        effective_url="synthetic://prices",
        retrieved_at=datetime(2026, 9, 2, tzinfo=UTC),
        fixture=True,
    )
    request = {
        "schema_version": "1",
        "prices": {
            "manifest": str(price_paths.parquet / "benchmark_prices.json"),
            "manifest_sha256": digest(price_paths.parquet / "benchmark_prices.json"),
            "start": "2024-01",
            "end": "2024-01",
            "series": ["cocoa"],
        },
    }
    # Exercise the real manual Dagster operation, not only its Python function.
    from coffee_cocoa_platform.revision_job import source_replacement_job

    result = source_replacement_job.execute_in_process(
        run_config={
            "ops": {
                "replace_source_partitions": {
                    "config": {"root": str(root), "request": request}
                }
            }
        }
    )
    assert result.success
    assert query(
        root,
        "select price_usd_per_kg, change_usd_per_kg from monthly_benchmark_prices where series_id='cocoa' and period_month='2024-02-01'",
    ) == [(Decimal("4.84"), Decimal("0.84"))]
    revised = snapshot(root)
    assert revised["fct_trade_observations"] == original["fct_trade_observations"]
    run_replacement(root, request, full_refresh=True)
    assert snapshot(root) == revised
    # An absent selected month is valid when the encompassing capture validates.
    request["prices"].update(start="2024-03", end="2024-03")
    run_replacement(root, request)
    assert snapshot(root) == revised


def test_manifest_selection_is_pinned_and_first_capture_uses_earliest_evidence(
    tmp_path,
):
    from coffee_cocoa_platform.revisions import _record_capture

    request = captures(tmp_path / "capture")
    manifest = Path(request["trade"]["manifest"])
    manifest.write_text(manifest.read_text() + "\n")
    with pytest.raises(RevisionError, match="Manifest checksum mismatch"):
        read_selection(
            "trade", request["trade"], ProjectPaths.from_root(tmp_path / "selected")
        )
    registry = {}
    late = {"capture_id": "same-bytes", "retrieved_at_utc": "2026-09-02T00:00:00+00:00"}
    early = {
        "capture_id": "same-bytes",
        "retrieved_at_utc": "2026-08-15T00:00:00+00:00",
    }
    _record_capture(registry, late, "2026-09-20T01:00:00+00:00")
    _record_capture(registry, early, "2026-09-20T02:00:00+00:00")
    assert (
        registry["same-bytes"]["first_seen_capture_at_utc"] == early["retrieved_at_utc"]
    )
    assert (
        registry["same-bytes"]["first_selected_at_utc"] == "2026-09-20T01:00:00+00:00"
    )
