"""Independent expected dbt rows for the offline two-source pipeline."""

import json
import os
import subprocess
import sys
from datetime import date
from decimal import Decimal
from itertools import pairwise

import duckdb
import pytest


@pytest.mark.integration
def test_two_source_fixture_pipeline(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "coffee_cocoa_platform.pipeline_cli",
            "--root",
            str(tmp_path),
        ],
        env=dict(os.environ, DAGSTER_DISABLE_TELEMETRY="1"),
        capture_output=True,
        text=True,
        timeout=480,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    trade_manifest = json.loads(
        (tmp_path / "data/parquet/trade_observations.json").read_text()
    )
    price_manifest = json.loads(
        (tmp_path / "data/parquet/benchmark_prices.json").read_text()
    )
    with duckdb.connect(
        str(tmp_path / "warehouse/coffee_cocoa.duckdb"), read_only=True
    ) as connection:
        rows = connection.execute(
            "select partner_code, partner_class, is_partner_detail, product_code, "
            "classification_year, comparability_segment, flow_name, indicator_code, "
            "source_unit, source_value, normalized_unit, normalized_value, "
            "source_status, status_available, period_month "
            "from stg_trade_observations order by period_month, partner_code, "
            "product_code, flow_code, indicator_code"
        ).fetchall()
        price_count = connection.execute(
            "select count(*) from monthly_benchmark_prices"
        ).fetchone()[0]
        product_code_type = connection.execute(
            "select typeof(product_code) from trade_product_groups limit 1"
        ).fetchone()[0]
        fact_count = connection.execute(
            "select count(*) from fct_trade_observations"
        ).fetchone()[0]
        product_metrics = connection.execute(
            "select month_key, product_group, flow_name, world_value_eur, "
            "world_net_mass_kg, observed_unit_value_eur_per_kg, "
            "observed_product_value_share, universe_observed_value_product_count, "
            "declared_universe_expected_product_count "
            "from monthly_trade_product_metrics order by 1, 2, 3"
        ).fetchall()
        balances = connection.execute(
            "select month_key, product_group, import_value_cif_eur, "
            "export_value_fob_eur, has_compatible_flow_coverage, trade_balance_eur "
            "from monthly_trade_balances order by 1, 2"
        ).fetchall()
        concentration = connection.execute(
            "select month_key, product_group, flow_name, named_partner_value_eur, "
            "named_partner_count, named_partner_hhi, observed_special_value_eur, "
            "world_value_eur, reconciliation_residual_eur, reconciliation_status "
            "from monthly_partner_concentration order by 1, 2, 3"
        ).fetchall()
        metric_count = connection.execute(
            "select count(*) from metric_dictionary"
        ).fetchone()[0]

        manifests = sorted((tmp_path / ".state/dbt").glob("*/manifest.json"))
        assert len(manifests) == 2
        manifest = json.loads(manifests[0].read_text())
        public_nodes = [
            node
            for node in manifest["nodes"].values()
            if node["resource_type"] == "model"
            and node["original_file_path"].startswith("models/marts/")
        ]
        for node in public_nodes:
            assert node["description"]
            assert node["config"]["meta"]["owner"] == "data-platform"
            assert node["config"]["meta"]["grain"]
            actual_columns = {
                row[1]
                for row in connection.execute(
                    "select * from pragma_table_info(?)", [node["name"]]
                ).fetchall()
            }
            described_columns = {
                name
                for name, column in node["columns"].items()
                if column["description"]
            }
            assert actual_columns == described_columns, node["name"]

    assert trade_manifest["transport_complete"] is True
    assert trade_manifest["row_count"] == 18
    assert trade_manifest["slice_count"] == 2
    assert price_manifest["row_count"] == 9
    assert price_count == 9
    assert product_code_type == "VARCHAR"
    assert fact_count == 9
    assert metric_count == 8
    by_key = {(row[0], row[3], row[4], row[6], row[7]): row for row in rows}
    assert by_key[("BR", "09011100", 2021, "imports", "QUANTITY_IN_100KG")][1:] == (
        "named",
        True,
        "09011100",
        2021,
        "no identified CN8 leaf break",
        "imports",
        "QUANTITY_IN_100KG",
        "100kg",
        Decimal("5.5000"),
        "kg",
        Decimal("550.0000"),
        None,
        True,
        date(2021, 12, 1),
    )
    qs = by_key[("QS", "09011100", 2021, "exports", "QUANTITY_IN_100KG")]
    assert qs[1:3] == ("special", True)
    assert qs[9:12] == (Decimal("0.0000"), "kg", Decimal("0.0000"))
    world = by_key[("WORLD", "09011100", 2021, "imports", "VALUE_IN_EUROS")]
    assert world[1:3] == ("aggregate", False)
    unavailable = by_key[("BR", "18069090", 2021, "imports", "QUANTITY_IN_100KG")]
    assert unavailable[9:14] == (None, "kg", None, "u", True)
    assert (
        by_key[("BR", "18069090", 2021, "imports", "VALUE_IN_EUROS")][5] == "2017-2021"
    )
    assert (
        by_key[("BR", "18069090", 2022, "imports", "VALUE_IN_EUROS")][5] == "2022-2026"
    )
    assert product_metrics[0][:5] == (
        date(2021, 12, 1),
        "coffee_unroasted",
        "exports",
        Decimal("250.0000"),
        Decimal("100.0000"),
    )
    assert product_metrics[0][5] == pytest.approx(2.5)
    assert product_metrics[0][6:] == (pytest.approx(1.0), 1, 33)
    assert product_metrics[1][5] == pytest.approx(13 / 7)
    assert product_metrics[2][6] == pytest.approx(5 / 19)
    assert product_metrics[3][6] == pytest.approx(14 / 19)
    assert balances == [
        (
            date(2021, 12, 1),
            "coffee_unroasted",
            Decimal("1300.0000"),
            Decimal("250.0000"),
            True,
            Decimal("-1050.0000"),
        ),
        (
            date(2022, 1, 1),
            "chocolate_cocoa_preparations",
            Decimal("500.0000"),
            None,
            False,
            None,
        ),
        (
            date(2022, 1, 1),
            "coffee_unroasted",
            Decimal("1400.0000"),
            None,
            False,
            None,
        ),
    ]
    assert concentration[0][3:] == (
        Decimal("300.0000"),
        1,
        pytest.approx(1.0),
        None,
        None,
        None,
        "WORLD unavailable",
    )
    assert concentration[1][3:] == (
        None,
        0,
        None,
        Decimal("200.0000"),
        Decimal("250.0000"),
        Decimal("50.0000"),
        "partial WORLD coverage",
    )
    assert concentration[2][8] == Decimal("300.0000")
    assert concentration[3][8] == Decimal("180.0000")
    assert concentration[4][8] == Decimal("300.0000")

    target = tmp_path / "catalog"
    catalog_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "coffee_cocoa_platform.catalog_cli",
            "--root",
            str(tmp_path),
            "--target-dir",
            str(target),
        ],
        env=dict(os.environ, DAGSTER_DISABLE_TELEMETRY="1"),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert catalog_result.returncode == 0, catalog_result.stdout + catalog_result.stderr
    assert (target / "index.html").is_file()
    provenance = json.loads((target / "provenance.json").read_text())
    assert price_manifest["sha256"] in provenance["captures"]
    assert {item["capture"]["sha256"] for item in trade_manifest["slices"]} >= {
        capture_id
        for capture_id, detail in provenance["captures"].items()
        if detail["dataset"] == "trade"
    }
    catalog_manifest = json.loads((target / "manifest.json").read_text())
    for path in (
        (
            "monthly_benchmark_dynamics",
            "fct_benchmark_prices",
            "monthly_benchmark_prices",
            "stg_benchmark_prices",
            "source.coffee_cocoa.world_bank_prices.monthly_prices",
        ),
        (
            "monthly_partner_concentration",
            "fct_trade_observations",
            "int_trade_observations_pivoted",
            "stg_trade_observations",
            "source.coffee_cocoa.eurostat_trade.monthly_trade",
        ),
    ):
        for parent, child in pairwise(path):
            parent_node = catalog_manifest["nodes"]["model.coffee_cocoa." + parent]
            child_id = (
                child if child.startswith("source.") else "model.coffee_cocoa." + child
            )
            assert child_id in parent_node["depends_on"]["nodes"]


def test_combined_definitions_import_without_io(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from coffee_cocoa_platform.platform_assets import defs; "
                "g=defs.resolve_asset_graph(); "
                "assert len(g.get_all_asset_keys()) == 20; "
                "assert any(str(k) == \"AssetKey(['eurostat_trade', 'monthly_trade'])\" "
                "for k in g.get_all_asset_keys()); "
                "assert any(str(k) == \"AssetKey(['monthly_partner_concentration'])\" "
                "for k in g.get_all_asset_keys())"
            ),
        ],
        env=dict(os.environ, COFFEE_COCOA_HOME=str(tmp_path)),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert not list(tmp_path.iterdir())
