"""Metadata declarations used by the local dbt and Dagster catalog."""

import copy
import json
from itertools import pairwise

from dagster import AssetKey

from coffee_cocoa_platform.catalog_cli import definition_errors
from coffee_cocoa_platform.catalog_metadata import (
    ensure_manifest,
    source_asset_description,
    source_asset_metadata,
    source_asset_owners,
)


def test_every_dbt_resource_has_complete_discovery_metadata():
    manifest = json.loads(ensure_manifest().read_text())
    assert definition_errors(manifest) == []


def test_discovery_check_rejects_missing_owner_and_unit():
    manifest = json.loads(ensure_manifest().read_text())
    altered = copy.deepcopy(manifest)
    model = altered["nodes"]["model.coffee_cocoa.monthly_trade_product_metrics"]
    model["config"]["meta"].pop("owner")
    model["columns"]["world_value_eur"]["meta"].pop("unit")
    assert definition_errors(altered) == [
        "monthly_trade_product_metrics: missing owner",
        "monthly_trade_product_metrics.world_value_eur: missing unit",
    ]


def test_dagster_source_metadata_comes_from_dbt_source_definition():
    metadata = source_asset_metadata("eurostat_trade", "monthly_trade")
    assert source_asset_owners("eurostat_trade", "monthly_trade") == [
        "team:data-platform"
    ]
    assert metadata["grain"]
    assert source_asset_description("eurostat_trade", "monthly_trade").startswith(
        "Validated Eurostat trade cells"
    )
    assert metadata["license_url"].startswith("https://ec.europa.eu/")
    columns = metadata["dagster/column_schema"].columns
    assert {column.name for column in columns} >= {
        "source_capture_id",
        "source_value",
        "period_month",
    }


def test_dagster_price_and_trade_lineage_reaches_source_assets():
    from coffee_cocoa_platform.platform_assets import defs

    graph = defs.resolve_asset_graph()
    for path in (
        (
            "monthly_benchmark_dynamics",
            "fct_benchmark_prices",
            "monthly_benchmark_prices",
            "stg_benchmark_prices",
            ["world_bank_prices", "monthly_prices"],
        ),
        (
            "monthly_partner_concentration",
            "fct_trade_observations",
            "int_trade_observations_pivoted",
            "stg_trade_observations",
            ["eurostat_trade", "monthly_trade"],
        ),
    ):
        for child, parent in pairwise(path):
            assert AssetKey(parent) in graph.get(AssetKey(child)).parent_keys
