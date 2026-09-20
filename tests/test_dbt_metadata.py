"""Public dbt metadata contract checks."""

import copy
import json
from pathlib import Path

MANIFEST_PATH = Path(__file__).parents[1] / "dbt/manifest.json"


def public_metadata_errors(manifest: dict) -> list[str]:
    errors = []
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model" or not node[
            "original_file_path"
        ].startswith("models/marts/"):
            continue
        model_name = node["name"]
        if not node["description"]:
            errors.append(f"{model_name}: missing model description")
        if not node["config"]["meta"].get("owner"):
            errors.append(f"{model_name}: missing owner")
        if not node["config"]["meta"].get("grain"):
            errors.append(f"{model_name}: missing grain")
        for column_name, column in node["columns"].items():
            if not column["description"]:
                errors.append(f"{model_name}.{column_name}: missing description")
    return errors


def test_public_dbt_models_have_complete_metadata():
    manifest = json.loads(MANIFEST_PATH.read_text())
    assert public_metadata_errors(manifest) == []


def test_public_metadata_check_rejects_a_missing_description():
    manifest = json.loads(MANIFEST_PATH.read_text())
    altered = copy.deepcopy(manifest)
    model = altered["nodes"]["model.coffee_cocoa.monthly_trade_product_metrics"]
    model["columns"]["coverage_basis"]["description"] = ""

    assert public_metadata_errors(altered) == [
        "monthly_trade_product_metrics.coverage_basis: missing description"
    ]
