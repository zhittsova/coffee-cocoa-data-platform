"""Observed availability, separate from retrieval success and source update time."""

from datetime import UTC, datetime
from itertools import product

from coffee_cocoa_platform.trades import FLOWS, SUPPORTED_PARTNERS


def partner_layer(partner):
    if partner == "WORLD":
        return "world"
    if partner in {
        "INT_EU",
        "EXT_EU",
        "INT_EU27_2020",
        "EXT_EU27_2020",
        "INT_EA",
        "EXT_EA",
        "INT_EA21",
        "EXT_EA21",
    }:
        return "aggregate"
    if partner in {"QP", "QQ", "QR", "QS", "QU", "QV", "QW", "QX", "QY", "QZ"}:
        return "special"
    return "detail"


def month_lag(month, now):
    if month is None:
        return None
    year, number = map(int, month[:7].split("-"))
    return now.year * 12 + now.month - year * 12 - number


def refresh_status(manifest, now=None):
    now = now or datetime.now(UTC)
    common = {
        "cadence": "monthly observations; manual refresh; no release SLA",
        "evaluated_at_utc": now.isoformat(),
        "complete_recent_observations": False,
        "lag_basis": "calendar months since non-null observation, not publication delay",
    }
    if "latest_observed_by_series" in manifest:
        series = [
            {
                "series": key,
                "latest_observed": value,
                "observed_age_months": month_lag(value, now),
            }
            for key, value in manifest["latest_observed_by_series"].items()
        ]
        return {
            **common,
            "series": series,
            "source_update": manifest["source_update_text"],
            "missing_month_count": manifest["missing_month_count"],
            "null_price_count": manifest["null_price_count"],
        }
    groups = {}
    # Include requested series with no represented cells. A maximum over returned
    # cells alone would hide entirely absent partners/products/flows.
    for item in manifest["slices"]:
        request = item["request"]
        coverage = {
            (r["partner_code"], r["product_code"], r["flow_code"]): r
            for r in item["coverage"]["series_coverage"]
        }
        for partner, leaf, flow in product(
            request["partners"] or SUPPORTED_PARTNERS, request["products"], FLOWS
        ):
            layer = partner_layer(partner)
            group = groups.setdefault((leaf, flow, layer), {})
            row = coverage.get((partner, leaf, flow), {})
            month = row.get("last_paired_month")
            old = group.get(partner)
            group[partner] = max(old, month) if old and month else old or month
    rows = []
    for (leaf, flow, layer), partners in sorted(groups.items()):
        observed = [month for month in partners.values() if month]
        latest = max(observed) if observed else None
        rows.append(
            {
                "product": leaf,
                "flow": flow,
                "layer": layer,
                "requested_series": len(partners),
                "series_with_paired_observations": len(observed),
                "latest_paired_month": latest,
                "oldest_series_latest_month": min(observed) if observed else None,
                "series_at_latest_month": sum(month == latest for month in observed),
                "observed_age_months": month_lag(latest, now),
            }
        )
    return {
        **common,
        "source_updates": manifest["source_updates"],
        "transport_complete": manifest["transport_complete"],
        "coverage": rows,
    }
