"""Deterministic JSON-stat fixtures for the Eurostat trade adapter."""

import json
from datetime import UTC, datetime
from decimal import Decimal

from coffee_cocoa_platform.trades import (
    DIMENSIONS,
    FLOWS,
    INDICATORS,
    FetchResult,
    TradeProfile,
    TradeSlice,
)

FIXTURE_PARTNERS = ("BR", "QS", "WORLD")
FIXTURE_PRODUCTS = ("09011100", "18069090")
PARTNER_LABELS = {
    "BR": "Brazil",
    "QS": "Stores and provisions within the framework of intra-EU trade",
    "WORLD": "All countries of the world",
}


def fixture_profile() -> TradeProfile:
    """Return two disjoint slices spanning the 2022 CN boundary."""
    slices = tuple(
        TradeSlice(
            slice_id=f"fixture-{year}",
            batch=0,
            products=FIXTURE_PRODUCTS,
            partners=FIXTURE_PARTNERS,
            start=month,
            end=month,
            url=f"synthetic://eurostat-trade-{year}",
        )
        for year, month in ((2021, "2021-12"), (2022, "2022-01"))
    )
    return TradeProfile(
        name="fixture",
        slices=slices,
        max_response_bytes=64 * 1024,
        max_batch_bytes=128 * 1024,
        max_batch_attempts=6,
        max_batch_seconds=30.0,
        max_total_bytes=128 * 1024,
        max_total_attempts=6,
        max_total_seconds=30.0,
    )


def _response(request: TradeSlice) -> bytes:
    categories = {
        "freq": ("M",),
        "reporter": ("DE",),
        "partner": request.partners,
        "product": request.products,
        "flow": FLOWS,
        "indicators": INDICATORS,
        "time": (request.start,),
    }
    dimensions = {
        name: {
            "label": name,
            "category": {
                "index": {code: index for index, code in enumerate(codes)},
                "label": (
                    PARTNER_LABELS
                    if name == "partner"
                    else {code: code for code in codes}
                ),
            },
        }
        for name, codes in categories.items()
    }
    sizes = [len(categories[name]) for name in DIMENSIONS]

    def flat_index(partner: str, product: str, flow: str, indicator: str) -> str:
        coordinate = {
            "freq": "M",
            "reporter": "DE",
            "partner": partner,
            "product": product,
            "flow": flow,
            "indicators": indicator,
            "time": request.start,
        }
        index = 0
        for name, size in zip(DIMENSIONS, sizes):
            index = index * size + categories[name].index(coordinate[name])
        return str(index)

    values: dict[str, float] = {}

    def add(
        partner: str,
        product: str,
        flow: str,
        indicator: str,
        value: float,
    ) -> None:
        values[flat_index(partner, product, flow, indicator)] = value

    if request.start == "2021-12":
        add("BR", "09011100", "1", "VALUE_IN_EUROS", 1000)
        add("BR", "09011100", "1", "QUANTITY_IN_100KG", 5.5)
        add("QS", "09011100", "2", "VALUE_IN_EUROS", 200)
        add("QS", "09011100", "2", "QUANTITY_IN_100KG", 0)
        add("WORLD", "09011100", "1", "VALUE_IN_EUROS", 1300)
        add("WORLD", "09011100", "1", "QUANTITY_IN_100KG", 7)
        add("WORLD", "09011100", "2", "VALUE_IN_EUROS", 250)
        add("WORLD", "09011100", "2", "QUANTITY_IN_100KG", 1)
        add("BR", "18069090", "1", "VALUE_IN_EUROS", 300)
        status = {flat_index("BR", "18069090", "1", "QUANTITY_IN_100KG"): "u"}
    else:
        add("BR", "09011100", "1", "VALUE_IN_EUROS", 1100)
        add("BR", "09011100", "1", "QUANTITY_IN_100KG", 6)
        add("WORLD", "09011100", "1", "VALUE_IN_EUROS", 1400)
        add("WORLD", "09011100", "1", "QUANTITY_IN_100KG", 8)
        add("BR", "18069090", "1", "VALUE_IN_EUROS", 320)
        add("BR", "18069090", "1", "QUANTITY_IN_100KG", 2)
        add("WORLD", "18069090", "1", "VALUE_IN_EUROS", 500)
        add("WORLD", "18069090", "1", "QUANTITY_IN_100KG", 3)
        status = None
    document = {
        "class": "dataset",
        "label": "Synthetic Eurostat-shaped trade fixture",
        "version": "2.0",
        "id": list(DIMENSIONS),
        "size": sizes,
        "dimension": dimensions,
        "updated": "2026-08-14T11:00:00+0200",
        "value": values,
    }
    if status is not None:
        document["status"] = status
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def fixture_transport(
    request: TradeSlice, max_bytes: int, deadline_seconds: float
) -> FetchResult:
    """Return the fixed response for a declared fixture slice."""
    if request not in fixture_profile().slices:
        raise ValueError("Unknown synthetic trade slice")
    data = _response(request)
    if len(data) > max_bytes or deadline_seconds <= 0:
        raise ValueError("Synthetic trade fixture exceeds its execution budget")
    return FetchResult(
        data=data,
        effective_url=request.url,
        retrieved_at=datetime(2026, 8, 15, tzinfo=UTC),
        content_type="application/json",
        elapsed_seconds=float(Decimal("0.001")),
    )
