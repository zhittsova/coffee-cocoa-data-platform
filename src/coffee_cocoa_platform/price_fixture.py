"""Checked-in, synthetic World Bank-shaped source workbook."""

from importlib.resources import files


def synthetic_workbook() -> bytes:
    """Read the fixed fixture with eight prices, one blank cell and missing March."""
    return (
        files("coffee_cocoa_platform")
        .joinpath("fixtures/world_bank_monthly_synthetic.xlsx")
        .read_bytes()
    )
