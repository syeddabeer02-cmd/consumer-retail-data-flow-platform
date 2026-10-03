from decimal import Decimal
from retail_flow.finance import calculate, money
from retail_flow.synthetic import generate
import csv
import pytest


def sample():
    return dict(
        quantity=2,
        unit_price=100,
        discount_rate="0.10",
        return_rate="0.05",
        commission_rate="0.12",
        tax_rate="0.18",
        withholding_rate="0.01",
        logistics_per_unit=15,
        marketing_rate="0.02",
        rebate_rate="0.03",
        penalty=5,
        adjustment="-2.50",
    )


def test_exact_accounting():
    out = calculate(sample())
    assert out["gross_sales"] == Decimal("200.00")
    assert out["eligible_sales"] == Decimal("171.00")
    assert out["net_payable"] == Decimal("109.29")
    assert out["net_payable"] == out["eligible_sales"] - out["total_deductions"] + out["total_credits"]
    assert len(out) >= 35


def test_rounding_and_zero():
    assert money("1.005") == Decimal("1.01")
    out = calculate(dict(sample(), unit_price=0))
    assert out["discount_pct"] == 0
    assert out["returns_pct"] == 0


def test_seeded_data(tmp_path):
    first = generate(tmp_path / "a", "2026-10-01", 40)
    second = generate(tmp_path / "b", "2026-10-01", 40)
    assert (first / "funding.csv").read_bytes() == (second / "funding.csv").read_bytes()
    with (first / "funding.csv").open() as f:
        assert len(list(csv.DictReader(f))) == 42
    with pytest.raises(ValueError):
        generate(tmp_path, "2026-10-01", 1)
