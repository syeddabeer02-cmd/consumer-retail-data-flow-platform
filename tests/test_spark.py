import csv
from decimal import Decimal
import pytest
from retail_flow.pipeline import spark_session
from retail_flow.synthetic import generate
from retail_flow.finance import calculate
from retail_flow.transforms import funding, validate, latest, reconcile

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    session = spark_session(master="local[2]")
    yield session
    session.stop()


def test_funding_matches_decimal_reference(spark, tmp_path):
    folder = generate(tmp_path, "2026-10-01", 40)
    source = spark.read.option("header", True).csv(str(folder / "funding.csv"))
    good, bad = validate(source, "funding", "2026-10-01")
    assert bad.count() == 1
    clean = latest(good, "record_id")
    assert clean.count() == 40
    with (folder / "funding.csv").open() as handle:
        reference = {r["record_id"]: calculate(r) for r in list(csv.DictReader(handle))[:40]}
    for row in funding(clean).collect():
        for key, value in reference[row.record_id].items():
            assert row[key] == value, (row.record_id, key, row[key], value)


def test_reconciliation_and_bad_reversal(spark):
    expected = spark.createDataFrame(
        [
            ("F1", "V1", "INR", "2026-10-01", Decimal("10.00")),
            ("F2", "V1", "INR", "2026-10-01", Decimal("10.00")),
            ("F3", "V1", "INR", "2026-10-01", Decimal("10.00")),
            ("F5", "V1", "INR", "2026-10-01", Decimal("-10.00")),
        ],
        "record_id string, vendor_id string, currency string, event_date string, net_payable decimal(20,2)",
    )
    payouts = spark.createDataFrame(
        [
            ("P1", "F1", "V1", "INR", "2026-10-01", "10", ""),
            ("R1", "F1", "V1", "INR", "2026-10-01", "-10", "P1"),
            ("R2", "F2", "V1", "INR", "2026-10-01", "-10", "MISSING"),
            ("P4", "F4", "V1", "INR", "2026-10-01", "10", ""),
            ("P5", "F5", "V1", "INR", "2026-10-01", "-10", ""),
            ("R5", "F5", "V1", "INR", "2026-10-01", "10", "P5"),
        ],
        "payout_id string, record_id string, vendor_id string, currency string, event_date string, amount string, reversal_of string",
    )
    statuses = {r.record_id: r.status for r in reconcile(expected, payouts).collect()}
    assert statuses == {
        "F1": "REVERSED",
        "F2": "INVALID_REVERSAL",
        "F3": "MISSING_PAYOUT",
        "F4": "ORPHAN_PAYOUT",
        "F5": "REVERSED",
    }


def test_contra_split_tolerance_and_duplicate_reversal(spark):
    keys = ["C", "S", "T", "D"]
    expected = spark.createDataFrame(
        [(k, "V", "INR", "2026-10-01", Decimal("10.00")) for k in keys],
        "record_id string, vendor_id string, currency string, event_date string, net_payable decimal(20,2)",
    )
    raw = [
        ("C1", "C", "10", ""),
        ("C2", "C", "-10", ""),
        ("S1", "S", "4", ""),
        ("S2", "S", "6", ""),
        ("T1", "T", "10.01", ""),
        ("D1", "D", "10", ""),
        ("D2", "D", "-10", "D1"),
        ("D3", "D", "-10", "D1"),
    ]
    payouts = spark.createDataFrame(
        [(p, k, "V", "INR", "2026-10-01", amount, ref) for p, k, amount, ref in raw],
        "payout_id string, record_id string, vendor_id string, currency string, event_date string, amount string, reversal_of string",
    )
    rows = {r.record_id: r for r in reconcile(expected, payouts).collect()}
    assert {k: r.status for k, r in rows.items()} == {
        "C": "CONTRA",
        "S": "MATCHED",
        "T": "MATCHED",
        "D": "INVALID_REVERSAL",
    }
    assert rows["C"].contra_pair_count == 1
    assert len(rows["C"].contra_pair_ids) == 1


def test_precision_and_quantity_contract(spark, tmp_path):
    folder = generate(tmp_path, "2026-10-01", 20)
    with (folder / "funding.csv").open() as handle:
        base = next(csv.DictReader(handle))
    rows = [
        dict(base, record_id="CENT", unit_price="1.005"),
        dict(base, record_id="BIG", quantity="2147483648"),
        dict(base, record_id="RATE", commission_rate="0.1234567"),
        dict(base, record_id="VALID", unit_price="1.01"),
    ]
    good, bad = validate(spark.createDataFrame(rows), "funding", "2026-10-01")
    assert good.select("record_id").first().record_id == "VALID"
    assert {r.record_id for r in bad.select("record_id").collect()} == {"CENT", "BIG", "RATE"}
