"""Seeded streaming fixtures; no real customer, vendor or bank information."""

import csv
import random
from datetime import date
from pathlib import Path
from .finance import calculate, money

FUNDING_FIELDS = [
    "record_id",
    "vendor_id",
    "order_id",
    "event_date",
    "updated_at",
    "currency",
    "quantity",
    "unit_price",
    "discount_rate",
    "return_rate",
    "commission_rate",
    "tax_rate",
    "withholding_rate",
    "logistics_per_unit",
    "marketing_rate",
    "rebate_rate",
    "penalty",
    "adjustment",
]
PAYOUT_FIELDS = [
    "payout_id",
    "record_id",
    "vendor_id",
    "event_date",
    "currency",
    "amount",
    "reversal_of",
    "updated_at",
]


def generate(root, batch_date, rows=1000, seed=42):
    if rows < 10:
        raise ValueError("Use at least 10 rows to cover exception scenarios")
    day = date.fromisoformat(batch_date)
    rng = random.Random(seed)
    folder = Path(root) / "raw" / batch_date
    folder.mkdir(parents=True, exist_ok=True)
    # Stream to disk so generation does not require memory proportional to row count.
    with (
        (folder / "funding.csv").open("w", newline="") as fh,
        (folder / "payouts.csv").open("w", newline="") as ph,
    ):
        fw = csv.DictWriter(fh, fieldnames=FUNDING_FIELDS)
        pw = csv.DictWriter(ph, fieldnames=PAYOUT_FIELDS)
        fw.writeheader()
        pw.writeheader()
        duplicate, invalid, orphan = None, None, None
        for i in range(rows):
            r = dict(
                record_id=f"F{i:09}",
                vendor_id=f"V{i % 20:03}",
                order_id=f"O{i:09}",
                event_date=str(day),
                updated_at=f"{day}T01:00:00Z",
                currency="INR",
                quantity=rng.randint(1, 5),
                unit_price=rng.randint(100, 3000),
                discount_rate="0.10",
                return_rate="0.05" if i % 7 == 0 else "0",
                commission_rate="0.12",
                tax_rate="0.18",
                withholding_rate="0.01",
                logistics_per_unit="15",
                marketing_rate="0.02",
                rebate_rate="0.03",
                penalty="5" if i % 11 == 0 else "0",
                adjustment="-2.50" if i % 13 == 0 else "0",
            )
            fw.writerow(r)
            if i == 3:
                duplicate = dict(r, updated_at=f"{day}T00:00:00Z", unit_price=1)
            if i == 4:
                invalid = dict(r, record_id="INVALID", quantity=-1)
            if i % 20 == 0:
                continue  # Missing payout
            amount = calculate(r)["net_payable"]
            if i % 20 == 1:
                amount += 3
            p = dict(
                payout_id=f"P{i:09}",
                record_id=r["record_id"],
                vendor_id=r["vendor_id"],
                event_date=str(day),
                currency="INR",
                amount=str(amount),
                reversal_of="",
                updated_at=f"{day}T02:00:00Z",
            )
            if i == 1:
                orphan = dict(p, payout_id="ORPHAN", record_id="UNKNOWN")
            if i % 20 == 4:
                first = money(amount / 2)
                pw.writerow(dict(p, amount=str(first)))
                pw.writerow(dict(p, payout_id=f"S{i:09}", amount=str(amount - first)))
            else:
                pw.writerow(p)
            if i % 20 in {2, 3}:
                pw.writerow(
                    dict(
                        p,
                        payout_id=f"R{i:09}",
                        amount=str(-amount),
                        reversal_of=p["payout_id"] if i % 20 == 2 else "",
                    )
                )
        fw.writerow(duplicate)
        fw.writerow(invalid)
        pw.writerow(orphan)
    return folder
