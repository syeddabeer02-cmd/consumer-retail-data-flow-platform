import json
from pathlib import Path
import pytest
from retail_flow.pipeline import run
from retail_flow.synthetic import generate

pytestmark = pytest.mark.spark


def test_replay_and_failure_preserve_published_snapshot(tmp_path):
    day = "2026-10-01"
    source = generate(tmp_path, day, 20)
    first = run(tmp_path, day, master="local[2]")
    second = run(tmp_path, day, master="local[2]")
    assert first["run_id"] != second["run_id"]
    assert first["inputs"] == second["inputs"]
    assert first["output_counts"] == second["output_counts"]
    assert first["reconciliation"] == second["reconciliation"]
    assert second["output_counts"]["gold_funding"] == 20
    assert second["quality"]["funding"]["quarantined"] == 1
    assert second["quality"]["funding"]["duplicate_versions"] == 1
    pointer = tmp_path / "published" / day / "CURRENT.json"
    published = pointer.read_bytes()
    (source / "funding.csv").write_text("wrong,header\nx,y\n")
    with pytest.raises(ValueError, match="schema/header"):
        run(tmp_path, day, master="local[2]")
    assert pointer.read_bytes() == published
    assert json.loads(published)["run_id"] == second["run_id"]
    assert Path(first["output_root"]).is_dir()
    assert not (tmp_path / ".ledger-lock").exists()


def test_historical_ledger_and_quality_failure(tmp_path):
    import csv
    from retail_flow.synthetic import FUNDING_FIELDS, PAYOUT_FIELDS
    from retail_flow.quality import QualityPolicy, QualityFailure
    from retail_flow.finance import calculate

    day1 = generate(tmp_path, "2026-09-30", 20)
    with (day1 / "funding.csv").open() as handle:
        base = next(csv.DictReader(handle))
    day2 = tmp_path / "raw" / "2026-10-01"
    day2.mkdir()
    with (day2 / "funding.csv").open("w", newline="") as handle:
        csv.DictWriter(handle, fieldnames=FUNDING_FIELDS).writeheader()
    with (day2 / "payouts.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PAYOUT_FIELDS)
        writer.writeheader()
        writer.writerow(
            dict(
                payout_id="LATE",
                record_id=base["record_id"],
                vendor_id=base["vendor_id"],
                event_date="2026-10-01",
                currency="INR",
                amount=str(calculate(base)["net_payable"]),
                reversal_of="",
                updated_at="2026-10-01T02:00:00Z",
            )
        )
    manifest = run(tmp_path, "2026-10-01", master="local[2]")
    assert manifest["output_counts"]["gold_funding"] == 20
    assert manifest["source_partitions"] == ["2026-09-30", "2026-10-01"]
    assert manifest["reconciliation"].get("MISSING_PAYOUT", 0) == 0
    pointer = tmp_path / "published" / "CURRENT_LEDGER.json"
    published = pointer.read_bytes()
    with pytest.raises(QualityFailure, match="quarantine ratio"):
        run(tmp_path, "2026-10-01", master="local[2]", quality_policy=QualityPolicy(max_quarantine_ratio=0))
    assert pointer.read_bytes() == published
    with pytest.raises(ValueError, match="backwards publication"):
        run(tmp_path, "2026-09-30", master="local[2]")
    assert pointer.read_bytes() == published
