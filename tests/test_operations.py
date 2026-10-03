import json
import os
import pytest
from retail_flow.operations import retain, alert
from retail_flow.quality import QualityPolicy


def test_retention_preserves_referenced_runs_and_is_dry_by_default(tmp_path):
    old = tmp_path / "runs" / "2026-01-01" / "old"
    keep = old.with_name("keep")
    for folder in [old, keep]:
        folder.mkdir(parents=True)
        os.utime(folder, (1, 1))
    pointer = tmp_path / "published" / "CURRENT_LEDGER.json"
    pointer.parent.mkdir()
    pointer.write_text(json.dumps({"output_root": str(keep)}))
    result = retain(tmp_path, 30)
    assert result["candidates"] == [str(old)]
    assert old.exists()
    retain(tmp_path, 30, apply=True)
    assert not old.exists() and keep.exists()
    (tmp_path / ".ledger-lock").mkdir()
    with pytest.raises(FileExistsError):
        retain(tmp_path, 30, apply=True)


def test_alert_and_invalid_policy(tmp_path):
    event = alert(tmp_path, "ERROR", "TEST", {"error": "example"})
    assert json.loads((tmp_path / "alerts" / f"{event['event_id']}.json").read_text()) == event
    with pytest.raises(ValueError):
        QualityPolicy(max_quarantine_ratio=-1)
