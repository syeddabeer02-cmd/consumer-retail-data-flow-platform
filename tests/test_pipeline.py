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
    assert not (pointer.parent / ".lock").exists()
