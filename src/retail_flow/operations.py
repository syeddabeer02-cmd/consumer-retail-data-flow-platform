"""Structured alert outbox and conservative retention; no external delivery."""

import json
import os
import shutil
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path


def alert(root, severity, category, details):
    folder = Path(root) / "alerts"
    folder.mkdir(parents=True, exist_ok=True)
    event = dict(
        event_id=uuid.uuid4().hex,
        created_at=datetime.now(timezone.utc).isoformat(),
        severity=severity,
        category=category,
        details=details,
    )
    path = folder / f"{event['event_id']}.json"
    path.write_text(json.dumps(event, indent=2))
    print(json.dumps(event), flush=True)
    return event


def retain(root, days=30, apply=False, now=None):
    if days < 1:
        raise ValueError("Retention must be at least one day")
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ledger-lock"
    lock.mkdir()
    try:
        cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=days)
        protected = set()
        for pointer in (root / "published").rglob("CURRENT*.json"):
            protected.add(Path(json.loads(pointer.read_text())["output_root"]).resolve())
        candidates = []
        runs_root = root / "runs"
        for run in sorted(runs_root.glob("*/*")):
            if run.is_symlink() or not run.is_dir() or run.resolve() in protected:
                continue
            if not run.resolve().is_relative_to(runs_root.resolve()):
                continue
            if datetime.fromtimestamp(run.stat().st_mtime, timezone.utc) < cutoff:
                candidates.append(str(run))
                if apply:
                    shutil.rmtree(run)
        return dict(applied=apply, retention_days=days, protected_runs=len(protected), candidates=candidates)
    finally:
        lock.rmdir()


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(f".{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, indent=2))
    os.replace(temporary, path)
