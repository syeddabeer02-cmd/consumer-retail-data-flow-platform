"""Reproducible measured benchmark of the entire validated snapshot pipeline."""

import json
import platform
import os
import threading
import time
from pathlib import Path
from .synthetic import generate
from .pipeline import run
from .operations import atomic_json


def benchmark(root, batch_date, rows=10000, master="local[2]", output_format="parquet", seed=42):
    started = time.monotonic()
    generate(root, batch_date, rows, seed)
    generation_seconds = time.monotonic() - started
    stop = threading.Event()
    peak = [0]

    def sample():
        while not stop.is_set():
            try:
                # Work runtimes can virtualize getpid while /proc exposes kernel IDs.
                kernel_pid = int(Path("/proc/self/stat").read_text().split()[0])
            except (OSError, ValueError, IndexError):
                stop.wait(0.2)
                continue
            pending, seen, total = [kernel_pid], set(), 0
            java_observed = False
            while pending:
                pid = pending.pop()
                if pid in seen:
                    continue
                seen.add(pid)
                try:
                    base = Path(f"/proc/{pid}")
                    java_observed = java_observed or (base / "comm").read_text().strip() == "java"
                    total += int((base / "statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
                    pending.extend(
                        int(c) for c in (base / "task" / str(pid) / "children").read_text().split()
                    )
                except (OSError, ValueError, IndexError):
                    continue
            if java_observed:
                peak[0] = max(peak[0], total)
            stop.wait(0.2)

    monitor = threading.Thread(target=sample, daemon=True)
    monitor.start()
    try:
        result = run(root, batch_date, output_format, master)
    finally:
        stop.set()
        monitor.join()

    output = Path(result["output_root"])
    record = dict(
        requested_funding_rows=rows,
        seed=seed,
        generation_seconds=round(generation_seconds, 3),
        pipeline_seconds=result["elapsed_seconds"],
        input_rows=sum(m["input"] for m in result["quality"].values()),
        funding_rows_per_second=round(rows / result["elapsed_seconds"], 2),
        output_bytes=sum(f.stat().st_size for f in output.rglob("*") if f.is_file()),
        cpu_count=os.cpu_count(),
        sampled_peak_process_tree_rss_bytes=peak[0] or None,
        memory_measurement="Linux /proc RSS sampled every 0.2s; null if the JVM tree is inaccessible; shared pages can be counted more than once",
        python=platform.python_version(),
        platform=platform.platform(),
        master=master,
        format=output_format,
        run_id=result["run_id"],
        quality=result["quality"],
        reconciliation=result["reconciliation"],
        note="Measured on this runtime; not evidence of a 50M-record production SLA",
    )
    atomic_json(Path(root) / "benchmark.json", record)
    print(json.dumps(record, indent=2))
    return record
