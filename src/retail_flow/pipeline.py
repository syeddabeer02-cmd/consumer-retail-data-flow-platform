"""Batch runner: immutable run outputs plus atomic publication of a success manifest."""

import hashlib
import json
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from .ledger import batches, assemble
from .quality import QualityPolicy, enforce
from .operations import atomic_json, alert
from .finance import POLICY_VERSION


def spark_session(output_format="parquet", master=None):
    builder = (
        SparkSession.builder.appName("consumer-retail-data-flow")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
    )
    if master:
        builder = builder.master(master)
        if master.startswith("local"):
            builder = builder.config("spark.driver.host", "127.0.0.1").config(
                "spark.driver.bindAddress", "127.0.0.1"
            )
    if output_format == "delta":
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run(root, batch_date, output_format="parquet", master=None, quality_policy=None):
    date.fromisoformat(batch_date)
    if output_format not in {"parquet", "delta"}:
        raise ValueError("Output format must be parquet or delta")
    root = Path(root).resolve()
    partitions = batches(root, batch_date)
    quality_policy = quality_policy or QualityPolicy()
    hashes = {}
    for folder in partitions:
        for name in ["funding", "payouts"]:
            path = folder / f"{name}.csv"
            h = hashlib.sha256()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    h.update(block)
            hashes[f"{folder.name}/{name}"] = h.hexdigest()
    publish = root / "published" / batch_date
    publish.mkdir(parents=True, exist_ok=True)
    lock = root / ".ledger-lock"
    lock.mkdir()  # Fail immediately if another process owns this date.
    global_pointer = root / "published" / "CURRENT_LEDGER.json"
    if global_pointer.exists() and json.loads(global_pointer.read_text())["batch_date"] > batch_date:
        lock.rmdir()
        raise ValueError(
            "Reprocess corrections at the latest published as-of date; backwards publication is blocked"
        )
    run_id = uuid.uuid4().hex
    destination = root / "runs" / batch_date / run_id
    destination.mkdir(parents=True)
    started = time.monotonic()
    spark = None
    cached = []
    try:
        spark = spark_session(output_format, master)
        datasets, metrics, cached = assemble(spark, partitions)
        gates = enforce(datasets, metrics, quality_policy)
        reconciled = datasets["gold_reconciliation"]
        output_counts = {}
        for name, df in datasets.items():
            enriched = df.withColumn("run_id", F.lit(run_id)).withColumn(
                "policy_version", F.lit(POLICY_VERSION)
            )
            enriched.write.format(output_format).mode("errorifexists").save(str(destination / name))
            output_counts[name] = df.count()
        statuses = {r["status"]: r["count"] for r in reconciled.groupBy("status").count().collect()}
        if output_counts["gold_funding"] != metrics["funding"]["valid"]:
            raise RuntimeError("Funding row count invariant failed")
        manifest = dict(
            run_id=run_id,
            batch_date=batch_date,
            status="SUCCEEDED",
            policy_version=POLICY_VERSION,
            completed_at=datetime.now(timezone.utc).isoformat(),
            elapsed_seconds=round(time.monotonic() - started, 2),
            output_format=output_format,
            output_root=str(destination),
            inputs=hashes,
            quality=metrics,
            output_counts=output_counts,
            reconciliation=statuses,
            snapshot_mode="historical_as_of",
            source_partitions=[p.name for p in partitions],
            quality_gates=gates,
        )
        for folder in partitions:
            for name in ["funding", "payouts"]:
                h = hashlib.sha256()
                with (folder / f"{name}.csv").open("rb") as handle:
                    for block in iter(lambda: handle.read(1024 * 1024), b""):
                        h.update(block)
                if h.hexdigest() != hashes[f"{folder.name}/{name}"]:
                    raise RuntimeError("Input changed during execution; publication refused")
        (destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
        if sum(m["quarantined"] for m in metrics.values()) or gates["exception_ratio"]:
            alert(
                root,
                "WARNING",
                "QUALITY_EXCEPTIONS",
                {
                    "run_id": run_id,
                    "as_of": batch_date,
                    "quality": metrics,
                    "exception_ratio": gates["exception_ratio"],
                },
            )
        # No fallible data writes after the authoritative global commit.
        atomic_json(publish / "CURRENT.json", manifest)
        atomic_json(global_pointer, manifest)
        return manifest
    except Exception as error:
        (destination / "failure.json").write_text(
            json.dumps({"run_id": run_id, "status": "FAILED", "error": str(error)})
        )
        alert(root, "ERROR", "PIPELINE_FAILED", {"run_id": run_id, "as_of": batch_date, "error": str(error)})
        raise
    finally:
        for df in cached:
            df.unpersist()
        if spark:
            spark.stop()
        lock.rmdir()
