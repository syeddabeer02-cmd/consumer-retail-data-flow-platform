"""Batch runner: immutable run outputs plus atomic publication of a success manifest."""

import hashlib
import json
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from .transforms import validate, latest, funding, reconcile, vendor_summary
from .synthetic import FUNDING_FIELDS, PAYOUT_FIELDS
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


def run(root, batch_date, output_format="parquet", master=None):
    date.fromisoformat(batch_date)
    if output_format not in {"parquet", "delta"}:
        raise ValueError("Output format must be parquet or delta")
    root = Path(root).resolve()
    raw = root / "raw" / batch_date
    hashes = {}
    for name in ["funding", "payouts"]:
        path = raw / f"{name}.csv"
        if not path.is_file():
            raise FileNotFoundError(path)
        h = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                h.update(block)
        hashes[name] = h.hexdigest()
    publish = root / "published" / batch_date
    publish.mkdir(parents=True, exist_ok=True)
    lock = publish / ".lock"
    lock.mkdir()  # Fail immediately if another process owns this date.
    run_id = uuid.uuid4().hex
    destination = root / "runs" / batch_date / run_id
    destination.mkdir(parents=True)
    started = time.monotonic()
    spark = None
    cached = []
    try:
        spark = spark_session(output_format, master)
        inputs, metrics, quarantines = {}, {}, []
        for name, fields in [("funding", FUNDING_FIELDS), ("payouts", PAYOUT_FIELDS)]:
            source = (
                spark.read.option("header", True).option("mode", "FAILFAST").csv(str(raw / f"{name}.csv"))
            )
            if source.columns != fields:
                raise ValueError(f"{name}: schema/header does not match the contract")
            source = source.withColumn("source_file", F.input_file_name()).cache()
            cached.append(source)
            good, bad = validate(source, name, batch_date)
            key = "record_id" if name == "funding" else "payout_id"
            clean = latest(good, key).cache()
            cached.append(clean)
            raw_count, good_count, clean_count, bad_count = (
                source.count(),
                good.count(),
                clean.count(),
                bad.count(),
            )
            metrics[name] = dict(
                input=raw_count,
                valid=clean_count,
                quarantined=bad_count,
                duplicate_versions=good_count - clean_count,
            )
            # Union heterogeneous rejected records as JSON, retaining error and lineage.
            quarantines.append(
                bad.select(
                    F.lit(name).alias("source_type"),
                    "quality_errors",
                    "source_file",
                    F.to_json(F.struct(*[F.col(c) for c in fields])).alias("raw_record"),
                )
            )
            inputs[name] = clean
        calculated = funding(inputs["funding"]).cache()
        cached.append(calculated)
        reconciled = reconcile(calculated, inputs["payouts"]).cache()
        cached.append(reconciled)
        datasets = {
            "silver_funding": inputs["funding"],
            "silver_payouts": inputs["payouts"],
            "gold_funding": calculated,
            "gold_reconciliation": reconciled,
            "gold_vendor_summary": vendor_summary(reconciled),
            "exceptions": reconciled.filter(F.col("status") != "MATCHED"),
            "quarantine": quarantines[0].unionByName(quarantines[1]),
        }
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
        )
        (destination / "manifest.json").write_text(json.dumps(manifest, indent=2))
        temporary = publish / f"{run_id}.json"
        temporary.write_text(json.dumps(manifest, indent=2))
        os.replace(temporary, publish / "CURRENT.json")
        return manifest
    except Exception as error:
        (destination / "failure.json").write_text(
            json.dumps({"run_id": run_id, "status": "FAILED", "error": str(error)})
        )
        raise
    finally:
        for df in cached:
            df.unpersist()
        if spark:
            spark.stop()
        lock.rmdir()
