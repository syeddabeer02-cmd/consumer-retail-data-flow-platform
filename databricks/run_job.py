"""Azure Databricks entry point: UC external volume input and managed Delta outputs.

The job identity needs access to the external volume and target schema.
Consumers select the latest SUCCEEDED audit run; partial runs are never published.
"""

import argparse
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from retail_flow.ledger import batches, assemble
from retail_flow.quality import QualityPolicy, enforce
from retail_flow.finance import POLICY_VERSION


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True, help="/Volumes/catalog/schema/raw_volume")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.05)
    parser.add_argument("--max-exception-ratio", type=float, default=0.50)
    args = parser.parse_args()
    from datetime import date

    date.fromisoformat(args.date)
    for identifier in [args.catalog, args.schema]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError("Catalog and schema require SQL-safe identifiers")
    if not args.input_root.startswith("/Volumes/"):
        raise ValueError("Use a Unity Catalog volume path for Azure inputs")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.ansi.enabled", "false")
    prefix = f"{args.catalog}.{args.schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
    run_id = uuid.uuid4().hex
    partitions = batches(Path(args.input_root), args.date)
    datasets, metrics, cached = assemble(spark, partitions)

    def cloud_alert(severity, category, details):
        row = [
            (
                run_id,
                args.date,
                severity,
                category,
                json.dumps(details),
                datetime.now(timezone.utc).isoformat(),
            )
        ]
        spark.createDataFrame(
            row,
            "run_id string, batch_date string, severity string, category string, details_json string, created_at string",
        ).write.format("delta").mode("append").saveAsTable(f"{prefix}.pipeline_alerts")

    try:
        gates = enforce(datasets, metrics, QualityPolicy(args.max_quarantine_ratio, args.max_exception_ratio))
    except Exception as error:
        cloud_alert("ERROR", "QUALITY_GATE_FAILED", {"error": str(error), "metrics": metrics})
        raise
    if sum(m["quarantined"] for m in metrics.values()) or gates["exception_ratio"]:
        cloud_alert("WARNING", "QUALITY_EXCEPTIONS", {"metrics": metrics, "gates": gates})
    for name, df in datasets.items():
        (
            df.withColumn("run_id", F.lit(run_id))
            .withColumn("batch_date", F.lit(args.date))
            .withColumn("written_at", F.current_timestamp())
            .withColumn("policy_version", F.lit(POLICY_VERSION))
            .write.format("delta")
            .mode("append")
            .partitionBy("batch_date")
            .saveAsTable(f"{prefix}.{name}")
        )
    audit = spark.createDataFrame(
        [
            (
                run_id,
                args.date,
                "SUCCEEDED",
                datetime.now(timezone.utc).isoformat(),
                POLICY_VERSION,
                json.dumps(
                    {"metrics": metrics, "gates": gates, "source_partitions": [p.name for p in partitions]}
                ),
            )
        ],
        "run_id string, batch_date string, status string, completed_at string, policy_version string, quality_json string",
    )
    audit.write.format("delta").mode("append").saveAsTable(f"{prefix}.pipeline_audit")
    # One historical ledger snapshot; never union cumulative snapshots across dates.
    spark.sql(f"""CREATE OR REPLACE VIEW {prefix}.current_reconciliation AS
      WITH published AS (
        SELECT run_id, batch_date, row_number() OVER(ORDER BY batch_date DESC, completed_at DESC, run_id DESC) AS rank
        FROM {prefix}.pipeline_audit WHERE status = 'SUCCEEDED'
      )
      SELECT r.* FROM {prefix}.gold_reconciliation r JOIN published p
      ON r.run_id = p.run_id AND r.batch_date = p.batch_date WHERE p.rank = 1""")
    for df in cached:
        df.unpersist()
    print(json.dumps({"run_id": run_id, "status": "SUCCEEDED", "quality": metrics}))


if __name__ == "__main__":
    main()
