"""Azure Databricks entry point: UC external volume input and managed Delta outputs.

The job identity needs access to the external volume and target schema.
Consumers select the latest SUCCEEDED audit run; partial runs are never published.
"""

import argparse
import json
import re
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from retail_flow.transforms import validate, latest, funding, reconcile, vendor_summary
from retail_flow.synthetic import FUNDING_FIELDS, PAYOUT_FIELDS
from retail_flow.finance import POLICY_VERSION


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True, help="/Volumes/catalog/schema/raw_volume")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    from datetime import date

    date.fromisoformat(args.date)
    for identifier in [args.catalog, args.schema]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError("Catalog and schema require SQL-safe identifiers")
    if not args.input_root.startswith("/Volumes/"):
        raise ValueError("Use a Unity Catalog volume path for Azure inputs")
    spark = SparkSession.builder.getOrCreate()
    prefix = f"{args.catalog}.{args.schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
    run_id = uuid.uuid4().hex
    inputs, metrics, rejects = {}, {}, []
    for name, fields in [("funding", FUNDING_FIELDS), ("payouts", PAYOUT_FIELDS)]:
        path = f"{args.input_root}/raw/{args.date}/{name}.csv"
        df = spark.read.option("header", True).option("mode", "FAILFAST").csv(path)
        if df.columns != fields:
            raise ValueError(f"{name}: schema/header does not match the contract")
        df = df.withColumn("source_file", F.lit(path))
        good, bad = validate(df, name, args.date)
        inputs[name] = latest(good, "record_id" if name == "funding" else "payout_id")
        metrics[name] = {"input": df.count(), "valid": inputs[name].count(), "quarantined": bad.count()}
        rejects.append(
            bad.select(
                F.lit(name).alias("source_type"),
                "quality_errors",
                "source_file",
                F.to_json(F.struct(*[F.col(c) for c in fields])).alias("raw_record"),
            )
        )
    calculated = funding(inputs["funding"])
    result = reconcile(calculated, inputs["payouts"])
    datasets = {
        "silver_funding": inputs["funding"],
        "silver_payouts": inputs["payouts"],
        "gold_funding": calculated,
        "gold_reconciliation": result,
        "gold_vendor_summary": vendor_summary(result),
        "exceptions": result.filter("status <> 'MATCHED'"),
        "quarantine": rejects[0].unionByName(rejects[1]),
    }
    for name, df in datasets.items():
        (
            df.withColumn("run_id", F.lit(run_id))
            .withColumn("batch_date", F.lit(args.date))
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
                json.dumps(metrics),
            )
        ],
        "run_id string, batch_date string, status string, completed_at string, policy_version string, quality_json string",
    )
    audit.write.format("delta").mode("append").saveAsTable(f"{prefix}.pipeline_audit")
    # Global view retains all dates while selecting the last complete run per date.
    spark.sql(f"""CREATE OR REPLACE VIEW {prefix}.current_reconciliation AS
      WITH published AS (
        SELECT run_id, batch_date, row_number() OVER(PARTITION BY batch_date ORDER BY completed_at DESC, run_id DESC) AS rank
        FROM {prefix}.pipeline_audit WHERE status = 'SUCCEEDED'
      )
      SELECT r.* FROM {prefix}.gold_reconciliation r JOIN published p
      ON r.run_id = p.run_id AND r.batch_date = p.batch_date WHERE p.rank = 1""")
    print(json.dumps({"run_id": run_id, "status": "SUCCEEDED", "quality": metrics}))


if __name__ == "__main__":
    main()
