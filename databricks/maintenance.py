"""Preview or prune superseded cloud run rows, preserving the latest run per as-of date."""

import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.days < 1:
        raise ValueError("Retention must be at least one day")
    for identifier in [args.catalog, args.schema]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError("Unsafe identifier")
    spark = SparkSession.builder.getOrCreate()
    prefix = f"{args.catalog}.{args.schema}"
    has_audit = spark.catalog.tableExists(f"{prefix}.pipeline_audit")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()
    candidates = []
    if has_audit:
        candidates = spark.sql(f"""WITH ranked AS (
          SELECT run_id, completed_at,
            row_number() OVER(PARTITION BY batch_date ORDER BY completed_at DESC, run_id DESC) AS rank
          FROM {prefix}.pipeline_audit WHERE status = 'SUCCEEDED')
          SELECT run_id FROM ranked WHERE rank > 1 AND completed_at < '{cutoff}'""").collect()
    ids = [r.run_id for r in candidates]
    if any(not re.fullmatch(r"[a-f0-9]{32}", run_id) for run_id in ids):
        raise ValueError("Invalid audit run ID")
    tables = [
        "silver_funding",
        "silver_payouts",
        "gold_funding",
        "gold_reconciliation",
        "gold_vendor_summary",
        "gold_month_end",
        "exceptions",
        "quarantine",
    ]
    orphan_counts = {}
    for table in tables:
        if not spark.catalog.tableExists(f"{prefix}.{table}"):
            continue
        predicate = f"written_at < to_timestamp('{cutoff}')"
        if has_audit:
            predicate += (
                f" AND run_id NOT IN (SELECT run_id FROM {prefix}.pipeline_audit WHERE status = 'SUCCEEDED')"
            )
        orphan_counts[table] = spark.sql(
            f"SELECT count(*) AS count FROM {prefix}.{table} WHERE {predicate}"
        ).first()["count"]
        if args.apply and orphan_counts[table]:
            spark.sql(f"DELETE FROM {prefix}.{table} WHERE {predicate}")
    if args.apply:
        for offset in range(0, len(ids), 500):
            selected = ",".join(f"'{run_id}'" for run_id in ids[offset : offset + 500])
            for table in tables:
                if spark.catalog.tableExists(f"{prefix}.{table}"):
                    spark.sql(f"DELETE FROM {prefix}.{table} WHERE run_id IN ({selected})")
    print(
        json.dumps(
            {
                "candidates": ids,
                "orphan_rows": orphan_counts,
                "applied": args.apply,
                "retention_days": args.days,
            }
        )
    )


if __name__ == "__main__":
    main()
