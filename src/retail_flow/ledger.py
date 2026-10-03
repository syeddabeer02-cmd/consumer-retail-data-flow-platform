"""Complete as-of ledger snapshots from finalized daily source partitions."""

from functools import reduce
from pathlib import Path
from pyspark.sql import functions as F
from .synthetic import FUNDING_FIELDS, PAYOUT_FIELDS
from .transforms import validate, latest, funding, reconcile, vendor_summary, month_end_summary


def batches(root, as_of):
    from datetime import date

    date.fromisoformat(as_of)
    root = Path(root)
    if not (root / "raw" / as_of).is_dir():
        raise FileNotFoundError(f"Missing source partition for {as_of}")
    selected = []
    for folder in sorted((root / "raw").iterdir()):
        if not folder.is_dir():
            continue
        date.fromisoformat(folder.name)
        if folder.name <= as_of:
            for name in ["funding", "payouts"]:
                path = folder / f"{name}.csv"
                if not path.is_file():
                    raise FileNotFoundError(path)
            selected.append(folder)
    return selected


def assemble(spark, partitions):
    valid, rejects, metrics = {}, [], {}
    for name, fields in [("funding", FUNDING_FIELDS), ("payouts", PAYOUT_FIELDS)]:
        accepted = []
        count_in = count_good = count_bad = 0
        for folder in partitions:
            source = (
                spark.read.option("header", True).option("mode", "FAILFAST").csv(str(folder / f"{name}.csv"))
            )
            if source.columns != fields:
                raise ValueError(f"{name}: schema/header does not match the contract")
            source = source.withColumn("source_file", F.lit(str(folder / f"{name}.csv")))
            good, bad = validate(source, name, folder.name)
            accepted.append(good)
            count_in += source.count()
            count_good += good.count()
            count_bad += bad.count()
            rejects.append(
                bad.select(
                    F.lit(name).alias("source_type"),
                    "quality_errors",
                    "source_file",
                    F.to_json(F.struct(*[F.col(c) for c in fields])).alias("raw_record"),
                )
            )
        valid[name] = latest(
            reduce(lambda a, b: a.unionByName(b), accepted), "record_id" if name == "funding" else "payout_id"
        ).cache()
        current = valid[name].count()
        metrics[name] = dict(
            input=count_in, valid=current, quarantined=count_bad, duplicate_versions=count_good - current
        )
    calculated = funding(valid["funding"]).cache()
    result = reconcile(calculated, valid["payouts"], cross_date=True).cache()
    datasets = dict(
        silver_funding=valid["funding"],
        silver_payouts=valid["payouts"],
        gold_funding=calculated,
        gold_reconciliation=result,
        gold_vendor_summary=vendor_summary(result),
        gold_month_end=month_end_summary(result),
        exceptions=result.filter(F.col("status") != "MATCHED"),
        quarantine=reduce(lambda a, b: a.unionByName(b), rejects),
    )
    return datasets, metrics, [*valid.values(), calculated, result]
