"""Distributed transformations: validation, version windows, funding and reconciliation."""

from pyspark.sql import functions as F, Window
from .finance import POLICY_VERSION

DECIMAL = "decimal(20,2)"
RATES = [
    "discount_rate",
    "return_rate",
    "commission_rate",
    "tax_rate",
    "withholding_rate",
    "marketing_rate",
    "rebate_rate",
]


def validate(df, kind, batch_date):
    reasons = []
    fields = (
        ["record_id", "vendor_id", "order_id", "event_date", "updated_at", "currency"]
        if kind == "funding"
        else ["payout_id", "record_id", "vendor_id", "event_date", "updated_at", "currency"]
    )
    for name in fields:
        reasons.append(F.when(F.col(name).isNull() | (F.trim(F.col(name)) == ""), F.lit(f"missing_{name}")))
    reasons.extend(
        [
            F.when(F.col("currency") != "INR", F.lit("unsupported_currency")),
            F.when(
                F.to_date("event_date").isNull() | (F.col("event_date") != batch_date),
                F.lit("invalid_batch_date"),
            ),
            F.when(F.to_timestamp("updated_at").isNull(), F.lit("invalid_updated_at")),
        ]
    )
    if kind == "funding":
        for name in ["quantity", "unit_price", "logistics_per_unit", "penalty"]:
            minimum = 1 if name == "quantity" else 0
            value = F.col(name).cast("decimal(20,6)")
            reasons.append(F.when(value.isNull() | (value < minimum), F.lit(f"invalid_{name}")))
        reasons.append(
            F.when(
                F.col("quantity").cast("decimal(20,6)") != F.col("quantity").cast("int"),
                F.lit("fractional_quantity"),
            )
        )
        for name in RATES:
            value = F.col(name).cast("decimal(12,6)")
            reasons.append(F.when(value.isNull() | (value < 0) | (value > 1), F.lit(f"invalid_{name}")))
        reasons.append(F.when(F.col("adjustment").cast(DECIMAL).isNull(), F.lit("invalid_adjustment")))
    else:
        reasons.append(F.when(F.col("amount").cast(DECIMAL).isNull(), F.lit("invalid_amount")))
    checked = df.withColumn("quality_errors", F.concat_ws("|", *reasons))
    return checked.filter(F.col("quality_errors") == "").drop("quality_errors"), checked.filter(
        F.col("quality_errors") != ""
    )


def latest(df, key):
    # Identical timestamp ties resolve by content hash so replay is deterministic.
    digest = F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])), 256)
    window = Window.partitionBy(key).orderBy(F.to_timestamp("updated_at").desc(), digest.desc())
    return (
        df.withColumn("_version_rank", F.row_number().over(window))
        .filter("_version_rank = 1")
        .drop("_version_rank")
    )


def funding(df):
    for name in RATES:
        df = df.withColumn(name, F.col(name).cast("decimal(12,6)"))
    for name in ["unit_price", "logistics_per_unit", "penalty", "adjustment"]:
        df = df.withColumn(name, F.col(name).cast(DECIMAL))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))

    def add(name, expression):
        nonlocal df
        df = df.withColumn(name, F.round(F.expr(expression), 2).cast(DECIMAL))

    formulas = {
        "gross_sales": "unit_price * quantity",
        "discount_amount": "gross_sales * discount_rate",
        "net_sales": "gross_sales - discount_amount",
        "return_amount": "net_sales * return_rate",
        "eligible_sales": "net_sales - return_amount",
        "commission_amount": "eligible_sales * commission_rate",
        "commission_tax": "commission_amount * tax_rate",
        "withholding_amount": "eligible_sales * withholding_rate",
        "logistics_fee": "quantity * logistics_per_unit",
        "marketing_fee": "eligible_sales * marketing_rate",
        "rebate_amount": "eligible_sales * rebate_rate",
        "penalty_amount": "penalty",
        "adjustment_amount": "adjustment",
        "net_payable": "eligible_sales - commission_amount - commission_tax - withholding_amount - logistics_fee - marketing_fee + rebate_amount - penalty_amount + adjustment_amount",
        "total_deductions": "commission_amount + commission_tax + withholding_amount + logistics_fee + marketing_fee + penalty_amount",
        "total_credits": "rebate_amount + adjustment_amount",
        "revenue_per_unit": "gross_sales / quantity",
        "discount_per_unit": "discount_amount / quantity",
        "eligible_per_unit": "eligible_sales / quantity",
        "commission_per_unit": "commission_amount / quantity",
        "tax_per_unit": "commission_tax / quantity",
        "withholding_per_unit": "withholding_amount / quantity",
        "payable_per_unit": "net_payable / quantity",
        "discount_pct": "case when gross_sales = 0 then 0 else discount_amount / gross_sales * 100 end",
        "returns_pct": "case when net_sales = 0 then 0 else return_amount / net_sales * 100 end",
        "commission_pct": "case when eligible_sales = 0 then 0 else commission_amount / eligible_sales * 100 end",
        "tax_pct": "case when commission_amount = 0 then 0 else commission_tax / commission_amount * 100 end",
        "withholding_pct": "case when eligible_sales = 0 then 0 else withholding_amount / eligible_sales * 100 end",
        "funding_margin": "eligible_sales - commission_amount - marketing_fee + rebate_amount",
        "pre_tax_payable": "net_payable + commission_tax + withholding_amount",
        "pre_adjustment_payable": "net_payable - adjustment_amount",
        "cash_deductions": "commission_tax + withholding_amount",
        "commercial_deductions": "commission_amount + logistics_fee + marketing_fee + penalty_amount",
        "funded_revenue": "eligible_sales + rebate_amount",
        "effective_deduction_pct": "case when eligible_sales = 0 then 0 else (eligible_sales - net_payable) / eligible_sales * 100 end",
    }
    for name, expression in formulas.items():
        add(name, expression)
    return df.withColumn("policy_version", F.lit(POLICY_VERSION))


def reconcile(expected, payouts, tolerance="0.01"):
    payouts = payouts.withColumn("amount", F.col("amount").cast(DECIMAL))
    original = payouts.select(
        F.col("payout_id").alias("original_id"),
        F.col("record_id").alias("original_record"),
        F.col("vendor_id").alias("original_vendor"),
        F.col("currency").alias("original_currency"),
        F.col("amount").alias("original_amount"),
        F.col("event_date").alias("original_date"),
        F.col("reversal_of").alias("original_reversal"),
    )
    linked = payouts.join(original, payouts.reversal_of == original.original_id, "left")
    is_reversal = F.coalesce(F.col("reversal_of") != "", F.lit(False))
    valid_link = (
        F.col("original_id").isNotNull()
        & (F.col("record_id") == F.col("original_record"))
        & (F.col("vendor_id") == F.col("original_vendor"))
        & (F.col("currency") == F.col("original_currency"))
        & (F.col("amount") == -F.col("original_amount"))
        & (F.col("event_date") == F.col("original_date"))
        & (F.col("amount") < 0)
        & (F.col("original_reversal").isNull() | (F.col("original_reversal") == ""))
    )
    linked = linked.withColumn(
        "reference_count", F.count("payout_id").over(Window.partitionBy("reversal_of"))
    )
    valid_link = valid_link & (F.col("reference_count") == 1)
    linked = linked.withColumn(
        "invalid_reversal", F.when(is_reversal & ~F.coalesce(valid_link, F.lit(False)), 1).otherwise(0)
    )
    # Deterministic one-to-one pairing of unreferenced exact opposite amounts.
    # Originals that have a referenced reversal cannot also participate in contra matching.
    referenced = (
        payouts.filter(F.col("reversal_of").isNotNull() & (F.col("reversal_of") != ""))
        .select(F.col("reversal_of").alias("referenced_id"))
        .distinct()
    )
    linked = linked.join(referenced, linked.payout_id == referenced.referenced_id, "left")
    eligible = (~is_reversal) & F.col("referenced_id").isNull() & (F.col("amount") != 0)
    group_keys = ["record_id", "vendor_id", "currency", "event_date"]
    linked = linked.withColumn("_abs_amount", F.abs("amount")).withColumn("_sign", F.signum("amount"))
    pair_group = Window.partitionBy(*group_keys, "_abs_amount")
    pair_order = Window.partitionBy(*group_keys, "_abs_amount", "_sign").orderBy(
        F.when(eligible, 0).otherwise(1), "payout_id"
    )
    linked = linked.withColumn(
        "_positive_count", F.sum(F.when(eligible & (F.col("amount") > 0), 1).otherwise(0)).over(pair_group)
    )
    linked = linked.withColumn(
        "_negative_count", F.sum(F.when(eligible & (F.col("amount") < 0), 1).otherwise(0)).over(pair_group)
    )
    linked = linked.withColumn("_pair_rank", F.row_number().over(pair_order))
    paired = eligible & (F.col("_pair_rank") <= F.least("_positive_count", "_negative_count"))
    linked = linked.withColumn(
        "contra_pair_id",
        F.when(paired, F.sha2(F.concat_ws("|", *group_keys, "_abs_amount", "_pair_rank"), 256)),
    )
    actual = linked.groupBy("record_id", "vendor_id", "currency", "event_date").agg(
        F.sum("amount").cast(DECIMAL).alias("paid_amount"),
        F.count("payout_id").alias("payout_count"),
        F.sum("invalid_reversal").alias("invalid_reversal_count"),
        F.countDistinct("contra_pair_id").alias("contra_pair_count"),
        F.collect_set("contra_pair_id").alias("contra_pair_ids"),
        F.sum(F.when(is_reversal, 1).otherwise(0)).alias("reversal_count"),
    )
    expected = expected.select("record_id", "vendor_id", "currency", "event_date", "net_payable")
    result = expected.join(actual, ["record_id", "vendor_id", "currency", "event_date"], "full")
    result = result.withColumn(
        "variance", (F.coalesce("paid_amount", F.lit(0)) - F.coalesce("net_payable", F.lit(0))).cast(DECIMAL)
    )
    return result.withColumn(
        "status",
        F.when(F.col("invalid_reversal_count") > 0, "INVALID_REVERSAL")
        .when(F.col("net_payable").isNull(), "ORPHAN_PAYOUT")
        .when(F.col("paid_amount").isNull(), "MISSING_PAYOUT")
        .when((F.col("reversal_count") > 0) & (F.col("paid_amount") == 0), "REVERSED")
        .when((F.col("contra_pair_count") > 0) & (F.col("paid_amount") == 0), "CONTRA")
        .when(F.abs("variance") <= F.lit(tolerance).cast(DECIMAL), "MATCHED")
        .otherwise("AMOUNT_MISMATCH"),
    )


def vendor_summary(df):
    return df.groupBy("event_date", "vendor_id", "currency", "status").agg(
        F.count("record_id").alias("record_count"),
        F.sum("net_payable").alias("expected_amount"),
        F.sum("paid_amount").alias("paid_amount"),
        F.sum("variance").alias("variance"),
    )
