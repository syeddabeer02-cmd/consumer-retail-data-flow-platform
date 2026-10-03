"""Publication gates; violations never replace a successful snapshot."""

from dataclasses import dataclass, asdict
from pyspark.sql import functions as F


class QualityFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class QualityPolicy:
    max_quarantine_ratio: float = 0.05
    max_exception_ratio: float = 0.50
    min_funding_records: int = 1

    def __post_init__(self):
        for ratio in [self.max_quarantine_ratio, self.max_exception_ratio]:
            if not 0 <= ratio <= 1:
                raise ValueError("Quality ratios must be between zero and one")
        if self.min_funding_records < 1:
            raise ValueError("Minimum funding records must be positive")


def enforce(datasets, metrics, policy):
    for name, counts in metrics.items():
        if counts["input"] != counts["valid"] + counts["quarantined"] + counts["duplicate_versions"]:
            raise QualityFailure(f"{name}: count conservation failed")
        ratio = counts["quarantined"] / max(counts["input"], 1)
        if ratio > policy.max_quarantine_ratio:
            raise QualityFailure(
                f"{name}: quarantine ratio {ratio:.4f} exceeds {policy.max_quarantine_ratio}"
            )
    if metrics["funding"]["valid"] < policy.min_funding_records:
        raise QualityFailure("Insufficient valid funding records")
    calculated = datasets["gold_funding"]
    broken = (
        calculated.filter(
            F.col("net_payable").isNull()
            | (
                F.col("net_payable")
                != F.col("eligible_sales") - F.col("total_deductions") + F.col("total_credits")
            )
        )
        .limit(1)
        .count()
    )
    if broken:
        raise QualityFailure("Funding arithmetic invariant failed")
    result = datasets["gold_reconciliation"]
    total = result.count()
    exceptions = datasets["exceptions"].count()
    ratio = exceptions / max(total, 1)
    if ratio > policy.max_exception_ratio:
        raise QualityFailure(f"Exception ratio {ratio:.4f} exceeds {policy.max_exception_ratio}")
    return {"policy": asdict(policy), "exception_ratio": ratio, "checks": "PASSED"}
