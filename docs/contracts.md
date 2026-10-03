# Contracts and accounting policy

Policy: `synthetic-v1`. Currency: INR only. Rates are fractions from 0 through 1.
Each batch contains only its event date in ISO YYYY-MM-DD form. Timestamps must
parse as timestamps. CSV headers must match the fields in `synthetic.py` exactly.
Identifiers are mandatory; quantity is a positive integer at most 100,000; unit price, logistics
and penalty are nonnegative. Adjustment may be signed. Monetary inputs must have
cent precision and absolute value at most INR 1 trillion; rates have at most six
fractional digits. These bounds prevent silent rounding or overflow in the daily
calculation contract. Invalid rows quarantine.
Malformed CSV or unexpected headers fail the batch. Valid record versions resolve
by latest updated_at then a deterministic row hash. Record IDs and payout IDs
are globally unique logical identifiers within the daily source snapshot.

Funding starts with unit price times quantity, subtracts discounts and returns,
then subtracts commission, commission tax, withholding, logistics, marketing and
penalty; rebates and signed adjustments add back. Each component rounds HALF_UP
to two decimals. The 35 measures in `finance.py` and `transforms.py` include
component amounts, per-unit values, effective percentages and subtotals. They
are illustrative financial calculations, not copied employer policy.

A batch is a complete daily snapshot. Corrections and late versions require
reprocessing their original event-date batch. Cross-date payout settlement and
cross-date reversal resolution are outside this daily contract. They require a
historical ledger and are not silently matched by this job.

Reconciliation status precedence:

| Status | Meaning |
| --- | --- |
| INVALID_REVERSAL | Referenced original is absent or inconsistent |
| ORPHAN_PAYOUT | No expected funding record with the same keys |
| MISSING_PAYOUT | No bank payout with the same keys |
| REVERSED | Reversal exists and aggregate payout is zero |
| CONTRA | Unreferenced exact opposite payouts pair and aggregate to zero |
| MATCHED | Absolute variance is at most INR 0.01 |
| AMOUNT_MISMATCH | Remaining difference |

A reversal must reference a non-reversal payout and negate its exact amount with
matching record, vendor, date and currency. Multiple reversal references to one
original are invalid. Unreferenced opposing amounts match one-to-one within the
same record/vendor/date/currency; originals with referenced reversals cannot
also match as contra. Pair hashes are retained in reconciliation lineage. Reversed records remain in exceptions so
an operator can decide whether a replacement payout is due. Ordinary split
payouts aggregate to one reconciliation record. Gold vendor summary groups by
vendor, currency, date and status; SQL rolls current daily snapshots into months.

Local input files must be finalized before starting a run and remain unchanged
throughout execution. Generation is separate from the daily scheduled DAG.
