# Operations and recovery

1. Finalize both daily CSV files under data/raw/YYYY-MM-DD/. Use headers for
   an empty source. Logical IDs are global; versions reuse IDs.
2. Run python -m retail_flow.cli run --date YYYY-MM-DD. The historical ledger
   includes every finalized partition through this as-of date.
3. Inspect report for the latest date and inspect alerts, exceptions, quarantine
   and gold_month_end. Resolve datasets through published/CURRENT_LEDGER.json.
4. Correct older input records and rebuild the latest as-of date. Consumers
   switch to the newly published complete snapshot; old run outputs remain.

## Publication gates

Defaults: at most 5% quarantined input and 50% reconciliation exceptions; at
least one valid funding record; input = current valid + rejected + superseded
versions; net payable = eligible sales - deductions + credits. Source hashes
must remain unchanged throughout the run. A failure returns a nonzero exit code,
records failure.json / an ERROR alert, and never advances the global pointer.
Threshold overrides are explicit CLI options and appear in the audit manifest.

CURRENT_LEDGER.json is the only authoritative publication commit. Per-date
CURRENT.json files are historical references and must not be unioned for
accounting reports because each snapshot is cumulative. The cloud reporting
view likewise selects one complete success run. A crash before the global
commit leaves previous reports readable; unreferenced outputs can be inspected
and later removed through retention.

## Alerts and health

python -m retail_flow.cli alerts lists durable JSON alert records. WARNINGS
record quarantines/exceptions below configured limits; ERRORS record rejected
runs and command failures. Airflow failure callbacks queue alerts for failed or
timed-out tasks. Cloud quality alerts append to pipeline_alerts and Databricks
job logs record task failures. No external message is sent automatically.

python -m retail_flow.cli health --max-age-hours 36 checks the latest publication
completion timestamp and fails when it is missing/stale. A new failed attempt
does not hide a previous healthy snapshot; inspect ERROR alerts alongside health.
The daily DAG retries twice and enforces the four-hour timeout.

## Locks and recovery

Generation, publication and local retention share root/.ledger-lock. A second
writer fails rather than overlapping. If forcibly terminated, a writer may
leave the empty lock folder. Confirm no writer is active before removing it.
Do not remove an active lock to run concurrent rebuilds. Inspect failure/audit
records, then retry. A backwards as-of publication is refused; run the latest
as-of date to incorporate a late correction.

## Retention

retain --days 30 previews superseded/failed run directories older than 30 days.
--apply prunes them. All paths referenced by any CURRENT manifest are protected;
symlinks are excluded and the shared lock prevents active-run deletion. Raw
inputs, alerts and published manifests are not pruned. The paused-by-default
retail_retention DAG schedules this safe pruning at 05:00 UTC.

Cloud maintenance previews superseded success run rows and preserves the latest
run per as-of date. --apply deletes matching rows, retains audit records and can
be safely rerun after partial failure. Delta physical file reclamation requires
VACUUM under a reviewed retention policy. Cloud writes also carry written_at timestamps. Maintenance previews/prunes
old rows with no successful audit record, using a horizon longer than the
maximum job runtime; this handles abandoned partial writes.

## Scale

Use benchmark --root data/benchmark --date YYYY-MM-DD --rows N to record complete
pipeline runtime, generation time, row counts, bytes and worker configuration.
Increase data size and history horizon separately; snapshot rebuild cost grows
with source history. Tune SPARK_SHUFFLE_PARTITIONS and cluster workers from
measurements. Never extrapolate a demo into a claimed production SLA.
