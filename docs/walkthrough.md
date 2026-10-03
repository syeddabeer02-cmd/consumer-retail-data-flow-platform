# Hands-on project walkthrough

## Start with folders

src/retail_flow is the business engine. synthetic.py produces reproducible data;
finance.py is the independent decimal reference; transforms.py executes the
calculations and joins in Spark; ledger.py assembles historical inputs;
quality.py decides whether a run is publishable; pipeline.py controls writing
and the success pointer; operations.py handles alerts/retention; cli.py exposes
commands. These responsibilities keep scheduling and storage separate from
accounting rules so the same tested logic runs locally and in Databricks.

dags contains Airflow schedules. infrastructure contains container runtimes.
databricks and databricks.yml package the shared code as cloud jobs. sql contains
reporting. tests exercises real failure/replay/accounting scenarios. docs records
contracts and operations. Generated data is ignored by Git.

## Follow one record

Generate a batch and run the demo. Raw CSV rows have record_id, vendor_id,
event_date and updated_at. Validation rejects malformed numeric/date/identity
fields into quarantine with reasons. A Spark window selects the newest valid
version per logical ID. Gross sales subtract discounts and returns; component
deductions and credits produce net_payable using exact decimal cent rounding.

Bank payouts aggregate by funding record, vendor and currency. Multiple entries
may be a split payout. Linked opposite entries are reversals; unreferenced
opposite pairs are contra. A late payout resolves an older funding record in
the historical ledger while retaining its original accounting month. Mismatches
remain in exceptions. Gold monthly/vendor summaries group the resulting ledger.

## Understand publication and recovery

Every run writes into a new immutable run folder. Quality gates and source hash
checks pass before CURRENT_LEDGER.json advances. Think of the pointer as the
traffic signal permitting readers to enter one complete set of outputs. Reruns
replace this signal's destination; they do not append duplicate report records.
If validation fails, readers still see the previous complete result.

Airflow starts processing only when scheduled/triggered, retries failed tasks,
limits runtime and queues structured failure alerts. It does not define the
financial formulas. PostgreSQL here stores orchestration metadata. Spark and
Parquet/Delta store business results. These are distinct architectural roles.

## Debug practical failures

- Missing source: both date-partition CSV files must exist, including header-only
  files when there are no events. Inspect raw/DATE, not the Airflow database.
- Quality gate failure: inspect alerts and rejected rows; correct the source or
  deliberately set a reviewed threshold. The selected threshold is audited.
- Amount mismatch: compare component net_payable with all bank entries for the
  record; inspect settlement dates and reversal references.
- Writer lock: confirm the process has stopped before removing a stale root
  .ledger-lock directory. Do not remove an active writer's lock.
- Late correction: edit the finalized original source and rerun the latest
  as-of date. Replaying an older date over the current ledger is blocked.
- Delta download: the local JVM needs access to Maven. Parquet requires no Delta
  jars. Databricks supplies Spark/Delta through its managed runtime.

## Explain it in interviews

Use the resume checklist to point to an implementation and a test for each
technology. Describe what was actually measured and distinguish it from prior
employment scale. Explain window partition/order keys, exact money arithmetic,
full joins for missing/orphan records, transaction boundaries, snapshot
idempotency, quality failure behavior, and the storage/runtime tradeoff of
rebuilding historical snapshots. Hands-on teaching can follow this sequence
using your own running checkout after the engineering work is complete.
