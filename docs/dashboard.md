# Visual ledger dashboard

Open http://localhost:8501 after `docker compose up --build -d dashboard`.
The service mounts the existing retail-data volume read-only at the same path
used by Airflow. It requires neither a new Spark job nor a regenerated demo.
Airflow workflow monitoring remains at http://localhost:8081.

The overview shows current funding, matched payments, exceptions, expected and
paid totals, outcome counts, quality gates and audit lineage. Six table views
include searchable records, vendor/status filters and pagination. Click a record
ID to inspect its complete funding calculation, reconciliation and bank payments.
Amounts come from published Parquet outputs; JSON preserves decimal values as
strings. Bank totals include orphan payouts. Reports aggregate the published
historical snapshot, not only the newest source partition.

## Check the late-payment scenario

For the October 3–4 scenario, the latest snapshot should show 801 matched
records, 49 missing payouts and 200 exceptions. Search for
`F20261003-000000000` in Reconciliation and click its record ID. It should be
MATCHED with expected and paid amounts of INR 125.75 and zero variance.
The funding section shows October 3; the bank payment shows October 4 and
payout ID `LATE-20261004-000000000`.

Switch to Bank payouts to inspect settlements, Month-end reports for vendor
aggregates, and Quarantine for the invalid quantity rejected from source data.
Audit lineage exposes the successful run ID, policy and source file hashes.

## Refresh and availability

Auto-refresh checks every 15 seconds. Results change only when a successful
pipeline run publishes a new snapshot; this is a batch pipeline. A failed run
leaves the previous successful results visible. Requests pin the displayed run
ID so a publication during browsing prompts a refresh instead of mixing runs.
An empty installation shows a message to run the demo rather than invented data.

The Docker port binds to localhost and the volume is read-only. The local
dashboard has no authentication and should remain local. Delta-format outputs
are not supported by this dashboard; use the standard Parquet Docker workflow.

For Python development: install `pip install -e '.[dashboard]'` and run
`python -m retail_flow.dashboard --root /absolute/path/to/data`.
