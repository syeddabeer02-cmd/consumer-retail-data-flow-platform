# Consumer Retail Data Flow Platform

A complete runnable retail data engineering portfolio implementation using
synthetic vendor funding and bank payouts. PySpark builds an as-of historical
ledger, calculates funding, resolves settlements and reversals across dates,
and publishes validated Parquet/Delta outputs with audit lineage.

## Implemented behavior

- 35 decimal financial measures with a separate exact reference implementation.
- Contract validation, deterministic record-version windows and quarantine.
- Split payouts, late settlements, contra pairs, signed reversals, duplicate
  reversal detection and original-month accounting.
- Historical snapshots: late payments resolve older funding records; reports
  read one complete published snapshot, so reruns do not duplicate totals.
- Eight datasets: silver funding/payouts, gold funding/reconciliation/vendor
  summary/month-end summary, exceptions and quarantine.
- Quality gates for count conservation, quarantine ratio, exception ratio,
  minimum funding count and accounting identity.
- Atomic local publication, source SHA-256 lineage, source-change detection,
  writer locks, failure recovery, durable alert outbox and freshness checks.
- Scheduled Airflow processing, failure callbacks and retention maintenance.
- Non-root containers, localhost UI, PostgreSQL metadata, CI and Azure bundle.
- Measured benchmark tooling, operations/security guides and resume checklist.

## Run locally

Requires Python 3.10–3.12 and Java 17. Windows users can start with Docker Desktop
using [the Windows guide](docs/windows.md); WSL2 is suitable for direct Spark use.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[spark,dev]'
python -m retail_flow.cli demo --date 2026-10-01 --rows 1000
python -m retail_flow.cli report --date 2026-10-01
python -m retail_flow.cli health
python -m pytest -q
```

`data/published/CURRENT_LEDGER.json` is the authoritative snapshot pointer.
Resolve its `output_root` to read datasets with Spark. Date-specific manifests
are historical audit references. Every run includes all finalized source
partitions through its as-of date. Source files live in `data/raw/YYYY-MM-DD/`.
Place both CSV headers/files even when a day's funding or payouts are empty.
Corrections to older inputs require rerunning the latest published as-of date;
publishing an older ledger over a newer one is refused.

Use `demo --format delta` for Delta output; the first local use downloads JVM jars
from Maven. `generate` and `run` separate source creation from processing.
Quality defaults allow at most 5% quarantined input and 50% reconciliation
exceptions, with at least one valid funding record. Configure thresholds using
`run --max-quarantine-ratio ... --max-exception-ratio ...`; the selected policy
is recorded in the audit manifest. A failed gate preserves the previous result.

## Airflow / containers

```bash
cp .env.example .env
# Replace all three example values in .env before starting Airflow.
docker compose up --build -d
```

Open http://localhost:8081 and sign in as `admin` with your chosen password.
Trigger `retail_synthetic_demo`; the daily and retention DAGs start paused.
Unpause daily processing after setting up finalized daily source partitions.
The daily DAG retries twice and limits processing to four hours. Retention
prunes only old, unreferenced run directories and never deletes raw inputs.
Allocate sufficient Docker memory for Spark, PostgreSQL and Airflow together
(8 GB preferred for this development stack).

For only the pipeline container:

```bash
docker compose --env-file .env.example --profile demo run --rm pipeline
```

## Operations and benchmarks

```bash
python -m retail_flow.cli alerts
python -m retail_flow.cli retain --days 30          # preview
python -m retail_flow.cli retain --days 30 --apply  # prune eligible runs
python -m retail_flow.cli benchmark --root data/benchmark --date 2026-10-01 --rows 100000
```

Alert records remain local; no email, Slack or webhook is sent. Freshness checks
return a failing exit code for a missing/stale ledger. Benchmark output records
actual elapsed time, row counts, output bytes, runtime and worker configuration.
See [measured results](docs/benchmarks.md) for workload size, runtime and
configuration.

## Azure

The [Azure guide](docs/azure.md) and Databricks bundle use an existing UC-enabled
cluster, ADLS Gen2 external volume, managed Delta tables, quality gates, cloud
alert records and retention preview. The shared transformations and gates are
tested locally. Actual deployment requires your Azure workspace/identity and
has not been performed here; no Azure resources are created by local commands.

## Guides

- [Architecture](docs/architecture.md) · [Contracts](docs/contracts.md)
- [Operations](docs/runbook.md) · [Security](docs/security.md)
- [Validation](docs/validation.md) · [Engineering coverage](docs/resume-alignment.md)
- [Hands-on walkthrough](docs/walkthrough.md) · [Month-end SQL](sql/month_end.sql)

Uses synthetic retail data, with reproducible tests and documented benchmark
results.

## Visual dashboard

The read-only dashboard displays the latest successful Parquet ledger at
http://localhost:8501. It refreshes every 15 seconds after a new snapshot is
published. Views cover reconciliation, funding calculations, bank payouts,
exceptions, month-end results and quarantined records. Search or filter by
vendor/status, then click a record ID for its funding and payment history.

For an existing Docker installation, keep your data and start the dashboard:

```powershell
git pull
docker compose up --build -d dashboard
```

See [the dashboard walkthrough](docs/dashboard.md) for the late-payment scenario.

## Deployment preparation and repeatable checks

[Hosting instructions](docs/hosting.md) describe the HTTPS Docker overlay for a
Linux portfolio server. [Remaining steps](docs/next-steps.md) record local
validation and the account-dependent deployment tasks.

Repeat the replay/accounting/failure-protection checks on your existing ledger:

```powershell
Get-Content -Raw scripts/verify_scenarios.py | docker compose exec -T airflow-scheduler python - --root /opt/airflow/data
```
