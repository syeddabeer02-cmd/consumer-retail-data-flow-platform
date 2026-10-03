# Consumer Retail Data Flow Platform

A resume-aligned retail data engineering portfolio project: synthetic vendor
funding and bank payouts flow through quality checks, PySpark calculations,
reconciliation and auditable lakehouse outputs.

**Implemented:** 35 decimal financial measures, version-window deduplication,
quarantine, payout aggregation, reversal matching, exception reporting, vendor
summaries, run lineage, safe local publication, Airflow DAGs, container definitions,
GitHub Actions and a Databricks deployment bundle. Data and rules are synthetic.

## Run locally

Requires Python 3.10–3.12 and Java 17. On Windows, use WSL2 for the simplest Spark
runtime. Run from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[spark,dev]'
python -m retail_flow.cli demo --date 2026-10-01 --rows 1000
python -m retail_flow.cli report --date 2026-10-01
python -m pytest -q
```

The demo creates synthetic inputs and executes the entire Parquet pipeline.
`data/published/2026-10-01/CURRENT.json` reports counts, exceptions, hashes and
output paths. Read any dataset using `spark.read.parquet(manifest_output_path)`.
Run `demo --format delta` for Delta outputs; this also downloads Delta JVM jars
from Maven on first use and needs outbound access. `generate` and `run` commands
allow ingestion and processing to happen separately. Use `--help` for options.

## Airflow and containers

```bash
cp .env.example .env
# Replace the three example values in .env.
docker compose up --build -d
```

Open http://localhost:8080 and sign in as `admin` with your chosen password.
Trigger `retail_synthetic_demo` to populate the shared lake volume. The scheduled
`retail_funding_daily` DAG expects inputs for its logical date and is paused by
default. Unpause after establishing a daily input feed. Docker Desktop should
have sufficient memory for Airflow, PostgreSQL and Spark together (8 GB preferred).

To run only the pipeline container:

```bash
docker compose --env-file .env.example --profile demo run --rm pipeline
```

## Azure Databricks / ADLS Gen2

Cloud is optional and requires your own Azure workspace and identity. The bundle
uses an existing cluster and Unity Catalog external volume backed by ADLS Gen2.
See [Azure setup](docs/azure.md). No cloud resource or credentials are required
for the local demo. Cloud execution has to be validated in your workspace.

## Documentation

- [Architecture](docs/architecture.md)
- [Contracts and accounting rules](docs/contracts.md)
- [Operations and recovery](docs/runbook.md)
- [Resume acceptance checklist](docs/resume-alignment.md)
- [Month-end SQL](sql/month_end.sql)

The project demonstrates engineering patterns associated with the resume;
it does not independently substantiate historical employer volume or savings.
