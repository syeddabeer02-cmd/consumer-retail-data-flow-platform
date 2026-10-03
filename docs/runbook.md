# Operations and recovery

1. Produce or place finalized daily CSV files under `data/raw/YYYY-MM-DD/`.
2. Run `python -m retail_flow.cli run --date YYYY-MM-DD`.
3. Inspect `python -m retail_flow.cli report --date YYYY-MM-DD` for quality counts
   and exception distribution. Read the exceptions and quarantine output paths.
4. Correct bad source records, then rerun the same date. Consumers use the newly
   published success manifest. Earlier run directories remain available for audit.

A failure returns a nonzero exit code, records `failure.json` and does not update
CURRENT. Airflow retries twice and limits each daily run to four hours. Missing
inputs fail; synthetic data is never generated automatically by the daily DAG.

If a process is forcibly terminated, it may leave `published/DATE/.lock`.
Confirm no writer is active for that date before removing this empty lock folder;
then rerun. Retain successful audit runs according to your retention policy.
Unpublished failed runs can be removed after investigating them. Do not delete
any run referenced by a CURRENT manifest or successful cloud audit row.

Cloud access uses the Databricks job identity, Unity Catalog permissions and an
ADLS external location. Keep credentials in the cloud identity/secret system;
never commit them. The local Airflow stack binds its UI to localhost and reads
passwords from an ignored .env file. It is a development deployment.

For scale experiments, generate a larger batch and record elapsed time, hardware,
Spark partitions and row counts from each manifest. The default demo is 1,000
records. Tune shuffle partitions and cluster workers from measurements. Do not
present a small local run as proof of 50M monthly throughput or a production SLA.
