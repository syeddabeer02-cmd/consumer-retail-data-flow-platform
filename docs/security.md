# Deployment security

Local Compose is a development deployment. Airflow and the dashboard bind host
ports on 127.0.0.1. PostgreSQL and scheduler have no published host ports.
Secrets come from an ignored .env file. The example file is for local testing;
replace every example value. Do not expose the development UI publicly.
Use a dedicated Docker volume and project checkout. Docker containers use a
non-root runtime identity; DAG code is mounted read-only in Compose.
Airflow should have only this project's metadata database and storage volume.

Azure uses a dedicated job identity. Grant READ VOLUME on source storage and
USE CATALOG / USE SCHEMA plus CREATE TABLE, MODIFY and SELECT on target tables
as required. Reporting users need SELECT on the reporting view and underlying
objects through your UC access model; they need no storage credentials or job
administration privileges. Use an Azure managed identity and access connector
instead of embedding storage keys. Restrict workspace job permissions to project
operators. Code intentionally performs no account-wide administration.

The four-hour runtime timeout, max concurrent job runs, quality gates, immutable
run output and success-only publication limit operational damage. Raw data is
never removed by retention because historical ledger rebuilds require it.
Local retention protects every publication pointer, avoids symlinks and shares
the writer lock. Retention previews candidates unless --apply is explicit.
Alerts are structured local JSON records / cloud Delta records and job logs;
no email, Slack or external webhook is sent by this repository.

CI has read-only repository token permissions. Secret files, raw/generated data,
warehouse files and virtual environments are ignored by Git. Cloud deployment
requires your actual workspace validation; this file describes the implemented
controls and configuration, not a security certification.

The optional hosted overlay routes only the synthetic-data dashboard over HTTPS.
Airflow remains accessible via SSH tunnel. See hosting.md for deployment scope.
