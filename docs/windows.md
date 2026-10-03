# Run on Windows with Docker Desktop

This is a separate checkout from FashionSense. The folder holds Python pipeline
code and Docker definitions; Docker supplies Python/Java/Airflow consistently,
so your Windows Java 26 installation is not used by these containers.

In PowerShell, from D:\Projects:

```powershell
cd D:\Projects
git clone https://github.com/syeddabeer02-cmd/consumer-retail-data-flow-platform.git
cd .\consumer-retail-data-flow-platform
Copy-Item .env.example .env
notepad .env
```

Replace all three values with passwords/secrets for your local development
stack. Save the file. It is ignored by Git. Start Docker Desktop with its WSL2
Linux engine, then run:

```powershell
docker compose up --build -d
docker compose ps
```

The first build downloads dependencies and may take several minutes. Compose
initializes PostgreSQL and the Airflow admin account. Visit localhost:8080,
log in as admin, and trigger retail_synthetic_demo. Its task log shows the
quality counts, as-of ledger output paths and reconciliation distribution.
The task creates synthetic funding/payout data; it uses no employer/customer data.

The UI shows DAGs: daily funding, synthetic demo and retention. DAGs are paused
initially so schedules do not run before an input feed exists. For the standalone
pipeline without the Airflow stack:

```powershell
docker compose --env-file .env.example --profile demo run --rm pipeline
```

For verification and troubleshooting:

```powershell
docker compose logs --tail 100 airflow-scheduler
docker compose exec airflow-scheduler python -m retail_flow.cli report --root /opt/airflow/data --date 2026-10-01
docker compose exec airflow-scheduler python -m retail_flow.cli health --root /opt/airflow/data
docker compose exec airflow-scheduler python -m retail_flow.cli alerts --root /opt/airflow/data
```

Stop services with docker compose down. Named volumes retain data. Removing
volumes discards the ledger, audit history and Airflow database, so do not use
--volumes as a routine stop command. Your computer's actual installation has
not been accessed or verified from this workspace.
