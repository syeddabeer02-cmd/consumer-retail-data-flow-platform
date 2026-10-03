# Release validation

Local runtime: Python 3.12.14, OpenJDK 17, PySpark 3.5.3. Version 0.2.0 expands
the original daily-only build into a historical as-of ledger.

The test suite covers exact accounting and 35 measures, contract precision and
range, seeded generation, deterministic versions, quarantine, split/tolerance
matching, signed reversals, duplicate reversal errors, cross-date settlement,
cross-date contra, original-month summary, replay consistency, failed-run
publication preservation, quality-gate rejection, backwards publication guard,
alert durability, retention dry-run/protected paths and writer exclusion.

Complete Parquet execution and a measured 1,000,000-funding / 1,100,001-payout run
succeeded locally. The wheel builds and Ruff checks pass. Health and retention
preview commands were exercised. See benchmarks.md for actual measurements.

GitHub Actions checks shared tests, Parquet and Delta execution, health/retention,
a 10,000-record benchmark, Compose syntax, both container builds, all three DAG
imports, standalone pipeline/Airflow execution and the full PostgreSQL/Airflow
Compose deployment plus a DAG run. All release checks passed on October 3, 2026:
13 tests, Parquet/Delta execution, health/retention, benchmark, both images,
three DAG imports, standalone execution, full Compose startup, PostgreSQL-backed
Airflow DAG execution and the final ledger health check.

[Release CI](https://github.com/syeddabeer02-cmd/consumer-retail-data-flow-platform/actions/runs/37154480198)
validates code commit 097018a81162c03824c3fe61bb3d14306e64e157. Subsequent changes
to the validation record and public wording are documentation-only.


Local Delta downloads remain blocked by Maven hostname resolution in this Work
runtime; the expanded release successfully exercised Delta on GitHub CI. Docker is
not installed locally, so container/Compose verification runs through CI.
Azure Databricks/ADLS deployment and the user's own Windows installation require
those environments and have not been accessed here. Cloud configuration uses
shared validated transformations/gates, but is not represented as deployed.
