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
Compose deployment plus a DAG run. Publication of the release was blocked by automatic approval review pending
explicit authorization to upload to the public repository. These new CI checks
are prepared and have not run for version 0.2.0. The earlier version had green
CI, including Delta and standalone containers; that result does not validate
this expanded release.

Local Delta downloads remain blocked by Maven hostname resolution in this Work
runtime; the earlier build successfully exercised Delta on GitHub CI. Docker is
not installed locally, so container/Compose verification runs through CI.
Azure Databricks/ADLS deployment and the user's own Windows installation require
those environments and have not been accessed here. Cloud configuration uses
shared validated transformations/gates, but is not represented as deployed.
