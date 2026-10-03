# Validation record

Local environment: Python 3.12.14, OpenJDK 17, PySpark 3.5.3.

- Decimal reference tests pass, including exact accounting identity and cent rounding.
- Shared Spark tests pass across 35 measures, latest-version selection, quarantine,
  explicit reversals, orphan/missing payouts, contra pairs, split payouts,
  tolerance boundaries and duplicate reversal references.
- Complete Parquet batch executes and writes all seven datasets plus a success manifest.
- Replay/failure publication validation is part of the integration suite.
- Ruff checks pass; modules compile; distributable wheel builds.

Delta local execution was attempted but the environment could not resolve
`repo1.maven.org` or `repos.spark-packages.org` to download Delta JVM jars.
The Parquet path ran successfully. CI includes Delta execution on its runner.

Docker is not installed in this execution environment. Container builds,
Airflow DAG import checks and Compose configuration validation are included in
GitHub Actions and must be judged from the actual workflow result.
Azure Databricks and ADLS have not been deployed or exercised. The shared
transformation logic is validated locally; cloud integration remains a separate
workspace validation step. No 50M-row benchmark or production SLA is claimed.
