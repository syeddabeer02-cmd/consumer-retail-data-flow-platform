# Validation record

Local environment: Python 3.12.14, OpenJDK 17, PySpark 3.5.3.

- Eight local tests pass, including precision/range contract checks.
- Decimal reference tests pass, including exact accounting identity and cent rounding.
- Shared Spark tests pass across 35 measures, latest-version selection, quarantine,
  explicit reversals, orphan/missing payouts, contra pairs, split payouts,
  tolerance boundaries and duplicate reversal references.
- Complete Parquet batch executes and writes all seven datasets plus a success manifest.
- Replay/failure publication validation is part of the integration suite.
- Ruff checks pass; modules compile; distributable wheel builds.

Delta local execution was attempted but the environment could not resolve
`repo1.maven.org` or `repos.spark-packages.org` to download Delta JVM jars.
The Parquet path ran successfully locally. GitHub CI subsequently executed the
Delta batch successfully on its runner.

Docker is not installed in this execution environment. GitHub CI subsequently
validated Compose configuration, built both container images and imported both
Airflow DAGs successfully. CI also executes the standalone pipeline container
and the synthetic Airflow DAG. The linked workflow is the authoritative result.
Azure Databricks and ADLS have not been deployed or exercised. The shared
transformation logic is validated locally; cloud integration remains a separate
workspace validation step. No 50M-row benchmark or production SLA is claimed.
