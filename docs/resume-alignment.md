# Engineering coverage

| Capability | Project evidence | Verification status |
| --- | --- | --- |
| Python / PySpark distributed transformations | Native expressions and Spark joins in transforms.py | Local Spark tests and batch demo |
| Apache Airflow | Daily DAG, retry policy, manual synthetic demo, PostgreSQL metadata | Code supplied; Docker execution requires runtime |
| 35+ financial calculations | 35 named measures with exact decimal reference | All measures compared across seeded sample records |
| Spark Window functions | Latest record versions and contra pair ranks | Duplicate-version and contra tests |
| Contra and reversal matching | Exact opposing payout pairs and linked reversal checks | Split, reversal, duplicate-reversal and contra tests |
| Bank payout / month-end reconciliation | Historical ledger, monthly dataset and current-snapshot SQL | Cross-date and monthly aggregation tested; cloud SQL needs workspace execution |
| Parquet outputs | Eight run datasets and success manifest | Complete local batch executed |
| Delta Lake | Optional local Delta output and Databricks tables | See validation.md for actual execution status |
| Azure Databricks / ADLS Gen2 | Bundle, UC volume ingestion and shared Spark transforms | Deployment configuration; cloud not provisioned |
| End-to-end audit lineage | Source paths, local SHA-256 hashes, run IDs, policy versions, exception keys, contra pair hashes | Manifest and generated outputs |

Uses synthetic retail data, with reproducible tests and documented benchmark results.

Additional acceptance coverage: publication quality gates, failed-run rollback,
late settlement, later reversal, cross-date contra, monthly accounting, global
key deduplication, writer exclusion, stale-ledger checks, durable alert records,
retention previews/protected runs, non-root runtime and measured benchmarks.

Performance evidence: 1,000,000 funding records plus 1,100,001 payout records
processed in 142.07 seconds using local[2]. See benchmarks.md for configuration
and measurements. Airflow and Databricks jobs have a four-hour execution limit.
