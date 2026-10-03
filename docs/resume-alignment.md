# Resume acceptance checklist

| Resume technology or claim | Project evidence | Verification status |
| --- | --- | --- |
| Python / PySpark distributed transformations | Native expressions and Spark joins in transforms.py | Local Spark tests and batch demo |
| Apache Airflow | Daily DAG, retry policy, manual synthetic demo, PostgreSQL metadata | Code supplied; Docker execution requires runtime |
| 35+ financial calculations | 35 named measures with exact decimal reference | All measures compared across seeded sample records |
| Spark Window functions | Latest record versions and contra pair ranks | Duplicate-version and contra tests |
| Contra and reversal matching | Exact opposing payout pairs and linked reversal checks | Split, reversal, duplicate-reversal and contra tests |
| Bank payout / month-end reconciliation | Daily exceptions and current-snapshot month-end SQL | Daily logic tested; cloud SQL needs workspace execution |
| Parquet outputs | Seven run datasets and success manifest | Complete local batch executed |
| Delta Lake | Optional local Delta output and Databricks tables | See validation.md for actual execution status |
| Azure Databricks / ADLS Gen2 | Bundle, UC volume ingestion and shared Spark transforms | Deployment configuration; cloud not provisioned |
| End-to-end audit lineage | Source paths, local SHA-256 hashes, run IDs, policy versions, exception keys, contra pair hashes | Manifest and generated outputs |
| 50M+ records/month and four-hour SLA | Scalable native Spark operators and four-hour orchestration timeout | Historical resume scale is not measured here |
| INR 1,756 Cr+ monthly payout volume | Decimal monetary aggregation and month-end grouping by currency | Historical business volume is not reproduced |
| 144+ person-hours/month and 5+ hours month-end savings | Automated calculations, exception classification and scheduling | Historical savings are not independently verified |

This project makes the technical patterns demonstrable with synthetic inputs.
Historical employment claims remain separate from this implementation's test results.
