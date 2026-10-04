# Remaining account steps and explanation order

## Confirmed local validation

The Windows October 4 replay on October 3 completed successfully as run
8856dad9d90148098ade441fb4bff8e3, preserving 1,000 funding records,
1,102 payouts, 801 matches, 49 missing payouts and 200 exceptions.
A zero-exception policy rejected run 5a83b3de77aa4eefb29a587b79dc757d.
The health command afterward confirmed the successful publication remained
8856dad9d90148098ade441fb4bff8e3. Airflow task logs independently showed the
October 3 demo passed its quality gates and exited with code zero.

For repeatable verification of an existing successful snapshot, run from
Windows PowerShell in the repository:

```powershell
Get-Content -Raw scripts/verify_scenarios.py | docker compose exec -T airflow-scheduler python - --root /opt/airflow/data
```

This deliberately publishes a new replay run and creates a failed audit run.
It compares source hashes, counts, reconciliation classifications and exact
financial totals, then confirms the failure leaves the publication unchanged.
It does not edit sources. Run while other pipeline writers are idle. A failure
indicates a check did not pass; inspect its output before proceeding.

## User-dependent work

- Add Fashion Sense to the GitHub connection's allowed repositories. Only the
  retail repository was available during this preparation. Supply its repository
  URL if it belongs to a different account. Its JWT/RBAC/service boundaries and
  coverage remain unreviewed here.
- Create/select a hosting server and hostname. Follow hosting.md; actual public
  deployment and HTTPS verification remain pending server access.
- For Azure, supply the workspace and existing compatible cluster, catalog,
  schema and external volume backed by ADLS. Complete identity permissions in
  azure.md. No cloud deployment has been performed.
- Activate a provider sandbox before Fashion Sense payment integration. Specify
  required payment methods and geography; do not assume a US sandbox supports
  Indian UPI/net-banking methods.
- Create/select an S3 bucket and scoped identity if proceeding with product image
  storage. Keep secrets out of repository commits and chat messages.
- Jenkins/GitLab execution requires its own available runner/controller and
  repository connection. Prepared configurations and instructions are in
  ci-alternatives.md. GitHub Actions currently performs executable CI.

AI/GraphRAG work is a separate project scope; it is not a prerequisite for
finishing these two application demonstrations.

## Explanation sequence after implementation handoff

1. The Telusko system-design video's concepts: scaling, load balancing,
   databases, caching, messaging, partitioning, replication and reliability,
   mapped to concrete implemented examples and any deliberate simplifications.
2. Fashion Sense folder structure and request path: Java/Spring Boot, REST,
   JPA/PostgreSQL, authentication/authorization, Next.js, Redis, Kafka,
   Docker and CI/CD. Verify its repository before teaching implementation details.
3. Retail folder structure and data path: input contracts, Python/PySpark,
   windows/deduplication, 35 measures, reconciliation, Parquet/Delta, Airflow,
   historical snapshots, quality gates, lineage, dashboard and deployment.
4. Resume walkthrough: explain each implemented engineering pattern, reproduce
   demonstrations and distinguish measured benchmarks from deployment targets.
