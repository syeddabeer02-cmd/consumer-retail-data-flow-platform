# Optional Azure deployment

Use an Azure Databricks workspace with Unity Catalog and an existing cluster
running a Spark 3.5 compatible Databricks runtime (for example 15.4 LTS).
The cloud wheel deliberately does not install PySpark or Delta over the runtime.
Local dependencies are isolated behind the package's `spark` extra.

1. Create or select an ADLS Gen2 storage account/container. Configure a Databricks
   access connector / managed identity with the required storage permissions.
2. In Unity Catalog, configure a storage credential and external location for
   that ADLS path. Create an external volume and grant the job identity READ
   VOLUME; grant USE CATALOG, USE SCHEMA and CREATE TABLE on the target schema.
   Create the target schema first if the identity lacks CREATE SCHEMA.
3. Upload both daily CSV inputs into
   `/Volumes/CATALOG/SCHEMA/VOLUME/raw/YYYY-MM-DD/`. They must match the contract.
   A simple local source is `python -m retail_flow.cli generate --date 2026-10-01`.
4. Install and authenticate the Databricks CLI to your workspace. Set bundle
   variables to your own catalog, schema, input volume root and existing cluster.
5. From the repository root:

```bash
databricks bundle validate --var 'catalog=YOUR_CATALOG,schema=retail_flow,input_root=/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME,cluster_id=YOUR_CLUSTER'
databricks bundle deploy --var 'catalog=YOUR_CATALOG,schema=retail_flow,input_root=/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME,cluster_id=YOUR_CLUSTER'
databricks bundle run retail_funding --var 'catalog=YOUR_CATALOG,schema=retail_flow,input_root=/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME,cluster_id=YOUR_CLUSTER' --params 'batch_date=2026-10-01'
```

Bundle deployment builds and uploads the wheel and Python job, and creates a job
that uses the existing cluster. Delta tables are partitioned by batch date.
Run the reporting SQL after selecting your catalog/schema. A success audit row
publishes the new snapshot. Reruns retain historical physical rows but the current
view selects one complete run per date, preventing duplicate report totals.

A dedicated cloud workspace/identity is compatible with project-only access.
Do not grant access to unrelated accounts or storage. This repository neither
provisions Azure resources nor runs cloud jobs automatically. Validation here
covers shared Spark logic; Azure deployment still requires workspace testing.

Official references:
- https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/
- https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities
- https://learn.microsoft.com/en-us/azure/databricks/volumes/
