# Host the retail portfolio demo

This configuration serves the read-only dashboard publicly over HTTPS. Airflow
stays on server localhost and PostgreSQL has no public port. It runs on a Linux
server with Docker Compose and sufficient memory; actual resource needs must be
checked on the chosen machine. ARM compatibility must be verified before using
an ARM server. No cloud resources are created by these files.

## Account-dependent setup

1. Obtain a server within your chosen provider's free allowance. Confirm its
   billing limits and available capacity before creating resources.
2. Point a public hostname's DNS A record at that server. Remove an incorrect
   AAAA record if the server has no working IPv6 route.
3. Allow inbound TCP 80/443. Restrict SSH access. Keep 5432, 8080 and 8501 closed
   publicly; local host bindings in Compose remain unchanged.
4. Clone this repository on the server and create `.env` from `.env.example`.
   Replace all example secrets; add `DASHBOARD_DOMAIN=your-hostname` and
   `ACME_EMAIL=your-email`. Do not use the local demonstration passwords.

## Start and verify

From the repository on the server:

```bash
docker compose -f compose.yaml -f compose.hosted.yaml config -q
docker compose -f compose.yaml -f compose.hosted.yaml up --build -d
docker compose -f compose.yaml -f compose.hosted.yaml ps
```

Caddy requests and renews the certificate once DNS and inbound ports work. Its
certificate state and the application outputs use persistent Docker volumes.
Do not run `down -v`, which removes those volumes.

For a new empty server, generate a small sample ledger once:

```bash
docker compose exec airflow-scheduler python -m retail_flow.cli demo --root /opt/airflow/data --date 2026-10-03 --rows 1000
```

This generates source data; do not use it to recreate an existing late-payment
scenario. To preserve the Windows scenario, migrate the retail-data volume with
consistent paths and permissions, or reproduce that scenario on the new server.
The dashboard otherwise explains that no successful ledger is available yet.

Open `https://YOUR_HOSTNAME`, verify summary counts and record details, then
restart the dashboard and check that results remain available:

```bash
docker compose restart dashboard
```

Access Airflow through an SSH tunnel from Windows:

```powershell
ssh -L 8080:127.0.0.1:8080 YOUR_USER@YOUR_SERVER
```

While the tunnel is open, use http://localhost:8080 and the server's Airflow
credentials. If local Airflow already uses port 8080, use
`-L 18080:127.0.0.1:8080` and http://localhost:18080 instead.

Only publish synthetic data. The dashboard exposes financial rows and source
hashes without login and is intended as a public portfolio demonstration.
Real business data requires authentication and a separate access policy.
Maintain backups of source files, successful outputs and metadata. Backups and
restore need validation on the chosen host; this configuration is not a managed
high-availability deployment.

Fashion Sense needs a separate deployment review and hostname. A single server
has only one listener on ports 80/443, so extend the same proxy to route its
frontend after reviewing its repository; do not start a competing proxy.

References:
- https://caddyserver.com/docs/automatic-https
- https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/
