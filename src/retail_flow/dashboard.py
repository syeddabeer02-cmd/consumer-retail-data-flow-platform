"""Read-only browser dashboard over the latest published Parquet ledger."""
import argparse
import json
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import duckdb

DATASETS = {"reconciliation": "gold_reconciliation", "funding": "gold_funding",
            "payouts": "silver_payouts", "exceptions": "exceptions",
            "monthly": "gold_month_end", "quarantine": "quarantine"}
ASSETS = Path(__file__).with_name("dashboard_assets")


class SnapshotMissing(Exception):
    pass


def snapshot(root):
    root = Path(root).resolve()
    pointer = root / "published" / "CURRENT_LEDGER.json"
    if not pointer.exists():
        raise SnapshotMissing("No published ledger yet. Run the synthetic demo in Airflow first.")
    manifest = json.loads(pointer.read_text())
    output = Path(manifest["output_root"]).resolve()
    if not output.is_relative_to(root / "runs") or not output.is_dir():
        raise ValueError("Published output is outside the ledger storage or unavailable")
    if manifest.get("output_format") != "parquet":
        raise ValueError("This dashboard requires Parquet output; use the standard local pipeline.")
    return manifest, output


def query(output, dataset, sql="SELECT * FROM ledger", parameters=None):
    if dataset not in DATASETS:
        raise ValueError("Unknown dataset")
    path = output / DATASETS[dataset]
    if path.is_symlink() or not path.resolve().is_relative_to(output):
        raise ValueError("Unsafe dataset path")
    with duckdb.connect(":memory:") as db:
        db.execute("SET threads=2")
        db.execute("SET memory_limit='128MB'")
        db.read_parquet(str(path / "*.parquet")).create_view("ledger")
        result = db.execute(sql, parameters or [])
        columns = [c[0] for c in result.description]
        return columns, [dict(zip(columns, row)) for row in result.fetchall()]


def table(output, dataset, args):
    columns, _ = query(output, dataset, "SELECT * FROM ledger LIMIT 0")
    page = int(args.get("page", "1"))
    size = int(args.get("size", "25"))
    if page < 1 or page > 100000 or not 1 <= size <= 100:
        raise ValueError("Invalid pagination")
    filters, values = [], []
    for field in ["status", "vendor_id"]:
        value = args.get(field, "")
        if value and field in columns:
            filters.append(f'"{field}" = ?')
            values.append(value)
    search = args.get("search", "").strip()
    if len(search) > 200:
        raise ValueError("Search is too long")
    searchable = [c for c in ["record_id", "payout_id", "vendor_id", "quality_errors", "raw_record"] if c in columns]
    if search and searchable:
        filters.append("(" + " OR ".join(f'contains(lower(CAST("{c}" AS VARCHAR)), ?)' for c in searchable) + ")")
        values.extend([search.lower()] * len(searchable))
    where = " WHERE " + " AND ".join(filters) if filters else ""
    _, count = query(output, dataset, "SELECT count(*) AS total FROM ledger" + where, values)
    order = [c for c in ["accounting_month", "record_id", "payout_id", "vendor_id", "status"] if c in columns]
    order_sql = " ORDER BY " + ",".join(f'"{c}"' for c in order) if order else ""
    _, rows = query(output, dataset, "SELECT * FROM ledger" + where + order_sql + " LIMIT ? OFFSET ?",
                    [*values, size, (page-1)*size])
    return dict(columns=columns, rows=rows, total=count[0]["total"], page=page, size=size)


def summary(manifest, output):
    _, totals = query(output, "reconciliation", """SELECT coalesce(sum(net_payable),0) AS expected,
        coalesce(sum(paid_amount),0) AS paid, coalesce(sum(variance),0) AS variance FROM ledger""")
    _, months = query(output, "monthly", """SELECT accounting_month, sum(expected_amount) AS expected,
        sum(paid_amount) AS paid, sum(variance) AS variance FROM ledger GROUP BY accounting_month ORDER BY accounting_month""")
    _, vendors = query(output, "reconciliation", "SELECT DISTINCT vendor_id FROM ledger ORDER BY vendor_id LIMIT 1000")
    return dict(run_id=manifest["run_id"], as_of=manifest["batch_date"], completed_at=manifest["completed_at"],
                elapsed_seconds=manifest["elapsed_seconds"], policy_version=manifest["policy_version"],
                source_partitions=manifest["source_partitions"], quality=manifest["quality"],
                quality_gates=manifest["quality_gates"], counts=manifest["output_counts"],
                statuses=manifest["reconciliation"], totals=totals[0], months=months,
                vendors=[r["vendor_id"] for r in vendors], inputs=manifest["inputs"])


def record(output, record_id):
    if not record_id or len(record_id) > 200:
        raise ValueError("Invalid record ID")
    _, funding = query(output, "funding", "SELECT * FROM ledger WHERE record_id = ? LIMIT 1", [record_id])
    _, reconciliation = query(output, "reconciliation", "SELECT * FROM ledger WHERE record_id = ? LIMIT 10", [record_id])
    _, payouts = query(output, "payouts", "SELECT * FROM ledger WHERE record_id = ? ORDER BY event_date, payout_id LIMIT 100", [record_id])
    return dict(funding=funding, reconciliation=reconciliation, payouts=payouts)


def serializer(value):
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def handler(root):
    class Handler(BaseHTTPRequestHandler):
        def respond(self, code, data):
            body = json.dumps(data, default=serializer).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self.respond(200, {"service": "ready"})
                return
            if parsed.path in {"/", "/app.js", "/style.css"}:
                name = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css"}[parsed.path]
                content = (ASSETS / name).read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", {"index.html": "text/html", "app.js": "text/javascript", "style.css": "text/css"}[name])
                self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(content)
                return
            if not parsed.path.startswith("/api/"):
                self.respond(404, {"error": "Not found"})
                return
            try:
                args = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                manifest, output = snapshot(root)
                if args.get("run_id") and args["run_id"] != manifest["run_id"]:
                    self.respond(409, {"error": "A new snapshot was published. Refresh to load it."})
                    return
                if parsed.path == "/api/summary":
                    result = summary(manifest, output)
                elif parsed.path == "/api/table":
                    result = table(output, args.get("dataset", "reconciliation"), args)
                elif parsed.path == "/api/record":
                    result = record(output, args.get("record_id", ""))
                else:
                    self.respond(404, {"error": "Not found"})
                    return
                result["run_id"] = manifest["run_id"]
                self.respond(200, result)
            except SnapshotMissing as error:
                self.respond(404, {"error": str(error), "empty": True})
            except (ValueError, KeyError) as error:
                self.respond(400, {"error": str(error)})
            except Exception:
                self.respond(503, {"error": "Could not read the published dataset. Retry after the run finishes."})

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/opt/airflow/data")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8501)
    args = parser.parse_args()
    ThreadingHTTPServer((args.host, args.port), handler(args.root)).serve_forever()


if __name__ == "__main__":
    main()
