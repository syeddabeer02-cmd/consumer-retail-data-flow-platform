import argparse
import json
from pathlib import Path
from .synthetic import generate
from .operations import retain, alert
from .quality import QualityPolicy


def main():
    parser = argparse.ArgumentParser(description="Synthetic retail funding historical lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["generate", "run", "report", "demo", "benchmark", "retain", "alerts", "health"]:
        sub = commands.add_parser(name)
        sub.add_argument("--root", default="data")
        if name in {"generate", "run", "report", "demo", "benchmark"}:
            sub.add_argument("--date", required=True, help="As-of ledger date")
        if name in {"generate", "demo", "benchmark"}:
            sub.add_argument("--rows", type=int, default=1000)
            sub.add_argument("--seed", type=int, default=42)
        if name in {"run", "demo", "benchmark"}:
            sub.add_argument("--format", choices=["parquet", "delta"], default="parquet")
            sub.add_argument("--master", default="local[2]")
        if name in {"run", "demo"}:
            sub.add_argument("--max-quarantine-ratio", type=float, default=0.05)
            sub.add_argument("--max-exception-ratio", type=float, default=0.50)
        if name == "health":
            sub.add_argument("--max-age-hours", type=float, default=36)
        if name == "retain":
            sub.add_argument("--days", type=int, default=30)
            sub.add_argument(
                "--apply", action="store_true", help="Delete eligible unreferenced run directories"
            )
    args = parser.parse_args()
    try:
        if args.command in {"generate", "demo"}:
            print(f"Generated: {generate(args.root, args.date, args.rows, args.seed)}")
        if args.command in {"run", "demo"}:
            from .pipeline import run

            policy = QualityPolicy(args.max_quarantine_ratio, args.max_exception_ratio)
            print(json.dumps(run(args.root, args.date, args.format, args.master, policy), indent=2))
        if args.command == "report":
            manifest = json.loads((Path(args.root) / "published" / "CURRENT_LEDGER.json").read_text())
            if manifest["batch_date"] != args.date:
                raise ValueError(
                    "Report the latest published as-of date; historical manifests remain under published/DATE"
                )
            print(json.dumps(manifest, indent=2))
        if args.command == "benchmark":
            from .benchmark import benchmark

            benchmark(args.root, args.date, args.rows, args.master, args.format, args.seed)
        if args.command == "retain":
            print(json.dumps(retain(args.root, args.days, args.apply), indent=2))
        if args.command == "health":
            from datetime import datetime, timezone, timedelta

            manifest = json.loads((Path(args.root) / "published" / "CURRENT_LEDGER.json").read_text())
            completed = datetime.fromisoformat(manifest["completed_at"])
            if args.max_age_hours <= 0 or datetime.now(timezone.utc) - completed > timedelta(
                hours=args.max_age_hours
            ):
                raise RuntimeError("Published ledger is stale or health threshold is invalid")
            print(
                json.dumps({"healthy": True, "run_id": manifest["run_id"], "as_of": manifest["batch_date"]})
            )
        if args.command == "alerts":
            for path in sorted((Path(args.root) / "alerts").glob("*.json")):
                print(path.read_text())
    except Exception as error:
        alert(args.root, "ERROR", "COMMAND_FAILED", {"command": args.command, "error": str(error)})
        raise


if __name__ == "__main__":
    main()
