import argparse
import json
from pathlib import Path
from .synthetic import generate


def main():
    parser = argparse.ArgumentParser(description="Synthetic retail funding lakehouse")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["generate", "run", "report", "demo"]:
        sub = commands.add_parser(name)
        sub.add_argument("--root", default="data")
        sub.add_argument("--date", required=True)
        if name in {"generate", "demo"}:
            sub.add_argument("--rows", type=int, default=1000)
            sub.add_argument("--seed", type=int, default=42)
        if name in {"run", "demo"}:
            sub.add_argument("--format", choices=["parquet", "delta"], default="parquet")
            sub.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    if args.command in {"generate", "demo"}:
        print(f"Generated: {generate(args.root, args.date, args.rows, args.seed)}")
    if args.command in {"run", "demo"}:
        from .pipeline import run

        print(json.dumps(run(args.root, args.date, args.format, args.master), indent=2))
    if args.command == "report":
        print((Path(args.root) / "published" / args.date / "CURRENT.json").read_text())


if __name__ == "__main__":
    main()
