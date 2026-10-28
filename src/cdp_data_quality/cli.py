"""Command-line interface for CDP data quality checks."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from .models import Status
from .runner import QualityRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdp-quality", description="Run data quality checks against CDP datasets"
    )
    parser.add_argument("--config", default="config/checks.yml", help="YAML check configuration")
    parser.add_argument(
        "--history", default=".quality_history.duckdb", help="DuckDB metric history path"
    )
    parser.add_argument("--run-id", help="Unique pipeline run identifier")
    parser.add_argument("--json", dest="json_path", help="Write a JSON report to this path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = QualityRunner(args.config, args.history, args.run_id)
    try:
        results = runner.run()
    except (ValueError, FileNotFoundError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    print(f"{'STATUS':<11} {'CHECK':<34} {'TABLE':<14} DETAILS")
    print("-" * 94)
    for result in results:
        print(
            f"{result.status.value.upper():<11} {result.name:<34} "
            f"{result.table:<14} {result.message}"
        )

    counts = Counter(result.status.value for result in results)
    print(
        f"\nSummary: {counts['pass']} passed, {counts['information']} informational, "
        f"{counts['warn']} warnings, {counts['fail']} failed"
    )

    if args.json_path:
        report_path = Path(args.json_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(
                {
                    "run_id": runner.run_id,
                    "summary": dict(counts),
                    "results": [result.to_dict() for result in results],
                },
                indent=2,
            )
            + "\n"
        )
        print(f"JSON report written to {report_path}")

    return 1 if any(result.status is Status.FAIL for result in results) else 0


if __name__ == "__main__":
    sys.exit(main())
