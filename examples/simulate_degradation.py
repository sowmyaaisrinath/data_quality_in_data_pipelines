"""Generate repeated runs whose email completeness slowly degrades.

Run after installation:
    python examples/simulate_degradation.py
"""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from cdp_data_quality.runner import QualityRunner

CONFIG = """\
tables:
  customers: customers.csv
checks:
  - name: email null trend
    type: trend
    table: customers
    metric: "null_rate:email"
    window: 6
    min_history: 4
    bad_direction: up
    max_bad_slope_pct: 0.10
    severity: warn
"""


def write_customers(path: Path, missing_emails: int) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["customer_id", "email"])
        for number in range(20):
            email = "" if number < missing_emails else f"customer{number}@example.com"
            writer.writerow([f"C{number:03}", email])


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        config = root / "checks.yml"
        history = root / "history.duckdb"
        config.write_text(CONFIG)

        for run_number, missing in enumerate([1, 2, 3, 4, 5, 6], start=1):
            write_customers(root / "customers.csv", missing)
            results = QualityRunner(config, history, run_id=f"run-{run_number}").run()
            result = results[0]
            print(
                f"run-{run_number}: null_rate={missing / 20:.0%}, "
                f"status={result.status.value}, {result.message}"
            )


if __name__ == "__main__":
    main()
