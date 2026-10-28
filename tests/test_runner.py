from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from cdp_data_quality.checks import CHECK_PARAMETERS, CheckEngine
from cdp_data_quality.cli import main
from cdp_data_quality.models import Status
from cdp_data_quality.runner import QualityRunner
from cdp_data_quality.store import MetricStore


def write_csv(path: Path, rows: list[list[object]]) -> None:
    with path.open("w", newline="") as handle:
        csv.writer(handle).writerows(rows)


def test_basic_and_relational_checks(tmp_path: Path) -> None:
    write_csv(
        tmp_path / "customers.csv",
        [["customer_id", "email"], ["C1", "valid@example.com"], ["C2", ""]],
    )
    write_csv(
        tmp_path / "events.csv",
        [["event_id", "customer_id"], ["E1", "C1"], ["E2", "missing"]],
    )
    config = tmp_path / "checks.yml"
    config.write_text(
        """
tables:
  customers: customers.csv
  events: events.csv
checks:
  - name: ids present
    type: required_columns
    table: customers
    columns: [customer_id, email]
  - name: email nulls
    type: null_rate
    table: customers
    column: email
    max: 0.4
  - name: event parent
    type: referential_integrity
    table: events
    column: customer_id
    parent_table: customers
    parent_column: customer_id
    max_orphan_rate: 0
"""
    )

    results = QualityRunner(config, tmp_path / "history.duckdb", "run-1").run()

    assert [result.status for result in results] == [Status.PASS, Status.FAIL, Status.FAIL]
    assert results[1].observed == 0.5
    assert results[2].observed == 0.5


def test_information_severity_reports_without_failing(tmp_path: Path) -> None:
    write_csv(tmp_path / "customers.csv", [["customer_id", "email"], ["C1", ""]])
    config = tmp_path / "checks.yml"
    config.write_text(
        """
tables:
  customers: customers.csv
checks:
  - name: optional email completeness
    type: null_rate
    table: customers
    column: email
    max: 0
    severity: information
"""
    )

    result = QualityRunner(config, tmp_path / "history.duckdb", "run-1").run()[0]

    assert result.status is Status.INFORMATION


def test_run_comparison_and_category_drift(tmp_path: Path) -> None:
    data = tmp_path / "events.csv"
    config = tmp_path / "checks.yml"
    config.write_text(
        """
tables:
  events: events.csv
checks:
  - name: volume change
    type: run_comparison
    table: events
    metric: row_count
    max_change_pct: 0.25
  - name: source drift
    type: category_drift
    table: events
    column: source
    max_total_variation: 0.2
"""
    )
    history = tmp_path / "history.duckdb"
    write_csv(data, [["event_id", "source"], ["E1", "web"], ["E2", "web"]])
    first = QualityRunner(config, history, "run-1").run()
    assert all(result.status is Status.WARN for result in first)

    write_csv(
        data,
        [
            ["event_id", "source"],
            ["E1", "mobile"],
            ["E2", "mobile"],
            ["E3", "mobile"],
            ["E4", "mobile"],
        ],
    )
    second = QualityRunner(config, history, "run-2").run()
    assert [result.status for result in second] == [Status.FAIL, Status.FAIL]
    assert second[0].observed == 1.0
    assert second[1].observed == 1.0


def test_slow_degradation_trend(tmp_path: Path) -> None:
    data = tmp_path / "customers.csv"
    config = tmp_path / "checks.yml"
    config.write_text(
        """
tables:
  customers: customers.csv
checks:
  - name: worsening email null rate
    type: trend
    table: customers
    metric: "null_rate:email"
    window: 5
    min_history: 4
    bad_direction: up
    max_bad_slope_pct: 0.1
    severity: warn
"""
    )
    history = tmp_path / "history.duckdb"
    final_result = None
    for run_number, missing in enumerate([0, 1, 2, 3], start=1):
        rows: list[list[object]] = [["customer_id", "email"]]
        rows.extend(
            [f"C{index}", "" if index < missing else f"c{index}@example.com"]
            for index in range(10)
        )
        write_csv(data, rows)
        final_result = QualityRunner(config, history, f"run-{run_number}").run()[0]

    assert final_result is not None
    assert final_result.status is Status.WARN
    assert final_result.observed > 0.1
    assert "trend slope" in final_result.message


def test_shared_metric_is_recorded_once_per_run(tmp_path: Path) -> None:
    data = tmp_path / "customers.csv"
    config = tmp_path / "checks.yml"
    config.write_text(
        """
tables:
  customers: customers.csv
checks:
  - name: email nulls
    type: null_rate
    table: customers
    column: email
    max: 1
  - name: email null trend
    type: trend
    table: customers
    metric: "null_rate:email"
    min_history: 3
    max_bad_slope_pct: 1
  - name: source drift
    type: category_drift
    table: customers
    column: source
    max_total_variation: 1
"""
    )
    history = tmp_path / "history.duckdb"
    write_csv(data, [["email", "source"], ["a@example.com", "web"], ["", "mobile"]])
    for run_id in ["run-1", "run-2", "run-2"]:
        QualityRunner(config, history, run_id).run()
    write_csv(data, [["email", "source"], ["a@example.com", "web"]])
    results = QualityRunner(config, history, "run-2").run()

    with MetricStore(history) as store:
        assert len(store.history("customers", "null_rate:email")) == 2
        assert store.latest_distribution("customers", "distribution:source:", "run-1") == {
            "web": 1.0
        }
    assert "baseline building (2/3)" in results[1].message


@pytest.mark.parametrize(
    ("check", "error"),
    [
        (
            "{name: c, type: null_rate, table: customers, column: email, max_rate: 0.5}",
            "unknown parameter 'max_rate' (did you mean 'max'?)",
        ),
        ("{name: c, type: range, table: customers, column: email}", "at least one of 'min', 'max'"),
        ("{name: c, type: regex, table: customers, column: email}", "parameter 'pattern'"),
        ("{name: c, type: uniqe, table: customers, column: email}", "did you mean 'unique'?"),
        ("{name: c, type: unique, table: profiles, column: email}", "'profiles' is not defined"),
        ("{name: c, type: row_count, table: customers, severity: pass}", "severity must be"),
        (
            "{name: c, type: trend, table: customers, metric: row_count, "
            "max_bad_slope_pct: 0.1, bad_direction: sideways}",
            "bad_direction must be",
        ),
    ],
)
def test_invalid_check_configuration_is_rejected(tmp_path: Path, check: str, error: str) -> None:
    write_csv(tmp_path / "customers.csv", [["email"], ["a@example.com"]])
    config = tmp_path / "checks.yml"
    config.write_text(f"tables:\n  customers: customers.csv\nchecks:\n  - {check}\n")

    with pytest.raises(ValueError, match="Invalid configuration") as raised:
        QualityRunner(config, tmp_path / "history.duckdb", "run-1").run()
    assert error in str(raised.value)


def test_cli_reports_configuration_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    write_csv(tmp_path / "customers.csv", [["email"], ["a@example.com"]])
    config = tmp_path / "checks.yml"
    config.write_text(
        "tables:\n  customers: customers.csv\n"
        "checks:\n  - {name: c, type: row_count, table: customers, minimum: 1}\n"
    )

    exit_code = main(["--config", str(config), "--history", str(tmp_path / "h.duckdb")])

    assert exit_code == 2
    assert "unknown parameter 'minimum'" in capsys.readouterr().err


def test_every_check_type_declares_its_parameters(tmp_path: Path) -> None:
    with MetricStore(tmp_path / "history.duckdb") as store:
        engine = CheckEngine(duckdb.connect(), store, "run-1")
    assert set(engine.handlers) == set(CHECK_PARAMETERS)


def test_example_configuration_runs_without_failures(tmp_path: Path) -> None:
    root = Path(__file__).parents[1]
    results = QualityRunner(
        root / "config/checks.yml", tmp_path / "history.duckdb", "example-run"
    ).run()

    failures = [result.message for result in results if result.status is Status.FAIL]
    assert failures == []
    assert len(results) >= 20
