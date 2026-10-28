"""Built-in data quality checks."""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from statistics import mean
from typing import Any

import duckdb

from .models import CheckParameters, CheckResult, CheckSpec, Status
from .store import MetricStore

_UNIQUE_PARAMETERS = CheckParameters(optional=("max_duplicates",), one_of=("column", "columns"))

CHECK_PARAMETERS: dict[str, CheckParameters] = {
    "required_columns": CheckParameters(required=("columns",)),
    "data_type": CheckParameters(required=("column", "allowed")),
    "null_rate": CheckParameters(required=("column",), optional=("max",)),
    "unique": _UNIQUE_PARAMETERS,
    "accepted_values": CheckParameters(required=("column", "values")),
    "regex": CheckParameters(required=("column", "pattern")),
    "range": CheckParameters(required=("column",), one_of=("min", "max")),
    "sql_expression": CheckParameters(required=("expression",)),
    "referential_integrity": CheckParameters(
        required=("column", "parent_table", "parent_column"), optional=("max_orphan_rate",)
    ),
    "freshness": CheckParameters(
        required=("column", "max_age_hours"), optional=("reference_time",)
    ),
    "row_count": CheckParameters(optional=("min", "max")),
    "late_arriving": CheckParameters(
        required=("event_time_column", "ingestion_time_column", "max_lag_hours"),
        optional=("max_late_rate",),
    ),
    "partition_completeness": CheckParameters(required=("column", "expected_partitions")),
    "duplicate_ingestion": _UNIQUE_PARAMETERS,
    "run_comparison": CheckParameters(required=("metric", "max_change_pct")),
    "rolling_baseline": CheckParameters(
        required=("metric", "max_deviation_pct"), optional=("window", "min_history")
    ),
    "category_drift": CheckParameters(required=("column", "max_total_variation")),
    "trend": CheckParameters(
        required=("metric", "max_bad_slope_pct"),
        optional=("window", "min_history", "bad_direction"),
    ),
}


def identifier(value: str) -> str:
    """Quote a trusted SQL identifier while rejecting SQL fragments."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return f'"{value}"'


class CheckEngine:
    def __init__(
        self, connection: duckdb.DuckDBPyConnection, store: MetricStore, run_id: str
    ) -> None:
        self.db = connection
        self.store = store
        self.run_id = run_id
        self.handlers: dict[str, Callable[[CheckSpec], CheckResult]] = {
            "required_columns": self.required_columns,
            "data_type": self.data_type,
            "null_rate": self.null_rate,
            "unique": self.unique,
            "accepted_values": self.accepted_values,
            "regex": self.regex,
            "range": self.value_range,
            "sql_expression": self.sql_expression,
            "referential_integrity": self.referential_integrity,
            "freshness": self.freshness,
            "row_count": self.row_count,
            "late_arriving": self.late_arriving,
            "partition_completeness": self.partition_completeness,
            "duplicate_ingestion": self.duplicate_ingestion,
            "run_comparison": self.run_comparison,
            "rolling_baseline": self.rolling_baseline,
            "category_drift": self.category_drift,
            "trend": self.trend,
        }

    def execute(self, spec: CheckSpec) -> CheckResult:
        handler = self.handlers.get(spec.type)
        if handler is None:
            raise ValueError(f"Unknown check type {spec.type!r} in {spec.name!r}")
        result = handler(spec)
        if result.metric_name is not None and result.metric_value is not None:
            self.store.save(self.run_id, spec.table, result.metric_name, result.metric_value)
        return result

    def _result(
        self,
        spec: CheckSpec,
        passed: bool,
        observed: Any,
        expected: str,
        message: str,
        metric_name: str | None = None,
        metric_value: float | None = None,
        no_baseline: bool = False,
    ) -> CheckResult:
        status = Status.WARN if no_baseline else (Status.PASS if passed else spec.severity)
        return CheckResult(
            name=spec.name,
            check_type=spec.type,
            table=spec.table,
            status=status,
            observed=observed,
            expected=expected,
            message=message,
            metric_name=metric_name,
            metric_value=metric_value,
        )

    def _column(self, spec: CheckSpec) -> str:
        if not spec.column:
            raise ValueError(f"{spec.name!r} requires 'column'")
        return identifier(spec.column)

    def _scalar_metric(self, table: str, metric: str) -> float:
        table_id = identifier(table)
        if metric == "row_count":
            return float(self.db.execute(f"SELECT count(*) FROM {table_id}").fetchone()[0])
        if metric.startswith("null_rate:"):
            column = identifier(metric.split(":", 1)[1])
            return float(
                self.db.execute(
                    f"SELECT coalesce(avg(({column} IS NULL)::INT), 0) FROM {table_id}"
                ).fetchone()[0]
            )
        if metric.startswith("distinct_count:"):
            column = identifier(metric.split(":", 1)[1])
            return float(
                self.db.execute(f"SELECT count(DISTINCT {column}) FROM {table_id}").fetchone()[0]
            )
        raise ValueError(
            f"Unsupported metric {metric!r}; use row_count, null_rate:<column>, "
            "or distinct_count:<column>"
        )

    def required_columns(self, spec: CheckSpec) -> CheckResult:
        expected = list(spec.params["columns"])
        present = {
            row[0]
            for row in self.db.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                [spec.table],
            ).fetchall()
        }
        missing = sorted(set(expected) - present)
        return self._result(
            spec, not missing, missing, f"all columns present: {expected}", "Required columns"
        )

    def data_type(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        actual = self.db.execute(
            f"SELECT typeof({column}) FROM {identifier(spec.table)} LIMIT 1"
        ).fetchone()
        actual_type = actual[0].upper() if actual else "UNKNOWN"
        allowed = [str(value).upper() for value in spec.params["allowed"]]
        return self._result(
            spec,
            actual_type in allowed,
            actual_type,
            f"type in {allowed}",
            f"Type of {spec.column}",
        )

    def null_rate(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        rate = float(
            self.db.execute(
                f"SELECT coalesce(avg(({column} IS NULL)::INT), 0) FROM {identifier(spec.table)}"
            ).fetchone()[0]
        )
        maximum = float(spec.params.get("max", 0))
        return self._result(
            spec,
            rate <= maximum,
            round(rate, 6),
            f"null rate <= {maximum}",
            f"{rate:.2%} NULL",
            f"null_rate:{spec.column}",
            rate,
        )

    def unique(self, spec: CheckSpec) -> CheckResult:
        columns = spec.params.get("columns") or [spec.column]
        if not all(columns):
            raise ValueError(f"{spec.name!r} requires 'column' or 'columns'")
        group = ", ".join(identifier(value) for value in columns)
        duplicates = int(
            self.db.execute(
                f"SELECT coalesce(sum(n - 1), 0) FROM "
                f"(SELECT count(*) n FROM {identifier(spec.table)} GROUP BY {group} HAVING n > 1)"
            ).fetchone()[0]
        )
        maximum = int(spec.params.get("max_duplicates", 0))
        return self._result(
            spec,
            duplicates <= maximum,
            duplicates,
            f"duplicate rows <= {maximum}",
            f"{duplicates} duplicates",
            f"duplicate_count:{','.join(columns)}",
            float(duplicates),
        )

    def accepted_values(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        values = list(spec.params["values"])
        placeholders = ", ".join("?" for _ in values)
        invalid = int(
            self.db.execute(
                f"SELECT count(*) FROM {identifier(spec.table)} "
                f"WHERE {column} IS NOT NULL AND {column} NOT IN ({placeholders})",
                values,
            ).fetchone()[0]
        )
        return self._result(
            spec, invalid == 0, invalid, f"values in {values}", f"{invalid} invalid values"
        )

    def regex(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        pattern = str(spec.params["pattern"])
        invalid = int(
            self.db.execute(
                f"SELECT count(*) FROM {identifier(spec.table)} "
                f"WHERE {column} IS NOT NULL AND NOT regexp_matches(CAST({column} AS VARCHAR), ?)",
                [pattern],
            ).fetchone()[0]
        )
        return self._result(
            spec, invalid == 0, invalid, f"all non-NULL values match {pattern!r}", "format check"
        )

    def value_range(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        clauses = [f"{column} IS NOT NULL"]
        params: list[Any] = []
        if "min" in spec.params:
            clauses.append(f"{column} < ?")
            params.append(spec.params["min"])
        if "max" in spec.params:
            operator = "OR" if len(clauses) > 1 else ""
            clauses.append(f"{operator} {column} > ?")
            params.append(spec.params["max"])
        range_clause = " ".join(clauses[1:])
        invalid = int(
            self.db.execute(
                f"SELECT count(*) FROM {identifier(spec.table)} "
                f"WHERE {column} IS NOT NULL AND ({range_clause})",
                params,
            ).fetchone()[0]
        )
        expected = f"between {spec.params.get('min', '-inf')} and {spec.params.get('max', 'inf')}"
        return self._result(spec, invalid == 0, invalid, expected, f"{invalid} out of range")

    def sql_expression(self, spec: CheckSpec) -> CheckResult:
        expression = str(spec.params["expression"])
        invalid = int(
            self.db.execute(
                f"SELECT count(*) FROM {identifier(spec.table)} "
                f"WHERE NOT coalesce(({expression}), FALSE)"
            ).fetchone()[0]
        )
        return self._result(
            spec, invalid == 0, invalid, f"expression is true: {expression}", "business rule"
        )

    def referential_integrity(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        parent_table = identifier(str(spec.params["parent_table"]))
        parent_column = identifier(str(spec.params["parent_column"]))
        orphans = int(
            self.db.execute(
                f"SELECT count(*) FROM {identifier(spec.table)} child "
                f"LEFT JOIN {parent_table} parent ON child.{column} = parent.{parent_column} "
                f"WHERE child.{column} IS NOT NULL AND parent.{parent_column} IS NULL"
            ).fetchone()[0]
        )
        max_rate = float(spec.params.get("max_orphan_rate", 0))
        total = int(
            self.db.execute(f"SELECT count(*) FROM {identifier(spec.table)}").fetchone()[0]
        )
        rate = orphans / total if total else 0.0
        return self._result(
            spec,
            rate <= max_rate,
            round(rate, 6),
            f"orphan rate <= {max_rate}",
            f"{orphans} orphan rows ({rate:.2%})",
            f"orphan_rate:{spec.column}",
            rate,
        )

    def freshness(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        reference = spec.params.get("reference_time")
        reference_sql = "CAST(? AS TIMESTAMP)" if reference else "CURRENT_TIMESTAMP"
        params = [reference] if reference else []
        age_hours = self.db.execute(
            f"SELECT date_diff('second', max({column}), {reference_sql}) / 3600.0 "
            f"FROM {identifier(spec.table)}",
            params,
        ).fetchone()[0]
        age = float(age_hours) if age_hours is not None else math.inf
        maximum = float(spec.params["max_age_hours"])
        return self._result(
            spec,
            age <= maximum,
            round(age, 3),
            f"last record age <= {maximum} hours",
            f"latest record is {age:.2f} hours old",
            f"freshness_hours:{spec.column}",
            age if math.isfinite(age) else None,
        )

    def row_count(self, spec: CheckSpec) -> CheckResult:
        count = int(
            self.db.execute(f"SELECT count(*) FROM {identifier(spec.table)}").fetchone()[0]
        )
        minimum = int(spec.params.get("min", 0))
        maximum = int(spec.params.get("max", 2**63 - 1))
        return self._result(
            spec,
            minimum <= count <= maximum,
            count,
            f"row count between {minimum} and {maximum}",
            f"{count} rows",
            "row_count",
            float(count),
        )

    def late_arriving(self, spec: CheckSpec) -> CheckResult:
        event_column = identifier(str(spec.params["event_time_column"]))
        ingestion_column = identifier(str(spec.params["ingestion_time_column"]))
        maximum = float(spec.params["max_lag_hours"])
        late = int(
            self.db.execute(
                f"SELECT count(*) FROM {identifier(spec.table)} "
                f"WHERE date_diff('second', {event_column}, {ingestion_column}) / 3600.0 > ?",
                [maximum],
            ).fetchone()[0]
        )
        max_rate = float(spec.params.get("max_late_rate", 0))
        total = int(
            self.db.execute(f"SELECT count(*) FROM {identifier(spec.table)}").fetchone()[0]
        )
        rate = late / total if total else 0.0
        return self._result(
            spec,
            rate <= max_rate,
            round(rate, 6),
            f"late row rate <= {max_rate}",
            f"{late} rows arrived over {maximum}h late",
            "late_arrival_rate",
            rate,
        )

    def partition_completeness(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        expected = int(spec.params["expected_partitions"])
        actual = int(
            self.db.execute(
                f"SELECT count(DISTINCT CAST({column} AS DATE)) FROM {identifier(spec.table)}"
            ).fetchone()[0]
        )
        return self._result(
            spec,
            actual >= expected,
            actual,
            f"at least {expected} date partitions",
            f"{actual} partitions found",
            f"partition_count:{spec.column}",
            float(actual),
        )

    def duplicate_ingestion(self, spec: CheckSpec) -> CheckResult:
        return self.unique(spec)

    def run_comparison(self, spec: CheckSpec) -> CheckResult:
        metric = str(spec.params["metric"])
        current = self._scalar_metric(spec.table, metric)
        history = self.store.history(spec.table, metric, limit=1, exclude_run=self.run_id)
        if not history:
            return self._result(
                spec,
                True,
                current,
                "previous run available",
                "baseline recorded; comparison starts next run",
                metric,
                current,
                no_baseline=True,
            )
        previous = history[0].value
        change = (
            (current - previous) / abs(previous)
            if previous
            else (0 if current == 0 else math.inf)
        )
        maximum = float(spec.params["max_change_pct"])
        passed = abs(change) <= maximum
        return self._result(
            spec,
            passed,
            round(change, 6),
            f"absolute run-over-run change <= {maximum}",
            f"{previous:g} -> {current:g} ({change:+.2%})",
            metric,
            current,
        )

    def rolling_baseline(self, spec: CheckSpec) -> CheckResult:
        metric = str(spec.params["metric"])
        current = self._scalar_metric(spec.table, metric)
        window = int(spec.params.get("window", 7))
        minimum = int(spec.params.get("min_history", 3))
        history = self.store.history(spec.table, metric, limit=window, exclude_run=self.run_id)
        if len(history) < minimum:
            return self._result(
                spec,
                True,
                current,
                f"at least {minimum} baseline runs",
                f"baseline building ({len(history)}/{minimum})",
                metric,
                current,
                no_baseline=True,
            )
        values = [point.value for point in history]
        baseline = mean(values)
        deviation = abs(current - baseline) / abs(baseline) if baseline else 0.0
        maximum = float(spec.params["max_deviation_pct"])
        return self._result(
            spec,
            deviation <= maximum,
            round(deviation, 6),
            f"deviation from {window}-run mean <= {maximum}",
            f"current {current:g}, baseline {baseline:g}, deviation {deviation:.2%}",
            metric,
            current,
        )

    def category_drift(self, spec: CheckSpec) -> CheckResult:
        column = self._column(spec)
        prefix = f"distribution:{spec.column}:"
        rows = self.db.execute(
            f"SELECT coalesce(CAST({column} AS VARCHAR), '<NULL>'), count(*) "
            f"FROM {identifier(spec.table)} GROUP BY 1"
        ).fetchall()
        total = sum(row[1] for row in rows)
        current = {str(value): count / total for value, count in rows} if total else {}
        previous = self.store.latest_distribution(spec.table, prefix, self.run_id)
        self.store.delete_prefix(self.run_id, spec.table, prefix)
        for value, proportion in current.items():
            self.store.save(self.run_id, spec.table, f"{prefix}{value}", proportion)
        if not previous:
            return self._result(
                spec,
                True,
                0.0,
                "previous distribution available",
                "baseline distribution recorded; comparison starts next run",
                no_baseline=True,
            )
        categories = set(current) | set(previous)
        distance = 0.5 * sum(abs(current.get(key, 0) - previous.get(key, 0)) for key in categories)
        maximum = float(spec.params["max_total_variation"])
        return self._result(
            spec,
            distance <= maximum,
            round(distance, 6),
            f"total variation distance <= {maximum}",
            f"category distribution changed by {distance:.2%}",
        )

    def trend(self, spec: CheckSpec) -> CheckResult:
        metric = str(spec.params["metric"])
        current = self._scalar_metric(spec.table, metric)
        window = int(spec.params.get("window", 7))
        minimum = int(spec.params.get("min_history", 4))
        history = self.store.history(spec.table, metric, limit=window - 1, exclude_run=self.run_id)
        values = [point.value for point in reversed(history)] + [current]
        if len(values) < minimum:
            return self._result(
                spec,
                True,
                current,
                f"at least {minimum} trend points",
                f"trend baseline building ({len(values)}/{minimum})",
                metric,
                current,
                no_baseline=True,
            )
        x_mean = (len(values) - 1) / 2
        y_mean = mean(values)
        denominator = sum((x - x_mean) ** 2 for x in range(len(values)))
        slope = sum((x - x_mean) * (y - y_mean) for x, y in enumerate(values)) / denominator
        slope_pct = slope / abs(y_mean) if y_mean else 0.0
        direction = str(spec.params.get("bad_direction", "down"))
        signed_bad_slope = -slope_pct if direction == "down" else slope_pct
        maximum = float(spec.params["max_bad_slope_pct"])
        passed = signed_bad_slope <= maximum
        return self._result(
            spec,
            passed,
            round(slope_pct, 6),
            f"{direction} degradation slope <= {maximum} per run",
            f"trend slope is {slope_pct:+.2%} per run over {len(values)} runs",
            metric,
            current,
        )
