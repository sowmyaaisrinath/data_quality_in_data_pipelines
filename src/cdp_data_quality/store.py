"""DuckDB-backed history for quality metrics."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .models import Metric


class MetricStore:
    def __init__(self, path: str | Path = ".quality_history.duckdb") -> None:
        self.connection = duckdb.connect(str(path))
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_metrics (
                run_id VARCHAR NOT NULL,
                recorded_at TIMESTAMP NOT NULL,
                table_name VARCHAR NOT NULL,
                metric_name VARCHAR NOT NULL,
                value DOUBLE NOT NULL
            )
            """
        )

    def save(self, run_id: str, table: str, metric_name: str, value: float) -> None:
        """Record a metric, keeping a single value per run, table, and metric."""
        self.connection.execute(
            "DELETE FROM quality_metrics WHERE run_id = ? AND table_name = ? AND metric_name = ?",
            [run_id, table, metric_name],
        )
        self.connection.execute(
            "INSERT INTO quality_metrics VALUES (?, ?, ?, ?, ?)",
            [run_id, datetime.now(timezone.utc), table, metric_name, value],
        )

    def delete_prefix(self, run_id: str, table: str, metric_prefix: str) -> None:
        self.connection.execute(
            "DELETE FROM quality_metrics "
            "WHERE run_id = ? AND table_name = ? AND starts_with(metric_name, ?)",
            [run_id, table, metric_prefix],
        )

    def history(
        self, table: str, metric_name: str, limit: int = 30, exclude_run: str | None = None
    ) -> list[Metric]:
        query = """
            SELECT run_id, CAST(recorded_at AS VARCHAR), table_name, metric_name, value
            FROM quality_metrics
            WHERE table_name = ? AND metric_name = ?
        """
        params: list[object] = [table, metric_name]
        if exclude_run is not None:
            query += " AND run_id <> ?"
            params.append(exclude_run)
        query += " ORDER BY recorded_at DESC LIMIT ?"
        params.append(limit)
        return [Metric(*row) for row in self.connection.execute(query, params).fetchall()]

    def latest_distribution(
        self, table: str, metric_prefix: str, exclude_run: str
    ) -> dict[str, float]:
        row = self.connection.execute(
            """
            SELECT run_id
            FROM quality_metrics
            WHERE table_name = ? AND metric_name LIKE ? AND run_id <> ?
            ORDER BY recorded_at DESC
            LIMIT 1
            """,
            [table, f"{metric_prefix}%", exclude_run],
        ).fetchone()
        if row is None:
            return {}
        rows = self.connection.execute(
            """
            SELECT metric_name, value
            FROM quality_metrics
            WHERE table_name = ? AND metric_name LIKE ? AND run_id = ?
            """,
            [table, f"{metric_prefix}%", row[0]],
        ).fetchall()
        return {name.removeprefix(metric_prefix): value for name, value in rows}

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> MetricStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
