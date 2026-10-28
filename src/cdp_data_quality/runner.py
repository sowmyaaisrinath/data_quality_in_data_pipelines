"""Configuration-driven quality runner."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .checks import CheckEngine, identifier
from .config import load_config
from .models import CheckResult, Status
from .store import MetricStore


class QualityRunner:
    def __init__(
        self,
        config_path: str | Path,
        history_path: str | Path = ".quality_history.duckdb",
        run_id: str | None = None,
    ) -> None:
        self.config_path = Path(config_path)
        self.history_path = Path(history_path)
        self.run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")

    def run(self) -> list[CheckResult]:
        tables, specs = load_config(self.config_path)
        connection = duckdb.connect()
        try:
            self._register_tables(connection, tables)
            with MetricStore(self.history_path) as store:
                engine = CheckEngine(connection, store, self.run_id)
                results: list[CheckResult] = []
                for spec in specs:
                    try:
                        results.append(engine.execute(spec))
                    except Exception as exc:  # Report bad checks without hiding later results.
                        results.append(
                            CheckResult(
                                name=spec.name,
                                check_type=spec.type,
                                table=spec.table,
                                status=Status.FAIL,
                                observed="check error",
                                expected="check executes successfully",
                                message=f"{type(exc).__name__}: {exc}",
                            )
                        )
                return results
        finally:
            connection.close()

    @staticmethod
    def _register_tables(
        connection: duckdb.DuckDBPyConnection, tables: dict[str, str]
    ) -> None:
        for name, source in tables.items():
            identifier(name)
            path = Path(source)
            if not path.exists():
                raise FileNotFoundError(f"Source for table {name!r} does not exist: {path}")
            suffix = path.suffix.lower()
            if suffix in {".csv", ".tsv"}:
                relation = connection.read_csv(
                    str(path), header=True, delimiter="\t" if suffix == ".tsv" else ","
                )
            elif suffix == ".parquet":
                relation = connection.read_parquet(str(path))
            elif suffix in {".json", ".jsonl", ".ndjson"}:
                relation = connection.read_json(str(path))
            else:
                raise ValueError(f"Unsupported source format for {path}")
            relation.create_view(name)
