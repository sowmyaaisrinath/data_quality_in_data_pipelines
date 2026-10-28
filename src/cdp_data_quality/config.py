"""YAML configuration loading and validation."""

from __future__ import annotations

from collections.abc import Iterable
from difflib import get_close_matches
from pathlib import Path
from typing import Any

import yaml

from .checks import CHECK_PARAMETERS
from .models import CheckSpec, Status

COMMON_FIELDS = ("name", "type", "table", "severity")
SEVERITIES = (Status.FAIL.value, Status.WARN.value, Status.INFORMATION.value)
TREND_DIRECTIONS = ("up", "down")


def load_config(path: str | Path) -> tuple[dict[str, str], list[CheckSpec]]:
    config_path = Path(path)
    raw: dict[str, Any] = yaml.safe_load(config_path.read_text()) or {}
    tables = raw.get("tables")
    checks = raw.get("checks")
    if not isinstance(tables, dict) or not tables:
        raise ValueError("Configuration must define a non-empty 'tables' mapping")
    if not isinstance(checks, list) or not checks:
        raise ValueError("Configuration must define a non-empty 'checks' list")

    resolved_tables = {
        name: str((config_path.parent / source).resolve())
        if not Path(source).is_absolute()
        else source
        for name, source in tables.items()
    }

    errors: list[str] = []
    specs: list[CheckSpec] = []
    for index, item in enumerate(checks, start=1):
        if not isinstance(item, dict):
            errors.append(f"check #{index}: must be a mapping")
            continue
        label = f"check #{index}" + (f" ({item['name']!r})" if "name" in item else "")
        problems = check_problems(item, tables)
        if problems:
            errors.extend(f"{label}: {problem}" for problem in problems)
            continue
        specs.append(
            CheckSpec(
                name=item["name"],
                type=item["type"],
                table=item["table"],
                column=item.get("column"),
                severity=Status(item.get("severity", "fail")),
                params={
                    key: value
                    for key, value in item.items()
                    if key not in COMMON_FIELDS and key != "column"
                },
            )
        )
    if errors:
        raise ValueError("Invalid configuration:\n" + "\n".join(f"  - {e}" for e in errors))
    return resolved_tables, specs


def check_problems(item: dict[str, Any], tables: dict[str, Any]) -> list[str]:
    """Describe every configuration mistake in one check definition."""
    missing = [key for key in ("name", "type", "table") if key not in item]
    if missing:
        return [f"missing {', '.join(repr(key) for key in missing)}"]

    problems: list[str] = []
    for key in ("table", "parent_table"):
        if key in item and item[key] not in tables:
            problems.append(f"{key} {item[key]!r} is not defined in 'tables'")
    if item.get("severity", "fail") not in SEVERITIES:
        problems.append(f"severity must be one of {_quoted(SEVERITIES)}")

    parameters = CHECK_PARAMETERS.get(item["type"])
    if parameters is None:
        suggestion = _suggest(item["type"], CHECK_PARAMETERS)
        return [*problems, f"unknown type {item['type']!r}{suggestion}"]

    given = set(item) - set(COMMON_FIELDS)
    for key in sorted(given - parameters.allowed):
        problems.append(f"unknown parameter {key!r}{_suggest(key, parameters.allowed)}")
    for key in parameters.required:
        if key not in item:
            problems.append(f"missing required parameter {key!r}")
    if parameters.one_of and not given & set(parameters.one_of):
        problems.append(f"requires at least one of {_quoted(parameters.one_of)}")
    if item["type"] == "trend" and item.get("bad_direction", "down") not in TREND_DIRECTIONS:
        problems.append(f"bad_direction must be one of {_quoted(TREND_DIRECTIONS)}")
    return problems


def _suggest(value: str, choices: Iterable[str]) -> str:
    matches = get_close_matches(str(value), list(choices), n=1, cutoff=0.5)
    return f" (did you mean {matches[0]!r}?)" if matches else ""


def _quoted(values: Iterable[str]) -> str:
    return ", ".join(repr(value) for value in values)
