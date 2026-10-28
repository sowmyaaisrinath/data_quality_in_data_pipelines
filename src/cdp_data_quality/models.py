"""Core configuration and result models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class Status(str, Enum):
    PASS = "pass"
    INFORMATION = "information"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True)
class CheckParameters:
    """Configuration keys accepted by a check type, beyond the common fields."""

    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    one_of: tuple[str, ...] = ()  # At least one of these keys must be present.

    @property
    def allowed(self) -> set[str]:
        return {*self.required, *self.optional, *self.one_of}


@dataclass(frozen=True)
class CheckSpec:
    name: str
    type: str
    table: str
    column: str | None = None
    severity: Status = Status.FAIL
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckResult:
    name: str
    check_type: str
    table: str
    status: Status
    observed: Any
    expected: str
    message: str
    metric_name: str | None = None
    metric_value: float | None = None

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["status"] = self.status.value
        return result


@dataclass(frozen=True)
class Metric:
    run_id: str
    recorded_at: str
    table_name: str
    metric_name: str
    value: float
