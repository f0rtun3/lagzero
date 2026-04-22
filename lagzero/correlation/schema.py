from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ExternalEvent:
    event_id: str
    timestamp: float
    event_type: str
    source: str
    service: str | None = None
    consumer_group: str | None = None
    topic: str | None = None
    partition: int | None = None
    severity: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CorrelationMatch:
    correlation_type: str
    event_id: str
    event_type: str
    source: str
    service: str | None
    confidence: float
    time_diff_sec: float
    role: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

