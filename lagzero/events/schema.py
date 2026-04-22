from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class IncidentEvent:
    timestamp: float
    consumer_group: str
    scope: str
    topic: str | None
    partition: int | None
    offset_lag: int
    processing_rate: float | None
    producer_rate: float | None
    consumer_efficiency: float | None
    backlog_growth_rate: float | None
    time_lag_sec: float | None
    time_lag_source: str
    timestamp_type: str | None
    backlog_head_timestamp: float | None
    latest_message_timestamp: float | None
    lag_divergence_sec: float | None
    lag_velocity: float | None
    anomaly: str | None
    severity: str
    service_health: str | None
    confidence: float
    correlations: list[dict[str, Any]] = field(default_factory=list)
    primary_cause: str | None = None
    primary_cause_confidence: float | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
