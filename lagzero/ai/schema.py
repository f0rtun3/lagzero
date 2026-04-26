from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class AIExplanation:
    summary: str
    probable_cause_explanation: str
    impact: str
    recommended_actions: list[str]
    caveats: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ExplanationContext:
    timestamp: float
    scope: str
    consumer_group: str | None
    topic: str | None
    partition: int | None
    anomaly: str
    health: str
    severity: str | None
    confidence: float | None
    offset_lag: int | None
    time_lag_sec: float | None
    time_lag_source: str | None
    lag_divergence_sec: float | None
    producer_rate: float | None
    consumer_rate: float | None
    backlog_growth_rate: float | None
    consumer_efficiency: float | None
    primary_cause: str | None
    primary_cause_confidence: float | None
    correlations: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

