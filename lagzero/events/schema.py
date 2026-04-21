from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True, slots=True)
class IncidentEvent:
    timestamp: float
    consumer_group: str
    scope: str
    topic: str | None
    partition: int | None
    offset_lag: int
    processing_rate: float | None
    time_lag_sec: float | None
    time_lag_source: str
    lag_velocity: float | None
    anomaly: str | None
    severity: str
    confidence: float
    correlations: list[str] = field(default_factory=list)
    diagnostics: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
