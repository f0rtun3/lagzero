from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True, slots=True)
class IncidentEvent:
    timestamp: float
    topic: str
    partition: int
    consumer_group: str
    offset_lag: int
    processing_rate: float | None
    time_lag_sec: float | None
    anomaly: str | None
    severity: str
    confidence: float
    correlations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

