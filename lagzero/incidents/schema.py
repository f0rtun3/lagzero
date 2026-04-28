from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class IncidentRecord:
    incident_id: str
    incident_key: str
    family: str
    status: str
    scope: str
    consumer_group: str | None
    topic: str | None
    partition: int | None
    opened_at: float
    updated_at: float
    resolved_at: float | None
    current_anomaly: str
    current_health: str | None
    current_severity: str | None
    current_primary_cause: str | None
    current_primary_cause_confidence: float | None
    current_payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TimelineEntry:
    timeline_id: str
    incident_id: str
    entry_type: str
    at: float
    summary: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class WebhookEventEnvelope:
    event_id: str
    event_type: str
    event_version: str
    timestamp: str
    source: str
    incident: dict[str, Any]
    timeline_entry: dict[str, Any]
    current_state: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    event_id: str
    incident_id: str
    timeline_id: str
    event_type: str
    event_version: str
    delivery_state: str
    delivery_attempts: int
    last_delivery_error: str | None
    last_delivery_at: float | None
    payload_json: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LifecycleResult:
    event_type: str
    incident: IncidentRecord
    timeline_entry: TimelineEntry
    webhook_delivered: bool = False
