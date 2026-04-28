from __future__ import annotations

import uuid

from lagzero.events.schema import IncidentEvent
from lagzero.incidents.schema import IncidentRecord, TimelineEntry


def build_opened_entry(incident: IncidentRecord, event: IncidentEvent) -> TimelineEntry:
    return TimelineEntry(
        timeline_id=str(uuid.uuid4()),
        incident_id=incident.incident_id,
        entry_type="incident_opened",
        at=event.timestamp,
        summary=f"Incident opened as {event.anomaly}",
        details={
            "anomaly": event.anomaly,
            "service_health": event.service_health,
            "severity": event.severity,
        },
    )


def build_updated_entry(
    incident: IncidentRecord,
    event: IncidentEvent,
    change_types: list[str],
) -> TimelineEntry:
    primary_change = change_types[0] if change_types else "incident_updated"
    if primary_change == "anomaly_changed":
        summary = f"Anomaly changed to {event.anomaly}"
    elif primary_change == "health_changed":
        summary = f"Health changed to {event.service_health}"
    elif primary_change == "severity_changed":
        summary = f"Severity changed to {event.severity}"
    elif primary_change in {"cause_changed", "cause_confidence_changed"}:
        summary = f"Primary cause changed to {event.primary_cause or 'unknown'}"
    elif primary_change == "lag_bucket_changed":
        summary = "Time-lag bucket changed"
    elif primary_change == "ai_explanation_updated":
        summary = "AI explanation changed"
    else:
        summary = "Incident state changed"
    return TimelineEntry(
        timeline_id=str(uuid.uuid4()),
        incident_id=incident.incident_id,
        entry_type=primary_change,
        at=event.timestamp,
        summary=summary,
        details={"changes": change_types},
    )


def build_resolved_entry(incident: IncidentRecord, event: IncidentEvent) -> TimelineEntry:
    return TimelineEntry(
        timeline_id=str(uuid.uuid4()),
        incident_id=incident.incident_id,
        entry_type="incident_resolved",
        at=event.timestamp,
        summary="Incident resolved",
        details={
            "anomaly": event.anomaly,
            "service_health": event.service_health,
        },
    )
