from __future__ import annotations

from lagzero.events.schema import IncidentEvent


def build_incident_key(event: IncidentEvent, family: str) -> str:
    if event.scope == "partition":
        topic = event.topic or "unknown-topic"
        partition = event.partition if event.partition is not None else -1
        consumer_group = event.consumer_group or "unknown-group"
        return f"partition:{consumer_group}:{topic}:{partition}:{family}"
    consumer_group = event.consumer_group or "unknown-group"
    return f"consumer_group:{consumer_group}:{family}"


def build_scope_key(event: IncidentEvent) -> str:
    if event.scope == "partition":
        topic = event.topic or "unknown-topic"
        partition = event.partition if event.partition is not None else -1
        consumer_group = event.consumer_group or "unknown-group"
        return f"partition:{consumer_group}:{topic}:{partition}"
    consumer_group = event.consumer_group or "unknown-group"
    return f"consumer_group:{consumer_group}"
