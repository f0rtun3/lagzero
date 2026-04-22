from __future__ import annotations

from lagzero.correlation.schema import ExternalEvent


def parse_external_event_payload(payload: dict[str, object]) -> ExternalEvent:
    return ExternalEvent(
        event_id=str(payload["event_id"]),
        timestamp=float(payload["timestamp"]),
        event_type=str(payload["event_type"]),
        source=str(payload["source"]),
        service=str(payload["service"]) if payload.get("service") is not None else None,
        consumer_group=(
            str(payload["consumer_group"]) if payload.get("consumer_group") is not None else None
        ),
        topic=str(payload["topic"]) if payload.get("topic") is not None else None,
        partition=int(payload["partition"]) if payload.get("partition") is not None else None,
        severity=str(payload["severity"]) if payload.get("severity") is not None else None,
        metadata=dict(payload.get("metadata", {})),
    )
