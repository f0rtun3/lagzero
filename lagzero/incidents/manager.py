from __future__ import annotations

import logging
import json
from dataclasses import replace
from datetime import UTC, datetime
import uuid

from lagzero.events.schema import IncidentEvent
from lagzero.incidents.families import incident_family_for_anomaly
from lagzero.incidents.keys import build_incident_key
from lagzero.incidents.lifecycle import material_change_types
from lagzero.incidents.schema import (
    DeliveryRecord,
    IncidentRecord,
    LifecycleResult,
    WebhookEventEnvelope,
)
from lagzero.incidents.store import IncidentStateStore
from lagzero.incidents.timeline import (
    build_opened_entry,
    build_resolved_entry,
    build_updated_entry,
)
from lagzero.persistence.repository import IncidentRepository
from lagzero.sinks.webhook import WebhookSink

logger = logging.getLogger(__name__)


class IncidentLifecycleManager:
    def __init__(
        self,
        *,
        repository: IncidentRepository | None = None,
        webhook_sink: WebhookSink | None = None,
        resolve_confirmations: int = 2,
        enabled: bool = False,
        source: str = "lagzero",
    ) -> None:
        self._repository = repository
        self._webhook_sink = webhook_sink
        self._resolve_confirmations = resolve_confirmations
        self._enabled = enabled and repository is not None
        self._source = source
        self._state_store = IncidentStateStore()

    def handle_stable_incident(self, event: IncidentEvent) -> list[LifecycleResult]:
        if not self._enabled:
            return []

        try:
            return self._handle(event)
        except Exception:
            logger.exception(
                "Incident lifecycle handling failed for scope=%s consumer_group=%s anomaly=%s",
                event.scope,
                event.consumer_group,
                event.anomaly,
            )
            return []

    def _handle(self, event: IncidentEvent) -> list[LifecycleResult]:
        family = incident_family_for_anomaly(event.anomaly)
        if family is None or event.anomaly == "normal" or event.service_health == "healthy":
            return self._maybe_resolve(event)
        return self._open_or_update(event, family)

    def _open_or_update(self, event: IncidentEvent, family: str) -> list[LifecycleResult]:
        incident_key = build_incident_key(event, family)
        existing = self._repository.get_active_incident_by_key(incident_key)
        payload = event.to_dict()
        results: list[LifecycleResult] = []

        if existing is None:
            incident = IncidentRecord(
                incident_id=str(uuid.uuid4()),
                incident_key=incident_key,
                family=family,
                status="open",
                scope=event.scope,
                consumer_group=event.consumer_group,
                topic=event.topic,
                partition=event.partition,
                opened_at=event.timestamp,
                updated_at=event.timestamp,
                resolved_at=None,
                current_anomaly=event.anomaly or "normal",
                current_health=event.service_health,
                current_severity=event.severity,
                current_primary_cause=event.primary_cause,
                current_primary_cause_confidence=event.primary_cause_confidence,
                current_payload=payload,
            )
            timeline_entry = build_opened_entry(incident, event)
            self._repository.insert_incident(incident)
            self._repository.insert_timeline_entry(timeline_entry)
            delivered = self._emit_webhook("incident.opened", incident, timeline_entry, payload)
            results.append(
                LifecycleResult(
                    event_type="incident.opened",
                    incident=incident,
                    timeline_entry=timeline_entry,
                    webhook_delivered=delivered,
                )
            )
            return results

        self._state_store.reset_resolution(existing.incident_id)
        changes = material_change_types(existing.current_payload, payload)
        updated = replace(
            existing,
            status="ongoing" if changes else existing.status,
            updated_at=event.timestamp,
            current_anomaly=event.anomaly or "normal",
            current_health=event.service_health,
            current_severity=event.severity,
            current_primary_cause=event.primary_cause,
            current_primary_cause_confidence=event.primary_cause_confidence,
            current_payload=payload,
        )
        self._repository.update_incident(updated)
        if not changes:
            return results

        timeline_entry = build_updated_entry(updated, event, changes)
        self._repository.insert_timeline_entry(timeline_entry)
        delivered = self._emit_webhook("incident.updated", updated, timeline_entry, payload)
        results.append(
            LifecycleResult(
                event_type="incident.updated",
                incident=updated,
                timeline_entry=timeline_entry,
                webhook_delivered=delivered,
            )
        )
        return results

    def _maybe_resolve(self, event: IncidentEvent) -> list[LifecycleResult]:
        active_incidents = self._repository.get_active_incidents_for_scope(
            scope=event.scope,
            consumer_group=event.consumer_group,
            topic=event.topic,
            partition=event.partition,
        )
        results: list[LifecycleResult] = []
        for incident in active_incidents:
            confirmations = self._state_store.increment_resolution(incident.incident_id)
            if confirmations < self._resolve_confirmations:
                continue
            resolved = replace(
                incident,
                status="resolved",
                updated_at=event.timestamp,
                resolved_at=event.timestamp,
                current_anomaly=event.anomaly or "normal",
                current_health=event.service_health,
                current_severity=event.severity,
                current_primary_cause=event.primary_cause,
                current_primary_cause_confidence=event.primary_cause_confidence,
                current_payload=event.to_dict(),
            )
            timeline_entry = build_resolved_entry(resolved, event)
            self._repository.update_incident(resolved)
            self._repository.insert_timeline_entry(timeline_entry)
            delivered = self._emit_webhook(
                "incident.resolved",
                resolved,
                timeline_entry,
                event.to_dict(),
            )
            self._state_store.reset_resolution(incident.incident_id)
            results.append(
                LifecycleResult(
                    event_type="incident.resolved",
                    incident=resolved,
                    timeline_entry=timeline_entry,
                    webhook_delivered=delivered,
                )
            )
        return results

    def _emit_webhook(
        self,
        event_type: str,
        incident: IncidentRecord,
        timeline_entry,
        current_state: dict[str, object],
    ) -> bool:
        if self._webhook_sink is None:
            return False
        event_id = str(uuid.uuid4())
        try:
            envelope = WebhookEventEnvelope(
                event_id=event_id,
                event_type=event_type,
                event_version=self._webhook_sink.event_version,
                timestamp=datetime.fromtimestamp(timeline_entry.at, tz=UTC).isoformat().replace(
                    "+00:00", "Z"
                ),
                source=self._source,
                incident={
                    "incident_id": incident.incident_id,
                    "incident_key": incident.incident_key,
                    "family": incident.family,
                    "status": incident.status,
                    "scope": incident.scope,
                    "consumer_group": incident.consumer_group,
                    "topic": incident.topic,
                    "partition": incident.partition,
                    "opened_at": incident.opened_at,
                    "updated_at": incident.updated_at,
                    "resolved_at": incident.resolved_at,
                },
                timeline_entry=timeline_entry.to_dict(),
                current_state=current_state,
            )
            raw_body = json.dumps(envelope.to_dict(), sort_keys=True)
            self._repository.insert_delivery(
                DeliveryRecord(
                    event_id=event_id,
                    incident_id=incident.incident_id,
                    timeline_id=timeline_entry.timeline_id,
                    event_type=event_type,
                    event_version=self._webhook_sink.event_version,
                    delivery_state="pending",
                    delivery_attempts=0,
                    last_delivery_error=None,
                    last_delivery_at=None,
                    payload_json=raw_body,
                )
            )
            self._webhook_sink.emit_serialized(
                raw_body=raw_body,
                event_type=event_type,
                event_id=event_id,
                timestamp=envelope.timestamp,
            )
            self._repository.update_delivery(
                DeliveryRecord(
                    event_id=event_id,
                    incident_id=incident.incident_id,
                    timeline_id=timeline_entry.timeline_id,
                    event_type=event_type,
                    event_version=self._webhook_sink.event_version,
                    delivery_state="delivered",
                    delivery_attempts=1,
                    last_delivery_error=None,
                    last_delivery_at=timeline_entry.at,
                    payload_json=raw_body,
                )
            )
            return True
        except Exception as exc:
            self._repository.update_delivery(
                DeliveryRecord(
                    event_id=event_id,
                    incident_id=incident.incident_id,
                    timeline_id=timeline_entry.timeline_id,
                    event_type=event_type,
                    event_version=self._webhook_sink.event_version,
                    delivery_state="failed",
                    delivery_attempts=1,
                    last_delivery_error=str(exc),
                    last_delivery_at=timeline_entry.at,
                    payload_json=raw_body,
                )
            )
            logger.exception(
                "Webhook delivery failed for incident_id=%s event_type=%s",
                incident.incident_id,
                event_type,
            )
            return False
