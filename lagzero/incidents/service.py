from __future__ import annotations

import json
import time
from dataclasses import replace

from lagzero.incidents.schema import DeliveryRecord, IncidentRecord, TimelineEntry
from lagzero.persistence.repository import IncidentRepository
from lagzero.sinks.webhook import WebhookSink


class IncidentHistoryService:
    def __init__(
        self,
        *,
        repository: IncidentRepository,
        webhook_sink: WebhookSink | None = None,
    ) -> None:
        self._repository = repository
        self._webhook_sink = webhook_sink

    def list_incidents(
        self,
        *,
        status: str | None = None,
        consumer_group: str | None = None,
        family: str | None = None,
        limit: int = 50,
    ) -> list[IncidentRecord]:
        return self._repository.list_incidents(
            status=status,
            consumer_group=consumer_group,
            family=family,
            limit=limit,
        )

    def get_incident(self, incident_id: str) -> IncidentRecord | None:
        return self._repository.get_incident_by_id(incident_id)

    def get_timeline(self, incident_id: str) -> list[TimelineEntry]:
        return self._repository.get_timeline_entries(incident_id)

    def list_deliveries(
        self,
        *,
        incident_id: str | None = None,
        delivery_state: str | None = None,
        limit: int = 50,
    ) -> list[DeliveryRecord]:
        return self._repository.list_deliveries(
            incident_id=incident_id,
            delivery_state=delivery_state,
            limit=limit,
        )

    def redeliver_event(self, event_id: str) -> DeliveryRecord:
        if self._webhook_sink is None:
            raise RuntimeError("Webhook sink is not configured.")
        record = self._repository.get_delivery_by_event_id(event_id)
        if record is None:
            raise KeyError(f"Unknown event_id '{event_id}'.")
        updated = self._attempt_delivery(record)
        self._repository.update_delivery(updated)
        return updated

    def redeliver_incident(self, incident_id: str) -> list[DeliveryRecord]:
        records = self._repository.list_deliveries(incident_id=incident_id, limit=500)
        return [self.redeliver_event(record.event_id) for record in records]

    def redeliver_by_state(self, delivery_state: str) -> list[DeliveryRecord]:
        records = self._repository.list_deliveries(
            delivery_state=delivery_state,
            limit=500,
        )
        return [self.redeliver_event(record.event_id) for record in records]

    def _attempt_delivery(self, record: DeliveryRecord) -> DeliveryRecord:
        payload = json.loads(record.payload_json)
        event_id = str(payload["event_id"])
        event_type = str(payload["event_type"])
        timestamp = str(payload["timestamp"])
        attempts = record.delivery_attempts + 1
        try:
            self._webhook_sink.emit_serialized(
                raw_body=record.payload_json,
                event_type=event_type,
                event_id=event_id,
                timestamp=timestamp,
            )
        except Exception as exc:
            return replace(
                record,
                delivery_state="failed",
                delivery_attempts=attempts,
                last_delivery_error=str(exc),
                last_delivery_at=time.time(),
            )
        return replace(
            record,
            delivery_state="delivered",
            delivery_attempts=attempts,
            last_delivery_error=None,
            last_delivery_at=time.time(),
        )
