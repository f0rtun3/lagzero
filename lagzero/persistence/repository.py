from __future__ import annotations

import json

from lagzero.incidents.schema import DeliveryRecord, IncidentRecord, TimelineEntry
from lagzero.persistence.sqlite import SQLiteIncidentStore


class IncidentRepository:
    def __init__(self, store: SQLiteIncidentStore) -> None:
        self._store = store

    def get_active_incident_by_key(self, incident_key: str) -> IncidentRecord | None:
        with self._store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM incidents WHERE incident_key = ? AND status != 'resolved' "
                "ORDER BY opened_at DESC LIMIT 1",
                (incident_key,),
            ).fetchone()
        return self._row_to_incident(row) if row is not None else None

    def get_active_incidents_for_scope(
        self,
        *,
        scope: str,
        consumer_group: str | None,
        topic: str | None,
        partition: int | None,
    ) -> list[IncidentRecord]:
        with self._store.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM incidents WHERE scope = ? "
                "AND ((consumer_group = ?) OR (consumer_group IS NULL AND ? IS NULL)) "
                "AND ((topic = ?) OR (topic IS NULL AND ? IS NULL)) "
                "AND ((partition = ?) OR (partition IS NULL AND ? IS NULL)) "
                "AND status != 'resolved' ORDER BY opened_at ASC",
                (scope, consumer_group, consumer_group, topic, topic, partition, partition),
            ).fetchall()
        return [self._row_to_incident(row) for row in rows]

    def list_incidents(
        self,
        *,
        status: str | None = None,
        consumer_group: str | None = None,
        family: str | None = None,
        limit: int = 50,
    ) -> list[IncidentRecord]:
        query = "SELECT * FROM incidents"
        clauses: list[str] = []
        params: list[object] = []
        if status and status != "all":
            if status == "active":
                clauses.append("status != 'resolved'")
            else:
                clauses.append("status = ?")
                params.append(status)
        if consumer_group:
            clauses.append("consumer_group = ?")
            params.append(consumer_group)
        if family:
            clauses.append("family = ?")
            params.append(family)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        with self._store.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_incident(row) for row in rows]

    def get_incident_by_id(self, incident_id: str) -> IncidentRecord | None:
        with self._store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM incidents WHERE incident_id = ?",
                (incident_id,),
            ).fetchone()
        return self._row_to_incident(row) if row is not None else None

    def get_timeline_entries(self, incident_id: str) -> list[TimelineEntry]:
        with self._store.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM incident_timeline WHERE incident_id = ? ORDER BY at ASC",
                (incident_id,),
            ).fetchall()
        return [self._row_to_timeline(row) for row in rows]

    def insert_incident(self, incident: IncidentRecord) -> None:
        with self._store.connect() as connection:
            connection.execute(
                "INSERT INTO incidents (incident_id, incident_key, family, status, scope, "
                "consumer_group, topic, partition, opened_at, updated_at, resolved_at, "
                "current_anomaly, current_health, current_severity, current_primary_cause, "
                "current_primary_cause_confidence, current_payload_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    incident.incident_id,
                    incident.incident_key,
                    incident.family,
                    incident.status,
                    incident.scope,
                    incident.consumer_group,
                    incident.topic,
                    incident.partition,
                    incident.opened_at,
                    incident.updated_at,
                    incident.resolved_at,
                    incident.current_anomaly,
                    incident.current_health,
                    incident.current_severity,
                    incident.current_primary_cause,
                    incident.current_primary_cause_confidence,
                    json.dumps(incident.current_payload, sort_keys=True),
                ),
            )

    def update_incident(self, incident: IncidentRecord) -> None:
        with self._store.connect() as connection:
            connection.execute(
                "UPDATE incidents SET status = ?, updated_at = ?, resolved_at = ?, "
                "current_anomaly = ?, current_health = ?, current_severity = ?, "
                "current_primary_cause = ?, current_primary_cause_confidence = ?, "
                "current_payload_json = ? WHERE incident_id = ?",
                (
                    incident.status,
                    incident.updated_at,
                    incident.resolved_at,
                    incident.current_anomaly,
                    incident.current_health,
                    incident.current_severity,
                    incident.current_primary_cause,
                    incident.current_primary_cause_confidence,
                    json.dumps(incident.current_payload, sort_keys=True),
                    incident.incident_id,
                ),
            )

    def insert_timeline_entry(self, entry: TimelineEntry) -> None:
        with self._store.connect() as connection:
            connection.execute(
                "INSERT INTO incident_timeline (timeline_id, incident_id, entry_type, at, summary, details_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    entry.timeline_id,
                    entry.incident_id,
                    entry.entry_type,
                    entry.at,
                    entry.summary,
                    json.dumps(entry.details, sort_keys=True),
                ),
            )

    def insert_delivery(self, record: DeliveryRecord) -> None:
        with self._store.connect() as connection:
            connection.execute(
                "INSERT INTO incident_deliveries (event_id, incident_id, timeline_id, event_type, "
                "event_version, delivery_state, delivery_attempts, last_delivery_error, "
                "last_delivery_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.event_id,
                    record.incident_id,
                    record.timeline_id,
                    record.event_type,
                    record.event_version,
                    record.delivery_state,
                    record.delivery_attempts,
                    record.last_delivery_error,
                    record.last_delivery_at,
                    record.payload_json,
                ),
            )

    def update_delivery(self, record: DeliveryRecord) -> None:
        with self._store.connect() as connection:
            connection.execute(
                "UPDATE incident_deliveries SET delivery_state = ?, delivery_attempts = ?, "
                "last_delivery_error = ?, last_delivery_at = ?, payload_json = ? WHERE event_id = ?",
                (
                    record.delivery_state,
                    record.delivery_attempts,
                    record.last_delivery_error,
                    record.last_delivery_at,
                    record.payload_json,
                    record.event_id,
                ),
            )

    def get_delivery_by_event_id(self, event_id: str) -> DeliveryRecord | None:
        with self._store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM incident_deliveries WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        return self._row_to_delivery(row) if row is not None else None

    def list_deliveries(
        self,
        *,
        incident_id: str | None = None,
        delivery_state: str | None = None,
        limit: int = 50,
    ) -> list[DeliveryRecord]:
        query = "SELECT * FROM incident_deliveries"
        clauses: list[str] = []
        params: list[object] = []
        if incident_id:
            clauses.append("incident_id = ?")
            params.append(incident_id)
        if delivery_state:
            clauses.append("delivery_state = ?")
            params.append(delivery_state)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY COALESCE(last_delivery_at, 0) DESC, event_id DESC LIMIT ?"
        params.append(limit)
        with self._store.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_delivery(row) for row in rows]

    @staticmethod
    def _row_to_incident(row: object) -> IncidentRecord:
        return IncidentRecord(
            incident_id=row["incident_id"],
            incident_key=row["incident_key"],
            family=row["family"],
            status=row["status"],
            scope=row["scope"],
            consumer_group=row["consumer_group"],
            topic=row["topic"],
            partition=row["partition"],
            opened_at=row["opened_at"],
            updated_at=row["updated_at"],
            resolved_at=row["resolved_at"],
            current_anomaly=row["current_anomaly"],
            current_health=row["current_health"],
            current_severity=row["current_severity"],
            current_primary_cause=row["current_primary_cause"],
            current_primary_cause_confidence=row["current_primary_cause_confidence"],
            current_payload=json.loads(row["current_payload_json"]),
        )

    @staticmethod
    def _row_to_timeline(row: object) -> TimelineEntry:
        return TimelineEntry(
            timeline_id=row["timeline_id"],
            incident_id=row["incident_id"],
            entry_type=row["entry_type"],
            at=row["at"],
            summary=row["summary"],
            details=json.loads(row["details_json"]),
        )

    @staticmethod
    def _row_to_delivery(row: object) -> DeliveryRecord:
        return DeliveryRecord(
            event_id=row["event_id"],
            incident_id=row["incident_id"],
            timeline_id=row["timeline_id"],
            event_type=row["event_type"],
            event_version=row["event_version"],
            delivery_state=row["delivery_state"],
            delivery_attempts=row["delivery_attempts"],
            last_delivery_error=row["last_delivery_error"],
            last_delivery_at=row["last_delivery_at"],
            payload_json=row["payload_json"],
        )
