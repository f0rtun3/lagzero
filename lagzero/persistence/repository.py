from __future__ import annotations

import json

from lagzero.incidents.schema import IncidentRecord, TimelineEntry
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
                "SELECT * FROM incidents WHERE scope = ? AND consumer_group IS ? AND topic IS ? "
                "AND partition IS ? AND status != 'resolved' ORDER BY opened_at ASC",
                (scope, consumer_group, topic, partition),
            ).fetchall()
        return [self._row_to_incident(row) for row in rows]

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
