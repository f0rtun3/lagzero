from lagzero.incidents.schema import DeliveryRecord, IncidentRecord, TimelineEntry
from lagzero.persistence.repository import IncidentRepository
from lagzero.persistence.sqlite import SQLiteIncidentStore


def _incident(incident_id: str, *, status: str = "open", family: str = "pressure") -> IncidentRecord:
    return IncidentRecord(
        incident_id=incident_id,
        incident_key=f"consumer_group:payments:{family}",
        family=family,
        status=status,
        scope="consumer_group",
        consumer_group="payments",
        topic=None,
        partition=None,
        opened_at=1.0,
        updated_at=2.0,
        resolved_at=None if status != "resolved" else 3.0,
        current_anomaly="system_under_pressure" if status != "resolved" else "normal",
        current_health="degraded" if status != "resolved" else "healthy",
        current_severity="warning",
        current_primary_cause=None,
        current_primary_cause_confidence=None,
        current_payload={"incident_id": incident_id, "anomaly": "system_under_pressure"},
    )


def test_repository_persists_and_fetches_active_incident(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    incident = _incident("inc-1")
    repository.insert_incident(incident)
    loaded = repository.get_active_incident_by_key("consumer_group:payments:pressure")
    assert loaded is not None
    assert loaded.incident_id == "inc-1"
    assert loaded.current_payload["incident_id"] == "inc-1"


def test_repository_lists_and_fetches_timeline(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    repository.insert_incident(_incident("inc-1", status="open"))
    repository.insert_incident(_incident("inc-2", status="resolved", family="delay"))
    repository.insert_timeline_entry(
        TimelineEntry(
            timeline_id="tl-1",
            incident_id="inc-1",
            entry_type="incident_opened",
            at=1.0,
            summary="opened",
            details={},
        )
    )
    repository.insert_timeline_entry(
        TimelineEntry(
            timeline_id="tl-2",
            incident_id="inc-1",
            entry_type="anomaly_changed",
            at=2.0,
            summary="updated",
            details={},
        )
    )

    active = repository.list_incidents(status="active", limit=10)
    assert [incident.incident_id for incident in active] == ["inc-1"]

    all_incidents = repository.list_incidents(status="all", family="delay", limit=10)
    assert [incident.incident_id for incident in all_incidents] == ["inc-2"]

    fetched = repository.get_incident_by_id("inc-1")
    assert fetched is not None
    assert fetched.incident_id == "inc-1"

    timeline = repository.get_timeline_entries("inc-1")
    assert [entry.timeline_id for entry in timeline] == ["tl-1", "tl-2"]


def test_repository_persists_and_lists_deliveries(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    repository.insert_delivery(
        DeliveryRecord(
            event_id="evt-1",
            incident_id="inc-1",
            timeline_id="tl-1",
            event_type="incident.opened",
            event_version="1.0",
            delivery_state="failed",
            delivery_attempts=1,
            last_delivery_error="boom",
            last_delivery_at=10.0,
            payload_json='{"event_id":"evt-1"}',
        )
    )

    loaded = repository.get_delivery_by_event_id("evt-1")
    assert loaded is not None
    assert loaded.delivery_state == "failed"

    failed = repository.list_deliveries(delivery_state="failed", limit=10)
    assert [record.event_id for record in failed] == ["evt-1"]

    repository.update_delivery(
        DeliveryRecord(
            event_id="evt-1",
            incident_id="inc-1",
            timeline_id="tl-1",
            event_type="incident.opened",
            event_version="1.0",
            delivery_state="delivered",
            delivery_attempts=2,
            last_delivery_error=None,
            last_delivery_at=11.0,
            payload_json='{"event_id":"evt-1"}',
        )
    )
    delivered = repository.get_delivery_by_event_id("evt-1")
    assert delivered is not None
    assert delivered.delivery_attempts == 2
    assert delivered.delivery_state == "delivered"
