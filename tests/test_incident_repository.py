from lagzero.events.schema import IncidentEvent
from lagzero.incidents.families import incident_family_for_anomaly
from lagzero.incidents.keys import build_incident_key
from lagzero.incidents.schema import IncidentRecord, TimelineEntry
from lagzero.persistence.repository import IncidentRepository
from lagzero.persistence.sqlite import SQLiteIncidentStore


def test_repository_persists_and_fetches_active_incident(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    payload = {"anomaly": "system_under_pressure"}
    incident = IncidentRecord(
        incident_id="inc-1",
        incident_key="consumer_group:payments:pressure",
        family="pressure",
        status="open",
        scope="consumer_group",
        consumer_group="payments",
        topic=None,
        partition=None,
        opened_at=1.0,
        updated_at=1.0,
        resolved_at=None,
        current_anomaly="system_under_pressure",
        current_health="degraded",
        current_severity="warning",
        current_primary_cause=None,
        current_primary_cause_confidence=None,
        current_payload=payload,
    )
    repository.insert_incident(incident)
    loaded = repository.get_active_incident_by_key("consumer_group:payments:pressure")
    assert loaded is not None
    assert loaded.incident_id == "inc-1"
    assert loaded.current_payload == payload

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
