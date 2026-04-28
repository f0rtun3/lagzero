from lagzero.events.schema import IncidentEvent
from lagzero.incidents.manager import IncidentLifecycleManager
from lagzero.persistence.repository import IncidentRepository
from lagzero.persistence.sqlite import SQLiteIncidentStore


def _event(
    *,
    timestamp: float,
    anomaly: str,
    service_health: str,
    primary_cause: str | None = None,
    time_lag_sec: float | None = 45.0,
) -> IncidentEvent:
    return IncidentEvent(
        timestamp=timestamp,
        consumer_group="payments",
        scope="consumer_group",
        topic=None,
        partition=None,
        offset_lag=100 if anomaly != "normal" else 0,
        processing_rate=10.0 if anomaly != "normal" else 0.0,
        producer_rate=20.0 if anomaly != "normal" else 0.0,
        consumer_efficiency=0.5 if anomaly != "normal" else None,
        backlog_growth_rate=10.0 if anomaly != "normal" else 0.0,
        time_lag_sec=time_lag_sec,
        time_lag_source="timestamp" if anomaly != "normal" else "unavailable",
        timestamp_type="create_time" if anomaly != "normal" else None,
        backlog_head_timestamp=1.0 if anomaly != "normal" else None,
        latest_message_timestamp=2.0 if anomaly != "normal" else None,
        lag_divergence_sec=None,
        lag_velocity=2.0 if anomaly != "normal" else 0.0,
        anomaly=anomaly,
        severity="warning" if anomaly != "normal" else "info",
        service_health=service_health,
        confidence=0.8 if anomaly != "normal" else 0.95,
        primary_cause=primary_cause,
        primary_cause_confidence=0.9 if primary_cause else None,
    )


def test_lifecycle_opens_updates_and_resolves_incident(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    manager = IncidentLifecycleManager(
        repository=repository,
        resolve_confirmations=2,
        enabled=True,
    )

    opened = manager.handle_stable_incident(
        _event(timestamp=10.0, anomaly="system_under_pressure", service_health="degraded")
    )
    assert [result.event_type for result in opened] == ["incident.opened"]
    incident_id = opened[0].incident.incident_id

    updated = manager.handle_stable_incident(
        _event(
            timestamp=20.0,
            anomaly="system_under_pressure",
            service_health="degraded",
            primary_cause="deploy_within_window",
        )
    )
    assert [result.event_type for result in updated] == ["incident.updated"]
    assert updated[0].incident.incident_id == incident_id

    first_healthy = manager.handle_stable_incident(
        _event(timestamp=30.0, anomaly="normal", service_health="healthy", time_lag_sec=None)
    )
    assert first_healthy == []

    resolved = manager.handle_stable_incident(
        _event(timestamp=40.0, anomaly="normal", service_health="healthy", time_lag_sec=None)
    )
    assert [result.event_type for result in resolved] == ["incident.resolved"]
    assert resolved[0].incident.incident_id == incident_id


def test_lifecycle_does_not_emit_update_without_material_change(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    manager = IncidentLifecycleManager(
        repository=repository,
        resolve_confirmations=2,
        enabled=True,
    )
    manager.handle_stable_incident(
        _event(timestamp=10.0, anomaly="system_under_pressure", service_health="degraded")
    )

    results = manager.handle_stable_incident(
        _event(timestamp=20.0, anomaly="system_under_pressure", service_health="degraded")
    )
    assert results == []
