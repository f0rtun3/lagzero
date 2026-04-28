from lagzero.events.schema import IncidentEvent
from lagzero.incidents.manager import IncidentLifecycleManager
from lagzero.incidents.service import IncidentHistoryService
from lagzero.persistence.repository import IncidentRepository
from lagzero.persistence.sqlite import SQLiteIncidentStore
from lagzero.sinks.webhook import WebhookSink


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


def test_lifecycle_persists_failed_delivery_without_blocking_incident(tmp_path, monkeypatch) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    sink = WebhookSink(
        url="https://example.com/webhook",
        secret="shared-secret",
        timeout_sec=5.0,
        max_retries=1,
        event_version="1.0",
    )

    def fail_emit(*, raw_body: str, event_type: str, event_id: str, timestamp: str) -> None:
        raise RuntimeError("network down")

    monkeypatch.setattr(sink, "emit_serialized", fail_emit)
    manager = IncidentLifecycleManager(
        repository=repository,
        webhook_sink=sink,
        resolve_confirmations=2,
        enabled=True,
    )

    opened = manager.handle_stable_incident(
        _event(timestamp=10.0, anomaly="system_under_pressure", service_health="degraded")
    )
    assert [result.event_type for result in opened] == ["incident.opened"]

    persisted = repository.list_incidents(status="active", limit=10)
    assert [incident.incident_id for incident in persisted] == [opened[0].incident.incident_id]

    deliveries = repository.list_deliveries(delivery_state="failed", limit=10)
    assert len(deliveries) == 1
    assert deliveries[0].event_type == "incident.opened"


def test_restart_continuity_does_not_emit_synthetic_update(tmp_path) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    first_manager = IncidentLifecycleManager(
        repository=repository,
        resolve_confirmations=2,
        enabled=True,
    )
    opened = first_manager.handle_stable_incident(
        _event(timestamp=10.0, anomaly="system_under_pressure", service_health="degraded")
    )
    incident_id = opened[0].incident.incident_id

    restarted_manager = IncidentLifecycleManager(
        repository=repository,
        resolve_confirmations=2,
        enabled=True,
    )
    results = restarted_manager.handle_stable_incident(
        _event(timestamp=20.0, anomaly="system_under_pressure", service_health="degraded")
    )
    assert results == []
    loaded = repository.get_active_incident_by_key("consumer_group:payments:pressure")
    assert loaded is not None
    assert loaded.incident_id == incident_id


def test_manual_redelivery_updates_existing_delivery_record(tmp_path, monkeypatch) -> None:
    repository = IncidentRepository(SQLiteIncidentStore(str(tmp_path / "lagzero.db")))
    sink = WebhookSink(
        url="https://example.com/webhook",
        secret="shared-secret",
        timeout_sec=5.0,
        max_retries=1,
        event_version="1.0",
    )
    manager = IncidentLifecycleManager(
        repository=repository,
        webhook_sink=sink,
        resolve_confirmations=2,
        enabled=True,
    )
    monkeypatch.setattr(sink, "emit_serialized", lambda **kwargs: None)
    opened = manager.handle_stable_incident(
        _event(timestamp=10.0, anomaly="system_under_pressure", service_health="degraded")
    )
    deliveries = repository.list_deliveries(limit=10)
    assert len(deliveries) == 1
    record = deliveries[0]

    service = IncidentHistoryService(repository=repository, webhook_sink=sink)
    redelivered = service.redeliver_event(record.event_id)

    assert redelivered.event_id == record.event_id
    assert redelivered.delivery_attempts == 2
    assert repository.get_incident_by_id(opened[0].incident.incident_id) is not None
