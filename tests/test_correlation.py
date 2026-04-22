from lagzero.correlation.engine import CorrelationEngine
from lagzero.correlation.schema import ExternalEvent
from lagzero.correlation.store import CorrelationEventStore
from lagzero.events.schema import IncidentEvent


def _incident() -> IncidentEvent:
    return IncidentEvent(
        timestamp=200.0,
        consumer_group="payments",
        scope="consumer_group",
        topic="orders",
        partition=None,
        offset_lag=100,
        processing_rate=20.0,
        producer_rate=30.0,
        consumer_efficiency=0.667,
        backlog_growth_rate=10.0,
        time_lag_sec=15.0,
        time_lag_source="estimated",
        timestamp_type=None,
        backlog_head_timestamp=None,
        latest_message_timestamp=None,
        lag_divergence_sec=None,
        lag_velocity=2.0,
        anomaly="system_under_pressure",
        severity="warning",
        service_health="degraded",
        confidence=0.6,
    )


def test_store_prunes_expired_events() -> None:
    store = CorrelationEventStore(retention_sec=60.0)
    store.add(ExternalEvent(event_id="old", timestamp=100.0, event_type="deploy", source="cli"))
    store.add(ExternalEvent(event_id="new", timestamp=180.0, event_type="error", source="cli"))

    recent = store.query_recent(200.0)

    assert [event.event_id for event in recent] == ["new"]


def test_correlation_engine_selects_primary_cause() -> None:
    store = CorrelationEventStore(retention_sec=900.0)
    store.add(
        ExternalEvent(
            event_id="deploy-1",
            timestamp=170.0,
            event_type="deploy",
            source="cli",
            consumer_group="payments",
            topic="orders",
        )
    )
    store.add(
        ExternalEvent(
            event_id="error-1",
            timestamp=195.0,
            event_type="error",
            source="cli",
            consumer_group="payments",
            topic="orders",
            severity="high",
        )
    )
    engine = CorrelationEngine(
        store,
        deploy_window_sec=300.0,
        error_window_sec=120.0,
        rebalance_window_sec=90.0,
        infra_window_sec=300.0,
        max_correlations=3,
    )

    incident = engine.enrich(_incident())

    assert incident.primary_cause == "error_nearby"
    assert incident.primary_cause_confidence is not None
    assert len(incident.correlations) == 2
    assert incident.correlations[0]["role"] == "probable_cause"

