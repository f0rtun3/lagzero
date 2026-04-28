from lagzero.events.schema import IncidentEvent
from lagzero.incidents.keys import build_incident_key, build_scope_key


def _event(*, scope: str, topic: str | None, partition: int | None) -> IncidentEvent:
    return IncidentEvent(
        timestamp=1.0,
        consumer_group="payments",
        scope=scope,
        topic=topic,
        partition=partition,
        offset_lag=10,
        processing_rate=1.0,
        producer_rate=2.0,
        consumer_efficiency=0.5,
        backlog_growth_rate=1.0,
        time_lag_sec=5.0,
        time_lag_source="estimated",
        timestamp_type=None,
        backlog_head_timestamp=None,
        latest_message_timestamp=None,
        lag_divergence_sec=None,
        lag_velocity=1.0,
        anomaly="system_under_pressure",
        severity="warning",
        service_health="degraded",
        confidence=0.6,
    )


def test_build_incident_key_for_group_scope() -> None:
    event = _event(scope="consumer_group", topic=None, partition=None)
    assert build_incident_key(event, "pressure") == "consumer_group:payments:pressure"
    assert build_scope_key(event) == "consumer_group:payments"


def test_build_incident_key_for_partition_scope() -> None:
    event = _event(scope="partition", topic="orders", partition=3)
    assert build_incident_key(event, "delay") == "partition:payments:orders:3:delay"
    assert build_scope_key(event) == "partition:payments:orders:3"
