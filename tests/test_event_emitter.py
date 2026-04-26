import json

from lagzero.events.emitter import StdoutEventEmitter
from lagzero.events.schema import IncidentEvent


def test_stdout_event_emitter_captures_jsonl(tmp_path, capsys) -> None:
    capture_path = tmp_path / "incidents.jsonl"
    emitter = StdoutEventEmitter(capture_path=str(capture_path))
    event = IncidentEvent(
        timestamp=1.0,
        consumer_group="payments",
        scope="consumer_group",
        topic=None,
        partition=None,
        offset_lag=10,
        processing_rate=2.0,
        producer_rate=3.0,
        consumer_efficiency=0.67,
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

    emitter.emit(event)

    printed = capsys.readouterr().out.strip()
    assert json.loads(printed)["anomaly"] == "system_under_pressure"
    assert json.loads(capture_path.read_text(encoding="utf-8").strip())["consumer_group"] == "payments"
