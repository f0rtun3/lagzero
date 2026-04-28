import json
from urllib.error import URLError

from lagzero.incidents.schema import WebhookEventEnvelope
from lagzero.sinks.signing import build_signature
from lagzero.sinks.webhook import WebhookSink


def test_build_signature_uses_timestamp_and_body() -> None:
    first = build_signature("secret", timestamp="2026-04-28T00:00:00Z", raw_body='{"a":1}')
    second = build_signature("secret", timestamp="2026-04-28T00:00:01Z", raw_body='{"a":1}')
    assert first.startswith("sha256=")
    assert first != second


def test_webhook_sink_sends_signed_headers(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data.decode("utf-8")
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    sink = WebhookSink(
        url="https://example.com/webhook",
        secret="shared-secret",
        timeout_sec=5.0,
        max_retries=2,
        event_version="1.0",
    )
    envelope = WebhookEventEnvelope(
        event_id="evt-1",
        event_type="incident.opened",
        event_version="1.0",
        timestamp="2026-04-28T00:00:00Z",
        source="lagzero",
        incident={"incident_id": "inc-1"},
        timeline_entry={"entry_type": "incident_opened"},
        current_state={"anomaly": "system_under_pressure"},
    )

    sink.emit(envelope)

    assert json.loads(captured["body"])["event_id"] == "evt-1"
    headers = {
        key.lower(): value
        for key, value in captured["headers"].items()
    }
    assert headers["x-lagzero-event"] == "incident.opened"
    assert headers["x-lagzero-event-id"] == "evt-1"
    assert headers["x-lagzero-timestamp"] == "2026-04-28T00:00:00Z"
    assert headers["x-lagzero-signature"].startswith("sha256=")


def test_webhook_sink_retries_transient_failures(monkeypatch) -> None:
    attempts = {"count": 0}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise URLError("temporary")
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    sink = WebhookSink(
        url="https://example.com/webhook",
        secret="shared-secret",
        timeout_sec=5.0,
        max_retries=3,
        event_version="1.0",
    )
    envelope = WebhookEventEnvelope(
        event_id="evt-1",
        event_type="incident.updated",
        event_version="1.0",
        timestamp="2026-04-28T00:00:00Z",
        source="lagzero",
        incident={"incident_id": "inc-1"},
        timeline_entry={"entry_type": "anomaly_changed"},
        current_state={"anomaly": "lag_spike"},
    )

    sink.emit(envelope)
    assert attempts["count"] == 3
