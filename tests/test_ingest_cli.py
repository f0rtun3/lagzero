import argparse
import json
from urllib.response import addinfourl

from lagzero.ingest.cli import (
    build_event_from_args,
    build_ingest_parser,
    send_external_event,
)


def test_build_event_from_args_parses_metadata() -> None:
    parser = build_ingest_parser()
    args = parser.parse_args(
        [
            "deploy",
            "--service",
            "orders-service",
            "--consumer-group",
            "payments",
            "--topic",
            "orders",
            "--partition",
            "2",
            "--severity",
            "warning",
            "--metadata",
            "version=v1.2.3",
            "--metadata-json",
            '{"build":"42"}',
        ]
    )

    event = build_event_from_args(args)

    assert event.event_type == "deploy"
    assert event.service == "orders-service"
    assert event.consumer_group == "payments"
    assert event.topic == "orders"
    assert event.partition == 2
    assert event.severity == "warning"
    assert event.metadata == {"version": "v1.2.3", "build": "42"}


def test_send_external_event_posts_json(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self) -> bytes:
            return b'{"status":"accepted","event_id":"evt-1"}'

    def fake_urlopen(request, timeout: float):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    event = build_event_from_args(
        argparse.Namespace(
            event_type="error",
            service="orders-service",
            consumer_group="payments",
            topic="orders",
            partition=None,
            severity="error",
            metadata=[],
            metadata_json='{"error_type":"decode"}',
        )
    )

    response = send_external_event(event, url="http://127.0.0.1:8787/events", timeout_sec=3.0)

    assert captured["url"] == "http://127.0.0.1:8787/events"
    assert captured["timeout"] == 3.0
    assert captured["body"]["event_type"] == "error"
    assert response["status"] == "accepted"
