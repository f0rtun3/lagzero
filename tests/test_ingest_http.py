from lagzero.ingest.http import IngestHTTPServer, parse_external_event_payload


def test_parse_external_event_payload_reads_fields() -> None:
    event = parse_external_event_payload(
        {
            "event_id": "evt-1",
            "timestamp": 1.0,
            "event_type": "deploy",
            "source": "cli",
            "service": "orders-service",
            "consumer_group": "payments",
            "topic": "orders",
            "partition": 0,
            "severity": "warning",
            "metadata": {"version": "v1.2.3"},
        }
    )

    assert event.event_id == "evt-1"
    assert event.event_type == "deploy"
    assert event.service == "orders-service"
    assert event.metadata == {"version": "v1.2.3"}


def test_ingest_http_server_starts_and_stops_with_fake_server(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeServer:
        def __init__(self, address, handler):
            captured["address"] = address
            captured["handler"] = handler
            self.server_address = ("127.0.0.1", 8787)
            self.served = False
            self.closed = False
            self.stopped = False

        def serve_forever(self) -> None:
            self.served = True

        def shutdown(self) -> None:
            self.stopped = True

        def server_close(self) -> None:
            self.closed = True

    monkeypatch.setattr("lagzero.ingest.http.ThreadingHTTPServer", FakeServer)
    server = IngestHTTPServer(
        host="127.0.0.1",
        port=8787,
        path="events",
        on_event=lambda event: None,
    )

    server.start()
    server.stop()

    assert captured["address"] == ("127.0.0.1", 8787)
    assert server.server_address == ("127.0.0.1", 8787)
