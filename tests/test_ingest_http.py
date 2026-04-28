import json

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


def test_operator_http_server_exposes_incidents_and_redelivery(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeService:
        def list_incidents(self, **kwargs):
            return [type("Obj", (), {"to_dict": lambda self: {"incident_id": "inc-1"}})()]

        def get_incident(self, incident_id):
            return type("Obj", (), {"to_dict": lambda self: {"incident_id": incident_id}})()

        def get_timeline(self, incident_id):
            return [type("Obj", (), {"to_dict": lambda self: {"timeline_id": "tl-1"}})()]

        def list_deliveries(self, **kwargs):
            return [type("Obj", (), {"to_dict": lambda self: {"event_id": "evt-1"}})()]

        def redeliver_event(self, event_id):
            return type("Obj", (), {"to_dict": lambda self: {"event_id": event_id, "delivery_state": "delivered"}})()

    class FakeServer:
        def __init__(self, address, handler):
            captured["handler"] = handler
            self.server_address = address

        def serve_forever(self) -> None:
            return

        def shutdown(self) -> None:
            return

        def server_close(self) -> None:
            return

    monkeypatch.setattr("lagzero.ingest.http.ThreadingHTTPServer", FakeServer)
    server = IngestHTTPServer(
        host="127.0.0.1",
        port=8788,
        operator_service=FakeService(),
    )
    handler_cls = captured["handler"]

    def run_handler(path: str, method: str = "GET") -> tuple[int, dict[str, object]]:
        handler = handler_cls.__new__(handler_cls)
        handler.path = path
        handler.headers = {"Content-Length": "0"}
        handler.rfile = type("R", (), {"read": lambda self, size: b""})()
        payload: dict[str, object] = {}
        status_holder = {"status": None}
        handler.send_response = lambda status: status_holder.__setitem__("status", status)
        handler.send_header = lambda *args, **kwargs: None
        handler.end_headers = lambda: None
        handler.wfile = type("W", (), {"write": lambda self, body: payload.__setitem__("body", json.loads(body.decode("utf-8")))})()
        getattr(handler, f"do_{method}")()
        return status_holder["status"], payload["body"]

    status, body = run_handler("/incidents")
    assert status == 200
    assert body["items"][0]["incident_id"] == "inc-1"

    status, body = run_handler("/incidents/inc-1")
    assert status == 200
    assert body["incident"]["incident_id"] == "inc-1"

    status, body = run_handler("/incidents/inc-1/timeline")
    assert status == 200
    assert body["items"][0]["timeline_id"] == "tl-1"

    status, body = run_handler("/incident-deliveries/evt-1/redeliver", method="POST")
    assert status == 200
    assert body["delivery"]["event_id"] == "evt-1"
