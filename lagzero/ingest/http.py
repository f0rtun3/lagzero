from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Callable

from lagzero.correlation.schema import ExternalEvent

logger = logging.getLogger(__name__)


def parse_external_event_payload(payload: dict[str, object]) -> ExternalEvent:
    return ExternalEvent(
        event_id=str(payload["event_id"]),
        timestamp=float(payload["timestamp"]),
        event_type=str(payload["event_type"]),
        source=str(payload["source"]),
        service=str(payload["service"]) if payload.get("service") is not None else None,
        consumer_group=(
            str(payload["consumer_group"]) if payload.get("consumer_group") is not None else None
        ),
        topic=str(payload["topic"]) if payload.get("topic") is not None else None,
        partition=int(payload["partition"]) if payload.get("partition") is not None else None,
        severity=str(payload["severity"]) if payload.get("severity") is not None else None,
        metadata=dict(payload.get("metadata", {})),
    )


class IngestHTTPServer:
    def __init__(
        self,
        host: str,
        port: int,
        path: str,
        on_event: Callable[[ExternalEvent], None],
    ) -> None:
        self._path = _normalize_path(path)
        self._on_event = on_event
        handler = self._build_handler()
        self._server = ThreadingHTTPServer((host, port), handler)
        self._thread: Thread | None = None

    @property
    def server_address(self) -> tuple[str, int]:
        return self._server.server_address

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(
            "LagZero ingest HTTP server listening on %s:%s%s",
            self._server.server_address[0],
            self._server.server_address[1],
            self._path,
        )

    def stop(self) -> None:
        if self._thread is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
        self._thread = None

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        path = self._path
        on_event = self._on_event

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path != "/health":
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return
                self._send_json(HTTPStatus.OK, {"status": "ok"})

            def do_POST(self) -> None:  # noqa: N802
                if self.path != path:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)
                try:
                    payload = json.loads(raw_body.decode("utf-8"))
                    event = parse_external_event_payload(payload)
                    on_event(event)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {"error": "invalid_payload", "detail": str(exc)},
                    )
                    return
                self._send_json(
                    HTTPStatus.ACCEPTED,
                    {"status": "accepted", "event_id": event.event_id},
                )

            def log_message(self, format: str, *args: object) -> None:
                logger.debug("LagZero ingest HTTP server: " + format, *args)

            def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


def _normalize_path(path: str) -> str:
    cleaned = path.strip() or "/events"
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned
