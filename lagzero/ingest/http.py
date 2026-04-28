from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Callable
from urllib.parse import parse_qs, urlparse

from lagzero.correlation.schema import ExternalEvent
from lagzero.incidents.service import IncidentHistoryService

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
        path: str | None = None,
        on_event: Callable[[ExternalEvent], None] | None = None,
        operator_service: IncidentHistoryService | None = None,
        operator_path_prefix: str = "/",
        health_payload: dict[str, object] | None = None,
    ) -> None:
        self._path = _normalize_path(path) if path is not None else None
        self._on_event = on_event
        self._operator_service = operator_service
        self._operator_path_prefix = _normalize_prefix(operator_path_prefix)
        self._health_payload = health_payload or {"status": "ok"}
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
            "LagZero local HTTP server listening on %s:%s",
            self._server.server_address[0],
            self._server.server_address[1],
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
        operator_service = self._operator_service
        operator_prefix = self._operator_path_prefix
        health_payload = self._health_payload

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                route = parsed.path
                if route == "/health":
                    self._send_json(HTTPStatus.OK, health_payload)
                    return
                if operator_service is None:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    return

                if route == _prefix_path(operator_prefix, "/incidents"):
                    params = parse_qs(parsed.query)
                    limit = int(params.get("limit", ["50"])[0])
                    incidents = operator_service.list_incidents(
                        status=params.get("status", ["all"])[0],
                        consumer_group=params.get("consumer_group", [None])[0],
                        family=params.get("family", [None])[0],
                        limit=limit,
                    )
                    self._send_json(
                        HTTPStatus.OK,
                        {"items": [incident.to_dict() for incident in incidents], "total": len(incidents)},
                    )
                    return

                if route == _prefix_path(operator_prefix, "/incident-deliveries"):
                    params = parse_qs(parsed.query)
                    limit = int(params.get("limit", ["50"])[0])
                    deliveries = operator_service.list_deliveries(
                        incident_id=params.get("incident_id", [None])[0],
                        delivery_state=params.get("delivery_state", [None])[0],
                        limit=limit,
                    )
                    self._send_json(
                        HTTPStatus.OK,
                        {"items": [record.to_dict() for record in deliveries], "total": len(deliveries)},
                    )
                    return

                incident_id = _match_path(route, operator_prefix, "/incidents/")
                if incident_id is not None and not route.endswith("/timeline"):
                    incident = operator_service.get_incident(incident_id)
                    if incident is None:
                        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                        return
                    self._send_json(HTTPStatus.OK, {"incident": incident.to_dict()})
                    return

                timeline_id = _match_path(route, operator_prefix, "/incidents/")
                if timeline_id is not None and route.endswith("/timeline"):
                    incident_id = timeline_id.removesuffix("/timeline")
                    entries = operator_service.get_timeline(incident_id)
                    self._send_json(
                        HTTPStatus.OK,
                        {"incident_id": incident_id, "items": [entry.to_dict() for entry in entries]},
                    )
                    return
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                route = parsed.path
                if operator_service is not None:
                    event_id = _match_path(route, operator_prefix, "/incident-deliveries/")
                    if event_id is not None and route.endswith("/redeliver"):
                        target_event_id = event_id.removesuffix("/redeliver")
                        try:
                            record = operator_service.redeliver_event(target_event_id)
                        except KeyError:
                            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                            return
                        except RuntimeError as exc:
                            self._send_json(
                                HTTPStatus.BAD_REQUEST,
                                {"error": "delivery_unavailable", "detail": str(exc)},
                            )
                            return
                        self._send_json(HTTPStatus.OK, {"delivery": record.to_dict()})
                        return

                if path is None or on_event is None or route != path:
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


def _normalize_prefix(prefix: str) -> str:
    cleaned = prefix.strip() or "/"
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    if cleaned != "/":
        cleaned = cleaned.rstrip("/")
    return cleaned


def _prefix_path(prefix: str, suffix: str) -> str:
    if prefix == "/":
        return suffix
    return f"{prefix}{suffix}"


def _match_path(path: str, prefix: str, base: str) -> str | None:
    full_base = _prefix_path(prefix, base)
    if not path.startswith(full_base):
        return None
    return path.removeprefix(full_base)
