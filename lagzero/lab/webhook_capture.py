from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any

from lagzero.lab.events import append_jsonl, clear_jsonl
from lagzero.sinks.signing import build_signature

logger = logging.getLogger(__name__)


class WebhookCaptureServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        path: str,
        secret: str,
    ) -> None:
        self._host = host
        self._port = port
        self._path = _normalize_path(path)
        self._secret = secret
        self._output_path: Path | None = None
        self._fail_remaining = 0
        self._server = ThreadingHTTPServer((host, port), self._build_handler())
        self._thread: Thread | None = None

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}{self._path}"

    def set_output_path(self, path: str | Path) -> None:
        self._output_path = Path(path)
        clear_jsonl(self._output_path)
        self._fail_remaining = 0

    def fail_next(self, count: int = 1) -> None:
        self._fail_remaining = max(0, count)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(
            "LagZero webhook capture server listening on %s:%s%s",
            self._host,
            self._port,
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
        server = self
        path = self._path

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
                raw_body = self.rfile.read(content_length).decode("utf-8")
                payload: dict[str, object] | None = None
                parse_error: str | None = None
                try:
                    decoded = json.loads(raw_body)
                    if isinstance(decoded, dict):
                        payload = decoded
                    else:
                        parse_error = "payload_not_object"
                except json.JSONDecodeError as exc:
                    parse_error = str(exc)

                record = build_capture_record(
                    headers={key: value for key, value in self.headers.items()},
                    raw_body=raw_body,
                    payload=payload,
                    secret=server._secret,
                    parse_error=parse_error,
                )
                should_fail = server._fail_remaining > 0
                if should_fail:
                    server._fail_remaining -= 1
                record["response_status"] = 500 if should_fail else 202
                if server._output_path is not None:
                    append_jsonl(server._output_path, record)

                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR if should_fail else HTTPStatus.ACCEPTED,
                    {
                        "status": "failed" if should_fail else "accepted",
                        "signature_valid": record["signature_valid"],
                    },
                )

            def log_message(self, format: str, *args: object) -> None:
                logger.debug("LagZero webhook capture: " + format, *args)

            def _send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


def build_capture_record(
    *,
    headers: dict[str, str],
    raw_body: str,
    payload: dict[str, object] | None,
    secret: str,
    parse_error: str | None = None,
) -> dict[str, Any]:
    normalized_headers = {key.lower(): value for key, value in headers.items()}
    timestamp = normalized_headers.get("x-lagzero-timestamp")
    signature = normalized_headers.get("x-lagzero-signature")
    verification = verify_capture_signature(
        raw_body=raw_body,
        timestamp=timestamp,
        signature=signature,
        secret=secret,
    )
    return {
        "headers": normalized_headers,
        "raw_body": raw_body,
        "payload": payload,
        "parse_error": parse_error,
        "event_id": payload.get("event_id") if payload else None,
        "event_type": payload.get("event_type") if payload else None,
        "incident_id": payload.get("incident", {}).get("incident_id")
        if isinstance(payload, dict)
        else None,
        "signature_valid": verification["valid"],
        "signature_error": verification["error"],
        "received_at": verification["received_at"],
    }


def verify_capture_signature(
    *,
    raw_body: str,
    timestamp: str | None,
    signature: str | None,
    secret: str,
) -> dict[str, object]:
    import time

    if timestamp is None:
        return {"valid": False, "error": "missing_timestamp", "received_at": time.time()}
    if signature is None:
        return {"valid": False, "error": "missing_signature", "received_at": time.time()}
    if not signature.startswith("sha256="):
        return {"valid": False, "error": "malformed_signature", "received_at": time.time()}
    expected = build_signature(secret, timestamp=timestamp, raw_body=raw_body)
    if expected != signature:
        return {"valid": False, "error": "signature_mismatch", "received_at": time.time()}
    return {"valid": True, "error": None, "received_at": time.time()}


def _normalize_path(path: str) -> str:
    cleaned = path.strip() or "/webhooks"
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned
