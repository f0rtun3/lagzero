from __future__ import annotations

import json
import logging
import urllib.request

from lagzero.incidents.schema import WebhookEventEnvelope
from lagzero.sinks.retry import retry_with_backoff
from lagzero.sinks.signing import build_signature

logger = logging.getLogger(__name__)


class WebhookSink:
    def __init__(
        self,
        *,
        url: str,
        secret: str,
        timeout_sec: float,
        max_retries: int,
        event_version: str,
    ) -> None:
        self._url = url
        self._secret = secret
        self._timeout_sec = timeout_sec
        self._max_retries = max_retries
        self._event_version = event_version

    @property
    def event_version(self) -> str:
        return self._event_version

    def emit(self, envelope: WebhookEventEnvelope) -> None:
        raw_body = json.dumps(envelope.to_dict(), sort_keys=True)
        self.emit_serialized(
            raw_body=raw_body,
            event_type=envelope.event_type,
            event_id=envelope.event_id,
            timestamp=envelope.timestamp,
        )

    def emit_serialized(
        self,
        *,
        raw_body: str,
        event_type: str,
        event_id: str,
        timestamp: str,
    ) -> None:
        signature = build_signature(self._secret, timestamp=timestamp, raw_body=raw_body)
        headers = {
            "Content-Type": "application/json",
            "X-LagZero-Event": event_type,
            "X-LagZero-Event-Id": event_id,
            "X-LagZero-Timestamp": timestamp,
            "X-LagZero-Signature": signature,
        }
        def deliver() -> None:
            request = urllib.request.Request(
                self._url,
                data=raw_body.encode("utf-8"),
                headers=headers,
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self._timeout_sec) as response:
                if response.status >= 400:
                    raise RuntimeError(f"Webhook delivery failed with status={response.status}")

        retry_with_backoff(
            deliver,
            max_retries=self._max_retries,
        )
