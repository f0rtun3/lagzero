from __future__ import annotations

import json
import logging
import urllib.request
from typing import Protocol

from lagzero.events.schema import IncidentEvent

logger = logging.getLogger(__name__)


class EventEmitter(Protocol):
    def emit(self, event: IncidentEvent) -> None:
        """Emit an incident event."""


class StdoutEventEmitter:
    def emit(self, event: IncidentEvent) -> None:
        print(json.dumps(event.to_dict(), sort_keys=True))


class SlackEventEmitter:
    def __init__(self, webhook_url: str) -> None:
        self._webhook_url = webhook_url

    def emit(self, event: IncidentEvent) -> None:
        if event.scope == "consumer_group":
            location = f"group `{event.consumer_group}`"
        else:
            location = f"`{event.topic}[{event.partition}]` for group `{event.consumer_group}`"

        payload = {
            "text": (
                f"LagZero detected `{event.anomaly or 'normal'}` on {location} "
                f"(lag={event.offset_lag}, rate={event.processing_rate}, time_lag_sec={event.time_lag_sec}, source={event.time_lag_source})"
            ),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*LagZero incident*\n"
                            f"*Scope:* `{event.scope}`\n"
                            f"*Location:* {location}\n"
                            f"*Consumer group:* `{event.consumer_group}`\n"
                            f"*Anomaly:* `{event.anomaly}`\n"
                            f"*Lag:* `{event.offset_lag}`\n"
                            f"*Rate:* `{event.processing_rate}`\n"
                            f"*Time lag sec:* `{event.time_lag_sec}`\n"
                            f"*Time lag source:* `{event.time_lag_source}`\n"
                            f"*Timestamp type:* `{event.timestamp_type}`\n"
                            f"*Lag divergence sec:* `{event.lag_divergence_sec}`\n"
                            f"*Lag velocity:* `{event.lag_velocity}`"
                        ),
                    },
                }
            ],
        }
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self._webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            logger.debug("Slack emitter response status=%s", response.status)
