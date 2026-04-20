from __future__ import annotations

import os
from dataclasses import dataclass


def _parse_topics(raw_topics: str) -> list[str]:
    topics = [topic.strip() for topic in raw_topics.split(",") if topic.strip()]
    if not topics:
        raise ValueError("LAGZERO_TOPICS must contain at least one topic.")
    return topics


def _parse_optional_float(value: str | None, default: float) -> float:
    if value is None or value == "":
        return default
    return float(value)


def _parse_optional_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


@dataclass(slots=True)
class Settings:
    bootstrap_servers: str
    consumer_group: str
    topics: list[str]
    poll_interval_sec: float = 10.0
    emitter: str = "stdout"
    log_level: str = "INFO"
    lag_spike_multiplier: float = 2.0
    stalled_intervals: int = 2
    slack_webhook_url: str | None = None
    correlation_window_sec: float = 900.0

    @classmethod
    def from_env(cls) -> "Settings":
        bootstrap_servers = os.getenv("LAGZERO_BOOTSTRAP_SERVERS", "localhost:9092")
        consumer_group = os.getenv("LAGZERO_CONSUMER_GROUP", "").strip()
        if not consumer_group:
            raise ValueError("LAGZERO_CONSUMER_GROUP is required.")

        topics_raw = os.getenv("LAGZERO_TOPICS", "")
        if not topics_raw.strip():
            raise ValueError("LAGZERO_TOPICS is required.")

        return cls(
            bootstrap_servers=bootstrap_servers,
            consumer_group=consumer_group,
            topics=_parse_topics(topics_raw),
            poll_interval_sec=_parse_optional_float(os.getenv("LAGZERO_POLL_INTERVAL_SEC"), 10.0),
            emitter=os.getenv("LAGZERO_EMITTER", "stdout").strip().lower() or "stdout",
            log_level=os.getenv("LAGZERO_LOG_LEVEL", "INFO").strip().upper() or "INFO",
            lag_spike_multiplier=_parse_optional_float(
                os.getenv("LAGZERO_LAG_SPIKE_MULTIPLIER"), 2.0
            ),
            stalled_intervals=_parse_optional_int(os.getenv("LAGZERO_STALLED_INTERVALS"), 2),
            slack_webhook_url=os.getenv("LAGZERO_SLACK_WEBHOOK_URL") or None,
            correlation_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_CORRELATION_WINDOW_SEC"), 900.0
            ),
        )

