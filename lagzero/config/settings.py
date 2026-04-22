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
    idle_intervals: int = 3
    rate_window_size: int = 3
    timestamp_sample_interval_sec: float = 30.0
    lag_divergence_threshold_sec: float = 120.0
    state_transition_confirmations: int = 2
    correlation_retention_sec: float = 900.0
    deploy_window_sec: float = 300.0
    error_window_sec: float = 120.0
    rebalance_window_sec: float = 90.0
    infra_window_sec: float = 300.0
    max_correlations: int = 3
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
            idle_intervals=_parse_optional_int(os.getenv("LAGZERO_IDLE_INTERVALS"), 3),
            rate_window_size=_parse_optional_int(os.getenv("LAGZERO_RATE_WINDOW_SIZE"), 3),
            timestamp_sample_interval_sec=_parse_optional_float(
                os.getenv("LAGZERO_TIMESTAMP_SAMPLE_INTERVAL_SEC"), 30.0
            ),
            lag_divergence_threshold_sec=_parse_optional_float(
                os.getenv("LAGZERO_LAG_DIVERGENCE_THRESHOLD_SEC"), 120.0
            ),
            state_transition_confirmations=_parse_optional_int(
                os.getenv("LAGZERO_STATE_TRANSITION_CONFIRMATIONS"), 2
            ),
            correlation_retention_sec=_parse_optional_float(
                os.getenv("LAGZERO_CORRELATION_RETENTION_SEC"), 900.0
            ),
            deploy_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_DEPLOY_WINDOW_SEC"), 300.0
            ),
            error_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_ERROR_WINDOW_SEC"), 120.0
            ),
            rebalance_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_REBALANCE_WINDOW_SEC"), 90.0
            ),
            infra_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_INFRA_WINDOW_SEC"), 300.0
            ),
            max_correlations=_parse_optional_int(
                os.getenv("LAGZERO_MAX_CORRELATIONS"), 3
            ),
            slack_webhook_url=os.getenv("LAGZERO_SLACK_WEBHOOK_URL") or None,
            correlation_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_CORRELATION_WINDOW_SEC"), 900.0
            ),
        )
