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
    min_incident_offset_lag: int = 25
    min_incident_time_lag_sec: float = 15.0
    min_stalled_offset_lag: int = 50
    min_stalled_time_lag_sec: float = 30.0
    min_lag_spike_delta: int = 25
    slow_consumer_efficiency_threshold: float = 0.8
    catching_up_efficiency_threshold: float = 1.0
    burst_grace_sec: float = 20.0
    partition_skew_min_total_lag: int = 100
    partition_skew_max_hot_partitions: int = 1
    partition_skew_min_share: float = 0.8
    timestamp_sample_interval_sec: float = 30.0
    lag_divergence_threshold_sec: float = 120.0
    state_transition_confirmations: int = 2
    correlation_retention_sec: float = 900.0
    deploy_window_sec: float = 300.0
    error_window_sec: float = 120.0
    rebalance_window_sec: float = 90.0
    infra_window_sec: float = 300.0
    max_correlations: int = 3
    ai_enabled: bool = False
    ai_provider: str = "openai"
    ai_model: str = "gpt-5-mini"
    ai_max_tokens: int = 400
    ai_temperature: float = 0.1
    ai_cache_ttl_sec: float = 120.0
    incidents_enabled: bool = False
    incident_resolve_confirmations: int = 2
    webhook_enabled: bool = False
    webhook_url: str | None = None
    webhook_timeout_sec: float = 5.0
    webhook_max_retries: int = 3
    webhook_secret: str | None = None
    webhook_event_version: str = "1.0"
    persistence_backend: str = "sqlite"
    sqlite_path: str = ".chaos/lagzero.db"
    event_log_path: str | None = None
    ingest_enabled: bool = False
    ingest_host: str = "127.0.0.1"
    ingest_port: int = 8787
    ingest_path: str = "/events"
    ingest_request_timeout_sec: float = 5.0
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
            min_incident_offset_lag=_parse_optional_int(
                os.getenv("LAGZERO_MIN_INCIDENT_OFFSET_LAG"), 25
            ),
            min_incident_time_lag_sec=_parse_optional_float(
                os.getenv("LAGZERO_MIN_INCIDENT_TIME_LAG_SEC"), 15.0
            ),
            min_stalled_offset_lag=_parse_optional_int(
                os.getenv("LAGZERO_MIN_STALLED_OFFSET_LAG"), 50
            ),
            min_stalled_time_lag_sec=_parse_optional_float(
                os.getenv("LAGZERO_MIN_STALLED_TIME_LAG_SEC"), 30.0
            ),
            min_lag_spike_delta=_parse_optional_int(
                os.getenv("LAGZERO_MIN_LAG_SPIKE_DELTA"), 25
            ),
            slow_consumer_efficiency_threshold=_parse_optional_float(
                os.getenv("LAGZERO_SLOW_CONSUMER_EFFICIENCY_THRESHOLD"), 0.8
            ),
            catching_up_efficiency_threshold=_parse_optional_float(
                os.getenv("LAGZERO_CATCHING_UP_EFFICIENCY_THRESHOLD"), 1.0
            ),
            burst_grace_sec=_parse_optional_float(
                os.getenv("LAGZERO_BURST_GRACE_SEC"), 20.0
            ),
            partition_skew_min_total_lag=_parse_optional_int(
                os.getenv("LAGZERO_PARTITION_SKEW_MIN_TOTAL_LAG"), 100
            ),
            partition_skew_max_hot_partitions=_parse_optional_int(
                os.getenv("LAGZERO_PARTITION_SKEW_MAX_HOT_PARTITIONS"), 1
            ),
            partition_skew_min_share=_parse_optional_float(
                os.getenv("LAGZERO_PARTITION_SKEW_MIN_SHARE"), 0.8
            ),
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
            ai_enabled=os.getenv("LAGZERO_AI_ENABLED", "false").strip().lower() == "true",
            ai_provider=os.getenv("LAGZERO_AI_PROVIDER", "openai").strip().lower() or "openai",
            ai_model=os.getenv("LAGZERO_AI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini",
            ai_max_tokens=_parse_optional_int(os.getenv("LAGZERO_AI_MAX_TOKENS"), 400),
            ai_temperature=_parse_optional_float(os.getenv("LAGZERO_AI_TEMPERATURE"), 0.1),
            ai_cache_ttl_sec=_parse_optional_float(
                os.getenv("LAGZERO_AI_CACHE_TTL_SEC"), 120.0
            ),
            incidents_enabled=os.getenv("LAGZERO_INCIDENTS_ENABLED", "false").strip().lower()
            == "true",
            incident_resolve_confirmations=_parse_optional_int(
                os.getenv("LAGZERO_INCIDENT_RESOLVE_CONFIRMATIONS"), 2
            ),
            webhook_enabled=os.getenv("LAGZERO_WEBHOOK_ENABLED", "false").strip().lower()
            == "true",
            webhook_url=os.getenv("LAGZERO_WEBHOOK_URL") or None,
            webhook_timeout_sec=_parse_optional_float(
                os.getenv("LAGZERO_WEBHOOK_TIMEOUT_SEC"), 5.0
            ),
            webhook_max_retries=_parse_optional_int(
                os.getenv("LAGZERO_WEBHOOK_MAX_RETRIES"), 3
            ),
            webhook_secret=os.getenv("LAGZERO_WEBHOOK_SECRET") or None,
            webhook_event_version=os.getenv("LAGZERO_WEBHOOK_EVENT_VERSION", "1.0").strip()
            or "1.0",
            persistence_backend=os.getenv("LAGZERO_PERSISTENCE_BACKEND", "sqlite").strip().lower()
            or "sqlite",
            sqlite_path=os.getenv("LAGZERO_SQLITE_PATH", ".chaos/lagzero.db").strip()
            or ".chaos/lagzero.db",
            event_log_path=os.getenv("LAGZERO_EVENT_LOG_PATH") or None,
            ingest_enabled=os.getenv("LAGZERO_INGEST_ENABLED", "false").strip().lower()
            == "true",
            ingest_host=os.getenv("LAGZERO_INGEST_HOST", "127.0.0.1").strip() or "127.0.0.1",
            ingest_port=_parse_optional_int(os.getenv("LAGZERO_INGEST_PORT"), 8787),
            ingest_path=os.getenv("LAGZERO_INGEST_PATH", "/events").strip() or "/events",
            ingest_request_timeout_sec=_parse_optional_float(
                os.getenv("LAGZERO_INGEST_REQUEST_TIMEOUT_SEC"), 5.0
            ),
            slack_webhook_url=os.getenv("LAGZERO_SLACK_WEBHOOK_URL") or None,
            correlation_window_sec=_parse_optional_float(
                os.getenv("LAGZERO_CORRELATION_WINDOW_SEC"), 900.0
            ),
        )
