from __future__ import annotations

from dataclasses import dataclass

ANOMALY_PRIORITY = {
    "consumer_stalled": 100,
    "offset_reset": 90,
    "system_under_pressure": 80,
    "partition_skew": 70,
    "lag_estimation_mismatch": 65,
    "lag_spike": 60,
    "idle_but_delayed": 50,
    "catching_up": 40,
    "normal": 0,
}

ANOMALY_SEVERITY = {
    "consumer_stalled": "critical",
    "offset_reset": "info",
    "system_under_pressure": "warning",
    "partition_skew": "warning",
    "lag_estimation_mismatch": "warning",
    "lag_spike": "warning",
    "idle_but_delayed": "warning",
    "catching_up": "info",
    "normal": "info",
}

ANOMALY_HEALTH = {
    "consumer_stalled": "failing",
    "offset_reset": "healthy",
    "system_under_pressure": "degraded",
    "partition_skew": "degraded",
    "lag_estimation_mismatch": "degraded",
    "lag_spike": "degraded",
    "idle_but_delayed": "degraded",
    "catching_up": "recovering",
    "normal": "healthy",
}


@dataclass(frozen=True, slots=True)
class AnomalyResult:
    name: str
    severity: str
    confidence: float


def detect_anomaly(
    *,
    current_lag: int,
    previous_lag: int | None,
    processing_rate: float | None,
    stalled_intervals: int,
    idle_intervals: int,
    consecutive_zero_rate_intervals: int,
    consecutive_no_movement_intervals: int,
    lag_spike_multiplier: float,
    rate_variance_high: bool,
    lag_velocity: float | None,
    no_offset_movement: bool,
    state_reset: bool,
    lag_divergence_sec: float | None,
    lag_divergence_threshold_sec: float,
    time_lag_source: str,
    timestamp_type: str | None,
    catching_up: bool,
    producer_rate: float | None,
    backlog_growth_rate: float | None,
) -> AnomalyResult:
    confidence = _compute_confidence(
        processing_rate=processing_rate,
        rate_variance_high=rate_variance_high,
        no_offset_movement=no_offset_movement,
        state_reset=state_reset,
        lag_divergence_sec=lag_divergence_sec,
        lag_divergence_threshold_sec=lag_divergence_threshold_sec,
        time_lag_source=time_lag_source,
        timestamp_type=timestamp_type,
    )

    candidates = ["normal"]
    if state_reset:
        return AnomalyResult(name="offset_reset", severity="info", confidence=0.95)

    if current_lag <= 0:
        return AnomalyResult(name="normal", severity="info", confidence=0.95)

    if catching_up:
        candidates.append("catching_up")

    if (
        producer_rate is not None
        and processing_rate is not None
        and backlog_growth_rate is not None
        and producer_rate > processing_rate
        and backlog_growth_rate > 0
    ):
        candidates.append("system_under_pressure")

    if (
        lag_divergence_sec is not None
        and lag_divergence_sec >= lag_divergence_threshold_sec
    ):
        candidates.append("lag_estimation_mismatch")

    if (
        no_offset_movement
        and consecutive_no_movement_intervals >= idle_intervals
        and current_lag > 0
    ):
        candidates.append("idle_but_delayed")

    if (
        processing_rate == 0
        and consecutive_zero_rate_intervals >= stalled_intervals
        and current_lag > 0
    ):
        candidates.append("consumer_stalled")

    if (
        previous_lag is not None
        and previous_lag > 0
        and (
            current_lag >= int(previous_lag * lag_spike_multiplier)
            or (lag_velocity is not None and lag_velocity > 0)
        )
    ):
        candidates.append("lag_spike")

    final_anomaly = resolve_anomaly(candidates)
    return AnomalyResult(
        name=final_anomaly,
        severity=severity_for_anomaly(final_anomaly),
        confidence=confidence,
    )


def _compute_confidence(
    *,
    processing_rate: float | None,
    rate_variance_high: bool,
    no_offset_movement: bool,
    state_reset: bool,
    lag_divergence_sec: float | None,
    lag_divergence_threshold_sec: float,
    time_lag_source: str,
    timestamp_type: str | None,
) -> float:
    if state_reset:
        return 0.95
    if time_lag_source == "timestamp":
        base_confidence = 0.9 if timestamp_type == "log_append_time" else 0.75
    elif time_lag_source == "estimated":
        base_confidence = 0.6
    elif time_lag_source == "estimated_fallback":
        base_confidence = 0.4
    else:
        base_confidence = 0.3
    if processing_rate == 0:
        return min(base_confidence, 0.2)
    if processing_rate is None:
        return min(base_confidence, 0.4)
    if (
        rate_variance_high
        or no_offset_movement
        or (
            lag_divergence_sec is not None
            and lag_divergence_sec >= lag_divergence_threshold_sec
        )
    ):
        return min(base_confidence, 0.5)
    return base_confidence


def resolve_anomaly(candidates: list[str]) -> str:
    return max(candidates, key=lambda name: ANOMALY_PRIORITY.get(name, 0))


def severity_for_anomaly(anomaly_name: str) -> str:
    return ANOMALY_SEVERITY.get(anomaly_name, "info")


def health_for_anomaly(anomaly_name: str) -> str:
    return ANOMALY_HEALTH.get(anomaly_name, "healthy")
