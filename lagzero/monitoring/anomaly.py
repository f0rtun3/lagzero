from __future__ import annotations

from dataclasses import dataclass

ANOMALY_PRIORITY = {
    "offset_reset": 100,
    "consumer_stalled": 90,
    "system_under_pressure": 80,
    "partition_skew": 70,
    "lag_estimation_mismatch": 65,
    "catching_up": 62,
    "lag_spike": 60,
    "slow_consumer": 55,
    "idle_but_delayed": 50,
    "bounded_lag": 10,
    "normal": 0,
}

ANOMALY_SEVERITY = {
    "offset_reset": "info",
    "consumer_stalled": "critical",
    "system_under_pressure": "warning",
    "partition_skew": "warning",
    "lag_estimation_mismatch": "warning",
    "lag_spike": "warning",
    "slow_consumer": "warning",
    "idle_but_delayed": "warning",
    "catching_up": "info",
    "bounded_lag": "info",
    "normal": "info",
}

ANOMALY_HEALTH = {
    "offset_reset": "healthy",
    "consumer_stalled": "failing",
    "system_under_pressure": "degraded",
    "partition_skew": "degraded",
    "lag_estimation_mismatch": "degraded",
    "lag_spike": "degraded",
    "slow_consumer": "degraded",
    "idle_but_delayed": "degraded",
    "catching_up": "recovering",
    "bounded_lag": "healthy",
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
    time_lag_sec: float | None,
    producer_rate: float | None,
    backlog_growth_rate: float | None,
    consumer_efficiency: float | None,
    lag_decreasing: bool,
    backlog_growth_rate_improving: bool,
    recent_lag_spike_active: bool,
    min_incident_offset_lag: int,
    min_incident_time_lag_sec: float,
    min_stalled_offset_lag: int,
    min_stalled_time_lag_sec: float,
    min_lag_spike_delta: int,
    slow_consumer_efficiency_threshold: float,
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

    candidates.append("bounded_lag")
    meets_incident_floor = _meets_floor(
        current_lag=current_lag,
        time_lag_sec=time_lag_sec,
        min_offset_lag=min_incident_offset_lag,
        min_time_lag_sec=min_incident_time_lag_sec,
    )
    meets_stalled_floor = _meets_floor(
        current_lag=current_lag,
        time_lag_sec=time_lag_sec,
        min_offset_lag=min_stalled_offset_lag,
        min_time_lag_sec=min_stalled_time_lag_sec,
    )

    if catching_up:
        candidates.append("catching_up")

    if (
        meets_incident_floor
        and
        producer_rate is not None
        and processing_rate is not None
        and backlog_growth_rate is not None
        and producer_rate > processing_rate
        and backlog_growth_rate > 0
    ):
        candidates.append("system_under_pressure")

    if (
        meets_incident_floor
        and
        lag_divergence_sec is not None
        and lag_divergence_sec >= lag_divergence_threshold_sec
    ):
        candidates.append("lag_estimation_mismatch")

    if (
        meets_incident_floor
        and
        no_offset_movement
        and consecutive_no_movement_intervals >= idle_intervals
        and current_lag > 0
    ):
        candidates.append("idle_but_delayed")

    if (
        meets_incident_floor
        and
        processing_rate is not None
        and processing_rate > 0
        and consumer_efficiency is not None
        and consumer_efficiency < slow_consumer_efficiency_threshold
        and backlog_growth_rate is not None
        and backlog_growth_rate >= 0
    ):
        candidates.append("slow_consumer")

    if (
        meets_stalled_floor
        and
        processing_rate == 0
        and no_offset_movement
        and consecutive_zero_rate_intervals >= stalled_intervals
        and current_lag > 0
        and not (
            producer_rate is not None
            and producer_rate <= 0.1
            and consecutive_no_movement_intervals >= idle_intervals
        )
        and not recent_lag_spike_active
    ):
        candidates.append("consumer_stalled")

    if (
        meets_incident_floor
        and
        previous_lag is not None
        and previous_lag > 0
        and (current_lag - previous_lag) >= min_lag_spike_delta
        and (
            current_lag >= int(previous_lag * lag_spike_multiplier)
            or (lag_velocity is not None and lag_velocity >= float(min_lag_spike_delta))
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


def _meets_floor(
    *,
    current_lag: int,
    time_lag_sec: float | None,
    min_offset_lag: int,
    min_time_lag_sec: float,
) -> bool:
    return current_lag >= min_offset_lag or (
        time_lag_sec is not None and time_lag_sec >= min_time_lag_sec
    )
