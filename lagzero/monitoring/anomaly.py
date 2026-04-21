from __future__ import annotations

from dataclasses import dataclass


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

    if state_reset:
        return AnomalyResult(name="offset_reset", severity="info", confidence=0.95)

    if current_lag <= 0:
        return AnomalyResult(name="normal", severity="info", confidence=0.95)

    if catching_up:
        return AnomalyResult(name="catching_up", severity="info", confidence=confidence)

    if (
        producer_rate is not None
        and processing_rate is not None
        and backlog_growth_rate is not None
        and producer_rate > processing_rate
        and backlog_growth_rate > 0
    ):
        return AnomalyResult(name="system_under_pressure", severity="warning", confidence=confidence)

    if (
        lag_divergence_sec is not None
        and lag_divergence_sec >= lag_divergence_threshold_sec
    ):
        return AnomalyResult(
            name="lag_estimation_mismatch",
            severity="warning",
            confidence=confidence,
        )

    if (
        no_offset_movement
        and consecutive_no_movement_intervals >= idle_intervals
        and current_lag > 0
    ):
        return AnomalyResult(name="idle_but_delayed", severity="warning", confidence=confidence)

    if (
        processing_rate == 0
        and consecutive_zero_rate_intervals >= stalled_intervals
        and current_lag > 0
    ):
        return AnomalyResult(name="consumer_stalled", severity="critical", confidence=confidence)

    if (
        previous_lag is not None
        and previous_lag > 0
        and (
            current_lag >= int(previous_lag * lag_spike_multiplier)
            or (lag_velocity is not None and lag_velocity > 0)
        )
    ):
        return AnomalyResult(name="lag_spike", severity="warning", confidence=confidence)

    if processing_rate is None:
        return AnomalyResult(name="normal", severity="info", confidence=confidence)

    return AnomalyResult(name="normal", severity="info", confidence=confidence)


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
