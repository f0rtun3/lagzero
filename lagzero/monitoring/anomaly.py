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
) -> AnomalyResult:
    confidence = _compute_confidence(
        processing_rate=processing_rate,
        rate_variance_high=rate_variance_high,
        no_offset_movement=no_offset_movement,
        state_reset=state_reset,
    )

    if state_reset:
        return AnomalyResult(name="offset_reset", severity="info", confidence=0.95)

    if current_lag <= 0:
        return AnomalyResult(name="normal", severity="info", confidence=0.95)

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
) -> float:
    if state_reset:
        return 0.95
    if processing_rate == 0:
        return 0.2
    if processing_rate is None:
        return 0.4
    if rate_variance_high or no_offset_movement:
        return 0.5
    return 0.9
