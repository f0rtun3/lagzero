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
    consecutive_zero_rate_intervals: int,
    lag_spike_multiplier: float,
) -> AnomalyResult:
    if current_lag <= 0:
        return AnomalyResult(name="normal", severity="info", confidence=0.95)

    if (
        processing_rate == 0
        and consecutive_zero_rate_intervals >= stalled_intervals
        and current_lag > 0
    ):
        return AnomalyResult(name="consumer_stalled", severity="critical", confidence=0.95)

    if (
        previous_lag is not None
        and previous_lag > 0
        and current_lag >= int(previous_lag * lag_spike_multiplier)
    ):
        return AnomalyResult(name="lag_spike", severity="warning", confidence=0.75)

    if processing_rate is None:
        return AnomalyResult(name="normal", severity="info", confidence=0.55)

    return AnomalyResult(name="normal", severity="info", confidence=0.9)

