from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TimeLagEstimate:
    seconds: float | None
    source: str
    offset_based_seconds: float | None
    timestamp_based_seconds: float | None
    lag_divergence_sec: float | None
    timestamp_type: str | None
    is_divergent: bool


def compute_offset_time_lag(offset_lag: int, processing_rate: float | None) -> float | None:
    if processing_rate is None or processing_rate <= 0:
        return None
    return offset_lag / processing_rate


def compute_timestamp_time_lag(
    observed_at: float,
    backlog_head_timestamp: float | None,
) -> float | None:
    if backlog_head_timestamp is None:
        return None
    return max(observed_at - backlog_head_timestamp, 0.0)


def compute_time_lag(
    *,
    offset_lag: int,
    processing_rate: float | None,
    observed_at: float,
    backlog_head_timestamp: float | None,
    timestamp_sampling_state: str,
    timestamp_type: str | None,
    lag_divergence_threshold_sec: float,
) -> TimeLagEstimate:
    offset_based_seconds = compute_offset_time_lag(
        offset_lag=offset_lag,
        processing_rate=processing_rate,
    )
    timestamp_based_seconds = compute_timestamp_time_lag(
        observed_at=observed_at,
        backlog_head_timestamp=backlog_head_timestamp,
    )
    lag_divergence_sec = None
    if offset_based_seconds is not None and timestamp_based_seconds is not None:
        lag_divergence_sec = abs(timestamp_based_seconds - offset_based_seconds)
    is_divergent = (
        lag_divergence_sec is not None and lag_divergence_sec >= lag_divergence_threshold_sec
    )

    if timestamp_based_seconds is not None and timestamp_sampling_state == "timestamp":
        return TimeLagEstimate(
            seconds=timestamp_based_seconds,
            source="timestamp",
            offset_based_seconds=offset_based_seconds,
            timestamp_based_seconds=timestamp_based_seconds,
            lag_divergence_sec=lag_divergence_sec,
            timestamp_type=timestamp_type,
            is_divergent=is_divergent,
        )

    if offset_based_seconds is not None:
        return TimeLagEstimate(
            seconds=offset_based_seconds,
            source="estimated_fallback"
            if timestamp_sampling_state in {"sampling_failed", "cold_start"}
            else "estimated",
            offset_based_seconds=offset_based_seconds,
            timestamp_based_seconds=timestamp_based_seconds,
            lag_divergence_sec=lag_divergence_sec,
            timestamp_type=timestamp_type,
            is_divergent=is_divergent,
        )

    return TimeLagEstimate(
        seconds=None,
        source="estimated_fallback"
        if timestamp_sampling_state in {"sampling_failed", "cold_start"}
        else "unavailable",
        offset_based_seconds=offset_based_seconds,
        timestamp_based_seconds=timestamp_based_seconds,
        lag_divergence_sec=lag_divergence_sec,
        timestamp_type=timestamp_type,
        is_divergent=is_divergent,
    )
