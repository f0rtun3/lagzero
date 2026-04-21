from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TimeLagEstimate:
    seconds: float | None
    source: str
    offset_based_seconds: float | None
    timestamp_based_seconds: float | None


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
) -> TimeLagEstimate:
    offset_based_seconds = compute_offset_time_lag(
        offset_lag=offset_lag,
        processing_rate=processing_rate,
    )
    timestamp_based_seconds = compute_timestamp_time_lag(
        observed_at=observed_at,
        backlog_head_timestamp=backlog_head_timestamp,
    )

    if timestamp_based_seconds is not None:
        return TimeLagEstimate(
            seconds=timestamp_based_seconds,
            source="timestamp",
            offset_based_seconds=offset_based_seconds,
            timestamp_based_seconds=timestamp_based_seconds,
        )

    if offset_based_seconds is not None:
        return TimeLagEstimate(
            seconds=offset_based_seconds,
            source="offset_rate",
            offset_based_seconds=offset_based_seconds,
            timestamp_based_seconds=timestamp_based_seconds,
        )

    return TimeLagEstimate(
        seconds=None,
        source="unavailable",
        offset_based_seconds=offset_based_seconds,
        timestamp_based_seconds=timestamp_based_seconds,
    )
