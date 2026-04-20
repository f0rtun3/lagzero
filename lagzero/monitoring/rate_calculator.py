from __future__ import annotations

from dataclasses import dataclass
from statistics import pstdev


@dataclass(frozen=True, slots=True)
class RateSample:
    processed_messages: int
    elapsed_seconds: float
    messages_per_second: float | None
    state_reset: bool = False


def compute_rate(
    previous_committed_offset: int | None,
    current_committed_offset: int,
    previous_timestamp: float | None,
    current_timestamp: float,
) -> RateSample:
    if previous_committed_offset is None or previous_timestamp is None:
        return RateSample(
            processed_messages=0,
            elapsed_seconds=0.0,
            messages_per_second=None,
        )

    elapsed_seconds = current_timestamp - previous_timestamp
    if elapsed_seconds <= 0:
        return RateSample(
            processed_messages=0,
            elapsed_seconds=max(elapsed_seconds, 0.0),
            messages_per_second=None,
        )

    delta = current_committed_offset - previous_committed_offset
    if delta < 0:
        return RateSample(
            processed_messages=0,
            elapsed_seconds=elapsed_seconds,
            messages_per_second=None,
            state_reset=True,
        )

    return RateSample(
        processed_messages=delta,
        elapsed_seconds=elapsed_seconds,
        messages_per_second=delta / elapsed_seconds,
    )


def smooth_rate(recent_rates: list[float], window_size: int) -> float | None:
    if window_size <= 0:
        raise ValueError("window_size must be positive")

    if not recent_rates:
        return None

    window = recent_rates[-window_size:]
    return sum(window) / len(window)


def rate_variance_high(recent_rates: list[float], window_size: int) -> bool:
    window = recent_rates[-window_size:]
    if len(window) < 2:
        return False

    average_rate = sum(window) / len(window)
    if average_rate <= 0:
        return True

    return (pstdev(window) / average_rate) >= 0.5
