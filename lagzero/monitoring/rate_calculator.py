from __future__ import annotations

from dataclasses import dataclass


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

