from __future__ import annotations


def compute_lag_velocity(
    previous_lag: int | None,
    current_lag: int,
    elapsed_seconds: float,
) -> float | None:
    if previous_lag is None or elapsed_seconds <= 0:
        return None

    return (current_lag - previous_lag) / elapsed_seconds

