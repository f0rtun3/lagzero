from __future__ import annotations


def compute_time_lag(offset_lag: int, processing_rate: float | None) -> float | None:
    if processing_rate is None or processing_rate <= 0:
        return None
    return offset_lag / processing_rate

