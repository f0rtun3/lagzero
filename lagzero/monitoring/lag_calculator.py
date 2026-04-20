from __future__ import annotations


def compute_lag(latest_offset: int, committed_offset: int) -> int:
    return max(latest_offset - committed_offset, 0)

