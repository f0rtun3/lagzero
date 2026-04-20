from __future__ import annotations

from dataclasses import dataclass


PartitionKey = tuple[str, int]


@dataclass(slots=True)
class PartitionState:
    committed_offset: int
    latest_offset: int
    observed_at: float
    offset_lag: int
    consecutive_zero_rate_intervals: int = 0


class InMemoryStateStore:
    def __init__(self) -> None:
        self._state: dict[PartitionKey, PartitionState] = {}

    def get(self, key: PartitionKey) -> PartitionState | None:
        return self._state.get(key)

    def set(self, key: PartitionKey, value: PartitionState) -> None:
        self._state[key] = value

