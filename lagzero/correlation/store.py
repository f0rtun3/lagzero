from __future__ import annotations

from collections import deque

from lagzero.correlation.schema import ExternalEvent


class CorrelationEventStore:
    def __init__(self, retention_sec: float, max_events: int = 1_000) -> None:
        self._retention_sec = retention_sec
        self._max_events = max_events
        self._events: deque[ExternalEvent] = deque()

    def add(self, event: ExternalEvent) -> None:
        self.prune(event.timestamp)
        self._events.append(event)
        while len(self._events) > self._max_events:
            self._events.popleft()

    def prune(self, reference_time: float) -> None:
        valid_after = reference_time - self._retention_sec
        while self._events and self._events[0].timestamp < valid_after:
            self._events.popleft()

    def query_recent(self, reference_time: float) -> list[ExternalEvent]:
        self.prune(reference_time)
        return list(self._events)

