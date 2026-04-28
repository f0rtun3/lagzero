from __future__ import annotations


class IncidentStateStore:
    def __init__(self) -> None:
        self._resolve_counts: dict[str, int] = {}

    def increment_resolution(self, incident_id: str) -> int:
        current = self._resolve_counts.get(incident_id, 0) + 1
        self._resolve_counts[incident_id] = current
        return current

    def reset_resolution(self, incident_id: str) -> None:
        self._resolve_counts.pop(incident_id, None)

    def clear_many(self, incident_ids: list[str]) -> None:
        for incident_id in incident_ids:
            self.reset_resolution(incident_id)
