from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable


IncidentPayload = dict[str, object]


def clear_incident_log(path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("", encoding="utf-8")


def load_incidents(path: str | Path) -> list[IncidentPayload]:
    target = Path(path)
    if not target.exists():
        return []
    incidents: list[IncidentPayload] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        incidents.append(json.loads(line))
    return incidents


def wait_for_incident(
    path: str | Path,
    predicate: Callable[[IncidentPayload], bool],
    *,
    timeout_sec: float,
    poll_interval_sec: float = 1.0,
) -> IncidentPayload:
    deadline = time.time() + timeout_sec
    last_incidents: list[IncidentPayload] = []
    while time.time() < deadline:
        last_incidents = load_incidents(path)
        for incident in reversed(last_incidents):
            if predicate(incident):
                return incident
        time.sleep(poll_interval_sec)
    raise TimeoutError(
        f"No matching incident observed within {timeout_sec} seconds. "
        f"Captured incidents: {len(last_incidents)}"
    )


def latest_group_incident(
    incidents: list[IncidentPayload],
    *,
    consumer_group: str,
) -> IncidentPayload | None:
    for incident in reversed(incidents):
        if (
            incident.get("scope") == "consumer_group"
            and incident.get("consumer_group") == consumer_group
        ):
            return incident
    return None
