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


def clear_jsonl(path: str | Path) -> None:
    clear_incident_log(path)


def append_jsonl(path: str | Path, payload: dict[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def load_jsonl(path: str | Path) -> list[dict[str, object]]:
    target = Path(path)
    if not target.exists():
        return []
    items: list[dict[str, object]] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        items.append(json.loads(line))
    return items


def load_incidents(path: str | Path) -> list[IncidentPayload]:
    return load_jsonl(path)


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
