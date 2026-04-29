from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def snapshot_sqlite_state(path: str | Path) -> dict[str, list[dict[str, object]]]:
    target = Path(path)
    if not target.exists():
        return {"incidents": [], "timeline": [], "deliveries": []}

    connection = sqlite3.connect(target)
    connection.row_factory = sqlite3.Row
    try:
        incidents = _query_rows(connection, "SELECT * FROM incidents ORDER BY opened_at ASC")
        timeline = _query_rows(connection, "SELECT * FROM incident_timeline ORDER BY at ASC")
        deliveries = _query_rows(
            connection,
            "SELECT * FROM incident_deliveries ORDER BY COALESCE(last_delivery_at, 0) ASC, event_id ASC",
        )
        return {
            "incidents": incidents,
            "timeline": timeline,
            "deliveries": deliveries,
        }
    finally:
        connection.close()


def build_lifecycle_events(state_snapshot: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    incidents_by_id = {
        str(incident["incident_id"]): incident
        for incident in state_snapshot.get("incidents", [])
    }
    items: list[dict[str, object]] = []
    for entry in state_snapshot.get("timeline", []):
        incident = incidents_by_id.get(str(entry["incident_id"]))
        if incident is None:
            continue
        family = str(incident["family"])
        incident_key = str(incident["incident_key"])
        entry_type = str(entry["entry_type"])
        if entry_type == "incident_opened":
            event_type = "incident.opened"
        elif entry_type == "incident_resolved":
            event_type = "incident.resolved"
        else:
            event_type = "incident.updated"
        details = _load_json_field(entry.get("details_json"), default={})
        items.append(
            {
                "logical_incident_key": build_logical_incident_key(
                    incident_key=incident_key,
                    family=family,
                ),
                "incident_id": incident["incident_id"],
                "incident_key": incident_key,
                "family": family,
                "timeline_id": entry["timeline_id"],
                "entry_type": entry_type,
                "event_type": event_type,
                "at": entry["at"],
                "summary": entry["summary"],
                "details": details,
            }
        )
    return items


def build_logical_incident_key(*, incident_key: str, family: str) -> str:
    return f"{incident_key}|{family}"


def _query_rows(connection: sqlite3.Connection, sql: str) -> list[dict[str, object]]:
    rows = connection.execute(sql).fetchall()
    return [{key: _maybe_decode_json(value) for key, value in dict(row).items()} for row in rows]


def _maybe_decode_json(value: object) -> object:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if (stripped.startswith("{") and stripped.endswith("}")) or (
        stripped.startswith("[") and stripped.endswith("]")
    ):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return value
    return value


def _load_json_field(value: object, *, default: dict[str, object]) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return default
        if isinstance(decoded, dict):
            return decoded
    return default
