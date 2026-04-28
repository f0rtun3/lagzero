from __future__ import annotations


INCIDENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY,
    incident_key TEXT NOT NULL,
    family TEXT NOT NULL,
    status TEXT NOT NULL,
    scope TEXT NOT NULL,
    consumer_group TEXT,
    topic TEXT,
    partition INTEGER,
    opened_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    resolved_at REAL,
    current_anomaly TEXT NOT NULL,
    current_health TEXT,
    current_severity TEXT,
    current_primary_cause TEXT,
    current_primary_cause_confidence REAL,
    current_payload_json TEXT NOT NULL
);
"""


TIMELINE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS incident_timeline (
    timeline_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    at REAL NOT NULL,
    summary TEXT NOT NULL,
    details_json TEXT NOT NULL
);
"""
