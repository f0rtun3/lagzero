from __future__ import annotations

import sqlite3
from pathlib import Path

from lagzero.persistence.models import DELIVERIES_TABLE_SQL, INCIDENTS_TABLE_SQL, TIMELINE_TABLE_SQL


class SQLiteIncidentStore:
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.execute(INCIDENTS_TABLE_SQL)
            connection.execute(TIMELINE_TABLE_SQL)
            connection.execute(DELIVERIES_TABLE_SQL)
            self._ensure_column(
                connection,
                table="incidents",
                column="current_primary_cause_confidence",
                column_sql="REAL",
            )
            self._ensure_column(
                connection,
                table="incidents",
                column="current_payload_json",
                column_sql="TEXT NOT NULL DEFAULT '{}'",
            )

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection,
        *,
        table: str,
        column: str,
        column_sql: str,
    ) -> None:
        rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {row["name"] for row in rows}
        if column in existing:
            return
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_sql}")
