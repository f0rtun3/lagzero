from __future__ import annotations

import sqlite3
from pathlib import Path

from lagzero.persistence.models import INCIDENTS_TABLE_SQL, TIMELINE_TABLE_SQL


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
