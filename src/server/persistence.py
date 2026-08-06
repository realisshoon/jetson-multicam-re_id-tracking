from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.server.camera_a_message import (
    compact_json,
    create_camera_a_event_key,
)


@dataclass(frozen=True)
class EntryStoreResult:
    status: str
    global_person_id: str
    event_key: str

    @property
    def inserted(self) -> bool:
        return self.status == "inserted"

    @property
    def duplicate(self) -> bool:
        return self.status == "duplicate"


class EventRepository(ABC):
    """Persistence boundary to be implemented by Memory or Django storage."""

    @abstractmethod
    def record_entry(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_match(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_unknown(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_timeout(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_node_status(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_camera_a_entry(
        self,
        message: dict[str, Any],
    ) -> EntryStoreResult:
        raise NotImplementedError


class MemoryEventRepository(EventRepository):
    """In-memory event log used until the Django repository is available."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.node_statuses: dict[str, dict[str, Any]] = {}

    def record_entry(self, message: dict[str, Any]) -> None:
        self._append("ENTRY", message)

    def record_match(self, message: dict[str, Any]) -> None:
        self._append("MATCH", message)

    def record_unknown(self, message: dict[str, Any]) -> None:
        self._append("UNKNOWN", message)

    def record_timeout(self, message: dict[str, Any]) -> None:
        self._append("TIMEOUT", message)

    def update_node_status(self, message: dict[str, Any]) -> None:
        saved = deepcopy(message)
        self.node_statuses[message["node_id"]] = saved
        self._append("NODE_STATUS", message)

    def record_camera_a_entry(
        self,
        message: dict[str, Any],
    ) -> EntryStoreResult:
        event_key = create_camera_a_event_key(message)
        self._append("ENTRY", message)
        return EntryStoreResult(
            status="inserted",
            global_person_id=message["global_person_id"],
            event_key=event_key,
        )

    def _append(self, event_type: str, message: dict[str, Any]) -> None:
        self.events.append(
            {
                "event_type": event_type,
                "message": deepcopy(message),
            }
        )


class SQLiteEventRepository(EventRepository):
    """SQLite persistence for validated origin/main Camera A ENTRY events."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def record_camera_a_entry(
        self,
        message: dict[str, Any],
    ) -> EntryStoreResult:
        event_key = create_camera_a_event_key(message)
        server_received_at = datetime.now(timezone.utc).isoformat()
        embedding_json = compact_json(message["embedding"])
        raw_payload_json = compact_json(message)
        next_nodes_json = compact_json(message["next_nodes"])

        connection = self._connect()
        try:
            connection.execute("BEGIN")
            connection.execute(
                """
                INSERT OR IGNORE INTO persons (
                    global_person_id,
                    created_at,
                    source_node,
                    first_local_track_id,
                    first_seen_at,
                    reid_model,
                    embedding_dim,
                    initial_embedding_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message["global_person_id"],
                    server_received_at,
                    message["node_id"],
                    message["local_track_id"],
                    message["timestamp"],
                    message["reid_model"],
                    message["embedding_dim"],
                    embedding_json,
                ),
            )
            cursor = connection.execute(
                """
                INSERT INTO tracking_events (
                    event_key,
                    global_person_id,
                    node_id,
                    event_type,
                    local_track_id,
                    event_timestamp,
                    server_received_at,
                    next_nodes_json,
                    reid_model,
                    embedding_dim,
                    embedding_json,
                    raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO NOTHING
                """,
                (
                    event_key,
                    message["global_person_id"],
                    message["node_id"],
                    message["event"],
                    message["local_track_id"],
                    message["timestamp"],
                    server_received_at,
                    next_nodes_json,
                    message["reid_model"],
                    message["embedding_dim"],
                    embedding_json,
                    raw_payload_json,
                ),
            )
            if cursor.rowcount == 0:
                connection.rollback()
                return EntryStoreResult(
                    status="duplicate",
                    global_person_id=message["global_person_id"],
                    event_key=event_key,
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        return EntryStoreResult(
            status="inserted",
            global_person_id=message["global_person_id"],
            event_key=event_key,
        )

    def record_entry(self, message: dict[str, Any]) -> None:
        self.record_camera_a_entry(message)

    def record_match(self, message: dict[str, Any]) -> None:
        raise NotImplementedError("SQLite MVP currently stores Camera A ENTRY only")

    def record_unknown(self, message: dict[str, Any]) -> None:
        raise NotImplementedError("SQLite MVP currently stores Camera A ENTRY only")

    def record_timeout(self, message: dict[str, Any]) -> None:
        raise NotImplementedError("SQLite MVP currently stores Camera A ENTRY only")

    def update_node_status(self, message: dict[str, Any]) -> None:
        raise NotImplementedError("SQLite MVP currently stores Camera A ENTRY only")

    def fetch_persons(
        self,
        global_person_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT global_person_id, source_node, first_local_track_id,
                   first_seen_at, reid_model, embedding_dim
            FROM persons
        """
        parameters: list[Any] = []
        if global_person_id is not None:
            query += " WHERE global_person_id = ?"
            parameters.append(global_person_id)
        query += " ORDER BY created_at DESC LIMIT ?"
        parameters.append(limit)
        return self._fetch_rows(query, parameters)

    def fetch_tracking_events(
        self,
        global_person_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT id, global_person_id, node_id, event_type,
                   local_track_id, event_timestamp, server_received_at,
                   event_key, embedding_dim, embedding_json
            FROM tracking_events
        """
        parameters: list[Any] = []
        if global_person_id is not None:
            query += " WHERE global_person_id = ?"
            parameters.append(global_person_id)
        query += " ORDER BY id DESC LIMIT ?"
        parameters.append(limit)
        return self._fetch_rows(query, parameters)

    def integrity_summary(self) -> dict[str, int]:
        connection = self._connect()
        try:
            persons_count = connection.execute(
                "SELECT COUNT(*) FROM persons"
            ).fetchone()[0]
            events_count = connection.execute(
                "SELECT COUNT(*) FROM tracking_events"
            ).fetchone()[0]
            orphan_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM tracking_events AS event
                LEFT JOIN persons AS person
                  ON person.global_person_id = event.global_person_id
                WHERE person.global_person_id IS NULL
                """
            ).fetchone()[0]
            embedding_mismatch_count = 0
            for row in connection.execute(
                "SELECT embedding_dim, embedding_json FROM tracking_events"
            ):
                try:
                    embedding = json.loads(row["embedding_json"])
                except (json.JSONDecodeError, TypeError):
                    embedding_mismatch_count += 1
                    continue
                if not isinstance(embedding, list) or len(embedding) != row[
                    "embedding_dim"
                ]:
                    embedding_mismatch_count += 1
            return {
                "persons": persons_count,
                "tracking_events": events_count,
                "orphan_events": orphan_count,
                "embedding_dim_mismatches": embedding_mismatch_count,
            }
        finally:
            connection.close()

    def foreign_keys_enabled(self) -> bool:
        connection = self._connect()
        try:
            return bool(connection.execute("PRAGMA foreign_keys").fetchone()[0])
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _fetch_rows(
        self,
        query: str,
        parameters: list[Any],
    ) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            return [
                dict(row)
                for row in connection.execute(query, parameters).fetchall()
            ]
        finally:
            connection.close()

    def _initialize_schema(self) -> None:
        connection = self._connect()
        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS persons (
                    global_person_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    source_node TEXT NOT NULL,
                    first_local_track_id INTEGER NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    reid_model TEXT NOT NULL,
                    embedding_dim INTEGER NOT NULL,
                    initial_embedding_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tracking_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    global_person_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    local_track_id INTEGER NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    server_received_at TEXT NOT NULL,
                    next_nodes_json TEXT NOT NULL,
                    reid_model TEXT NOT NULL,
                    embedding_dim INTEGER NOT NULL,
                    embedding_json TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL,
                    FOREIGN KEY(global_person_id)
                        REFERENCES persons(global_person_id)
                );
                """
            )
            connection.commit()
        finally:
            connection.close()
