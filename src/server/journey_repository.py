from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.server.journey_protocol import (
    CanonicalJourneyEvent,
    compact_json,
    create_raw_event_key,
)


STATUS_RANK = {
    "CREATED": 0,
    "CANDIDATE_SENT": 1,
    "PENDING": 2,
    "MATCHED_AT_B": 3,
    "GALLERY_COLLECTING": 4,
    "PASSED": 5,
    "COMPLETED": 6,
    "FAILED": 6,
}


@dataclass(frozen=True)
class JourneyStoreResult:
    status: str
    event_key: str
    journey_id: str | None = None
    category: str | None = None

    @property
    def duplicate(self) -> bool:
        return self.status == "duplicate"


class JourneySQLiteRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def store_raw_message(
        self,
        topic: str,
        payload: Any,
        *,
        received_at: str | None = None,
        journey_id: str | None = None,
        source_node: str | None = None,
        event_key: str | None = None,
    ) -> JourneyStoreResult:
        received_at = received_at or _now()
        event_key = event_key or create_raw_event_key(topic, payload)
        payload_json = compact_json(payload)
        connection = self._connect()
        try:
            cursor = connection.execute(
                """
                INSERT INTO raw_mqtt_messages (
                    event_key, topic, payload_json, journey_id,
                    source_node, received_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO NOTHING
                """,
                (
                    event_key,
                    topic,
                    payload_json,
                    journey_id,
                    source_node,
                    received_at,
                ),
            )
            connection.commit()
            return JourneyStoreResult(
                status="duplicate" if cursor.rowcount == 0 else "raw_stored",
                event_key=event_key,
                journey_id=journey_id,
                category="raw",
            )
        finally:
            connection.close()

    def store_event(
        self,
        event: CanonicalJourneyEvent,
        *,
        received_at: str | None = None,
    ) -> JourneyStoreResult:
        received_at = received_at or _now()
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            raw_cursor = connection.execute(
                """
                INSERT INTO raw_mqtt_messages (
                    event_key, topic, payload_json, journey_id,
                    source_node, received_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO NOTHING
                """,
                (
                    event.event_key,
                    event.raw_topic,
                    compact_json(event.raw_payload),
                    event.journey_id,
                    event.source_node,
                    received_at,
                ),
            )
            if raw_cursor.rowcount == 0:
                connection.rollback()
                return JourneyStoreResult(
                    status="duplicate",
                    event_key=event.event_key,
                    journey_id=event.journey_id,
                )

            self._upsert_journey(connection, event, received_at)
            category = self._store_category(connection, event, received_at)
            connection.commit()
            return JourneyStoreResult(
                status="inserted",
                event_key=event.event_key,
                journey_id=event.journey_id,
                category=category,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def fetch_journey(self, journey_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM journeys WHERE journey_id = ?",
            [journey_id],
        )

    def fetch_node_matches(self, journey_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            "SELECT * FROM node_matches WHERE journey_id = ? ORDER BY id",
            [journey_id],
        )

    def fetch_gallery_samples(self, journey_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            "SELECT * FROM gallery_samples WHERE journey_id = ? ORDER BY id",
            [journey_id],
        )

    def fetch_passages(self, journey_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            "SELECT * FROM passage_events WHERE journey_id = ? ORDER BY id",
            [journey_id],
        )

    def fetch_completions(self, journey_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            "SELECT * FROM journey_completions WHERE journey_id = ? ORDER BY id",
            [journey_id],
        )

    def counts(self) -> dict[str, int]:
        tables = (
            "journeys",
            "node_matches",
            "gallery_samples",
            "passage_events",
            "journey_completions",
            "raw_mqtt_messages",
        )
        connection = self._connect()
        try:
            return {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in tables
            }
        finally:
            connection.close()

    def _upsert_journey(
        self,
        connection: sqlite3.Connection,
        event: CanonicalJourneyEvent,
        received_at: str,
    ) -> None:
        current = connection.execute(
            "SELECT status, route_json, created_at FROM journeys "
            "WHERE journey_id = ?",
            (event.journey_id,),
        ).fetchone()
        if current is None:
            route = event.route or []
            completed_at = (
                event.timestamp
                if event.status == "COMPLETED"
                else None
            )
            connection.execute(
                """
                INSERT INTO journeys (
                    journey_id, status, route_json, created_at,
                    updated_at, completed_at, raw_first_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.journey_id,
                    event.status,
                    compact_json(route),
                    event.timestamp,
                    received_at,
                    completed_at,
                    compact_json(event.raw_payload),
                ),
            )
            return

        status = _advance_status(
            current["status"],
            event.status,
            event.event_type,
        )
        route_json = (
            compact_json(event.route)
            if event.route is not None
            else current["route_json"]
        )
        completed_at = (
            event.timestamp
            if status == "COMPLETED"
            else None
        )
        connection.execute(
            """
            UPDATE journeys
            SET status = ?, route_json = ?, updated_at = ?,
                completed_at = COALESCE(completed_at, ?)
            WHERE journey_id = ?
            """,
            (
                status,
                route_json,
                received_at,
                completed_at,
                event.journey_id,
            ),
        )

    def _store_category(
        self,
        connection: sqlite3.Connection,
        event: CanonicalJourneyEvent,
        received_at: str,
    ) -> str:
        event_type = event.event_type.upper()
        if event.status == "COMPLETED" or event_type == "COMPLETED":
            self._insert_completion(connection, event)
            return "completion"
        if event.raw_topic == "cctv/passage/b" or event_type in {
            "PASSAGE",
            "PASSED",
        }:
            self._insert_passage(connection, event, received_at)
            return "passage"
        if "GALLERY" in event_type:
            self._insert_gallery(connection, event)
            return "gallery"
        if "MATCH" in event_type or event_type == "TRACK_LOST":
            self._insert_node_match(connection, event)
            return "match"
        return "journey"

    def _insert_node_match(
        self,
        connection: sqlite3.Connection,
        event: CanonicalJourneyEvent,
    ) -> None:
        if event.local_track_id is None:
            raise ValueError("node match requires local_track_id")
        if event.event_type.upper() == "TRACK_LOST":
            updated = connection.execute(
                """
                UPDATE node_matches
                SET lost_at = ?, match_status = ?
                WHERE id = (
                    SELECT id FROM node_matches
                    WHERE journey_id = ? AND node_id = ?
                      AND local_track_id = ? AND lost_at IS NULL
                    ORDER BY id DESC LIMIT 1
                )
                """,
                (
                    event.timestamp,
                    event.status,
                    event.journey_id,
                    event.source_node,
                    event.local_track_id,
                ),
            )
            if updated.rowcount:
                return
        lost_at = event.timestamp if event.event_type.upper() == "TRACK_LOST" else None
        matched_at = None if lost_at else event.timestamp
        connection.execute(
            """
            INSERT INTO node_matches (
                event_key, journey_id, node_id, local_track_id,
                similarity, match_status, matched_at, lost_at,
                raw_payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_key,
                event.journey_id,
                event.source_node,
                event.local_track_id,
                event.similarity,
                event.status,
                matched_at,
                lost_at,
                compact_json(event.raw_payload),
            ),
        )

    def _insert_gallery(
        self,
        connection: sqlite3.Connection,
        event: CanonicalJourneyEvent,
    ) -> None:
        connection.execute(
            """
            INSERT INTO gallery_samples (
                event_key, journey_id, node_id, local_track_id,
                sample_index, quality, embedding_dim, embedding_json,
                captured_at, raw_payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_key,
                event.journey_id,
                event.source_node,
                event.local_track_id,
                event.sample_index,
                event.quality,
                event.embedding_dim,
                compact_json(event.embedding) if event.embedding is not None else None,
                event.timestamp,
                compact_json(event.raw_payload),
            ),
        )

    def _insert_passage(
        self,
        connection: sqlite3.Connection,
        event: CanonicalJourneyEvent,
        received_at: str,
    ) -> None:
        source_gallery_count = event.raw_payload.get("source_gallery_count")
        total_gallery_count = event.raw_payload.get(
            "total_gallery_count",
            event.gallery_count,
        )
        connection.execute(
            """
            INSERT INTO passage_events (
                event_key, journey_id, source_node, target_node,
                source_local_track_id, route_json,
                source_gallery_count, total_gallery_count,
                published_topic, published_at, server_received_at,
                raw_payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_key,
                event.journey_id,
                event.source_node,
                event.target_node,
                event.local_track_id,
                compact_json(event.route or []),
                source_gallery_count,
                total_gallery_count,
                event.raw_topic,
                event.timestamp,
                received_at,
                compact_json(event.raw_payload),
            ),
        )

    def _insert_completion(
        self,
        connection: sqlite3.Connection,
        event: CanonicalJourneyEvent,
    ) -> None:
        connection.execute(
            """
            INSERT INTO journey_completions (
                event_key, journey_id, destination_node,
                destination_local_track_id, route_json,
                best_similarity, top2_mean, combined_score,
                total_duration_sec, previous_node,
                previous_to_destination_sec, status, completed_at,
                raw_payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_key,
                event.journey_id,
                event.source_node,
                event.local_track_id,
                compact_json(event.route or []),
                event.best_similarity,
                event.top2_mean,
                event.combined_score,
                event.total_duration_sec,
                event.previous_node,
                event.previous_to_destination_sec,
                event.status,
                event.timestamp,
                compact_json(event.raw_payload),
            ),
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _fetch_one(
        self,
        query: str,
        parameters: list[Any],
    ) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(query, parameters).fetchone()
            return dict(row) if row is not None else None
        finally:
            connection.close()

    def _fetch_all(
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
                CREATE TABLE IF NOT EXISTS journeys (
                    journey_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    route_json TEXT NOT NULL,
                    created_at TEXT,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    raw_first_payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS node_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE NOT NULL,
                    journey_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    local_track_id INTEGER NOT NULL,
                    similarity REAL,
                    match_status TEXT,
                    matched_at TEXT,
                    lost_at TEXT,
                    raw_payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS gallery_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE NOT NULL,
                    journey_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    local_track_id INTEGER,
                    sample_index INTEGER,
                    quality REAL,
                    embedding_dim INTEGER,
                    embedding_json TEXT,
                    captured_at TEXT,
                    raw_payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS passage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE NOT NULL,
                    journey_id TEXT NOT NULL,
                    source_node TEXT NOT NULL,
                    target_node TEXT,
                    source_local_track_id INTEGER,
                    route_json TEXT NOT NULL,
                    source_gallery_count INTEGER,
                    total_gallery_count INTEGER,
                    published_topic TEXT NOT NULL,
                    published_at TEXT,
                    server_received_at TEXT NOT NULL,
                    raw_payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS journey_completions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE NOT NULL,
                    journey_id TEXT NOT NULL,
                    destination_node TEXT NOT NULL,
                    destination_local_track_id INTEGER,
                    route_json TEXT NOT NULL,
                    best_similarity REAL,
                    top2_mean REAL,
                    combined_score REAL,
                    total_duration_sec REAL,
                    previous_node TEXT,
                    previous_to_destination_sec REAL,
                    status TEXT NOT NULL,
                    completed_at TEXT,
                    raw_payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS raw_mqtt_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE NOT NULL,
                    topic TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    journey_id TEXT,
                    source_node TEXT,
                    received_at TEXT NOT NULL
                );
                """
            )
            connection.commit()
        finally:
            connection.close()


def _advance_status(
    current: str,
    incoming: str,
    event_type: str = "",
) -> str:
    current_rank = STATUS_RANK.get(current, -1)
    incoming_rank = STATUS_RANK.get(incoming, -1)
    if current in {"COMPLETED", "FAILED"}:
        return current
    if (
        event_type.upper() == "TRACK_LOST"
        and current_rank < STATUS_RANK["PASSED"]
    ):
        return "PENDING"
    return incoming if incoming_rank >= current_rank else current


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
