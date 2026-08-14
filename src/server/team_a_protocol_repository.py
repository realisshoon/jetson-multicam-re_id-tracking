from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.server.journey_protocol import compact_json
from src.server.journey_repository import JourneySQLiteRepository
from src.server.team_a_protocol_adapter import TeamAAdaptedEvent


@dataclass(frozen=True)
class TeamAStoreResult:
    status: str
    event_key: str
    journey_id: str
    category: str | None
    gallery_inserted: int = 0

    @property
    def duplicate(self) -> bool:
        return self.status == "duplicate"


class TeamAProtocolRepository(JourneySQLiteRepository):
    """Add Team-A identity metadata without changing the base repository."""

    def __init__(self, db_path: str | Path) -> None:
        super().__init__(db_path)
        self._initialize_team_a_schema()

    def store_adapted(
        self,
        adapted: TeamAAdaptedEvent,
        *,
        received_at: str | None = None,
    ) -> TeamAStoreResult:
        event = adapted.canonical
        received_at = received_at or datetime.now(timezone.utc).isoformat()
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
                return TeamAStoreResult(
                    status="duplicate",
                    event_key=event.event_key,
                    journey_id=event.journey_id,
                    category=None,
                )

            self._upsert_journey(connection, event, received_at)
            category = self._store_category(connection, event, received_at)
            gallery_inserted = 0
            for sample in adapted.gallery_samples:
                cursor = connection.execute(
                    """
                    INSERT INTO gallery_samples (
                        event_key, journey_id, node_id, local_track_id,
                        sample_index, quality, embedding_dim, embedding_json,
                        captured_at, raw_payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(event_key) DO NOTHING
                    """,
                    (
                        sample.event_key,
                        sample.journey_id,
                        sample.source_node,
                        sample.local_track_id,
                        sample.sample_index,
                        sample.quality,
                        sample.embedding_dim,
                        compact_json(sample.embedding),
                        sample.timestamp,
                        compact_json(sample.raw_payload),
                    ),
                )
                gallery_inserted += cursor.rowcount

            identity = adapted.identity
            connection.execute(
                """
                INSERT INTO team_a_event_metadata (
                    event_key, request_id, journey_id, person_uid,
                    legacy_global_person_id, local_track_id, source_node,
                    event_type, event_timestamp, capture_path, raw_topic,
                    gallery_received_count, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_key,
                    adapted.request_id,
                    identity.journey_id,
                    identity.person_uid,
                    identity.legacy_global_person_id,
                    identity.local_track_id,
                    event.source_node,
                    event.event_type,
                    event.timestamp,
                    adapted.capture_path,
                    event.raw_topic,
                    len(adapted.gallery_samples),
                    received_at,
                ),
            )
            connection.commit()
            return TeamAStoreResult(
                status="inserted",
                event_key=event.event_key,
                journey_id=event.journey_id,
                category=category,
                gallery_inserted=gallery_inserted,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def team_a_metadata(self) -> list[dict[str, object]]:
        return self._fetch_all(
            "SELECT * FROM team_a_event_metadata ORDER BY id",
            [],
        )

    def _initialize_team_a_schema(self) -> None:
        connection = self._connect()
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS team_a_event_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT UNIQUE NOT NULL,
                    request_id TEXT,
                    journey_id TEXT NOT NULL,
                    person_uid TEXT,
                    legacy_global_person_id TEXT,
                    local_track_id INTEGER NOT NULL,
                    source_node TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    capture_path TEXT,
                    raw_topic TEXT NOT NULL,
                    gallery_received_count INTEGER NOT NULL,
                    received_at TEXT NOT NULL
                )
                """
            )
            connection.commit()
        finally:
            connection.close()
