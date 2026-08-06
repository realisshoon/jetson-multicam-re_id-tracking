from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any

from src.server.camera_a_message import validate_camera_a_entry
from src.server.persistence import SQLiteEventRepository


def valid_payload() -> dict[str, Any]:
    return validate_camera_a_entry(
        {
            "timestamp": "2026-08-06T11:00:00+09:00",
            "node_id": "A",
            "event": "ENTRY",
            "local_track_id": 1,
            "global_person_id": "G000001",
            "next_nodes": ["B", "C"],
            "reid_model": "osnet_x0_25",
            "embedding_dim": 512,
            "embedding": [0.01] * 512,
        }
    )


class SQLiteEventRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary_directory.name) / "tracking.db"
        self.repository = SQLiteEventRepository(self.db_path)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_person_insert_succeeds(self) -> None:
        result = self.repository.record_camera_a_entry(valid_payload())

        self.assertTrue(result.inserted)
        self.assertEqual(len(self.repository.fetch_persons()), 1)

    def test_tracking_event_insert_succeeds(self) -> None:
        result = self.repository.record_camera_a_entry(valid_payload())

        self.assertTrue(result.inserted)
        self.assertEqual(len(self.repository.fetch_tracking_events()), 1)

    def test_global_id_is_stored_in_persons(self) -> None:
        self.repository.record_camera_a_entry(valid_payload())

        self.assertEqual(
            self.repository.fetch_persons()[0]["global_person_id"],
            "G000001",
        )

    def test_event_is_stored_in_tracking_events(self) -> None:
        self.repository.record_camera_a_entry(valid_payload())
        event = self.repository.fetch_tracking_events()[0]

        self.assertEqual(event["event_type"], "ENTRY")
        self.assertEqual(event["node_id"], "A")

    def test_embedding_dim_512_is_stored(self) -> None:
        self.repository.record_camera_a_entry(valid_payload())

        self.assertEqual(
            self.repository.fetch_tracking_events()[0]["embedding_dim"],
            512,
        )

    def test_embedding_json_restores_512_values(self) -> None:
        self.repository.record_camera_a_entry(valid_payload())
        stored = self.repository.fetch_tracking_events()[0]

        self.assertEqual(len(json.loads(stored["embedding_json"])), 512)

    def test_same_event_is_not_inserted_twice(self) -> None:
        first = self.repository.record_camera_a_entry(valid_payload())
        second = self.repository.record_camera_a_entry(valid_payload())

        self.assertTrue(first.inserted)
        self.assertTrue(second.duplicate)

    def test_duplicate_does_not_increase_counts(self) -> None:
        self.repository.record_camera_a_entry(valid_payload())
        self.repository.record_camera_a_entry(valid_payload())

        summary = self.repository.integrity_summary()
        self.assertEqual(summary["persons"], 1)
        self.assertEqual(summary["tracking_events"], 1)

    def test_data_survives_repository_recreation(self) -> None:
        self.repository.record_camera_a_entry(valid_payload())

        reopened = SQLiteEventRepository(self.db_path)

        self.assertEqual(reopened.integrity_summary()["persons"], 1)
        self.assertEqual(reopened.integrity_summary()["tracking_events"], 1)

    def test_foreign_keys_are_enabled(self) -> None:
        self.assertTrue(self.repository.foreign_keys_enabled())

    def test_transaction_rolls_back_on_event_insert_error(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.executescript(
                """
                CREATE TRIGGER force_tracking_event_failure
                BEFORE INSERT ON tracking_events
                BEGIN
                    SELECT RAISE(ABORT, 'forced failure');
                END;
                """
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaises(sqlite3.IntegrityError):
            self.repository.record_camera_a_entry(valid_payload())

        self.assertEqual(self.repository.integrity_summary()["persons"], 0)
        self.assertEqual(
            self.repository.integrity_summary()["tracking_events"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
