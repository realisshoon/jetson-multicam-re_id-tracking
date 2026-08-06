from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any

from src.server.camera_a_message import validate_camera_a_entry
from src.server.camera_a_sqlite_server import CameraASqliteMessageHandler
from src.server.persistence import EntryStoreResult, SQLiteEventRepository


def valid_payload() -> dict[str, Any]:
    return {
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


def encode(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


class CameraASqliteMessageHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary_directory.name) / "tracking.db"
        self.repository = SQLiteEventRepository(self.db_path)
        self.handler = CameraASqliteMessageHandler(self.repository)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_valid_json_is_persisted(self) -> None:
        result = self.handler.handle_raw("cctv/entry", encode(valid_payload()))

        self.assertEqual(result.status, "inserted")
        self.assertEqual(result.global_person_id, "G000001")

    def test_valid_json_after_invalid_json_is_processed(self) -> None:
        rejected = self.handler.handle_raw("cctv/entry", b"{not-json")
        accepted = self.handler.handle_raw(
            "cctv/entry",
            encode(valid_payload()),
        )

        self.assertEqual(rejected.status, "rejected")
        self.assertEqual(accepted.status, "inserted")

    def test_valid_payload_after_invalid_payload_is_processed(self) -> None:
        invalid = valid_payload()
        invalid["embedding"] = [0.01] * 511

        rejected = self.handler.handle_raw("cctv/entry", encode(invalid))
        accepted = self.handler.handle_raw(
            "cctv/entry",
            encode(valid_payload()),
        )

        self.assertEqual(rejected.status, "rejected")
        self.assertEqual(accepted.status, "inserted")

    def test_sqlite_error_does_not_escape_handler(self) -> None:
        class FlakyRepository:
            def __init__(self) -> None:
                self.calls = 0

            def record_camera_a_entry(
                self,
                message: dict[str, Any],
            ) -> EntryStoreResult:
                self.calls += 1
                if self.calls == 1:
                    raise sqlite3.OperationalError("temporary failure")
                normalized = validate_camera_a_entry(message)
                return EntryStoreResult(
                    status="inserted",
                    global_person_id=normalized["global_person_id"],
                    event_key="event-key",
                )

        handler = CameraASqliteMessageHandler(FlakyRepository())

        failed = handler.handle_raw("cctv/entry", encode(valid_payload()))
        recovered = handler.handle_raw("cctv/entry", encode(valid_payload()))

        self.assertEqual(failed.status, "error")
        self.assertEqual(recovered.status, "inserted")


if __name__ == "__main__":
    unittest.main()
