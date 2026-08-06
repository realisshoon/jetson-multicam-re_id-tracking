from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.server.journey_repository import JourneySQLiteRepository
from src.server.journey_sqlite_server import JourneyMessageProcessor, decode_object


class JourneyMessageProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repository = JourneySQLiteRepository(
            Path(self.temp_dir.name) / "server.db"
        )
        self.processor = JourneyMessageProcessor(self.repository)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def entry_body(journey_id: str, timestamp: str) -> bytes:
        import json

        return json.dumps(
            {
                "timestamp": timestamp,
                "node_id": "A",
                "event": "ENTRY",
                "local_track_id": 1,
                "global_person_id": journey_id,
                "next_nodes": ["B", "C"],
                "reid_model": "osnet",
                "embedding_dim": 512,
                "embedding": [0.1] * 512,
            }
        ).encode()

    def test_invalid_json_is_raw_stored_then_valid_message_is_processed(self) -> None:
        invalid = self.processor.process("cctv/passage/b", b"not-json")
        valid = self.processor.process(
            "cctv/entry",
            self.entry_body("journey-1", "2026-08-06T01:00:00Z"),
        )

        self.assertEqual(invalid.status, "raw_stored")
        self.assertEqual(valid.status, "inserted")
        self.assertIsNotNone(self.repository.fetch_journey("journey-1"))
        self.assertEqual(self.repository.counts()["raw_mqtt_messages"], 2)

    def test_unknown_b_payload_is_not_promoted_to_guessed_schema(self) -> None:
        result = self.processor.process(
            "cctv/passage/b",
            b'{"journey_id":"journey-1","node_id":"B","mystery":7}',
        )
        self.assertEqual(result.status, "raw_stored")
        self.assertIsNone(self.repository.fetch_journey("journey-1"))
        self.assertEqual(self.repository.counts()["passage_events"], 0)

    def test_invalid_known_contract_is_raw_stored_without_stopping(self) -> None:
        invalid = self.processor.process(
            "cctv/entry",
            b'{"timestamp":"bad","node_id":"A","event":"ENTRY",'
            b'"global_person_id":"journey-bad"}',
        )
        valid = self.processor.process(
            "cctv/entry",
            self.entry_body("journey-good", "2026-08-06T01:00:00Z"),
        )
        self.assertEqual(invalid.status, "raw_stored")
        self.assertEqual(valid.status, "inserted")
        self.assertIsNone(self.repository.fetch_journey("journey-bad"))
        self.assertIsNotNone(self.repository.fetch_journey("journey-good"))

    def test_replayed_raw_message_is_deduplicated(self) -> None:
        body = b'{"unpublished_contract":true}'
        first = self.processor.process("cctv/passage/b", body)
        second = self.processor.process("cctv/passage/b", body)
        self.assertEqual(first.status, "raw_stored")
        self.assertTrue(second.duplicate)

    def test_non_finite_json_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            decode_object(b'{"score":NaN}')


if __name__ == "__main__":
    unittest.main()
