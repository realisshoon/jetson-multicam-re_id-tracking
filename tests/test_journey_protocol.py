from __future__ import annotations

import unittest

from src.server.journey_protocol import (
    JourneyProtocolError,
    adapt_known_mqtt_payload,
)


class JourneyProtocolTest(unittest.TestCase):
    def test_actual_entry_uses_global_person_id(self) -> None:
        payload = {
            "timestamp": "2026-08-06T01:02:03+00:00",
            "node_id": "A",
            "event": "ENTRY",
            "local_track_id": 7,
            "global_person_id": "person-a-7",
            "next_nodes": ["B", "C"],
            "reid_model": "osnet",
            "embedding_dim": 512,
            "embedding": [0.1] * 512,
        }

        event = adapt_known_mqtt_payload("cctv/entry", payload)

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.journey_id, "person-a-7")
        self.assertEqual(event.status, "CREATED")
        self.assertIsNone(event.route)
        self.assertEqual(event.raw_payload, payload)

    def test_unknown_passage_contract_remains_raw_only(self) -> None:
        self.assertIsNone(
            adapt_known_mqtt_payload(
                "cctv/passage/b",
                {"journey_id": "j-1", "unknown": True},
            )
        )

    def test_canonical_envelope_is_adapted(self) -> None:
        payload = {
            "schema_version": "1",
            "message_id": "message-1",
            "event_type": "PASSAGE",
            "journey_id": "j-1",
            "source_node": "B",
            "target_node": "D",
            "local_track_id": 14,
            "timestamp": "2026-08-06T01:03:00Z",
            "status": "PASSED",
            "route": ["A", "B"],
            "gallery_count": 3,
        }

        event = adapt_known_mqtt_payload("cctv/passage/b", payload)

        assert event is not None
        self.assertEqual(event.event_key, "message:message-1")
        self.assertEqual(event.target_node, "D")
        self.assertEqual(event.route, ["A", "B"])

    def test_event_key_is_deterministic_without_message_id(self) -> None:
        payload = {
            "timestamp": "2026-08-06T01:02:03Z",
            "node_id": "A",
            "event": "ENTRY",
            "local_track_id": 7,
            "global_person_id": "person-a-7",
            "next_nodes": ["B", "C"],
            "reid_model": "osnet",
            "embedding_dim": 512,
            "embedding": [0.1] * 512,
        }
        first = adapt_known_mqtt_payload("cctv/entry", payload)
        second = adapt_known_mqtt_payload("cctv/entry", dict(payload))
        assert first is not None and second is not None
        self.assertEqual(first.event_key, second.event_key)

    def test_embedding_dimension_mismatch_is_rejected(self) -> None:
        payload = {
            "timestamp": "2026-08-06T01:02:03Z",
            "node_id": "A",
            "event": "ENTRY",
            "local_track_id": 7,
            "global_person_id": "person-a-7",
            "next_nodes": ["B", "C"],
            "reid_model": "osnet",
            "embedding_dim": 3,
            "embedding": [0.1, 0.2],
        }
        with self.assertRaises(JourneyProtocolError):
            adapt_known_mqtt_payload("cctv/entry", payload)


if __name__ == "__main__":
    unittest.main()
