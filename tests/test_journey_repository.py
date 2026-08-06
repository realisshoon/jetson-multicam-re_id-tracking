from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from src.server.journey_protocol import adapt_known_mqtt_payload
from src.server.journey_repository import JourneySQLiteRepository


def canonical_event(
    event_type: str,
    status: str,
    timestamp: str,
    *,
    message_id: str,
    source_node: str = "B",
    local_track_id: int | None = None,
    topic: str = "cctv/journey/event",
    **extra: Any,
):
    payload = {
        "schema_version": "1",
        "message_id": message_id,
        "event_type": event_type,
        "journey_id": "journey-1",
        "source_node": source_node,
        "local_track_id": local_track_id,
        "timestamp": timestamp,
        "status": status,
        **extra,
    }
    event = adapt_known_mqtt_payload(topic, payload)
    assert event is not None
    return event


class JourneyRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "journeys.db"
        self.repository = JourneySQLiteRepository(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_local_id_changes_and_lost_then_rematch_are_preserved(self) -> None:
        events = [
            canonical_event(
                "MATCH", "MATCHED_AT_B", "2026-08-06T01:01:00Z",
                message_id="match-10", local_track_id=10, similarity=0.91,
            ),
            canonical_event(
                "TRACK_LOST", "PENDING", "2026-08-06T01:02:00Z",
                message_id="lost-10", local_track_id=10,
            ),
            canonical_event(
                "MATCH", "MATCHED_AT_B", "2026-08-06T01:03:00Z",
                message_id="match-14", local_track_id=14, similarity=0.93,
            ),
        ]
        self.repository.store_event(events[0])
        self.repository.store_event(events[1])
        self.assertEqual(
            self.repository.fetch_journey("journey-1")["status"],
            "PENDING",
        )
        self.repository.store_event(events[2])

        matches = self.repository.fetch_node_matches("journey-1")
        self.assertEqual([row["local_track_id"] for row in matches], [10, 14])
        self.assertIsNotNone(matches[0]["lost_at"])
        self.assertEqual(
            self.repository.fetch_journey("journey-1")["status"],
            "MATCHED_AT_B",
        )

    def test_passed_status_is_not_downgraded_by_late_track_lost(self) -> None:
        self.repository.store_event(
            canonical_event(
                "PASSAGE", "PASSED", "2026-08-06T01:04:00Z",
                message_id="pass", local_track_id=14,
                topic="cctv/passage/b", target_node="D", route=["A", "B"],
            )
        )
        self.repository.store_event(
            canonical_event(
                "TRACK_LOST", "PENDING", "2026-08-06T01:05:00Z",
                message_id="late-lost", local_track_id=14,
            )
        )

        journey = self.repository.fetch_journey("journey-1")
        assert journey is not None
        self.assertEqual(journey["status"], "PASSED")

    def test_gallery_samples_have_no_three_sample_cap(self) -> None:
        for index in range(1, 5):
            self.repository.store_event(
                canonical_event(
                    "GALLERY_SAMPLE",
                    "GALLERY_COLLECTING",
                    f"2026-08-06T01:0{index}:30Z",
                    message_id=f"gallery-{index}",
                    local_track_id=14,
                    sample_index=index,
                    quality=0.8 + index / 100,
                    embedding_dim=2,
                    embedding=[0.1, 0.2],
                )
            )

        samples = self.repository.fetch_gallery_samples("journey-1")
        self.assertEqual(len(samples), 4)
        self.assertEqual([row["sample_index"] for row in samples], [1, 2, 3, 4])

    def test_passage_and_completion_keep_route_and_metrics(self) -> None:
        passage = canonical_event(
            "PASSAGE", "PASSED", "2026-08-06T01:06:00Z",
            message_id="passage", local_track_id=14,
            topic="cctv/passage/b", target_node="D", route=["A", "B"],
            gallery_count=4, source_gallery_count=3, total_gallery_count=4,
        )
        completion = canonical_event(
            "COMPLETED", "COMPLETED", "2026-08-06T01:08:00Z",
            message_id="complete", source_node="D", local_track_id=21,
            route=["A", "B", "D"], best_similarity=0.94, top2_mean=0.92,
            combined_score=0.93, total_duration_sec=420.5,
            previous_node="B", previous_to_destination_sec=120.0,
        )
        self.repository.store_event(passage)
        self.repository.store_event(completion)

        saved_passage = self.repository.fetch_passages("journey-1")[0]
        saved_completion = self.repository.fetch_completions("journey-1")[0]
        self.assertEqual(json.loads(saved_passage["route_json"]), ["A", "B"])
        self.assertEqual(saved_passage["source_gallery_count"], 3)
        self.assertEqual(json.loads(saved_completion["route_json"]), ["A", "B", "D"])
        self.assertEqual(saved_completion["combined_score"], 0.93)
        self.assertEqual(saved_completion["previous_node"], "B")
        self.assertEqual(
            self.repository.fetch_journey("journey-1")["status"],
            "COMPLETED",
        )

    def test_qos_duplicate_is_ignored_and_data_survives_restart(self) -> None:
        event = canonical_event(
            "MATCH", "MATCHED_AT_B", "2026-08-06T01:01:00Z",
            message_id="same-message", local_track_id=10,
        )
        first = self.repository.store_event(event)
        second = self.repository.store_event(event)

        reopened = JourneySQLiteRepository(self.db_path)
        self.assertEqual(first.status, "inserted")
        self.assertTrue(second.duplicate)
        self.assertEqual(len(reopened.fetch_node_matches("journey-1")), 1)
        self.assertEqual(reopened.counts()["raw_mqtt_messages"], 1)


if __name__ == "__main__":
    unittest.main()
