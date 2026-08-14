from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path

from src.server.team_a_protocol_adapter import (
    A_ENTRY_TOPIC,
    B_CANDIDATE_TOPIC,
    B_PASSAGE_TOPIC,
    D_ARRIVAL_TOPIC,
    D_CANDIDATE_TOPIC,
    TeamAProtocolError,
    adapt_team_a_payload,
    validate_team_a_candidate,
)
from src.server.team_a_protocol_repository import TeamAProtocolRepository


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "team_a"


def load_fixture(name: str) -> dict[str, object]:
    loaded = json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


class TeamAProtocolAdapterTest(unittest.TestCase):
    def test_a_entry_maps_request_id_and_keeps_capture_path_as_metadata(self) -> None:
        payload = load_fixture("a_entry.json")
        adapted = adapt_team_a_payload(A_ENTRY_TOPIC, payload)

        self.assertEqual(adapted.canonical.event_key, "message:REQ-A-20260807-0001")
        self.assertEqual(adapted.canonical.message_id, payload["request_id"])
        self.assertEqual(
            adapted.canonical.journey_id,
            "team-a-request:REQ-A-20260807-0001",
        )
        self.assertEqual(adapted.identity.local_track_id, 2)
        self.assertIsNone(adapted.identity.person_uid)
        self.assertEqual(adapted.canonical.embedding_dim, 512)
        self.assertEqual(len(adapted.canonical.embedding or []), 512)
        self.assertEqual(len(adapted.gallery_samples), 1)
        self.assertEqual(adapted.capture_path, payload["capture_path"])
        assert adapted.embedding_summary is not None
        self.assertTrue(math.isfinite(adapted.embedding_summary.l2_norm))

    def test_b_identity_prefers_person_uid_over_legacy_global_id(self) -> None:
        payload = load_fixture("b_passage.json")
        adapted = adapt_team_a_payload(B_PASSAGE_TOPIC, payload)

        self.assertEqual(adapted.identity.journey_id, "J000005")
        self.assertEqual(adapted.identity.person_uid, "P000004")
        self.assertEqual(adapted.identity.legacy_global_person_id, "J000005")
        self.assertEqual(adapted.identity.local_track_id, 7)
        self.assertEqual(adapted.canonical.route, ["A", "B"])
        self.assertEqual(len(adapted.gallery_samples), 3)

        replay = adapt_team_a_payload(B_PASSAGE_TOPIC, dict(payload))
        self.assertEqual(adapted.canonical.event_key, replay.canonical.event_key)

    def test_d_arrival_maps_completion_metrics(self) -> None:
        payload = load_fixture("d_arrival.json")
        adapted = adapt_team_a_payload(D_ARRIVAL_TOPIC, payload)

        self.assertEqual(adapted.identity.person_uid, "P000004")
        self.assertEqual(adapted.identity.legacy_global_person_id, "P000004")
        self.assertEqual(adapted.canonical.status, "COMPLETED")
        self.assertEqual(adapted.canonical.route, ["A", "B", "D"])
        self.assertEqual(adapted.canonical.combined_score, 0.865)
        self.assertEqual(adapted.canonical.total_duration_sec, 120.0)
        self.assertEqual(adapted.canonical.previous_to_destination_sec, 59.0)

    def test_all_candidate_gallery_samples_are_validated(self) -> None:
        b = validate_team_a_candidate(
            B_CANDIDATE_TOPIC,
            load_fixture("b_candidate.json"),
        )
        d = validate_team_a_candidate(
            D_CANDIDATE_TOPIC,
            load_fixture("d_candidate.json"),
        )
        self.assertEqual(b.gallery_count, 1)
        self.assertEqual(d.gallery_count, 3)
        self.assertTrue(all(summary.embedding_count == 512 for summary in d.gallery_summaries))

    def test_embedding_dimension_and_non_finite_values_are_rejected(self) -> None:
        wrong_dimension = load_fixture("a_entry.json")
        wrong_dimension["embedding_dim"] = 511
        with self.assertRaises(TeamAProtocolError):
            adapt_team_a_payload(A_ENTRY_TOPIC, wrong_dimension)

        non_finite = load_fixture("a_entry.json")
        embedding = list(non_finite["embedding"])
        embedding[0] = float("nan")
        non_finite["embedding"] = embedding
        with self.assertRaises(TeamAProtocolError):
            adapt_team_a_payload(A_ENTRY_TOPIC, non_finite)

    def test_timezone_is_required(self) -> None:
        payload = load_fixture("a_entry.json")
        payload["timestamp"] = "2026-08-07T14:00:00"
        with self.assertRaises(TeamAProtocolError):
            adapt_team_a_payload(A_ENTRY_TOPIC, payload)


class TeamAProtocolRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repository = TeamAProtocolRepository(
            Path(self.temp_dir.name) / "team_a.db"
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_a_duplicate_then_b_and_d_preserve_separate_ids(self) -> None:
        a = adapt_team_a_payload(A_ENTRY_TOPIC, load_fixture("a_entry.json"))
        b = adapt_team_a_payload(B_PASSAGE_TOPIC, load_fixture("b_passage.json"))
        d = adapt_team_a_payload(D_ARRIVAL_TOPIC, load_fixture("d_arrival.json"))

        first = self.repository.store_adapted(a)
        duplicate = self.repository.store_adapted(a)
        b_result = self.repository.store_adapted(b)
        d_result = self.repository.store_adapted(d)

        self.assertEqual(first.status, "inserted")
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(b_result.category, "passage")
        self.assertEqual(d_result.category, "completion")
        self.assertEqual(
            self.repository.counts(),
            {
                "journeys": 2,
                "node_matches": 0,
                "gallery_samples": 5,
                "passage_events": 1,
                "journey_completions": 1,
                "raw_mqtt_messages": 3,
            },
        )
        external = self.repository.fetch_journey("J000005")
        assert external is not None
        self.assertEqual(external["status"], "COMPLETED")
        self.assertEqual(json.loads(external["route_json"]), ["A", "B", "D"])
        metadata = self.repository.team_a_metadata()
        self.assertEqual(len(metadata), 3)
        self.assertEqual(metadata[1]["person_uid"], "P000004")
        self.assertEqual(metadata[1]["legacy_global_person_id"], "J000005")
        self.assertEqual(metadata[2]["legacy_global_person_id"], "P000004")


if __name__ == "__main__":
    unittest.main()
