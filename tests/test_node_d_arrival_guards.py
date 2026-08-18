from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import numpy as np
import paho.mqtt.client as mqtt

from src.common.node_d_matching import (
    MatchConfirmation,
    TrackEligibility,
    add_confirmation_sample,
    temporal_rejection_reason,
    update_track_entry,
)
from src.nodes import node_d


class PublishInfo:
    rc = mqtt.MQTT_ERR_SUCCESS
    mid = 1

    def wait_for_publish(self, timeout=None):
        return None

    def is_published(self):
        return True


class FakeMqttClient:
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def publish(self, topic, payload, qos, retain):
        self.payloads.append(json.loads(payload))
        return PublishInfo()


def unit_embedding(index: int = 0) -> np.ndarray:
    embedding = np.zeros(512, dtype=np.float32)
    embedding[index] = 1.0
    return embedding


def add_candidate(
    journey_id: str,
    passage_at: datetime,
    embedding: np.ndarray | None = None,
) -> node_d.Candidate:
    reference = embedding if embedding is not None else unit_embedding()
    candidate = node_d.Candidate(
        journey_id=journey_id,
        person_uid=f"P{journey_id[-6:]}",
        received_at=(passage_at + timedelta(milliseconds=100)).isoformat(),
        entry_timestamp=(passage_at - timedelta(seconds=5)).isoformat(),
        entry_epoch=(passage_at - timedelta(seconds=5)).timestamp(),
        b_passage_timestamp=passage_at.isoformat(),
        b_passage_epoch=passage_at.timestamp(),
        route=["A", "B"],
        gallery=[reference.copy(), reference.copy()],
        gallery_nodes=["A", "B"],
    )
    node_d.candidates[journey_id] = candidate
    return candidate


def crossed_track(local_id: int, first_seen_at: datetime) -> TrackEligibility:
    state = TrackEligibility(local_id, first_seen_at)
    update_track_entry(
        state,
        (0, 160, 64, 400),
        640,
        480,
        first_seen_at,
        node_d.MATCHING_CONFIG,
    )
    update_track_entry(
        state,
        (180, 120, 420, 420),
        640,
        480,
        first_seen_at + timedelta(seconds=1),
        node_d.MATCHING_CONFIG,
    )
    return state


class NodeDArrivalGuardsTest(unittest.TestCase):
    def setUp(self) -> None:
        node_d.candidates.clear()
        node_d.completed_journey_ids.clear()
        node_d.terminal_journey_ids.clear()
        node_d.consumed_track_ids.clear()
        node_d.completed_tracks.clear()
        node_d.arrival_inflight.clear()
        node_d.expired_journey_count = 0
        self.base = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)

    def test_normal_a_b_d_track_is_eligible_and_ranked(self) -> None:
        passage = self.base
        add_candidate("J000001", passage)
        track = crossed_track(10, passage + timedelta(seconds=2))

        ranked, rejected = node_d.rank_eligible_journeys(
            unit_embedding(),
            track,
            passage + timedelta(seconds=5),
        )

        self.assertEqual(rejected, [])
        self.assertEqual([score.journey_id for score in ranked], ["J000001"])
        self.assertAlmostEqual(ranked[0].combined, 1.0)

    def test_person_only_at_d_has_no_waiting_journey(self) -> None:
        track = crossed_track(11, self.base)
        ranked, rejected = node_d.rank_eligible_journeys(
            unit_embedding(),
            track,
            self.base + timedelta(seconds=5),
        )
        self.assertEqual(ranked, [])
        self.assertEqual(rejected, [])

    def test_track_seen_before_passage_is_preexisting(self) -> None:
        passage = self.base + timedelta(seconds=20)
        add_candidate("J000002", passage)
        track = crossed_track(12, self.base)

        ranked, rejected = node_d.rank_eligible_journeys(
            unit_embedding(),
            track,
            passage + timedelta(seconds=5),
        )

        self.assertEqual(ranked, [])
        self.assertEqual(rejected[0][1], "PREEXISTING_TRACK")

    def test_new_track_id_created_inside_is_not_eligible(self) -> None:
        passage = self.base
        track = TrackEligibility(13, passage + timedelta(seconds=2))
        update_track_entry(
            track,
            (180, 120, 420, 420),
            640,
            480,
            passage + timedelta(seconds=2),
            node_d.MATCHING_CONFIG,
        )

        reason, _ = temporal_rejection_reason(
            track,
            passage,
            passage + timedelta(seconds=5),
            node_d.MATCHING_CONFIG,
        )

        self.assertFalse(track.crossed_entry_boundary)
        self.assertEqual(reason, "PREEXISTING_TRACK")

    def test_completed_person_reissued_inside_cannot_match_new_journey(self) -> None:
        node_d.completed_journey_ids.add("J000100")
        node_d.consumed_track_ids.add(100)
        add_candidate("J000101", self.base)
        reissued = TrackEligibility(101, self.base + timedelta(seconds=2))
        update_track_entry(
            reissued,
            (180, 120, 420, 420),
            640,
            480,
            self.base + timedelta(seconds=2),
            node_d.MATCHING_CONFIG,
        )

        ranked, rejected = node_d.rank_eligible_journeys(
            unit_embedding(),
            reissued,
            self.base + timedelta(seconds=5),
        )

        self.assertEqual(ranked, [])
        self.assertEqual(rejected[0][1], "PREEXISTING_TRACK")

    def test_too_early_duration_is_rejected(self) -> None:
        passage = self.base
        track = crossed_track(14, passage)
        reason, duration = temporal_rejection_reason(
            track,
            passage,
            passage + timedelta(milliseconds=500),
            node_d.MATCHING_CONFIG,
        )
        self.assertEqual(duration, 0.5)
        self.assertEqual(reason, "TOO_EARLY")

    def test_single_high_sample_does_not_confirm(self) -> None:
        state = MatchConfirmation()
        result = add_confirmation_sample(
            state,
            "J000003",
            self.base,
            True,
            0.75,
            node_d.MATCHING_CONFIG,
        )
        self.assertFalse(result.confirmed)
        self.assertEqual(result.sample_count, 1)
        self.assertEqual(result.pass_count, 1)

    def test_first_high_then_score_collapse_resets_confirmation(self) -> None:
        state = MatchConfirmation()
        add_confirmation_sample(
            state, "J000004", self.base, True, 0.705,
            node_d.MATCHING_CONFIG,
        )
        result = add_confirmation_sample(
            state,
            "J000004",
            self.base + timedelta(seconds=1),
            False,
            0.522,
            node_d.MATCHING_CONFIG,
        )
        self.assertFalse(result.confirmed)
        self.assertEqual(result.sample_count, 0)
        self.assertEqual(result.pass_count, 0)
        self.assertEqual(result.reset_reason, "SCORE_DROP")

    def test_confirmation_requires_independent_n_of_k_samples(self) -> None:
        state = MatchConfirmation()
        results = []
        for index, passed in enumerate([True, True, False, True, True]):
            results.append(
                add_confirmation_sample(
                    state,
                    "J000005",
                    self.base + timedelta(seconds=index),
                    passed,
                    0.74 - index * 0.005,
                    node_d.MATCHING_CONFIG,
                )
            )
        self.assertFalse(any(result.confirmed for result in results[:-1]))
        self.assertTrue(results[-1].confirmed)
        self.assertEqual(results[-1].pass_count, 4)

    def test_similar_waiting_journeys_have_insufficient_margin(self) -> None:
        passage = self.base
        first = unit_embedding(0)
        second = unit_embedding(0)
        second[0] = 0.99
        second[1] = 0.141067
        second /= np.linalg.norm(second)
        add_candidate("J000006", passage, first)
        add_candidate("J000007", passage, second)
        track = crossed_track(15, passage + timedelta(seconds=2))

        ranked, _ = node_d.rank_eligible_journeys(
            first,
            track,
            passage + timedelta(seconds=5),
        )
        margin = ranked[0].combined - ranked[1].combined

        self.assertEqual(len(ranked), 2)
        self.assertLess(margin, node_d.MATCHING_CONFIG.min_journey_margin)

    def test_completed_track_cannot_publish_second_arrival(self) -> None:
        passage = self.base
        add_candidate("J000008", passage)
        diagnostics = node_d.ArrivalDiagnostics(
            track_first_seen_at=passage + timedelta(seconds=2),
            candidate_received_at=passage + timedelta(milliseconds=100),
            passage_at=passage,
            arrival_at=passage + timedelta(seconds=5),
            confirmation_sample_count=5,
            confirmation_pass_count=5,
            best_journey_score=1.0,
            second_journey_score=0.0,
            journey_margin=1.0,
            eligibility_reason="ELIGIBLE_NEW_ENTRY",
        )
        client = FakeMqttClient()

        with patch.object(
            node_d,
            "save_arrival_capture",
            return_value="capture.jpg",
        ), patch.object(node_d, "append_csv"), patch.object(
            node_d,
            "append_arrival_tx_jsonl",
        ):
            first = node_d.complete_arrival(
                client, "J000008", 16, 1.0, 1.0, 1.0,
                unit_embedding(), np.zeros((8, 8, 3), dtype=np.uint8),
                0.9, diagnostics,
            )
            add_candidate("J000009", passage + timedelta(seconds=6))
            second = node_d.complete_arrival(
                client, "J000009", 16, 1.0, 1.0, 1.0,
                unit_embedding(), np.zeros((8, 8, 3), dtype=np.uint8),
                0.9, diagnostics,
            )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(client.payloads), 1)

    def test_expired_journey_is_removed_immediately(self) -> None:
        add_candidate("J000010", self.base)
        expired = node_d.cleanup_candidates(
            self.base
            + timedelta(seconds=node_d.CANDIDATE_TIMEOUT_SECONDS + 1)
        )
        self.assertEqual([item.journey_id for item in expired], ["J000010"])
        self.assertNotIn("J000010", node_d.candidates)
        self.assertIn("J000010", node_d.terminal_journey_ids)

    def test_cancelled_journey_message_removes_candidate(self) -> None:
        add_candidate("J000011", self.base)

        class Message:
            topic = node_d.CANDIDATE_TOPIC
            payload = json.dumps(
                {
                    "event": "CANDIDATE",
                    "journey_id": "J000011",
                    "stage": "CANCELLED",
                }
            ).encode("utf-8")

        with patch.object(node_d, "append_candidate_diagnostic"):
            node_d.on_message(None, None, Message())

        self.assertNotIn("J000011", node_d.candidates)
        self.assertIn("J000011", node_d.terminal_journey_ids)


if __name__ == "__main__":
    unittest.main()
