from __future__ import annotations

import json
import unittest
from datetime import datetime
from unittest.mock import patch

import numpy as np
import paho.mqtt.client as mqtt

from src.nodes import node_d


class PublishInfo:
    def __init__(
        self,
        rc: int = mqtt.MQTT_ERR_SUCCESS,
        mid: int = 1,
        published: bool = True,
    ) -> None:
        self.rc = rc
        self.mid = mid
        self.published = published

    def wait_for_publish(self, timeout: float | None = None) -> None:
        return None

    def is_published(self) -> bool:
        return self.published


class FakeMqttClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | bytes, int, bool]] = []

    def publish(
        self,
        topic: str,
        payload: str | bytes,
        qos: int,
        retain: bool,
    ) -> PublishInfo:
        self.calls.append((topic, payload, qos, retain))
        return PublishInfo()


class NodeDTimingTest(unittest.TestCase):
    def setUp(self) -> None:
        node_d.timing_sessions.clear()
        node_d.timing_sent.clear()
        node_d.candidates.clear()
        node_d.completed_journey_ids.clear()
        node_d.terminal_journey_ids.clear()
        node_d.consumed_track_ids.clear()
        node_d.completed_tracks.clear()
        node_d.arrival_inflight.clear()

    def test_existing_settings_are_unchanged(self) -> None:
        self.assertEqual(node_d.CANDIDATE_TOPIC, "cctv/candidates/d")
        self.assertEqual(
            node_d.JOURNEY_CONTROL_TOPIC,
            "cctv/control/d/journey",
        )
        self.assertEqual(node_d.ARRIVAL_TOPIC, "cctv/events/d/arrival")
        self.assertEqual(node_d.TIMING_TOPIC, "cctv/events/d/timing")
        self.assertEqual(node_d.MQTT_CLIENT_ID, "camera-d")
        self.assertEqual(node_d.CAMERA_SOURCE, "/dev/video0")
        self.assertEqual(node_d.WEB_PORT, 8003)
        self.assertEqual(node_d.MATCH_BEST_THRESHOLD, 0.70)
        self.assertEqual(node_d.MATCH_TOP2_THRESHOLD, 0.62)
        self.assertEqual(node_d.MATCH_MARGIN, 0.04)
        self.assertEqual(node_d.MATCH_CONFIRMATIONS, 4)
        self.assertEqual(
            node_d.MATCHING_CONFIG.confirmation_window_size,
            5,
        )
        self.assertEqual(
            node_d.MATCHING_CONFIG.confirmation_min_sample_interval_seconds,
            0.5,
        )
        self.assertEqual(node_d.VERIFY_THRESHOLD, 0.55)

    def test_timing_requires_arrival_session_and_publishes_once(self) -> None:
        client = FakeMqttClient()

        self.assertFalse(
            node_d.publish_timing_on_track_lost(
                client,
                "J000075",
                177,
                1008.421,
            )
        )
        self.assertEqual(client.calls, [])

        node_d.register_timing_match(
            journey_id="J000075",
            person_uid="P000006",
            local_id=177,
            entered_epoch=1000.0,
            matched_epoch=1003.0,
        )

        self.assertTrue(
            node_d.publish_timing_on_track_lost(
                client,
                "J000075",
                177,
                1008.421,
            )
        )
        self.assertFalse(
            node_d.publish_timing_on_track_lost(
                client,
                "J000075",
                177,
                1009.0,
            )
        )
        self.assertEqual(len(client.calls), 1)

        topic, raw_payload, qos, retain = client.calls[0]
        payload = json.loads(raw_payload)
        self.assertEqual(topic, "cctv/events/d/timing")
        self.assertEqual(qos, node_d.MQTT_QOS)
        self.assertFalse(retain)
        self.assertEqual(
            set(payload),
            {
                "schema_version",
                "event",
                "node_id",
                "person_uid",
                "global_person_id",
                "journey_id",
                "local_track_id",
                "entered_at",
                "matched_at",
                "exited_at",
                "dwell_seconds",
                "exit_reason",
            },
        )
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["event"], "NODE_TIMING")
        self.assertEqual(payload["node_id"], "D")
        self.assertEqual(payload["person_uid"], "P000006")
        self.assertEqual(payload["global_person_id"], payload["person_uid"])
        self.assertEqual(payload["journey_id"], "J000075")
        self.assertEqual(payload["local_track_id"], 177)
        self.assertLess(
            node_d.parse_time(payload["entered_at"]),
            node_d.parse_time(payload["exited_at"]),
        )
        self.assertEqual(payload["dwell_seconds"], 8.421)
        self.assertEqual(payload["exit_reason"], "TRACK_LOST")

    def test_journey_session_preserves_earliest_times_across_track_ids(self) -> None:
        client = FakeMqttClient()
        node_d.register_timing_match(
            "J000075", "P000006", 177, 1000.0, 1003.0
        )
        node_d.register_timing_match(
            "J000075", "P000006", 188, 999.0, 1004.0
        )

        self.assertFalse(
            node_d.publish_timing_on_track_lost(
                client, "J000075", 177, 1007.0
            )
        )
        self.assertEqual(client.calls, [])
        self.assertTrue(
            node_d.publish_timing_on_track_lost(
                client, "J000075", 188, 1010.0
            )
        )

        payload = json.loads(client.calls[0][1])
        self.assertEqual(payload["entered_at"], node_d.epoch_iso(999.0))
        self.assertEqual(payload["matched_at"], node_d.epoch_iso(1003.0))
        self.assertEqual(payload["local_track_id"], 188)
        self.assertEqual(payload["dwell_seconds"], 11.0)

    @patch.object(node_d, "append_arrival_tx_jsonl")
    @patch.object(node_d, "append_csv")
    @patch.object(node_d, "save_arrival_capture", return_value="capture.jpg")
    def test_existing_arrival_payload_and_topic(
        self,
        _save_capture,
        _append_csv,
        _append_arrival_tx_jsonl,
    ) -> None:
        embedding = np.ones(512, dtype=np.float32)
        embedding /= np.linalg.norm(embedding)
        node_d.candidates["J000075"] = node_d.Candidate(
            journey_id="J000075",
            person_uid="P000006",
            received_at="2026-01-01T00:00:00+00:00",
            entry_timestamp="2026-01-01T00:00:00+00:00",
            entry_epoch=1767225600.0,
            b_passage_timestamp="2026-01-01T00:00:05+00:00",
            b_passage_epoch=1767225605.0,
            route=["A", "B"],
            gallery=[embedding],
            gallery_nodes=["A"],
        )
        client = FakeMqttClient()
        diagnostics = node_d.ArrivalDiagnostics(
            track_first_seen_at=datetime.fromisoformat(
                "2026-01-01T00:00:06+00:00"
            ),
            candidate_received_at=datetime.fromisoformat(
                "2026-01-01T00:00:05+00:00"
            ),
            passage_at=datetime.fromisoformat(
                "2026-01-01T00:00:05+00:00"
            ),
            arrival_at=datetime.fromisoformat(
                "2026-01-01T00:00:10+00:00"
            ),
            confirmation_sample_count=5,
            confirmation_pass_count=4,
            best_journey_score=0.78,
            second_journey_score=0.70,
            journey_margin=0.08,
            eligibility_reason="ELIGIBLE_NEW_ENTRY",
        )

        self.assertTrue(
            node_d.complete_arrival(
                client=client,
                journey_id="J000075",
                local_id=177,
                best=0.8,
                top2=0.75,
                combined=0.78,
                d_embedding=embedding,
                capture_crop=np.zeros((8, 8, 3), dtype=np.uint8),
                capture_quality=0.9,
                diagnostics=diagnostics,
            )
        )

        self.assertEqual(len(client.calls), 1)
        topic, raw_payload, qos, retain = client.calls[0]
        payload = json.loads(raw_payload)
        self.assertEqual(topic, "cctv/events/d/arrival")
        self.assertEqual(qos, node_d.MQTT_QOS)
        self.assertFalse(retain)
        self.assertEqual(
            set(payload),
            {
                "schema_version",
                "event",
                "arrival_event_id",
                "journey_id",
                "person_uid",
                "global_person_id",
                "node_id",
                "current_node",
                "route",
                "entry_timestamp",
                "passage_timestamp",
                "d_arrival_timestamp",
                "total_duration_seconds",
                "passage_to_d_duration_seconds",
                "d_local_track_id",
                "gallery_count",
                "best_similarity",
                "top2_mean",
                "combined_score",
                "embedding_dim",
                "embedding",
                "quality",
                "quality_source",
                "match",
                "local_track_id",
                "capture_path",
                "similarity",
                "capture_quality",
                "verification_status",
                "status",
                "d_track_first_seen_at",
                "candidate_received_at",
                "confirmation_sample_count",
                "confirmation_pass_count",
                "best_journey_score",
                "second_journey_score",
                "journey_margin",
                "eligibility_reason",
            },
        )
        self.assertEqual(payload["event"], "ARRIVAL")
        self.assertEqual(
            payload["arrival_event_id"],
            node_d.make_arrival_event_id(
                "J000075",
                "2026-01-01T00:00:05+00:00",
            ),
        )
        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["journey_id"], "J000075")
        self.assertEqual(payload["person_uid"], "P000006")
        self.assertEqual(payload["global_person_id"], "P000006")
        self.assertEqual(payload["node_id"], "D")
        self.assertEqual(payload["current_node"], "D")
        self.assertEqual(payload["d_local_track_id"], 177)
        self.assertEqual(payload["local_track_id"], 177)
        self.assertEqual(payload["confirmation_sample_count"], 5)
        self.assertEqual(payload["confirmation_pass_count"], 4)
        self.assertEqual(payload["best_journey_score"], 0.78)
        self.assertEqual(payload["second_journey_score"], 0.70)
        self.assertEqual(payload["journey_margin"], 0.08)
        self.assertEqual(payload["eligibility_reason"], "ELIGIBLE_NEW_ENTRY")
        self.assertNotIn("J000075", node_d.candidates)
        self.assertIn("J000075", node_d.completed_journey_ids)
        self.assertIn(177, node_d.consumed_track_ids)


if __name__ == "__main__":
    unittest.main()
