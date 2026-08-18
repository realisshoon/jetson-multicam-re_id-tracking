from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np
import paho.mqtt.client as mqtt

from src.common.node_d_matching import TrackEligibility, update_track_entry
from src.nodes import node_d


def unit_embedding() -> list[float]:
    embedding = np.zeros(512, dtype=np.float32)
    embedding[0] = 1.0
    return embedding.tolist()


def candidate_payload(
    journey_id: str,
    passage_at: datetime,
    *,
    rebuild: bool = False,
) -> dict:
    return {
        "event": "CANDIDATE",
        "stage": "WAITING_D",
        "journey_id": journey_id,
        "person_uid": f"P{journey_id[-6:]}",
        "entry_timestamp": (passage_at - timedelta(seconds=5)).isoformat(),
        "passage_timestamp": passage_at.isoformat(),
        "route": ["A", "C"],
        "gallery": [
            {"node_id": "A", "embedding": unit_embedding()},
            {"node_id": "C", "embedding": unit_embedding()},
        ],
        "rebuild": rebuild,
    }


class FixtureMessage:
    def __init__(
        self,
        topic: str,
        payload: dict,
        *,
        qos: int = 0,
        retain: bool = False,
        dup: bool = False,
    ) -> None:
        self.topic = topic
        self.payload = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.qos = qos
        self.retain = retain
        self.dup = dup


class FixtureSubscribeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def subscribe(self, topic: str, qos: int):
        self.calls.append((topic, qos))
        return mqtt.MQTT_ERR_SUCCESS, 100 + len(self.calls)


class FixtureQos0Broker:
    """A non-retained QoS 0 fixture: disconnected subscribers miss messages."""

    def __init__(self) -> None:
        self.connected = False

    def publish(self, message: FixtureMessage) -> bool:
        if not self.connected:
            return False
        node_d.on_message(None, None, message)
        return True


class NodeDCandidateDiagnosticsTest(unittest.TestCase):
    def setUp(self) -> None:
        node_d.candidates.clear()
        node_d.completed_journey_ids.clear()
        node_d.terminal_journey_ids.clear()
        node_d.consumed_track_ids.clear()
        node_d.completed_tracks.clear()
        node_d.arrival_inflight.clear()
        node_d.pending_subscriptions.clear()
        node_d.track_reid_diagnostics.clear()
        self.base = datetime(2026, 8, 14, 2, 16, 10, tzinfo=timezone.utc)

    @staticmethod
    def read_records(log_dir: str) -> list[dict]:
        path = Path(log_dir) / node_d.CANDIDATE_RX_DIAGNOSTICS_NAME
        if not path.exists():
            return []
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]

    def test_connect_suback_logs_topics_and_granted_qos(self) -> None:
        client = FixtureSubscribeClient()
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            node_d,
            "LOG_DIR",
            Path(temp_dir),
        ):
            node_d.on_connect(
                client,
                None,
                {"session present": False},
                0,
                None,
            )
            node_d.on_subscribe(client, None, 101, [1], None)
            node_d.on_subscribe(client, None, 102, [1], None)
            records = self.read_records(temp_dir)

        self.assertEqual(
            client.calls,
            [
                (node_d.CANDIDATE_TOPIC, node_d.MQTT_QOS),
                (node_d.JOURNEY_CONTROL_TOPIC, node_d.MQTT_QOS),
            ],
        )
        connect = [item for item in records if item["record_type"] == "mqtt_connect"]
        subacks = [item for item in records if item["record_type"] == "mqtt_suback"]
        self.assertTrue(connect[0]["connected"])
        self.assertEqual(
            [item["topic"] for item in subacks],
            [node_d.CANDIDATE_TOPIC, node_d.JOURNEY_CONTROL_TOPIC],
        )
        self.assertEqual([item["granted_qos"] for item in subacks], [[1], [1]])
        self.assertTrue(all(item["accepted"] for item in subacks))

    def test_candidate_matches_before_expired_control_and_logs_track_once(self) -> None:
        message = FixtureMessage(
            node_d.CANDIDATE_TOPIC,
            candidate_payload("J000040", self.base),
        )
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            node_d,
            "LOG_DIR",
            Path(temp_dir),
        ), patch.object(node_d, "append_csv"):
            node_d.on_message(None, None, message)
            self.assertIn("J000040", node_d.candidates)

            first_seen = self.base + timedelta(seconds=2)
            track = TrackEligibility(40, first_seen)
            update_track_entry(
                track,
                (0, 160, 64, 400),
                640,
                480,
                first_seen,
                node_d.MATCHING_CONFIG,
            )
            update_track_entry(
                track,
                (180, 120, 420, 420),
                640,
                480,
                first_seen + timedelta(seconds=1),
                node_d.MATCHING_CONFIG,
            )
            ranked, rejected = node_d.rank_eligible_journeys(
                np.asarray(unit_embedding(), dtype=np.float32),
                track,
                self.base + timedelta(seconds=5),
            )
            self.assertEqual(rejected, [])
            self.assertEqual([item.journey_id for item in ranked], ["J000040"])

            winner = ranked[0]
            node_d.update_track_reid_diagnostic(
                journey_id=winner.journey_id,
                local_track_id=40,
                evaluated_at=self.base + timedelta(seconds=5),
                best_similarity=winner.best,
                top2_mean=winner.top2,
                combined_score=winner.combined,
                matched=True,
            )
            node_d.update_track_reid_diagnostic(
                journey_id=winner.journey_id,
                local_track_id=40,
                evaluated_at=self.base + timedelta(seconds=6),
                best_similarity=winner.best,
                top2_mean=winner.top2,
                combined_score=winner.combined,
                matched=True,
            )
            self.assertEqual(
                node_d.flush_track_reid_diagnostics(40, "FIXTURE_MATCHED"),
                1,
            )
            self.assertEqual(
                node_d.flush_track_reid_diagnostics(40, "FIXTURE_MATCHED"),
                0,
            )

            control = FixtureMessage(
                node_d.JOURNEY_CONTROL_TOPIC,
                {"journey_id": "J000040", "action": "EXPIRED"},
            )
            node_d.on_message(None, None, control)
            self.assertNotIn("J000040", node_d.candidates)
            records = self.read_records(temp_dir)

        candidate = next(
            item
            for item in records
            if item["record_type"] == "candidate_rx"
            and item["journey_id"] == "J000040"
        )
        self.assertEqual(candidate["result"], "LOADED")
        self.assertEqual(candidate["gallery_count"], 2)
        self.assertEqual(candidate["gallery_node_ids"], ["A", "C"])
        self.assertEqual(candidate["payload_sha256"], hashlib.sha256(message.payload).hexdigest())
        self.assertTrue(candidate["registered"])
        self.assertEqual(candidate["active_candidate_count"], 1)
        self.assertEqual(candidate["active_gallery_count"], 2)
        reid = [item for item in records if item["record_type"] == "reid_track"]
        self.assertEqual(len(reid), 1)
        self.assertEqual(reid[0]["journey_id"], "J000040")
        self.assertEqual(reid[0]["local_track_id"], 40)
        self.assertTrue(reid[0]["matched"])
        self.assertEqual(reid[0]["threshold"]["best_similarity"], 0.70)
        control_record = next(
            item for item in records if item["record_type"] == "journey_control_rx"
        )
        self.assertEqual(control_record["journey_id"], "J000040")
        self.assertEqual(control_record["action"], "EXPIRED")
        self.assertTrue(control_record["removed"])

    def test_qos0_is_lost_while_disconnected_then_rebuild_is_loaded(self) -> None:
        broker = FixtureQos0Broker()
        initial = FixtureMessage(
            node_d.CANDIDATE_TOPIC,
            candidate_payload("J000041", self.base),
            qos=0,
        )
        rebuild = FixtureMessage(
            node_d.CANDIDATE_TOPIC,
            candidate_payload("J000041", self.base, rebuild=True),
            qos=0,
        )
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            node_d,
            "LOG_DIR",
            Path(temp_dir),
        ), patch.object(node_d, "append_csv"):
            self.assertFalse(broker.publish(initial))
            self.assertNotIn("J000041", node_d.candidates)
            self.assertEqual(self.read_records(temp_dir), [])

            broker.connected = True
            self.assertTrue(broker.publish(rebuild))
            self.assertIn("J000041", node_d.candidates)
            records = self.read_records(temp_dir)

        candidate_records = [
            item for item in records if item["record_type"] == "candidate_rx"
        ]
        self.assertEqual(len(candidate_records), 1)
        self.assertEqual(candidate_records[0]["result"], "LOADED")
        self.assertEqual(candidate_records[0]["message_qos"], 0)
        self.assertTrue(candidate_records[0]["rebuild"])

    def test_rejected_candidate_records_reason(self) -> None:
        payload = candidate_payload("J000042", self.base)
        payload["gallery"] = []
        message = FixtureMessage(node_d.CANDIDATE_TOPIC, payload)
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            node_d,
            "LOG_DIR",
            Path(temp_dir),
        ), patch.object(node_d, "append_csv"):
            node_d.on_message(None, None, message)
            records = self.read_records(temp_dir)

        rejected = next(item for item in records if item["record_type"] == "candidate_rx")
        self.assertEqual(rejected["result"], "REJECTED")
        self.assertEqual(rejected["reason"], "EMPTY_GALLERY")
        self.assertNotIn("J000042", node_d.candidates)


if __name__ == "__main__":
    unittest.main()
