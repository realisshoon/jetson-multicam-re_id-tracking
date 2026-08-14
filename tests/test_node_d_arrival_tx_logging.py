from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import paho.mqtt.client as mqtt

from src.nodes import node_d


class FixturePublishInfo:
    def __init__(
        self,
        mid: int,
        published: bool,
        rc: int = mqtt.MQTT_ERR_SUCCESS,
    ) -> None:
        self.rc = rc
        self.mid = mid
        self._published = published

    def wait_for_publish(self, timeout: float | None = None) -> None:
        return None

    def is_published(self) -> bool:
        return self._published


class FixtureMqttClient:
    def __init__(self, publish_results: list[FixturePublishInfo]) -> None:
        self.publish_results = list(publish_results)
        self.calls: list[tuple[str, bytes, int, bool]] = []

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int,
        retain: bool,
    ) -> FixturePublishInfo:
        self.calls.append((topic, payload, qos, retain))
        return self.publish_results.pop(0)


class NodeDArrivalTxLoggingTest(unittest.TestCase):
    def setUp(self) -> None:
        node_d.candidates.clear()
        node_d.completed_journey_ids.clear()
        node_d.terminal_journey_ids.clear()
        node_d.consumed_track_ids.clear()
        node_d.completed_tracks.clear()
        node_d.arrival_inflight.clear()

    def test_daily_jsonl_preserves_payload_and_retry_event_id(self) -> None:
        embedding = np.ones(512, dtype=np.float32)
        embedding /= np.linalg.norm(embedding)
        node_d.candidates["J000814"] = node_d.Candidate(
            journey_id="J000814",
            person_uid="P000814",
            received_at="2026-08-14T09:00:05+09:00",
            entry_timestamp="2026-08-14T09:00:00+09:00",
            entry_epoch=datetime.fromisoformat(
                "2026-08-14T09:00:00+09:00"
            ).timestamp(),
            b_passage_timestamp="2026-08-14T09:00:05+09:00",
            b_passage_epoch=datetime.fromisoformat(
                "2026-08-14T09:00:05+09:00"
            ).timestamp(),
            route=["A", "B"],
            gallery=[embedding.copy()],
            gallery_nodes=["A"],
            tracking_person_uid="TP000814",
            canonical_person_uid="CP000814",
        )
        diagnostics = node_d.ArrivalDiagnostics(
            track_first_seen_at=datetime.fromisoformat(
                "2026-08-14T09:00:06+09:00"
            ),
            candidate_received_at=datetime.fromisoformat(
                "2026-08-14T09:00:05+09:00"
            ),
            passage_at=datetime.fromisoformat(
                "2026-08-14T09:00:05+09:00"
            ),
            arrival_at=datetime.fromisoformat(
                "2026-08-14T09:00:10+09:00"
            ),
            confirmation_sample_count=5,
            confirmation_pass_count=4,
            best_journey_score=0.79,
            second_journey_score=0.70,
            journey_margin=0.09,
            eligibility_reason="ELIGIBLE_NEW_ENTRY",
        )
        client = FixtureMqttClient(
            [
                FixturePublishInfo(mid=41, published=False),
                FixturePublishInfo(
                    mid=42,
                    published=False,
                    rc=mqtt.MQTT_ERR_NO_CONN,
                ),
                FixturePublishInfo(mid=43, published=True),
            ]
        )
        published_at = datetime.fromisoformat(
            "2026-08-14T09:00:11+09:00"
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            node_d,
            "LOG_DIR",
            Path(temp_dir),
        ), patch.object(
            node_d,
            "now_aware",
            return_value=published_at,
        ), patch.object(
            node_d,
            "save_arrival_capture",
            return_value="outputs/captures/D/capture.jpg",
        ), patch.object(node_d, "append_csv"), contextlib.redirect_stdout(
            io.StringIO()
        ) as console:
            first = node_d.complete_arrival(
                client,
                "J000814",
                814,
                0.81,
                0.76,
                0.79,
                embedding,
                np.zeros((8, 8, 3), dtype=np.uint8),
                0.9,
                diagnostics,
            )
            second = node_d.complete_arrival(
                client,
                "J000814",
                814,
                0.81,
                0.76,
                0.79,
                embedding,
                np.zeros((8, 8, 3), dtype=np.uint8),
                0.9,
                diagnostics,
            )
            third = node_d.complete_arrival(
                client,
                "J000814",
                814,
                0.81,
                0.76,
                0.79,
                embedding,
                np.zeros((8, 8, 3), dtype=np.uint8),
                0.9,
                diagnostics,
            )

            log_path = Path(temp_dir) / "d_arrival_tx_20260814.jsonl"
            records = [
                json.loads(line)
                for line in log_path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertFalse(first)
        self.assertFalse(second)
        self.assertTrue(third)
        self.assertEqual(len(client.calls), 3)
        first_payload = json.loads(client.calls[0][1])
        second_payload = json.loads(client.calls[1][1])
        third_payload = json.loads(client.calls[2][1])
        self.assertEqual(
            first_payload["arrival_event_id"],
            second_payload["arrival_event_id"],
        )
        self.assertEqual(
            first_payload["arrival_event_id"],
            third_payload["arrival_event_id"],
        )
        self.assertNotEqual(
            first_payload["arrival_event_id"],
            node_d.make_arrival_event_id(
                "J000815",
                "2026-08-14T09:01:05+09:00",
            ),
        )
        self.assertEqual(len(records), 6)

        pre_publish = records[0]
        timeout_result = records[1]
        rc_failure_result = records[3]
        success_result = records[5]
        self.assertEqual(pre_publish["phase"], "PRE_PUBLISH")
        self.assertEqual(pre_publish["topic"], node_d.ARRIVAL_TOPIC)
        self.assertEqual(pre_publish["qos"], node_d.MQTT_QOS)
        self.assertEqual(pre_publish["person_uid"], "P000814")
        self.assertEqual(pre_publish["tracking_person_uid"], "TP000814")
        self.assertEqual(pre_publish["canonical_person_uid"], "CP000814")
        self.assertEqual(pre_publish["journey_id"], "J000814")
        self.assertEqual(pre_publish["local_track_id"], 814)
        self.assertEqual(pre_publish["route"], ["A", "B", "D"])
        self.assertEqual(pre_publish["stage"], "WAITING_D")
        self.assertEqual(pre_publish["status"], "COMPLETED")
        self.assertEqual(pre_publish["confirmation_count"], 4)
        self.assertEqual(pre_publish["embedding_dim"], 512)
        self.assertAlmostEqual(pre_publish["embedding_norm"], 1.0)
        self.assertIn('"embedding":[', pre_publish["payload_raw"])
        self.assertEqual(
            pre_publish["payload_size_bytes"],
            len(client.calls[0][1]),
        )
        self.assertEqual(
            pre_publish["payload_sha256"],
            hashlib.sha256(client.calls[0][1]).hexdigest(),
        )
        self.assertEqual(timeout_result["publish_rc"], 0)
        self.assertEqual(timeout_result["mid"], 41)
        self.assertFalse(timeout_result["puback_received"])
        self.assertTrue(timeout_result["failed"])
        self.assertTrue(timeout_result["timeout"])
        self.assertEqual(rc_failure_result["publish_rc"], 4)
        self.assertEqual(rc_failure_result["mid"], 42)
        self.assertFalse(rc_failure_result["puback_received"])
        self.assertTrue(rc_failure_result["failed"])
        self.assertFalse(rc_failure_result["timeout"])
        self.assertIsNotNone(rc_failure_result["error"])
        self.assertEqual(success_result["mid"], 43)
        self.assertTrue(success_result["puback_received"])
        self.assertFalse(success_result["failed"])
        self.assertFalse(success_result["timeout"])
        self.assertNotIn('"embedding"', console.getvalue())


if __name__ == "__main__":
    unittest.main()
