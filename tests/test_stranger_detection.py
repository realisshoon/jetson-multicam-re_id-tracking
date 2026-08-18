from __future__ import annotations

import contextlib
import io
import json
import unittest
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

from src.common.stranger_detection import (
    STRANGER_DETECTION_TOPICS,
    StrangerDetectionGate,
    publish_stranger_detection,
)


class FixturePublishInfo:
    def __init__(self, *, mid: int, published: bool) -> None:
        self.rc = mqtt.MQTT_ERR_SUCCESS
        self.mid = mid
        self._published = published

    def wait_for_publish(self, timeout: float | None = None) -> None:
        return None

    def is_published(self) -> bool:
        return self._published


class FixtureMqttClient:
    def __init__(self, info: FixturePublishInfo) -> None:
        self.info = info
        self.calls: list[tuple[str, str, int, bool]] = []

    def publish(
        self,
        topic: str,
        payload: str,
        qos: int,
        retain: bool,
    ) -> FixturePublishInfo:
        self.calls.append((topic, payload, qos, retain))
        return self.info


class StrangerDetectionGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.base = datetime(2026, 8, 14, 15, 0, tzinfo=timezone(timedelta(hours=9)))

    def test_requires_stability_and_emits_only_once_per_track(self) -> None:
        gate = StrangerDetectionGate("C", stable_seconds=2.0, valid_frames=10)
        payload = None
        for index in range(9):
            payload = gate.observe(
                local_track_id=15,
                observed_at=self.base + timedelta(milliseconds=50 * index),
                is_unregistered=True,
                matching_in_progress=False,
            )
            self.assertIsNone(payload)

        payload = gate.observe(
            local_track_id=15,
            observed_at=self.base + timedelta(milliseconds=450),
            is_unregistered=True,
            matching_in_progress=False,
        )
        self.assertEqual(
            payload,
            {
                "detection_id": "C-20260814T150000-L15",
                "at": "2026-08-14T15:00:00+09:00",
                "node": "C",
                "kind": "STRANGER_DETECTED",
                "identity_status": "UNREGISTERED",
                "local_track_id": 15,
                "journey_id": None,
                "person_uid": None,
                "canonical_person_uid": None,
            },
        )
        self.assertIsNone(
            gate.observe(
                local_track_id=15,
                observed_at=self.base + timedelta(seconds=5),
                is_unregistered=True,
                matching_in_progress=False,
            )
        )

    def test_matching_candidate_defers_and_resets_stability(self) -> None:
        gate = StrangerDetectionGate("D", stable_seconds=2.0, valid_frames=10)
        for index in range(12):
            self.assertIsNone(
                gate.observe(
                    local_track_id=20,
                    observed_at=self.base + timedelta(seconds=index),
                    is_unregistered=True,
                    matching_in_progress=True,
                )
            )

        self.assertIsNone(
            gate.observe(
                local_track_id=20,
                observed_at=self.base + timedelta(seconds=12),
                is_unregistered=True,
                matching_in_progress=False,
            )
        )
        payload = gate.observe(
            local_track_id=20,
            observed_at=self.base + timedelta(seconds=14),
            is_unregistered=True,
            matching_in_progress=False,
        )
        self.assertIsNotNone(payload)

    def test_confirmed_identity_suppresses_track_until_removed(self) -> None:
        gate = StrangerDetectionGate("B", stable_seconds=2.0, valid_frames=1)
        self.assertIsNone(
            gate.observe(
                local_track_id=30,
                observed_at=self.base,
                is_unregistered=False,
                matching_in_progress=False,
            )
        )
        self.assertIsNone(
            gate.observe(
                local_track_id=30,
                observed_at=self.base + timedelta(seconds=5),
                is_unregistered=True,
                matching_in_progress=False,
            )
        )

        gate.remove_track(30)
        payload = gate.observe(
            local_track_id=30,
            observed_at=self.base + timedelta(seconds=6),
            is_unregistered=True,
            matching_in_progress=False,
        )
        self.assertIsNotNone(payload)
        self.assertEqual(payload["detection_id"], "B-20260814T150006-L30")

        gate.remove_track(30)
        repeated_second = gate.observe(
            local_track_id=30,
            observed_at=self.base + timedelta(seconds=6),
            is_unregistered=True,
            matching_in_progress=False,
        )
        self.assertEqual(
            repeated_second["detection_id"],
            "B-20260814T150006-L30-R2",
        )

    def test_topic_mapping_includes_b_c_d(self) -> None:
        self.assertEqual(
            STRANGER_DETECTION_TOPICS,
            {
                "B": "cctv/events/b/detection",
                "C": "cctv/events/c/detection",
                "D": "cctv/events/d/detection",
            },
        )

    def test_publish_logs_rc_mid_puback_and_detection_id(self) -> None:
        gate = StrangerDetectionGate("C", stable_seconds=2.0, valid_frames=1)
        payload = gate.observe(
            local_track_id=15,
            observed_at=self.base,
            is_unregistered=True,
            matching_in_progress=False,
        )
        self.assertIsNotNone(payload)
        client = FixtureMqttClient(
            FixturePublishInfo(mid=150, published=True)
        )
        with contextlib.redirect_stdout(io.StringIO()) as console:
            result = publish_stranger_detection(
                client,
                STRANGER_DETECTION_TOPICS["C"],
                payload,
                qos=1,
            )

        self.assertEqual(result.rc, 0)
        self.assertEqual(result.mid, 150)
        self.assertTrue(result.puback_received)
        self.assertFalse(result.failed)
        topic, raw_payload, qos, retain = client.calls[0]
        self.assertEqual(topic, "cctv/events/c/detection")
        self.assertEqual(json.loads(raw_payload), payload)
        self.assertEqual(qos, 1)
        self.assertFalse(retain)
        output = console.getvalue()
        self.assertIn("detection_id=C-20260814T150000-L15", output)
        self.assertIn("rc=0", output)
        self.assertIn("mid=150", output)
        self.assertIn("puback=True", output)

    def test_publish_timeout_is_reported_without_puback(self) -> None:
        gate = StrangerDetectionGate("D", stable_seconds=2.0, valid_frames=1)
        payload = gate.observe(
            local_track_id=16,
            observed_at=self.base,
            is_unregistered=True,
            matching_in_progress=False,
        )
        client = FixtureMqttClient(
            FixturePublishInfo(mid=160, published=False)
        )
        result = publish_stranger_detection(
            client,
            STRANGER_DETECTION_TOPICS["D"],
            payload,
            qos=1,
        )
        self.assertFalse(result.puback_received)
        self.assertTrue(result.failed)
        self.assertTrue(result.timeout)


if __name__ == "__main__":
    unittest.main()
