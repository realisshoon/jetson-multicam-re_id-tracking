from __future__ import annotations

import unittest
from typing import Any

from src.server.camera_a_message import (
    CameraAEntryValidationError,
    validate_camera_a_entry,
)


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
        "future_field": "preserved",
    }


class CameraAEntryValidationTests(unittest.TestCase):
    def test_origin_main_payload_is_accepted(self) -> None:
        normalized = validate_camera_a_entry(valid_payload())

        self.assertEqual(normalized["global_person_id"], "G000001")
        self.assertEqual(normalized["future_field"], "preserved")

    def test_embedding_length_512_is_accepted(self) -> None:
        normalized = validate_camera_a_entry(valid_payload())

        self.assertEqual(len(normalized["embedding"]), 512)

    def test_embedding_length_511_is_rejected(self) -> None:
        payload = valid_payload()
        payload["embedding"] = [0.01] * 511

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_embedding_dim_other_than_512_is_rejected(self) -> None:
        payload = valid_payload()
        payload["embedding_dim"] = 511
        payload["embedding"] = [0.01] * 511

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_embedding_dim_and_length_mismatch_is_rejected(self) -> None:
        payload = valid_payload()
        payload["embedding_dim"] = 511

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_nan_is_rejected(self) -> None:
        payload = valid_payload()
        payload["embedding"][0] = float("nan")

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_infinity_is_rejected(self) -> None:
        payload = valid_payload()
        payload["embedding"][0] = float("inf")

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_string_embedding_value_is_rejected(self) -> None:
        payload = valid_payload()
        payload["embedding"][0] = "0.01"

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_node_other_than_a_is_rejected(self) -> None:
        payload = valid_payload()
        payload["node_id"] = "B"

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_event_other_than_entry_is_rejected(self) -> None:
        payload = valid_payload()
        payload["event"] = "EXIT"

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_invalid_local_track_id_is_rejected(self) -> None:
        for invalid_value in (True, "1", -1):
            with self.subTest(local_track_id=invalid_value):
                payload = valid_payload()
                payload["local_track_id"] = invalid_value
                with self.assertRaises(CameraAEntryValidationError):
                    validate_camera_a_entry(payload)

    def test_invalid_global_person_id_is_rejected(self) -> None:
        payload = valid_payload()
        payload["global_person_id"] = "person-1"

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)

    def test_invalid_timestamp_is_rejected(self) -> None:
        payload = valid_payload()
        payload["timestamp"] = "not-a-timestamp"

        with self.assertRaises(CameraAEntryValidationError):
            validate_camera_a_entry(payload)


if __name__ == "__main__":
    unittest.main()
