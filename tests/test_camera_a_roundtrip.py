from __future__ import annotations

import unittest
from typing import Any

from src.server.camera_a_roundtrip_server import CameraARoundtripHandler


def valid_request(message_id: str = "request-001") -> dict[str, Any]:
    return {
        "message_id": message_id,
        "message_type": "entry_candidate",
        "node_id": "A",
        "local_id": 3,
        "timestamp": "2026-08-06T11:00:00+09:00",
        "embedding_dim": 512,
        "embedding": [0.01] * 512,
    }


class CameraARoundtripHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = CameraARoundtripHandler()

    def handle(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.handler.handle("nodes/A/data", payload)

    def test_valid_embedding_is_accepted(self) -> None:
        response = self.handle(valid_request())

        self.assertTrue(response["accepted"])

    def test_response_preserves_message_id(self) -> None:
        response = self.handle(valid_request("same-request-id"))

        self.assertEqual(response["message_id"], "same-request-id")

    def test_accepted_response_contains_global_id(self) -> None:
        response = self.handle(valid_request())

        self.assertEqual(response["global_id"], "G000001")

    def test_global_id_increments_in_order(self) -> None:
        first = self.handle(valid_request("request-001"))
        second = self.handle(valid_request("request-002"))

        self.assertEqual(first["global_id"], "G000001")
        self.assertEqual(second["global_id"], "G000002")

    def test_embedding_length_511_is_rejected(self) -> None:
        request = valid_request()
        request["embedding"] = [0.01] * 511

        response = self.handle(request)

        self.assertFalse(response["accepted"])
        self.assertEqual(response["error_code"], "INVALID_EMBEDDING")

    def test_embedding_length_513_is_rejected(self) -> None:
        request = valid_request()
        request["embedding"] = [0.01] * 513

        response = self.handle(request)

        self.assertFalse(response["accepted"])
        self.assertEqual(response["error_code"], "INVALID_EMBEDDING")

    def test_embedding_dim_and_length_mismatch_is_rejected(self) -> None:
        request = valid_request()
        request["embedding_dim"] = 511

        response = self.handle(request)

        self.assertFalse(response["accepted"])
        self.assertEqual(response["error_code"], "INVALID_EMBEDDING")

    def test_nan_is_rejected(self) -> None:
        request = valid_request()
        request["embedding"][100] = float("nan")

        response = self.handle(request)

        self.assertFalse(response["accepted"])
        self.assertEqual(response["error_code"], "INVALID_EMBEDDING")

    def test_infinity_is_rejected(self) -> None:
        request = valid_request()
        request["embedding"][100] = float("inf")

        response = self.handle(request)

        self.assertFalse(response["accepted"])
        self.assertEqual(response["error_code"], "INVALID_EMBEDDING")

    def test_topic_and_payload_node_mismatch_is_rejected(self) -> None:
        response = self.handler.handle(
            "nodes/B/data",
            valid_request(),
        )

        self.assertFalse(response["accepted"])
        self.assertEqual(response["error_code"], "NODE_MISMATCH")

    def test_missing_message_id_is_rejected(self) -> None:
        request = valid_request()
        del request["message_id"]

        response = self.handle(request)

        self.assertFalse(response["accepted"])
        self.assertIsNone(response["message_id"])
        self.assertEqual(response["error_code"], "INVALID_MESSAGE_ID")

    def test_valid_request_succeeds_after_rejected_request(self) -> None:
        invalid = valid_request("invalid-request")
        invalid["embedding"] = [0.01] * 511

        rejected = self.handle(invalid)
        accepted = self.handle(valid_request("valid-request"))

        self.assertFalse(rejected["accepted"])
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["global_id"], "G000001")


if __name__ == "__main__":
    unittest.main()
