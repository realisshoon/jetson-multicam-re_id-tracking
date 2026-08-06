from __future__ import annotations

import unittest

from src.server.message_protocol import (
    ProtocolValidationError,
    build_reid_candidate,
    validate_incoming_message,
)


def entry_candidate() -> dict[str, object]:
    return {
        "message_type": "entry_candidate",
        "node_id": "A",
        "local_id": 3,
        "global_id": "G000001",
        "timestamp": "2026-08-06T11:00:00+09:00",
        "embedding_dim": 512,
        "embedding": [0.01] * 512,
    }


def match_result(node_id: str = "B") -> dict[str, object]:
    return {
        "message_type": "match_result",
        "node_id": node_id,
        "local_id": 7,
        "global_id": "G000001",
        "timestamp": "2026-08-06T11:00:15+09:00",
        "similarity": 0.842,
        "status": "matched",
    }


class MessageProtocolTests(unittest.TestCase):
    def test_valid_entry_candidate(self) -> None:
        message = entry_candidate()
        self.assertEqual(
            validate_incoming_message("nodes/A/data", message),
            "entry_candidate",
        )

    def test_reid_candidate_preserves_embedding(self) -> None:
        message = entry_candidate()
        candidate = build_reid_candidate(message, "B")

        self.assertEqual(candidate["source_node"], "A")
        self.assertEqual(candidate["target_node"], "B")
        self.assertEqual(candidate["embedding_dim"], 512)
        self.assertEqual(len(candidate["embedding"]), 512)
        self.assertIsNot(candidate["embedding"], message["embedding"])

    def test_non_finite_embedding_is_rejected(self) -> None:
        message = entry_candidate()
        message["embedding"][10] = float("nan")

        with self.assertRaises(ProtocolValidationError):
            validate_incoming_message("nodes/A/data", message)

    def test_embedding_dimension_must_be_512(self) -> None:
        message = entry_candidate()
        message["embedding_dim"] = 256
        message["embedding"] = [0.01] * 256

        with self.assertRaises(ProtocolValidationError):
            validate_incoming_message("nodes/A/data", message)

    def test_embedding_dimension_and_length_must_match(self) -> None:
        message = entry_candidate()
        message["embedding"] = [0.01] * 511

        with self.assertRaises(ProtocolValidationError):
            validate_incoming_message("nodes/A/data", message)

    def test_invalid_global_id_is_rejected(self) -> None:
        message = entry_candidate()
        message["global_id"] = "person-1"

        with self.assertRaises(ProtocolValidationError):
            validate_incoming_message("nodes/A/data", message)

    def test_similarity_must_be_finite_and_in_range(self) -> None:
        for invalid_value in (-0.01, 1.01, float("inf"), True):
            with self.subTest(similarity=invalid_value):
                message = match_result()
                message["similarity"] = invalid_value
                with self.assertRaises(ProtocolValidationError):
                    validate_incoming_message("nodes/B/data", message)


if __name__ == "__main__":
    unittest.main()
