from __future__ import annotations

import unittest

from src.server.pending_manager import (
    CandidateState,
    PendingCandidateError,
    PendingManager,
)
from src.server.route_manager import RouteError, RouteManager


ENTRY = {
    "message_type": "entry_candidate",
    "node_id": "A",
    "local_id": 3,
    "global_id": "G000001",
    "timestamp": "2026-08-06T11:00:00+09:00",
    "embedding_dim": 512,
    "embedding": [0.01] * 512,
}


class RouteManagerTests(unittest.TestCase):
    def test_expected_camera_routes(self) -> None:
        routes = RouteManager()

        self.assertEqual(routes.targets_for("A"), ("B", "C"))
        self.assertEqual(routes.targets_for("B"), ("D",))
        self.assertEqual(routes.targets_for("C"), ("D",))
        self.assertEqual(routes.targets_for("D"), ())

    def test_unknown_node_is_rejected(self) -> None:
        with self.assertRaises(RouteError):
            RouteManager().targets_for("Z")


class PendingManagerTests(unittest.TestCase):
    def test_candidate_progresses_to_completed(self) -> None:
        pending = PendingManager()
        pending.register(ENTRY)
        pending.mark_forwarded("G000001", "B")
        pending.record_match("G000001", "B")
        pending.mark_forwarded("G000001", "D")
        pending.record_match("G000001", "D")

        self.assertEqual(
            pending.get("G000001").state,
            CandidateState.COMPLETED,
        )

    def test_unregistered_result_is_rejected(self) -> None:
        with self.assertRaises(PendingCandidateError):
            PendingManager().record_match("G999999", "B")

    def test_duplicate_target_is_not_forwarded_twice(self) -> None:
        pending = PendingManager()
        pending.register(ENTRY)

        self.assertTrue(pending.mark_forwarded("G000001", "B"))
        self.assertFalse(pending.mark_forwarded("G000001", "B"))


if __name__ == "__main__":
    unittest.main()
