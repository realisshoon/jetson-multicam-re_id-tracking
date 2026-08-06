from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

from src.network.mqtt_config import load_mqtt_config
from src.server.central_server import CentralServer
from src.server.pending_manager import CandidateState
from src.server.persistence import MemoryEventRepository


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG = PROJECT_ROOT / "configs" / "mqtt_config.example.yaml"


def entry_candidate(global_id: str = "G000001") -> dict[str, Any]:
    return {
        "message_type": "entry_candidate",
        "node_id": "A",
        "local_id": 3,
        "global_id": global_id,
        "timestamp": "2026-08-06T11:00:00+09:00",
        "embedding_dim": 512,
        "embedding": [0.01] * 512,
    }


def match_result(
    node_id: str,
    global_id: str = "G000001",
) -> dict[str, Any]:
    return {
        "message_type": "match_result",
        "node_id": node_id,
        "local_id": 7,
        "global_id": global_id,
        "timestamp": "2026-08-06T11:00:15+09:00",
        "similarity": 0.842,
        "status": "matched",
    }


class CentralServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.logs: list[str] = []
        self.repository = MemoryEventRepository()
        self.server = CentralServer(
            config=load_mqtt_config(EXAMPLE_CONFIG),
            repository=self.repository,
            publish_json=lambda topic, message: self.published.append(
                (topic, message)
            ),
            logger=self.logs.append,
        )

    def send_entry(self) -> bool:
        return self.server.handle_message(
            "nodes/A/data",
            entry_candidate(),
        )

    def test_a_entry_is_sent_only_to_b_and_c(self) -> None:
        self.assertTrue(self.send_entry())

        self.assertEqual(
            [topic for topic, _ in self.published],
            ["server/B/result", "server/C/result"],
        )
        self.assertEqual(
            [message["target_node"] for _, message in self.published],
            ["B", "C"],
        )

    def test_a_entry_is_not_sent_directly_to_d(self) -> None:
        self.send_entry()

        self.assertNotIn(
            "server/D/result",
            [topic for topic, _ in self.published],
        )

    def test_b_match_is_sent_to_d(self) -> None:
        self.send_entry()
        self.published.clear()

        accepted = self.server.handle_message(
            "nodes/B/data",
            match_result("B"),
        )

        self.assertTrue(accepted)
        self.assertEqual([topic for topic, _ in self.published], ["server/D/result"])
        self.assertEqual(self.published[0][1]["source_node"], "A")

    def test_c_match_is_sent_to_d(self) -> None:
        self.send_entry()
        self.published.clear()

        accepted = self.server.handle_message(
            "nodes/C/data",
            match_result("C"),
        )

        self.assertTrue(accepted)
        self.assertEqual([topic for topic, _ in self.published], ["server/D/result"])

    def test_d_match_completes_without_more_publish(self) -> None:
        self.send_entry()
        self.server.handle_message("nodes/B/data", match_result("B"))
        self.published.clear()

        accepted = self.server.handle_message(
            "nodes/D/data",
            match_result("D"),
        )

        self.assertTrue(accepted)
        self.assertEqual(self.published, [])
        self.assertEqual(
            self.server.pending.get("G000001").state,
            CandidateState.COMPLETED,
        )

    def test_embedding_length_other_than_512_is_rejected(self) -> None:
        message = entry_candidate()
        message["embedding_dim"] = 511
        message["embedding"] = [0.01] * 511

        self.assertFalse(self.server.handle_message("nodes/A/data", message))
        self.assertEqual(self.published, [])

    def test_embedding_dimension_mismatch_is_rejected(self) -> None:
        message = entry_candidate()
        message["embedding"] = [0.01] * 511

        self.assertFalse(self.server.handle_message("nodes/A/data", message))

    def test_topic_and_payload_node_mismatch_is_rejected(self) -> None:
        self.assertFalse(
            self.server.handle_message("nodes/B/data", entry_candidate())
        )

    def test_invalid_global_id_is_rejected(self) -> None:
        self.assertFalse(
            self.server.handle_message(
                "nodes/A/data",
                entry_candidate("invalid-id"),
            )
        )

    def test_similarity_out_of_range_is_rejected(self) -> None:
        self.send_entry()
        message = match_result("B")
        message["similarity"] = 1.5

        self.assertFalse(self.server.handle_message("nodes/B/data", message))

    def test_unregistered_global_id_match_is_rejected(self) -> None:
        self.assertFalse(
            self.server.handle_message(
                "nodes/B/data",
                match_result("B", "G999999"),
            )
        )

    def test_unknown_event_is_saved(self) -> None:
        message = {
            "message_type": "unknown",
            "node_id": "C",
            "timestamp": "2026-08-06T11:00:20+09:00",
            "reason": "no candidate",
        }

        self.assertTrue(self.server.handle_message("nodes/C/data", message))
        self.assertEqual(self.repository.events[-1]["event_type"], "UNKNOWN")

    def test_heartbeat_updates_node_status(self) -> None:
        message = {
            "message_type": "heartbeat",
            "node_id": "B",
            "timestamp": "2026-08-06T11:00:20+09:00",
        }

        self.assertTrue(self.server.handle_message("nodes/B/data", message))
        self.assertEqual(self.repository.node_statuses["B"]["status"], "online")

    def test_invalid_message_does_not_stop_server(self) -> None:
        self.assertFalse(
            self.server.handle_message("nodes/A/data", {"not": "valid"})
        )
        heartbeat = {
            "message_type": "heartbeat",
            "node_id": "A",
            "timestamp": "2026-08-06T11:00:20+09:00",
        }
        self.assertTrue(self.server.handle_message("nodes/A/data", heartbeat))
        self.assertTrue(any(log.startswith("[REJECTED]") for log in self.logs))

    def test_timeout_is_saved_and_candidate_is_closed(self) -> None:
        self.send_entry()
        timeout = {
            "message_type": "timeout",
            "node_id": "A",
            "global_id": "G000001",
            "timestamp": "2026-08-06T11:01:00+09:00",
        }

        self.assertTrue(self.server.handle_message("nodes/A/data", timeout))
        self.assertEqual(self.repository.events[-1]["event_type"], "TIMEOUT")
        self.assertEqual(
            self.server.pending.get("G000001").state,
            CandidateState.TIMED_OUT,
        )

    def test_memory_repository_preserves_event_order(self) -> None:
        self.send_entry()
        self.server.handle_message("nodes/B/data", match_result("B"))

        self.assertEqual(
            [event["event_type"] for event in self.repository.events],
            ["ENTRY", "MATCH"],
        )


if __name__ == "__main__":
    unittest.main()
