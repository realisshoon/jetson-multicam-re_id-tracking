from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import paho.mqtt.client as mqtt

from src.nodes import node_c
from src.nodes.node_c import (
    C_PASSAGE_MIN_QUALITY,
    add_temporal_candidate,
    build_passage_payload,
    calculate_gallery_diagnostics,
    try_add_gallery,
    selected_temporal_candidates,
    validate_c_passage_evidence,
    verify_wire_payload,
)


def unit(index: int, offset: float = 0.0) -> np.ndarray:
    value = np.zeros(512, dtype=np.float32)
    value[index] = 1.0
    value[(index + 1) % 512] = offset
    return value / np.linalg.norm(value)


class TemporaryMain:
    """SQLite/MQTT-shaped harness for the C -> Main -> D transaction."""

    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.execute(
            "CREATE TABLE journeys (journey_id TEXT PRIMARY KEY, stage TEXT, route TEXT)"
        )
        self.db.execute(
            "CREATE TABLE events (event_key TEXT PRIMARY KEY, payload TEXT)"
        )
        self.db.execute(
            "INSERT INTO journeys VALUES (?, ?, ?)",
            ("J-E2E-1", "WAITING_B_OR_C", json.dumps(["A"])),
        )
        self.published: list[tuple[str, dict]] = []

    def on_c_passage(self, payload: dict) -> bool:
        scores = payload["per_frame_best_scores"]
        qualities = payload["quality_samples"]
        accepted = (
            isinstance(scores, list)
            and len(scores) >= 2
            and all(isinstance(value, float) for value in scores)
            and isinstance(qualities, list)
            and len(qualities) >= 2
            and all(isinstance(value, float) for value in qualities)
            and isinstance(payload["best_score"], float)
            and payload["best_score"] >= 0.75
            and isinstance(payload["topk_score"], float)
            and payload["topk_score"] >= 0.68
            and isinstance(payload["combined_score"], float)
            and payload["combined_score"] >= 0.72
            and isinstance(payload["multiframe_consistency"], int)
            and payload["multiframe_consistency"] >= 2
            and isinstance(payload["consistency_count"], int)
            and isinstance(payload["multiframe_consistency_ratio"], float)
            and isinstance(payload["final_quality"], float)
            and payload["final_quality"] >= 0.8
            and isinstance(payload["final_similarity"], float)
            and payload["final_similarity"] >= 0.72
        )
        if not accepted:
            return False

        event_key = f'C_PASSAGE:{payload["journey_id"]}'
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO events VALUES (?, ?)",
                (event_key, json.dumps(payload)),
            ).rowcount
            if not inserted:
                return True
            self.db.execute(
                "UPDATE journeys SET stage=?, route=? WHERE journey_id=?",
                ("WAITING_D", json.dumps(["A", "C"]), payload["journey_id"]),
            )
            d_payload = dict(payload)
            d_payload["event"] = "CANDIDATE"
            d_payload["stage"] = "WAITING_D"
            d_payload["gallery"] = payload["gallery"]
            self.published.append(("cctv/candidates/d", d_payload))
        return True


class NodeCPassageE2ETest(unittest.TestCase):
    def setUp(self) -> None:
        self.a_embedding = unit(0)
        self.c_embeddings = [unit(0, 0.05), unit(0, -0.04)]
        self.samples = [
            {"frame_index": 10, "best_score": 0.94, "quality": 0.86,
             "gallery_selected": True},
            {"frame_index": 21, "best_score": 0.92, "quality": 0.84,
             "gallery_selected": True},
            {"frame_index": 24, "best_score": 0.91, "quality": 0.85,
             "gallery_selected": False},
            {"frame_index": 27, "best_score": 0.93, "quality": 0.88,
             "gallery_selected": False},
        ]
        self.payload = build_passage_payload(
            journey_id="J-E2E-1",
            person_uid="P-E2E-1",
            entry_timestamp="2026-08-14T09:00:00+09:00",
            incoming_gallery=[{
                "node_id": "A", "captured_at": "2026-08-14T09:00:00+09:00",
                "embedding_dim": 512, "embedding": self.a_embedding.tolist(),
                "quality": 0.9,
            }],
            c_embeddings=self.c_embeddings,
            a_local_track_id=7,
            c_local_track_id=31,
            c_passage_timestamp="2026-08-14T09:02:00+09:00",
            selected_wire_samples=[
                sample for sample in self.samples if sample["gallery_selected"]
            ],
        )

    def test_payload_uses_real_samples_and_both_c_gallery_entries(self) -> None:
        expected_scores = [
            float(np.dot(embedding, self.a_embedding))
            for embedding in self.c_embeddings
        ]
        np.testing.assert_allclose(
            self.payload["per_frame_best_scores"], expected_scores, atol=1e-6
        )
        self.assertEqual(self.payload["quality_samples"], [0.86, 0.84])
        self.assertEqual(self.payload["gallery_sample_count"], 2)
        self.assertEqual(self.payload["multiframe_consistency"], 2)
        self.assertEqual(self.payload["consistency_count"], 2)
        self.assertEqual(self.payload["multiframe_consistency_ratio"], 1.0)
        c_gallery = [item for item in self.payload["gallery"] if item["node_id"] == "C"]
        self.assertEqual(len(c_gallery), 2)
        self.assertEqual([item["quality"] for item in c_gallery], [0.86, 0.84])
        self.assertAlmostEqual(self.payload["final_quality"], 0.85)

    def test_operational_low_quality_journeys_are_held_locally(self) -> None:
        cases = {"below-boundary": (0.90, 0.699999), "low-demo": (0.90, 0.69)}
        for journey_id, (score, quality) in cases.items():
            gallery = [
                {"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.9},
                {"node_id": "C", "embedding": unit(0).tolist(), "quality": quality},
                {"node_id": "C", "embedding": unit(0).tolist(), "quality": quality},
            ]
            accepted, reason, diagnostics = validate_c_passage_evidence(gallery)
            with self.subTest(journey_id=journey_id):
                self.assertFalse(accepted)
                self.assertEqual(reason, "INSUFFICIENT_QUALITY")
                self.assertIsNone(diagnostics)

    def test_low_quality_is_discarded_and_later_frames_can_fill_gallery(self) -> None:
        galleries: dict[int, list[np.ndarray]] = {}
        last_frames: dict[int, int] = {}
        self.assertFalse(
            try_add_gallery(31, unit(1), C_PASSAGE_MIN_QUALITY - 0.01, 10,
                            galleries, last_frames)
        )
        self.assertEqual(galleries, {})
        self.assertTrue(
            try_add_gallery(31, unit(1), 0.81, 20, galleries, last_frames)
        )
        self.assertTrue(
            try_add_gallery(31, unit(2), 0.83, 31, galleries, last_frames)
        )
        self.assertEqual(len(galleries[31]), 2)

    def test_c_passage_quality_boundary(self) -> None:
        galleries: dict[int, list[np.ndarray]] = {}
        last_frames: dict[int, int] = {}
        self.assertFalse(
            try_add_gallery(53, unit(1), 0.699999, 10, galleries, last_frames)
        )
        self.assertTrue(
            try_add_gallery(53, unit(1), 0.700000, 20, galleries, last_frames)
        )

    def test_j000053_demo_quality_distribution_selects_at_least_two(self) -> None:
        observed_qualities = [0.690, 0.699999, 0.700000, 0.751, 0.777]
        galleries: dict[int, list[np.ndarray]] = {}
        last_frames: dict[int, int] = {}
        selected_wire_samples: list[dict] = []
        for index, quality in enumerate(observed_qualities):
            selected = try_add_gallery(
                53, unit(index + 1), quality, index * 11,
                galleries, last_frames,
            )
            if selected:
                selected_wire_samples.append({"quality": quality})
        self.assertGreaterEqual(len(selected_wire_samples), 2)
        self.assertTrue(all(
            sample["quality"] >= C_PASSAGE_MIN_QUALITY
            for sample in selected_wire_samples
        ))

    def test_moving_person_temporal_windows_replace_and_publish_once(self) -> None:
        def cosine_embedding(score: float, axis: int) -> np.ndarray:
            value = np.zeros(512, dtype=np.float32)
            value[0] = score
            value[axis] = np.sqrt(1.0 - score * score)
            return value

        incoming = [{
            "node_id": "A", "captured_at": "2026-08-14T13:34:09+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.9,
        }]
        window: list[dict] = []
        bank: list[dict] = []
        observed: list[dict] = []
        client = Mock()
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)
        reference = {
            "person_uid": "P-MOVING", "entry_timestamp": "2026-08-14T13:34:09+09:00",
            "incoming_gallery": incoming, "a_local_id": "",
        }
        published = False
        publish_frame = None
        similarity_windows = [0.70, 0.71, 0.91, 0.93]
        with patch.object(node_c, "candidate_reference", return_value=reference), \
             patch.object(node_c, "save_match_capture", return_value="capture.jpg"), \
             patch.object(node_c, "append_jsonl") as append_jsonl, \
             patch.object(node_c, "append_csv"), \
             patch.object(node_c, "mark_passed"), \
             patch("builtins.print") as print_mock:
            frame_index = 0
            for window_index, window_score in enumerate(similarity_windows, start=1):
                for _ in range(3):
                    frame_index += 1
                    embedding = cosine_embedding(window_score, window_index)
                    observed.append({
                        "frame_index": frame_index, "best_score": window_score,
                        "quality": 0.75, "gallery_selected": False,
                    })
                    completed = add_temporal_candidate(
                        embedding, 0.75, frame_index, incoming, window, bank
                    )
                self.assertTrue(completed)
                selected = selected_temporal_candidates(bank)
                if len(selected) < 2 or published:
                    continue
                publish_frame = frame_index
                published = node_c.publish_passage(
                    client, 59, "J-MOVING",
                    [item["embedding"] for item in selected],
                    np.zeros((8, 8, 3), dtype=np.uint8), 0.75, window_score,
                    observed, selected, rejection_is_final=False,
                )

        self.assertTrue(published)
        self.assertEqual(publish_frame, 12)
        self.assertEqual(client.publish.call_count, 1)
        self.assertEqual(append_jsonl.call_count, 1)
        self.assertFalse(any(
            "거부" in " ".join(str(arg) for arg in call.args)
            for call in print_mock.call_args_list
        ))
        selected = selected_temporal_candidates(bank)
        self.assertEqual(
            [(item["window_start_frame"], item["window_end_frame"]) for item in selected],
            [(10, 12), (7, 9)],
        )
        self.assertNotIn(0.70, [round(item["best_score"], 2) for item in selected])

    def test_temporal_candidate_bank_is_bounded(self) -> None:
        incoming = [{"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.9}]
        window: list[dict] = []
        bank: list[dict] = []
        frame_index = 0
        for window_index in range(node_c.TEMPORAL_CANDIDATE_BANK_MAX + 3):
            for _ in range(node_c.TEMPORAL_WINDOW_SIZE):
                frame_index += 1
                add_temporal_candidate(
                    unit(0, 0.01 * (window_index + 1)), 0.75, frame_index,
                    incoming, window, bank,
                )
        self.assertEqual(len(bank), node_c.TEMPORAL_CANDIDATE_BANK_MAX)

    def test_all_main_contract_thresholds_are_required_locally(self) -> None:
        base = [
            {"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.9},
            {"node_id": "C", "embedding": unit(0, 0.05).tolist(), "quality": 0.81},
            {"node_id": "C", "embedding": unit(0, -0.04).tolist(), "quality": 0.82},
        ]
        accepted, reason, diagnostics = validate_c_passage_evidence(base)
        self.assertTrue(accepted, reason)
        self.assertIsNotNone(diagnostics)

    def test_j000050_actual_wire_gallery_is_rejected_with_main_scores(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures/j000050_wire_gallery.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        diagnostics = calculate_gallery_diagnostics(fixture["gallery"])
        self.assertAlmostEqual(diagnostics["best_score"], 0.680535, places=6)
        self.assertAlmostEqual(diagnostics["topk_score"], 0.670193, places=6)
        self.assertAlmostEqual(diagnostics["combined_score"], 0.674847, places=6)
        self.assertEqual(diagnostics["consistency_count"], 0)
        accepted, reason, _ = validate_c_passage_evidence(fixture["gallery"])
        self.assertFalse(accepted)
        self.assertEqual(reason, "REJECTED_BEST_SCORE")

    def test_j000050_is_rejected_by_actual_publish_path(self) -> None:
        fixture_path = Path(__file__).parent / "fixtures/j000050_wire_gallery.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        a_gallery = [item for item in fixture["gallery"] if item["node_id"] == "A"]
        c_gallery = [item for item in fixture["gallery"] if item["node_id"] == "C"]
        c_embeddings = [np.asarray(item["embedding"], dtype=np.float32) for item in c_gallery]
        selected = [
            {"frame_index": index, "best_score": score,
             "quality": item["quality"], "gallery_selected": True}
            for index, (score, item) in enumerate(zip(
                [0.6805351972579956, 0.65985107421875], c_gallery
            ))
        ]
        observed = [
            {"frame_index": index, "best_score": score, "quality": 0.81,
             "gallery_selected": index in {0, 4}}
            for index, score in enumerate([
                0.6805351972579956, 0.7049833536148071,
                0.659564733505249, 0.6748944520950317,
                0.65985107421875,
            ])
        ]
        client = Mock()
        reference = {
            "person_uid": "P000037", "entry_timestamp": "2026-08-14T12:43:31+09:00",
            "incoming_gallery": a_gallery, "a_local_id": "",
        }
        with patch.object(node_c, "candidate_reference", return_value=reference), \
             patch.object(node_c, "append_jsonl") as append_jsonl:
            published = node_c.publish_passage(
                client, 77, "J000050", c_embeddings,
                np.zeros((8, 8, 3), dtype=np.uint8), 0.83, 0.7348398,
                observed, selected, rejection_is_final=True,
            )
        self.assertFalse(published)
        client.publish.assert_not_called()
        diagnostic = append_jsonl.call_args.args[1]
        payload = diagnostic["payload"]
        self.assertEqual(len(payload["per_frame_best_scores"]), 2)
        self.assertNotIn(0.7049833536148071, payload["per_frame_best_scores"])
        self.assertAlmostEqual(payload["best_score"], 0.6805351972579956, delta=1e-6)
        self.assertAlmostEqual(payload["topk_score"], 0.6701931357383728, delta=1e-6)
        self.assertAlmostEqual(payload["combined_score"], 0.6748470634222031, delta=1e-6)
        self.assertEqual(payload["consistency_count"], 0)

    def test_positive_wire_fixture_and_roundtrip_invariants(self) -> None:
        self.payload["quality"] = self.payload["final_quality"]
        accepted, reason, _ = validate_c_passage_evidence(self.payload["gallery"])
        self.assertTrue(accepted, reason)
        wire_ok, wire_payload, mismatches = verify_wire_payload(self.payload)
        self.assertTrue(wire_ok, mismatches)
        self.assertEqual(wire_payload["quality"], wire_payload["final_quality"])

        tampered = dict(self.payload, combined_score=self.payload["combined_score"] + 1e-5)
        wire_ok, _, mismatches = verify_wire_payload(tampered)
        self.assertFalse(wire_ok)
        self.assertIn("combined_score", mismatches)

    def test_positive_actual_publish_path_excludes_unselected_observation(self) -> None:
        client = Mock()
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)
        reference = {
            "person_uid": "P-E2E-1", "entry_timestamp": "2026-08-14T09:00:00+09:00",
            "incoming_gallery": self.payload["gallery"][:1], "a_local_id": 7,
        }
        observed = self.samples + [{
            "frame_index": 99, "best_score": 0.123456, "quality": 0.99,
            "gallery_selected": False,
        }]
        selected = [sample for sample in self.samples if sample["gallery_selected"]]
        with patch.object(node_c, "candidate_reference", return_value=reference), \
             patch.object(node_c, "save_match_capture", return_value="capture.jpg"), \
             patch.object(node_c, "append_jsonl"), \
             patch.object(node_c, "append_csv"), \
             patch.object(node_c, "mark_passed"):
            published = node_c.publish_passage(
                client, 31, "J-E2E-1", self.c_embeddings,
                np.zeros((8, 8, 3), dtype=np.uint8), 0.86, 0.95,
                observed, selected,
            )
        self.assertTrue(published)
        encoded = client.publish.call_args.args[1]
        wire_payload = json.loads(encoded)
        self.assertEqual(len(wire_payload["per_frame_best_scores"]), 2)
        self.assertNotIn(0.123456, wire_payload["per_frame_best_scores"])
        self.assertEqual(wire_payload["quality_samples"], [0.86, 0.84])

    def test_main_transition_single_d_publish_and_duplicate_idempotency(self) -> None:
        main = TemporaryMain()
        self.assertTrue(main.on_c_passage(self.payload))
        self.assertTrue(main.on_c_passage(self.payload))

        stage, route = main.db.execute(
            "SELECT stage, route FROM journeys WHERE journey_id='J-E2E-1'"
        ).fetchone()
        self.assertEqual(stage, "WAITING_D")
        self.assertEqual(json.loads(route), ["A", "C"])
        self.assertEqual(len(main.published), 1)
        topic, candidate = main.published[0]
        self.assertEqual(topic, "cctv/candidates/d")
        self.assertEqual(
            [item["node_id"] for item in candidate["gallery"]], ["A", "C", "C"]
        )
        self.assertEqual(
            main.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1
        )

    @unittest.skipUnless(shutil.which("mosquitto"), "mosquitto is not installed")
    def test_temporary_mqtt_and_sqlite_roundtrip(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        broker = subprocess.Popen(
            ["mosquitto", "-p", str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        main = TemporaryMain()
        d_messages: list[dict] = []
        received = threading.Event()

        main_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        observer = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

        def on_passage(client, userdata, message) -> None:
            payload = json.loads(message.payload)
            before = len(main.published)
            main.on_c_passage(payload)
            if len(main.published) > before:
                topic, candidate = main.published[-1]
                client.publish(topic, json.dumps(candidate), qos=1)

        def on_d_candidate(client, userdata, message) -> None:
            d_messages.append(json.loads(message.payload))
            received.set()

        try:
            deadline = time.time() + 3
            while True:
                try:
                    main_client.connect("127.0.0.1", port)
                    break
                except ConnectionRefusedError:
                    if time.time() >= deadline:
                        raise
                    time.sleep(0.05)
            observer.connect("127.0.0.1", port)
            publisher.connect("127.0.0.1", port)
            main_client.on_message = on_passage
            observer.on_message = on_d_candidate
            main_client.subscribe("cctv/events/c/passage", qos=1)
            observer.subscribe("cctv/candidates/d", qos=1)
            main_client.loop_start()
            observer.loop_start()
            publisher.loop_start()
            encoded = json.dumps(self.payload)
            publisher.publish("cctv/events/c/passage", encoded, qos=1).wait_for_publish()
            publisher.publish("cctv/events/c/passage", encoded, qos=1).wait_for_publish()
            self.assertTrue(received.wait(3), "D candidate was not published")
            time.sleep(0.25)
            self.assertEqual(len(d_messages), 1)
            self.assertEqual(
                [item["node_id"] for item in d_messages[0]["gallery"]],
                ["A", "C", "C"],
            )
            self.assertEqual(
                main.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1
            )
        finally:
            for client in (publisher, observer, main_client):
                client.loop_stop()
                try:
                    client.disconnect()
                except Exception:
                    pass
            broker.terminate()
            broker.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
