from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import subprocess
import tempfile
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
    finalize_partial_temporal_window,
    promote_confirmation_observations,
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
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS, mid=101)
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
             patch.object(node_c, "log_revisit_event"), \
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
             patch.object(node_c, "append_jsonl") as append_jsonl, \
             patch.object(node_c, "log_revisit_event"):
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
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS, mid=102)
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
             patch.object(node_c, "mark_passed"), \
             patch.object(node_c, "log_revisit_event"):
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

    def test_revisit_log_is_structured_and_contains_no_raw_embedding(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "camera_c_revisit.jsonl"
            payload = {
                "event": "CANDIDATE",
                "stage": "WAITING_B_OR_C",
                "request_id": "REQ-1",
                "journey_id": "J-LOG-1",
                "person_uid": "P-LOG-1",
                "entry_timestamp": "2026-08-14T09:00:00+09:00",
                "gallery": [{
                    "node_id": "A", "embedding_dim": 512,
                    "embedding": unit(0).tolist(), "quality": 0.91,
                }],
            }
            with patch.object(node_c, "REVISIT_LOG", log_path), \
                 patch.object(node_c, "REVISIT_RUN_ID", "RUN-LOG-1"), \
                 patch.dict(node_c.candidates, {}, clear=True), \
                 patch.object(node_c, "append_csv"):
                node_c.save_candidate(payload)
                node_c.pending_passage_pubacks[77] = {
                    "request_id": "REQ-1", "journey_id": "J-LOG-1",
                    "person_uid": "P-LOG-1", "local_track_id": 7,
                    "topic": node_c.PASSAGE_TOPIC, "rc": 0,
                }
                node_c.on_publish(None, None, 77, 0, None)

            lines = log_path.read_text(encoding="utf-8").splitlines()
            records = [json.loads(line) for line in lines]
            self.assertEqual(
                [record["event"] for record in records],
                ["C_CANDIDATE_RECEIVED", "C_CANDIDATE_ACTIVATED", "C_PASSAGE_PUBLISH"],
            )
            received = records[0]
            self.assertEqual(received["request_id"], "REQ-1")
            self.assertEqual(received["samples"][0]["embedding_dim"], 512)
            self.assertAlmostEqual(received["samples"][0]["norm"], 1.0)
            self.assertEqual(received["samples"][0]["quality"], 0.91)
            self.assertTrue(records[-1]["puback"])
            serialized = "\n".join(lines).lower()
            self.assertNotIn('"embedding":', serialized)
            self.assertNotIn('"token"', serialized)

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
    def test_confirmation_observations_reuse_enables_passage_on_normal_short_walk(self) -> None:
        """A. Normal walk short transit: confirmation seeds complete Window 1, and post-match completes Window 2."""
        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T10:00:00+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.92,
        }]
        candidate_id = "J-SHORT-1"
        reference = {
            "person_uid": "P-SHORT-1", "entry_timestamp": "2026-08-15T10:00:00+09:00",
            "incoming_gallery": incoming, "a_local_id": 5,
        }

        # 3 confirmation observations (MATCH_CONFIRMATIONS = 3)
        confirmation_seeds = [
            {"embedding": unit(0, 0.02), "quality": 0.85, "frame_index": 3,
             "crop": np.zeros((10, 10, 3), dtype=np.uint8), "candidate_id": candidate_id, "score": 0.88},
            {"embedding": unit(0, 0.03), "quality": 0.87, "frame_index": 6,
             "crop": np.zeros((10, 10, 3), dtype=np.uint8), "candidate_id": candidate_id, "score": 0.89},
            {"embedding": unit(0, 0.01), "quality": 0.86, "frame_index": 9,
             "crop": np.zeros((10, 10, 3), dtype=np.uint8), "candidate_id": candidate_id, "score": 0.87},
        ]

        window: list[dict] = []
        bank: list[dict] = []
        observed: list[dict] = []

        promoted, completed = promote_confirmation_observations(
            candidate_id, incoming, confirmation_seeds, window, bank, observed
        )
        self.assertEqual(len(promoted), 3)
        self.assertTrue(completed)
        self.assertEqual(len(bank), 1)
        self.assertEqual(len(observed), 3)

        # 3 post-match observations
        for offset, frame_idx in [(0.02, 12), (0.01, 15), (0.03, 18)]:
            completed = add_temporal_candidate(
                unit(0, offset), 0.86, frame_idx, incoming, window, bank
            )
        self.assertTrue(completed)
        self.assertEqual(len(bank), 2)

        selected = selected_temporal_candidates(bank)
        self.assertEqual(len(selected), 2)
        c_embeddings = [item["embedding"] for item in selected]

        client = Mock()
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS, mid=201)
        with patch.object(node_c, "candidate_reference", return_value=reference), \
             patch.object(node_c, "save_match_capture", return_value="capture.jpg"), \
             patch.object(node_c, "append_jsonl"), \
             patch.object(node_c, "append_csv"), \
             patch.object(node_c, "mark_passed"), \
             patch.object(node_c, "log_revisit_event"):
            published = node_c.publish_passage(
                client, 42, candidate_id, c_embeddings,
                np.zeros((8, 8, 3), dtype=np.uint8), 0.87, 0.89,
                observed, selected, rejection_is_final=False,
            )
        self.assertTrue(published)
        client.publish.assert_called_once()
        wire_payload = json.loads(client.publish.call_args.args[1])
        self.assertEqual(wire_payload["journey_id"], candidate_id)
        self.assertEqual(wire_payload["gallery_count"], 3)  # 1 A + 2 C
        self.assertGreaterEqual(wire_payload["best_score"], 0.75)
        self.assertGreaterEqual(wire_payload["topk_score"], 0.68)
        self.assertGreaterEqual(wire_payload["combined_score"], 0.72)
        self.assertEqual(wire_payload["consistency_count"], 2)

    def test_baseline_j000010_strong_match_maintains_passage_success(self) -> None:
        """B. J000010 baseline strong match: PASSAGE success maintained with high qualities and scores."""
        a_emb = unit(0)
        c_emb1 = unit(0, 0.8)
        c_emb2 = unit(0, 0.82)

        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T09:00:00+09:00",
            "embedding_dim": 512, "embedding": a_emb.tolist(), "quality": 0.95,
        }]
        reference = {
            "person_uid": "P-J000010", "entry_timestamp": "2026-08-15T09:00:00+09:00",
            "incoming_gallery": incoming, "a_local_id": 10,
        }
        selected = [
            {"embedding": c_emb1, "quality": 0.949259, "best_score": float(np.dot(c_emb1, a_emb)), "gallery_selected": True},
            {"embedding": c_emb2, "quality": 0.904701, "best_score": float(np.dot(c_emb2, a_emb)), "gallery_selected": True},
        ]
        gallery = [dict(item) for item in incoming]
        gallery.extend([
            {"node_id": "C", "embedding": c_emb1.tolist(), "quality": 0.949259},
            {"node_id": "C", "embedding": c_emb2.tolist(), "quality": 0.904701},
        ])
        diagnostics = calculate_gallery_diagnostics(gallery)
        self.assertGreaterEqual(diagnostics["best_score"], 0.75)
        self.assertGreaterEqual(diagnostics["combined_score"], 0.72)

        client = Mock()
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS, mid=202)
        with patch.object(node_c, "candidate_reference", return_value=reference), \
             patch.object(node_c, "save_match_capture", return_value="capture.jpg"), \
             patch.object(node_c, "append_jsonl"), \
             patch.object(node_c, "append_csv"), \
             patch.object(node_c, "mark_passed"), \
             patch.object(node_c, "log_revisit_event"):
            published = node_c.publish_passage(
                client, 10, "J000010", [c_emb1, c_emb2],
                np.zeros((8, 8, 3), dtype=np.uint8), 0.95, 0.773,
                [], selected, rejection_is_final=False,
            )
        self.assertTrue(published)

    def test_stranger_and_wrong_identity_never_promoted_or_published(self) -> None:
        """C. Stranger / candidate mismatch / below boundary: confirmation samples never promoted or published."""
        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T09:00:00+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.90,
        }]

        # Candidate mismatch: seeds gathered for J-OTHER should not be promoted for J-CONFIRMED
        seeds = [
            {"embedding": unit(0, 0.05), "quality": 0.85, "frame_index": 3,
             "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": "J-OTHER", "score": 0.85},
            {"embedding": unit(0, 0.05), "quality": 0.85, "frame_index": 6,
             "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": "J-OTHER", "score": 0.85},
        ]
        window: list[dict] = []
        bank: list[dict] = []
        observed: list[dict] = []
        promoted, completed = promote_confirmation_observations(
            "J-CONFIRMED", incoming, seeds, window, bank, observed
        )
        self.assertEqual(len(promoted), 0)
        self.assertFalse(completed)
        self.assertEqual(len(bank), 0)
        self.assertEqual(len(observed), 0)

        # Below-boundary similarity: e.g. similarity 0.707 < 0.75 best score fails validation
        c_low = unit(0, 1.0)
        gallery = [
            {"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.90},
            {"node_id": "C", "embedding": c_low.tolist(), "quality": 0.85},
            {"node_id": "C", "embedding": c_low.tolist(), "quality": 0.85},
        ]
        accepted, reason, _ = validate_c_passage_evidence(gallery)
        self.assertFalse(accepted)
        self.assertIn(reason, ["REJECTED_BEST_SCORE", "REJECTED_COMBINED_SCORE"])

    def test_track_id_switch_strictly_isolates_evidence(self) -> None:
        """D. Track ID switch: observations from different local tracks are never mixed."""
        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T09:00:00+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.90,
        }]
        tentative_obs: dict[int, list[dict]] = {
            10: [
                {"embedding": unit(0, 0.01), "quality": 0.85, "frame_index": 3,
                 "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": "J-1", "score": 0.88},
            ],
            11: [
                {"embedding": unit(0, 0.05), "quality": 0.86, "frame_index": 6,
                 "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": "J-2", "score": 0.84},
            ],
        }
        window10: list[dict] = []
        bank10: list[dict] = []
        observed10: list[dict] = []
        promoted10, _ = promote_confirmation_observations(
            "J-1", incoming, tentative_obs[10], window10, bank10, observed10
        )
        self.assertEqual(len(promoted10), 1)
        self.assertEqual(promoted10[0]["candidate_id"], "J-1")

        window11: list[dict] = []
        bank11: list[dict] = []
        observed11: list[dict] = []
        promoted11, _ = promote_confirmation_observations(
            "J-2", incoming, tentative_obs[11], window11, bank11, observed11
        )
        self.assertEqual(len(promoted11), 1)
        self.assertEqual(promoted11[0]["candidate_id"], "J-2")

        self.assertNotEqual(observed10[0]["quality"], observed11[0]["quality"])

    def test_low_quality_frame_is_never_used_as_seed_evidence(self) -> None:
        """E. Low quality frame (< 0.70): prohibited from seed evidence."""
        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T09:00:00+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.90,
        }]
        candidate_id = "J-LOW-1"
        seeds = [
            {"embedding": unit(0, 0.01), "quality": 0.699999, "frame_index": 3,
             "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": candidate_id, "score": 0.88},
            {"embedding": unit(0, 0.02), "quality": 0.65, "frame_index": 6,
             "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": candidate_id, "score": 0.88},
            {"embedding": unit(0, 0.03), "quality": 0.75, "frame_index": 9,
             "crop": np.zeros((8, 8, 3), dtype=np.uint8), "candidate_id": candidate_id, "score": 0.88},
        ]
        window: list[dict] = []
        bank: list[dict] = []
        observed: list[dict] = []
        promoted, completed = promote_confirmation_observations(
            candidate_id, incoming, seeds, window, bank, observed
        )
        self.assertEqual(len(promoted), 1)
        self.assertEqual(promoted[0]["quality"], 0.75)
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]["quality"], 0.75)
        self.assertFalse(completed)

    def test_partial_temporal_window_finalization_on_track_lost(self) -> None:
        """Partial window finalization: 2 valid observations in window form Window 2 on Track Lost."""
        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T09:00:00+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.90,
        }]
        bank: list[dict] = [{
            "embedding": unit(0, 0.01), "quality": 0.85, "best_score": 0.90,
            "frame_index": 9, "window_start_frame": 3, "window_end_frame": 9,
            "gallery_selected": True,
        }]
        window: list[dict] = [
            {"embedding": unit(0, 0.02), "quality": 0.84, "frame_index": 12},
            {"embedding": unit(0, 0.03), "quality": 0.86, "frame_index": 15},
        ]
        finalized = finalize_partial_temporal_window(incoming, window, bank)
        self.assertTrue(finalized)
        self.assertEqual(len(window), 0)
        self.assertEqual(len(bank), 2)

        selected = selected_temporal_candidates(bank)
        self.assertEqual(len(selected), 2)
        self.assertTrue(all(item["quality"] >= C_PASSAGE_MIN_QUALITY for item in selected))

    def test_duplicate_same_frame_never_counts_as_multiple_gallery(self) -> None:
        """Case B: Passing the same frame index repeatedly never counts as multiple distinct observations or completes windows."""
        incoming = [{"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.90}]
        window: list[dict] = []
        bank: list[dict] = []

        # Try adding the exact same frame index 3 times
        added1 = add_temporal_candidate(unit(0, 0.01), 0.85, 10, incoming, window, bank)
        self.assertFalse(added1)
        self.assertEqual(len(window), 1)

        # 2nd attempt with same frame_index 10 is rejected
        added2 = add_temporal_candidate(unit(0, 0.01), 0.85, 10, incoming, window, bank)
        self.assertFalse(added2)
        self.assertEqual(len(window), 1)

        # In promote_confirmation_observations: duplicates with same frame_index are ignored
        observed: list[dict] = []
        seeds = [
            {"embedding": unit(0, 0.01), "quality": 0.85, "frame_index": 5, "candidate_id": "J-DUP", "score": 0.88},
            {"embedding": unit(0, 0.01), "quality": 0.85, "frame_index": 5, "candidate_id": "J-DUP", "score": 0.88},
        ]
        promoted, completed = promote_confirmation_observations("J-DUP", incoming, seeds, window, bank, observed)
        self.assertEqual(len(promoted), 1)
        self.assertEqual(len(observed), 1)

    def test_insufficient_evidence_only_one_sample_rejects_passage(self) -> None:
        """Case F: If only 1 valid observation exists, validate_c_passage_evidence rejects and publish_passage fails."""
        incoming = [{
            "node_id": "A", "captured_at": "2026-08-15T09:00:00+09:00",
            "embedding_dim": 512, "embedding": unit(0).tolist(), "quality": 0.90,
        }]
        reference = {
            "person_uid": "P-ONLY-ONE", "entry_timestamp": "2026-08-15T09:00:00+09:00",
            "incoming_gallery": incoming, "a_local_id": 9,
        }
        single_gallery = [
            {"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.90},
            {"node_id": "C", "embedding": unit(0, 0.01).tolist(), "quality": 0.85},
        ]
        accepted, reason, diagnostics = validate_c_passage_evidence(single_gallery)
        self.assertFalse(accepted)
        self.assertEqual(reason, "INSUFFICIENT_QUALITY")
        self.assertIsNone(diagnostics)

        client = Mock()
        selected = [{"embedding": unit(0, 0.01), "quality": 0.85, "best_score": 0.95, "gallery_selected": True}]
        with patch.object(node_c, "candidate_reference", return_value=reference), \
             patch.object(node_c, "append_jsonl"), \
             patch.object(node_c, "log_revisit_event"):
            published = node_c.publish_passage(
                client, 99, "J-ONLY-ONE", [unit(0, 0.01)],
                np.zeros((8, 8, 3), dtype=np.uint8), 0.85, 0.95,
                [], selected, rejection_is_final=True,
            )
        self.assertFalse(published)

    def test_partial_temporal_window_rejects_empty_or_subthreshold_window(self) -> None:
        """Partial window finalization: returns False if empty or observations below quality threshold."""
        incoming = [{"node_id": "A", "embedding": unit(0).tolist(), "quality": 0.90}]
        bank: list[dict] = []
        window: list[dict] = []
        self.assertFalse(finalize_partial_temporal_window(incoming, window, bank))
        self.assertEqual(len(bank), 0)

        window_low = [
            {"embedding": unit(0), "quality": 0.69, "frame_index": 12},
            {"embedding": unit(0), "quality": 0.50, "frame_index": 15},
        ]
        self.assertFalse(finalize_partial_temporal_window(incoming, window_low, bank))
        self.assertEqual(len(bank), 0)


if __name__ == "__main__":
    unittest.main()
