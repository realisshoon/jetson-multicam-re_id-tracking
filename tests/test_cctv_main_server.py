from __future__ import annotations

import gc
import hashlib
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from cctv_main import main_server


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "team_a"


class FakePublishResult:
    rc = 0


class FakeMqttClient:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any], int, bool]] = []
        self.subscriptions: list[tuple[str, int]] = []

    def publish(
        self,
        topic: str,
        payload: str,
        qos: int,
        retain: bool,
    ) -> FakePublishResult:
        self.messages.append(
            (topic, json.loads(payload), qos, retain)
        )
        return FakePublishResult()

    def subscribe(self, topic: str, qos: int) -> None:
        self.subscriptions.append((topic, qos))

    def payloads(self, topic: str) -> list[dict[str, Any]]:
        return [
            payload
            for published_topic, payload, _, _ in self.messages
            if published_topic == topic
        ]


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads(
        (FIXTURE_ROOT / name).read_text(encoding="utf-8")
    )


class CctvMainServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = main_server.DB_PATH
        self.original_capture_settings = main_server.CAPTURE_CACHE_SETTINGS
        self.original_enable_camera_c = main_server.ENABLE_CAMERA_C
        self.original_d_arrival_rx_log_dir = main_server.D_ARRIVAL_RX_LOG_DIR
        # Most legacy tests explicitly cover the long-standing A→B→D mode.
        # C-enabled behavior is enabled only in its dedicated tests below.
        main_server.ENABLE_CAMERA_C = False
        main_server.CAPTURE_CACHE_SETTINGS = replace(
            self.original_capture_settings, enabled=False
        )
        main_server.DB_PATH = Path(self.temp_dir.name) / "main_server.db"
        main_server.D_ARRIVAL_RX_LOG_DIR = Path(self.temp_dir.name) / "logs"
        main_server.initialize_database()
        self.client = FakeMqttClient()

    def tearDown(self) -> None:
        main_server.DB_PATH = self.original_db_path
        main_server.CAPTURE_CACHE_SETTINGS = self.original_capture_settings
        main_server.ENABLE_CAMERA_C = self.original_enable_camera_c
        main_server.D_ARRIVAL_RX_LOG_DIR = self.original_d_arrival_rx_log_dir
        # sqlite3 context managers commit/rollback but do not close; collect
        # short-lived Reference-handler connections before Windows cleanup.
        gc.collect()
        self.temp_dir.cleanup()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(main_server.DB_PATH)
        connection.row_factory = sqlite3.Row
        return connection

    def fresh_entry(
        self,
        request_id: str,
        local_track_id: int,
    ) -> dict[str, Any]:
        entry = load_fixture("a_entry.json")
        entry["request_id"] = request_id
        entry["local_track_id"] = local_track_id
        entry["timestamp"] = main_server.now_iso()
        return entry

    @staticmethod
    def add_consistent_body_frames(
        entry: dict[str, Any], count: int = 2
    ) -> dict[str, Any]:
        embedding = list(entry["embedding"])
        entry.update(
            {
                "body_count": count,
                "body_embedding_dim": len(embedding),
                "body_embeddings": [list(embedding) for _ in range(count)],
                "body_qualities": [0.95] * count,
                "body_confidences": [0.99] * count,
                "body_frame_indices": list(range(count)),
            }
        )
        return entry

    def complete_journey(
        self,
        journey_id: str,
        person_uid: str,
    ) -> None:
        with closing(self.connect()) as connection:
            gallery_row = connection.execute(
                "SELECT embedding, embedding_dim FROM journey_gallery "
                "WHERE journey_id = ? AND modality = 'BODY' "
                "ORDER BY gallery_id LIMIT 1",
                (journey_id,),
            ).fetchone()
        same_embedding = (
            main_server.blob_to_embedding(
                gallery_row["embedding"],
                int(gallery_row["embedding_dim"]),
            ).tolist()
            if gallery_row is not None
            else load_fixture("a_entry.json")["embedding"]
        )
        passage = load_fixture("b_passage.json")
        passage["journey_id"] = journey_id
        passage["person_uid"] = person_uid
        passage["b_passage_timestamp"] = main_server.now_iso()
        for item in passage["gallery"]:
            item["embedding"] = same_embedding
        main_server.handle_passage(self.client, passage, "B")

        arrival = self.arrival_for(journey_id, person_uid)
        arrival["embedding"] = same_embedding
        main_server.handle_d_arrival(self.client, arrival)

    def passage_for(
        self,
        journey_id: str,
        person_uid: str,
        node_id: str,
    ) -> dict[str, Any]:
        passage = load_fixture("b_passage.json")
        passage["journey_id"] = journey_id
        passage["person_uid"] = person_uid
        passage["global_person_id"] = person_uid
        passage.pop("b_passage_timestamp", None)
        passage[f"{node_id.lower()}_passage_timestamp"] = (
            main_server.now_iso()
        )
        with closing(self.connect()) as connection:
            a_row = connection.execute(
                "SELECT embedding,embedding_dim FROM journey_gallery "
                "WHERE journey_id=? AND node_id='A' AND modality='BODY' "
                "ORDER BY gallery_id LIMIT 1",
                (journey_id,),
            ).fetchone()
        a_embedding = (
            main_server.blob_to_embedding(
                a_row["embedding"], int(a_row["embedding_dim"])
            ).tolist()
            if a_row is not None
            else next(
                list(item["embedding"])
                for item in passage["gallery"]
                if item.get("node_id") == "A"
            )
        )
        for item in passage["gallery"]:
            if item.get("node_id") == "B":
                item["node_id"] = node_id
                if node_id == "C":
                    item["embedding"] = list(a_embedding)
                    item["quality"] = 0.95
        if node_id == "C":
            passage["similarity"] = 0.95
            passage["quality"] = 0.95
        return passage

    def arrival_for(
        self,
        journey_id: str,
        person_uid: str,
    ) -> dict[str, Any]:
        arrival = load_fixture("d_arrival.json")
        arrival["journey_id"] = journey_id
        arrival["person_uid"] = person_uid
        arrival["global_person_id"] = person_uid
        with closing(self.connect()) as connection:
            passage_at = connection.execute(
                "SELECT passage_at FROM journeys WHERE journey_id = ?",
                (journey_id,),
            ).fetchone()[0]
        passage = datetime.fromisoformat(str(passage_at))
        numeric_journey_id = int("".join(filter(str.isdigit, journey_id)) or 0)
        arrival.update(
            {
                "d_local_track_id": 10_000 + numeric_journey_id,
                "local_track_id": 10_000 + numeric_journey_id,
                "passage_timestamp": passage.isoformat(timespec="seconds"),
                "candidate_received_at": (
                    passage + timedelta(seconds=1)
                ).isoformat(timespec="seconds"),
                "d_track_first_seen_at": (
                    passage + timedelta(seconds=2)
                ).isoformat(timespec="seconds"),
                "d_arrival_timestamp": (
                    passage + timedelta(seconds=10)
                ).isoformat(timespec="seconds"),
                "passage_to_d_duration_seconds": 10.0,
                "confirmation_sample_count": 3,
                "confirmation_pass_count": 3,
                "best_journey_score": 0.88,
                "second_journey_score": 0.78,
                "journey_margin": 0.10,
                "eligibility_reason": "MULTIFRAME_CONFIRMED",
                "status": "COMPLETED",
            }
        )
        return arrival

    def waiting_d_journey(
        self,
        request_id: str,
        local_track_id: int,
        node_id: str = "B",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry(request_id, local_track_id),
        )
        response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        main_server.handle_passage(
            self.client,
            self.passage_for(
                response["journey_id"],
                response["person_uid"],
                node_id,
            ),
            node_id,
        )
        return response, self.arrival_for(
            response["journey_id"], response["person_uid"]
        )

    @staticmethod
    def axis_embedding(dimension: int, index: int) -> list[float]:
        embedding = [0.0] * dimension
        embedding[index] = 1.0
        return embedding

    def multimodal_entry(
        self,
        request_id: str,
        local_track_id: int,
        *,
        body_embeddings: list[list[float]] | None = None,
        face_embeddings: list[list[float]] | None = None,
        face_available: bool = True,
    ) -> dict[str, Any]:
        # 같은 track에서 추출한 정상 다중 프레임은 같은 identity 축에
        # 모이도록 만든다. 서로 직교하는 벡터는 일관성 실패 fixture에서
        # 명시적으로 전달한다.
        body_embeddings = body_embeddings or [
            self.axis_embedding(512, 0)
            for _ in range(3)
        ]
        face_embeddings = face_embeddings or [
            self.axis_embedding(128, 0)
            for _ in range(3)
        ]
        entry = self.fresh_entry(request_id, local_track_id)
        entry.update(
            {
                "embedding": body_embeddings[0],
                "embedding_dim": 512,
                "quality": 0.95,
                "capture_path": f"/tmp/{request_id}-legacy.jpg",
                "body_count": len(body_embeddings),
                "body_embedding_dim": 512,
                "body_embeddings": body_embeddings,
                "body_qualities": [0.95, 0.9, 0.85][
                    : len(body_embeddings)
                ],
                "body_confidences": [0.99, 0.98, 0.97][
                    : len(body_embeddings)
                ],
                "body_frame_indices": [10, 20, 30][
                    : len(body_embeddings)
                ],
                "body_capture_paths": [
                    f"/tmp/{request_id}-body-{index}.jpg"
                    for index in range(len(body_embeddings))
                ],
                "face_available": face_available,
                "face_embedding_dim": 128,
                "face_embeddings": face_embeddings,
                "face_qualities": [0.92, 0.88, 0.84][
                    : len(face_embeddings)
                ],
                "face_confidences": [0.96, 0.94, 0.91][
                    : len(face_embeddings)
                ],
                "face_frontal_scores": [0.9, 0.85, 0.8][
                    : len(face_embeddings)
                ],
                "face_sharpness": [120.0, 110.0, 100.0][
                    : len(face_embeddings)
                ],
                "face_capture_paths": [
                    f"/tmp/{request_id}-face-{index}.jpg"
                    for index in range(len(face_embeddings))
                ],
            }
        )
        return entry

    def create_review_scenario(
        self,
        prefix: str,
        *,
        complete_review: bool = True,
    ) -> tuple[dict[str, Any], dict[str, Any], sqlite3.Row]:
        known_entry = self.multimodal_entry(
            f"{prefix}-KNOWN",
            8001,
        )
        main_server.handle_a_entry(self.client, known_entry)
        known_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.complete_journey(
            known_response["journey_id"],
            known_response["person_uid"],
        )

        related_face = self.axis_embedding(128, 0)
        related_face[0] = 0.9
        related_face[10] = 0.4
        review_entry = self.multimodal_entry(
            f"{prefix}-REVIEW",
            8002,
            body_embeddings=[
                self.axis_embedding(512, 100)
                for _ in range(3)
            ],
            face_embeddings=[related_face],
        )
        main_server.handle_a_entry(self.client, review_entry)
        review_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        if complete_review:
            self.complete_pending_as_manual(review_response)

        with closing(self.connect()) as connection:
            review_case = connection.execute(
                "SELECT * FROM review_cases WHERE journey_id = ?",
                (review_response["journey_id"],),
            ).fetchone()
        if review_case is None:
            self.fail("REVIEW_REQUIRED Journey의 review_case가 생성되지 않음")
        return known_response, review_response, review_case

    def complete_pending_as_manual(
        self,
        response: dict[str, Any],
        middle_node: str = "B",
    ) -> None:
        ambiguous_route_embedding = self.axis_embedding(512, 0)
        ambiguous_route_embedding[0] = 0.73
        ambiguous_route_embedding[200] = (1.0 - 0.73**2) ** 0.5
        passage = self.passage_for(
            response["journey_id"],
            response["person_uid"],
            middle_node,
        )
        for item in passage["gallery"]:
            item["embedding"] = ambiguous_route_embedding
        main_server.handle_passage(self.client, passage, middle_node)
        arrival = self.arrival_for(
            response["journey_id"],
            response["person_uid"],
        )
        arrival["embedding"] = ambiguous_route_embedding
        arrival["embedding_dim"] = 512
        main_server.handle_d_arrival(self.client, arrival)

    def create_pending_identity_scenario(
        self,
        prefix: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        known_entry = self.multimodal_entry(
            f"{prefix}-KNOWN",
            8201,
        )
        main_server.handle_a_entry(self.client, known_entry)
        known = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.complete_journey(known["journey_id"], known["person_uid"])

        ambiguous = self.axis_embedding(512, 0)
        ambiguous[0] = 0.71
        ambiguous[200] = (1.0 - 0.71**2) ** 0.5
        pending_entry = self.multimodal_entry(
            f"{prefix}-PENDING",
            8202,
            body_embeddings=[ambiguous, ambiguous, ambiguous],
            face_available=False,
        )
        main_server.handle_a_entry(self.client, pending_entry)
        pending = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        historical_entry_at = "2026-08-11T13:00:00+09:00"
        with closing(self.connect()) as connection:
            connection.execute(
                "UPDATE journeys SET entry_at = ? WHERE journey_id = ?",
                (historical_entry_at, pending["journey_id"]),
            )
            connection.commit()
        pending["timestamp"] = historical_entry_at
        pending["entry_timestamp"] = historical_entry_at
        return known, pending

    def complete_pending_with_evidence(
        self,
        response: dict[str, Any],
        evidence: list[float],
        middle_node: str,
    ) -> dict[str, Any]:
        passage = self.passage_for(
            response["journey_id"],
            response["person_uid"],
            middle_node,
        )
        for item in passage["gallery"]:
            item["embedding"] = evidence
        main_server.handle_passage(self.client, passage, middle_node)
        arrival = self.arrival_for(
            response["journey_id"],
            response["person_uid"],
        )
        arrival["embedding"] = evidence
        arrival["embedding_dim"] = 512
        main_server.handle_d_arrival(self.client, arrival)
        return arrival

    def start_timing_journey(
        self,
        request_id: str,
        entry_timestamp: str | None = None,
    ) -> dict[str, Any]:
        entry = self.fresh_entry(request_id, 9001)
        main_server.handle_a_entry(
            self.client,
            entry,
        )
        response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        if entry_timestamp is not None:
            # Timeline tests intentionally use deterministic historical times.
            # Create the active candidate with a live clock first, then replace
            # only the stored timeline origin so TTL enforcement does not turn
            # these fixtures into stale-candidate tests.
            with closing(self.connect()) as connection:
                connection.execute(
                    "UPDATE journeys SET entry_at = ? WHERE journey_id = ?",
                    (entry_timestamp, response["journey_id"]),
                )
                connection.commit()
            response["timestamp"] = entry_timestamp
            response["entry_timestamp"] = entry_timestamp
        return response

    def advance_timing_journey(
        self,
        response: dict[str, Any],
        node_id: str,
    ) -> None:
        main_server.handle_passage(
            self.client,
            self.passage_for(
                response["journey_id"],
                response["person_uid"],
                node_id,
            ),
            node_id,
        )

    def complete_timing_journey(
        self,
        response: dict[str, Any],
    ) -> None:
        main_server.handle_d_arrival(
            self.client,
            self.arrival_for(
                response["journey_id"],
                response["person_uid"],
            ),
        )

    @staticmethod
    def timing_payload(
        response: dict[str, Any],
        node_id: str,
        entered_at: str,
        matched_at: str | None,
        exited_at: str | None,
        *,
        local_track_id: int = 52,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "event": "NODE_TIMING",
            "node_id": node_id,
            "person_uid": response["person_uid"],
            "global_person_id": response["person_uid"],
            "journey_id": response["journey_id"],
            "local_track_id": local_track_id,
            "entered_at": entered_at,
            "matched_at": matched_at,
            "exited_at": exited_at,
            "dwell_seconds": 999.0,
            "exit_reason": "TRACK_LOST",
        }

    def test_new_person_creates_first_visit_and_journey(self) -> None:
        entry = self.fresh_entry("REQ-NEW-1", 101)

        main_server.handle_a_entry(self.client, entry)
        response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(response["person_status"], "NEW")
        self.assertEqual(response["visit_count"], 1)
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_B)),
            1,
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)),
            0,
        )

        with closing(self.connect()) as connection:
            person = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (response["person_uid"],),
            ).fetchone()
            journey = connection.execute(
                "SELECT visit_no, status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()

        self.assertEqual(person["visit_count"], 1)
        self.assertEqual(journey["visit_no"], 1)
        self.assertEqual(journey["status"], "WAITING_B_OR_C")

    def test_same_person_waiting_b_reuses_active_journey(self) -> None:
        first_entry = self.fresh_entry("REQ-WAITING-B-1", 201)
        main_server.handle_a_entry(self.client, first_entry)
        first_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        redetection = self.add_consistent_body_frames(
            self.fresh_entry("REQ-WAITING-B-2", 202)
        )
        main_server.handle_a_entry(self.client, redetection)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        main_server.handle_a_entry(self.client, redetection)
        duplicate_redetection_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(
            second_response["journey_id"],
            first_response["journey_id"],
        )
        self.assertEqual(second_response["person_uid"], first_response["person_uid"])
        self.assertEqual(second_response["visit_count"], 1)
        self.assertEqual(
            duplicate_redetection_response["journey_id"],
            second_response["journey_id"],
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_B)),
            3,
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)),
            0,
        )

        with closing(self.connect()) as connection:
            journey_count = connection.execute(
                "SELECT COUNT(*) FROM journeys"
            ).fetchone()[0]
            visit_count = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (first_response["person_uid"],),
            ).fetchone()[0]
            request_count = connection.execute(
                "SELECT COUNT(*) FROM a_entry_requests"
            ).fetchone()[0]

        self.assertEqual(journey_count, 1)
        self.assertEqual(visit_count, 1)
        self.assertEqual(request_count, 2)

    def test_same_person_waiting_d_reuses_active_journey(self) -> None:
        entry = self.fresh_entry("REQ-WAITING-D-1", 301)
        main_server.handle_a_entry(self.client, entry)
        first_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        passage = load_fixture("b_passage.json")
        passage["journey_id"] = first_response["journey_id"]
        passage["person_uid"] = first_response["person_uid"]
        passage["b_passage_timestamp"] = main_server.now_iso()
        main_server.handle_passage(self.client, passage, "B")

        redetection = self.add_consistent_body_frames(
            self.fresh_entry("REQ-WAITING-D-2", 302)
        )
        main_server.handle_a_entry(self.client, redetection)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(
            second_response["journey_id"],
            first_response["journey_id"],
        )
        self.assertEqual(second_response["person_uid"], first_response["person_uid"])
        self.assertEqual(second_response["stage"], "WAITING_D")
        self.assertEqual(second_response["visit_count"], 1)
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_B)),
            1,
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)),
            0,
        )

        with closing(self.connect()) as connection:
            journey_count = connection.execute(
                "SELECT COUNT(*) FROM journeys"
            ).fetchone()[0]
            visit_count = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (first_response["person_uid"],),
            ).fetchone()[0]

        self.assertEqual(journey_count, 1)
        self.assertEqual(visit_count, 1)

    def test_completed_person_reentry_creates_second_visit(self) -> None:
        first_entry = self.fresh_entry("REQ-VISIT-1", 401)
        main_server.handle_a_entry(self.client, first_entry)
        first_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.complete_journey(
            first_response["journey_id"],
            first_response["person_uid"],
        )

        second_entry = self.add_consistent_body_frames(
            self.fresh_entry("REQ-VISIT-2", 402)
        )
        main_server.handle_a_entry(self.client, second_entry)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(
            second_response["person_uid"],
            first_response["person_uid"],
        )
        self.assertNotEqual(
            second_response["journey_id"],
            first_response["journey_id"],
        )
        self.assertEqual(second_response["person_status"], "RETURNING")
        self.assertEqual(second_response["visit_count"], 2)
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_B)),
            2,
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)),
            0,
        )

        with closing(self.connect()) as connection:
            journeys = connection.execute(
                """
                SELECT journey_id, visit_no, status
                FROM journeys
                ORDER BY journey_id
                """
            ).fetchall()
            visit_count = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (first_response["person_uid"],),
            ).fetchone()[0]

        self.assertEqual(len(journeys), 2)
        self.assertEqual(journeys[0]["visit_no"], 1)
        self.assertEqual(journeys[0]["status"], "COMPLETED")
        self.assertEqual(journeys[1]["visit_no"], 2)
        self.assertEqual(journeys[1]["status"], "WAITING_B_OR_C")
        self.assertEqual(visit_count, 2)

    def test_current_a_b_d_payloads_complete_the_journey(self) -> None:
        entry = load_fixture("a_entry.json")
        entry["timestamp"] = main_server.now_iso()
        entry["face_available"] = True
        entry["face_capture_paths"] = ["/tmp/face-a.jpg"]

        main_server.handle_a_entry(self.client, entry)

        self.assertEqual(
            [topic for topic, _, _, _ in self.client.messages],
            [
                main_server.TOPIC_A_ENTRY_RESPONSE,
                main_server.TOPIC_CANDIDATE_B,
            ],
        )
        b_candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_B
        )[-1]
        self.assertEqual(b_candidate["stage"], "WAITING_B_OR_C")
        self.assertEqual(b_candidate["route"], ["A"])
        self.assertEqual(b_candidate["gallery_count"], 1)
        self.assertEqual(b_candidate["gallery"][0]["node_id"], "A")
        self.assertEqual(b_candidate["gallery"][0]["embedding_dim"], 512)
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_C), [])
        self.assertEqual(
            b_candidate["global_person_id"],
            b_candidate["person_uid"],
        )

        passage = load_fixture("b_passage.json")
        passage["journey_id"] = b_candidate["journey_id"]
        passage["person_uid"] = b_candidate["person_uid"]
        passage["b_passage_timestamp"] = main_server.now_iso()
        main_server.handle_passage(self.client, passage, "B")

        d_candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_D
        )[-1]
        self.assertEqual(d_candidate["stage"], "WAITING_D")
        self.assertEqual(d_candidate["route"], ["A", "B"])
        self.assertEqual(d_candidate["middle_node"], "B")
        self.assertEqual(
            d_candidate["global_person_id"],
            d_candidate["person_uid"],
        )
        self.assertEqual(
            [item["node_id"] for item in d_candidate["gallery"]],
            ["A", "B", "B"],
        )

        arrival = self.arrival_for(
            b_candidate["journey_id"], b_candidate["person_uid"]
        )
        main_server.handle_d_arrival(self.client, arrival)

        completed = self.client.payloads(
            main_server.TOPIC_JOURNEY_COMPLETED
        )[-1]
        self.assertEqual(completed["event"], "JOURNEY_COMPLETED")
        self.assertEqual(completed["status"], "COMPLETED")
        self.assertEqual(completed["route"], ["A", "B", "D"])
        self.assertEqual(completed["middle_node"], "B")
        self.assertEqual(
            completed["global_person_id"],
            completed["person_uid"],
        )
        self.assertEqual(completed["journey_id"], b_candidate["journey_id"])

        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT * FROM journeys WHERE journey_id = ?",
                (b_candidate["journey_id"],),
            ).fetchone()
            raw_entry = connection.execute(
                """
                SELECT payload_json
                FROM journey_events
                WHERE journey_id = ? AND event_type = 'ENTRY'
                """,
                (b_candidate["journey_id"],),
            ).fetchone()

        self.assertEqual(journey["status"], "COMPLETED")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "B", "D"])
        self.assertTrue(json.loads(raw_entry["payload_json"])["face_available"])

    def test_d_arrival_rejects_track_seen_before_central_passage(self) -> None:
        response, arrival = self.waiting_d_journey(
            "A_D_GUARD_FIRST_SEEN", 701
        )
        passage = datetime.fromisoformat(arrival["passage_timestamp"])
        arrival["d_track_first_seen_at"] = (
            passage - timedelta(seconds=2)
        ).isoformat(timespec="seconds")

        result = main_server.handle_d_arrival(self.client, arrival)

        self.assertFalse(result["accepted"])
        self.assertIn("D_TRACK_SEEN_BEFORE_PASSAGE", result["reason_codes"])
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
            rejected = connection.execute(
                "SELECT event_type, payload_json FROM journey_events "
                "WHERE journey_id = ? AND event_type = 'ARRIVAL_REJECTED'",
                (response["journey_id"],),
            ).fetchone()
        self.assertEqual(journey["status"], "WAITING_D")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "B"])
        self.assertIsNotNone(rejected)
        self.assertIn(
            "D_TRACK_SEEN_BEFORE_PASSAGE",
            json.loads(rejected["payload_json"])["reason_codes"],
        )
        self.assertEqual(
            self.client.payloads(main_server.TOPIC_D_JOURNEY_CONTROL), []
        )

    def test_d_arrival_rejects_zero_passage_duration(self) -> None:
        response, arrival = self.waiting_d_journey(
            "A_D_GUARD_ZERO_DURATION", 702
        )
        passage = arrival["passage_timestamp"]
        arrival.update(
            {
                "candidate_received_at": passage,
                "d_track_first_seen_at": passage,
                "d_arrival_timestamp": passage,
                "passage_to_d_duration_seconds": 0,
            }
        )

        result = main_server.handle_d_arrival(self.client, arrival)

        self.assertFalse(result["accepted"])
        self.assertIn("NON_POSITIVE_TRAVEL_TIME", result["reason_codes"])
        self.assertIn(
            "NON_POSITIVE_REPORTED_TRAVEL_TIME", result["reason_codes"]
        )
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
        self.assertEqual(journey["status"], "WAITING_D")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "B"])

    def test_d_arrival_rejects_arrival_after_maximum_and_ttl(self) -> None:
        response, arrival = self.waiting_d_journey(
            "A_D_GUARD_TOO_LATE", 703
        )
        passage = datetime.fromisoformat(arrival["passage_timestamp"])
        arrival.update(
            {
                "d_arrival_timestamp": (
                    passage + timedelta(seconds=301)
                ).isoformat(timespec="seconds"),
                "passage_to_d_duration_seconds": 301,
            }
        )

        result = main_server.handle_d_arrival(self.client, arrival)

        self.assertFalse(result["accepted"])
        self.assertIn("TRAVEL_TIME_ABOVE_MAXIMUM", result["reason_codes"])
        self.assertIn("WAITING_D_TTL_EXCEEDED", result["reason_codes"])
        with closing(self.connect()) as connection:
            status = connection.execute(
                "SELECT status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(status, "WAITING_D")

    def test_d_arrival_rejects_timezone_mismatch(self) -> None:
        _, arrival = self.waiting_d_journey(
            "A_D_GUARD_TIMEZONE", 710
        )
        for field_name in (
            "passage_timestamp",
            "candidate_received_at",
            "d_track_first_seen_at",
            "d_arrival_timestamp",
        ):
            arrival[field_name] = datetime.fromisoformat(
                arrival[field_name]
            ).astimezone(timezone.utc).isoformat(timespec="seconds")

        result = main_server.handle_d_arrival(self.client, arrival)

        self.assertFalse(result["accepted"])
        self.assertIn(
            "TIMEZONE_MISMATCH_D_ARRIVAL_TIMESTAMP",
            result["reason_codes"],
        )
        self.assertIn(
            "TIMEZONE_MISMATCH_PASSAGE_TIMESTAMP",
            result["reason_codes"],
        )

    def test_d_arrival_rejects_single_frame_confirmation(self) -> None:
        _, arrival = self.waiting_d_journey(
            "A_D_GUARD_SINGLE_FRAME", 704
        )
        arrival["confirmation_sample_count"] = 1
        arrival["confirmation_pass_count"] = 1

        result = main_server.handle_d_arrival(self.client, arrival)

        self.assertFalse(result["accepted"])
        self.assertIn(
            "INSUFFICIENT_CONFIRMATION_SAMPLES", result["reason_codes"]
        )
        self.assertIn(
            "INSUFFICIENT_CONFIRMATION_PASSES", result["reason_codes"]
        )

    def test_d_arrival_rejects_insufficient_journey_margin(self) -> None:
        _, arrival = self.waiting_d_journey(
            "A_D_GUARD_MARGIN", 705
        )
        arrival.update(
            {
                "best_journey_score": 0.80,
                "second_journey_score": 0.78,
                "journey_margin": 0.02,
            }
        )

        result = main_server.handle_d_arrival(self.client, arrival)

        self.assertFalse(result["accepted"])
        self.assertIn("INSUFFICIENT_JOURNEY_MARGIN", result["reason_codes"])
        self.assertIn(
            "INSUFFICIENT_CALCULATED_JOURNEY_MARGIN",
            result["reason_codes"],
        )

    def test_valid_a_c_d_eligible_new_entry_completes_exactly_once(self) -> None:
        response, arrival = self.waiting_d_journey(
            "REQ-D-ELIGIBLE-NEW-ENTRY", 951, "C"
        )
        arrival.update(
            {
                "eligibility_reason": "ELIGIBLE_NEW_ENTRY",
                "confirmation_sample_count": 5,
                "confirmation_pass_count": 4,
                "best_journey_score": 0.803515,
                "second_journey_score": 0.730653,
                "journey_margin": 0.072862,
                "best_similarity": 0.810826,
                "top2_mean": 0.792549,
                "combined_score": 0.803515,
            }
        )

        first = main_server.handle_d_arrival(self.client, arrival)
        second = main_server.handle_d_arrival(self.client, dict(arrival))

        self.assertTrue(first["accepted"])
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["accepted"])
        self.assertTrue(second["duplicate"])
        eligibility = next(
            item
            for item in first["validation"]["predicates"]
            if item["name"] == "eligibility_reason"
        )
        self.assertTrue(eligibility["pass"])
        self.assertTrue(
            all(item["pass"] for item in first["validation"]["predicates"])
        )
        self.assertEqual(
            first["validation"]["context"]["identity_completion_policy"],
            "CONFIRMED_NEW_COMPLETE_AND_PROMOTE_IF_ALLOWED",
        )
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status,route_json FROM journeys WHERE journey_id=?",
                (response["journey_id"],),
            ).fetchone()
            arrival_events = connection.execute(
                "SELECT COUNT(*) FROM journey_events "
                "WHERE journey_id=? AND event_type='ARRIVAL'",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(journey["status"], "COMPLETED")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "C", "D"])
        self.assertEqual(arrival_events, 1)
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_JOURNEY_COMPLETED)), 1
        )

    def test_historical_d_arrival_profiles_report_specific_checks(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        cases = json.loads(
            (Path(__file__).parent / "fixtures" / "d_arrival_validation_cases.json")
            .read_text(encoding="utf-8")
        )
        for index, case in enumerate(cases, start=20):
            with self.subTest(case_id=case["case_id"]):
                entry = self.fresh_entry(f"REQ-{case['case_id']}", index)
                entry["embedding"] = [1.0 if i == index else 0.0 for i in range(512)]
                main_server.handle_a_entry(self.client, entry)
                response = self.client.payloads(main_server.TOPIC_A_ENTRY_RESPONSE)[-1]
                main_server.handle_passage(
                    self.client,
                    self.passage_for(
                        response["journey_id"], response["person_uid"], "C"
                    ),
                    "C",
                )
                arrival = self.arrival_for(
                    response["journey_id"], response["person_uid"]
                )
                arrival.update(case)
                arrival["arrival_event_id"] = f"ARR-{case['case_id']}"
                result = main_server.handle_d_arrival(self.client, arrival)

                checks = {
                    check["predicate"]: check
                    for check in result["validation"]["predicates"]
                }
                if case["expected_reason"] is None:
                    self.assertTrue(result["accepted"])
                    self.assertTrue(all(check["passed"] for check in checks.values()))
                    self.assertEqual(result["journey_status"], "COMPLETED")
                else:
                    self.assertFalse(result["accepted"])
                    self.assertIn(case["expected_reason"], result["reason_codes"])
                    self.assertTrue(
                        any(
                            case["expected_reason"] in check["failure_codes"]
                            and not check["passed"]
                            for check in checks.values()
                        )
                    )
                    with closing(self.connect()) as connection:
                        connection.execute(
                            "UPDATE journeys SET status='REJECTED' WHERE journey_id=?",
                            (response["journey_id"],),
                        )
                        connection.commit()

    def test_d_mqtt_rx_jsonl_sha_envelope_and_event_id_idempotency(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client, self.fresh_entry("REQ-D-RX-LOG", 990)
        )
        response = self.client.payloads(main_server.TOPIC_A_ENTRY_RESPONSE)[-1]
        main_server.handle_passage(
            self.client,
            self.passage_for(response["journey_id"], response["person_uid"], "C"),
            "C",
        )
        arrival = self.arrival_for(response["journey_id"], response["person_uid"])
        arrival.update(
            {
                "arrival_event_id": "D-ARRIVAL-J000019-001",
                "tracking_person_uid": response["person_uid"],
                "canonical_person_uid": response["canonical_person_uid"],
                "route": ["A", "C", "D"],
                "stage": "COMPLETED",
                "matched_at": arrival["d_track_first_seen_at"],
                "published_at": arrival["d_arrival_timestamp"],
            }
        )
        raw = json.dumps(arrival, ensure_ascii=False, separators=(",", ":")).encode()
        raw_sha256 = hashlib.sha256(raw).hexdigest()
        events: list[tuple[str, dict[str, Any]]] = []

        def capture_log(event: str, **fields: Any) -> None:
            events.append((event, fields))

        with patch.object(main_server, "structured_log", side_effect=capture_log):
            for duplicate in (False, True):
                main_server.process_mqtt_message(
                    self.client,
                    main_server.TOPIC_D_ARRIVAL,
                    raw,
                    qos=1,
                    duplicate=duplicate,
                    received_at="2026-08-14T10:11:12+09:00",
                )

        log_path = main_server.D_ARRIVAL_RX_LOG_DIR / "d_arrival_rx_20260814.jsonl"
        records = [
            json.loads(line)
            for line in log_path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["arrival_event_id"], "D-ARRIVAL-J000019-001")
        self.assertEqual(records[0]["topic"], main_server.TOPIC_D_ARRIVAL)
        self.assertEqual(records[0]["qos"], 1)
        self.assertFalse(records[0]["duplicate"])
        self.assertTrue(records[1]["duplicate"])
        self.assertEqual(records[0]["payload_bytes"], len(raw))
        self.assertEqual(records[0]["raw_sha256"], raw_sha256)
        self.assertEqual(records[0]["raw_payload"].encode(), raw)
        self.assertEqual(records[0]["timestamps"]["matched"], arrival["matched_at"])

        received_logs = [f for event, f in events if event == "d_arrival_received"]
        validation_logs = [f for event, f in events if event == "d_arrival_validation"]
        decision_logs = [
            f for event, f in events
            if event == "d_arrival_decision" and f.get("duplicate")
        ]
        committed_logs = [
            f for event, f in events
            if event == "d_arrival_db_transaction" and f.get("phase") == "COMMITTED"
        ]
        self.assertEqual(len(received_logs), 2)
        self.assertEqual(len(validation_logs), 1)
        self.assertEqual(validation_logs[0]["decision"], "ACCEPTED")
        self.assertTrue(validation_logs[0]["db_state"]["c_passage_stored"])
        self.assertTrue(all(check["passed"] for check in validation_logs[0]["checks"]))
        self.assertEqual(len(decision_logs), 1)
        self.assertEqual(decision_logs[0]["decision"], "DUPLICATE")
        self.assertEqual(len(committed_logs), 1)
        self.assertTrue(committed_logs[0]["route_includes_d"])
        self.assertTrue(committed_logs[0]["completed_at_saved"])

        with closing(self.connect()) as connection:
            attempt_count = connection.execute(
                "SELECT COUNT(*) FROM d_arrival_attempts WHERE journey_id=?",
                (response["journey_id"],),
            ).fetchone()[0]
            journey = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id=?",
                (response["journey_id"],),
            ).fetchone()
        self.assertEqual(attempt_count, 1)
        self.assertEqual(journey["status"], "COMPLETED")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "C", "D"])

    def test_d_identity_completion_policies_are_separated(self) -> None:
        response, arrival = self.waiting_d_journey(
            "REQ-D-IDENTITY-POLICY", 952, "C"
        )
        cases = (
            (
                "NEW",
                "NEW",
                "NOT_REQUIRED",
                response["person_uid"],
                "CONFIRMED_NEW_COMPLETE_AND_PROMOTE_IF_ALLOWED",
            ),
            (
                "RETURNING",
                "RETURNING",
                "NOT_REQUIRED",
                response["person_uid"],
                "CONFIRMED_RETURNING_COMPLETE_AND_PROMOTE_HIGH_CONFIDENCE_ONLY",
            ),
            (
                "IDENTITY_PENDING",
                "UNKNOWN",
                "PENDING",
                None,
                "PENDING_COMPLETE_ROUTE_THEN_FINAL_REVIEW_NO_AUTO_PROMOTION",
            ),
        )
        with closing(self.connect()) as connection:
            for (
                person_status,
                identity_result,
                review_status,
                canonical_uid,
                expected_policy,
            ) in cases:
                connection.execute(
                    "UPDATE journeys SET person_status=?,identity_result=?,"
                    "review_status=?,canonical_person_uid=? WHERE journey_id=?",
                    (
                        person_status,
                        identity_result,
                        review_status,
                        canonical_uid,
                        response["journey_id"],
                    ),
                )
                journey = connection.execute(
                    "SELECT * FROM journeys WHERE journey_id=?",
                    (response["journey_id"],),
                ).fetchone()
                validation = main_server.validate_d_arrival(
                    connection, journey, arrival
                )
                self.assertEqual(
                    validation["context"]["identity_completion_policy"],
                    expected_policy,
                )
    def test_duplicate_d_arrival_is_idempotent(self) -> None:
        response, arrival = self.waiting_d_journey(
            "A_D_GUARD_DUPLICATE", 706
        )

        first = main_server.handle_d_arrival(self.client, arrival)
        retry = dict(arrival)
        retry["event_id"] = "D-DUPLICATE-SAME-JOURNEY-TRACK"
        second = main_server.handle_d_arrival(self.client, retry)

        self.assertTrue(first["accepted"])
        self.assertTrue(second["accepted"])
        self.assertTrue(second["duplicate"])
        with closing(self.connect()) as connection:
            attempts = connection.execute(
                "SELECT COUNT(*) FROM d_arrival_attempts WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
            arrivals = connection.execute(
                "SELECT COUNT(*) FROM journey_events "
                "WHERE journey_id = ? AND event_type = 'ARRIVAL'",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(attempts, 1)
        self.assertEqual(arrivals, 1)
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_JOURNEY_COMPLETED)), 1
        )

    def test_completed_journey_cannot_be_reused_by_new_d_event(self) -> None:
        response, arrival = self.waiting_d_journey(
            "A_D_GUARD_COMPLETED_REUSE", 707
        )
        main_server.handle_d_arrival(self.client, arrival)
        retry = dict(arrival)
        retry["event_id"] = "D-NEW-EVENT-AFTER-COMPLETION"
        retry["d_local_track_id"] += 1
        retry["local_track_id"] += 1
        retry["d_track_first_seen_at"] = (
            datetime.fromisoformat(retry["d_track_first_seen_at"])
            + timedelta(seconds=1)
        ).isoformat(timespec="seconds")

        result = main_server.handle_d_arrival(self.client, retry)

        self.assertFalse(result["accepted"])
        self.assertIn("JOURNEY_ALREADY_TERMINAL", result["reason_codes"])
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
            d_nodes = connection.execute(
                "SELECT COUNT(*) FROM journey_events "
                "WHERE journey_id = ? AND node_id = 'D' "
                "AND event_type = 'ARRIVAL'",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(journey["status"], "COMPLETED")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "B", "D"])
        self.assertEqual(d_nodes, 1)
        release = self.client.payloads(
            main_server.TOPIC_D_JOURNEY_CONTROL
        )[-1]
        self.assertEqual(release["status"], "COMPLETED")

    def test_same_d_track_cannot_complete_two_journeys(self) -> None:
        first_response, first_arrival = self.waiting_d_journey(
            "A_D_GUARD_TRACK_OWNER_1", 708
        )
        second_entry = self.fresh_entry("A_D_GUARD_TRACK_OWNER_2", 709)
        second_entry["embedding"] = self.axis_embedding(512, 300)
        main_server.handle_a_entry(self.client, second_entry)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        main_server.handle_passage(
            self.client,
            self.passage_for(
                second_response["journey_id"],
                second_response["person_uid"],
                "B",
            ),
            "B",
        )
        second_arrival = self.arrival_for(
            second_response["journey_id"], second_response["person_uid"]
        )
        second_arrival["d_local_track_id"] = first_arrival[
            "d_local_track_id"
        ]
        second_arrival["local_track_id"] = first_arrival["local_track_id"]
        second_arrival["d_track_first_seen_at"] = first_arrival[
            "d_track_first_seen_at"
        ]

        first = main_server.handle_d_arrival(self.client, first_arrival)
        second = main_server.handle_d_arrival(self.client, second_arrival)

        self.assertTrue(first["accepted"])
        self.assertFalse(second["accepted"])
        self.assertIn(
            "D_TRACK_ALREADY_LINKED_TO_OTHER_JOURNEY",
            second["reason_codes"],
        )
        with closing(self.connect()) as connection:
            states = {
                row["journey_id"]: (row["status"], json.loads(row["route_json"]))
                for row in connection.execute(
                    "SELECT journey_id, status, route_json FROM journeys "
                    "WHERE journey_id IN (?, ?)",
                    (
                        first_response["journey_id"],
                        second_response["journey_id"],
                    ),
                )
            }
        self.assertEqual(
            states[first_response["journey_id"]],
            ("COMPLETED", ["A", "B", "D"]),
        )
        self.assertEqual(
            states[second_response["journey_id"]],
            ("WAITING_D", ["A", "B"]),
        )

    def test_waiting_d_expiry_publishes_candidate_release(self) -> None:
        response, _ = self.waiting_d_journey(
            "A_D_GUARD_EXPIRED_RELEASE", 711
        )
        stale_passage = (
            datetime.now().astimezone()
            - timedelta(seconds=main_server.WAITING_D_TIMEOUT_SECONDS + 1)
        ).isoformat(timespec="seconds")
        with closing(self.connect()) as connection:
            connection.execute(
                "UPDATE journeys SET passage_at = ? WHERE journey_id = ?",
                (stale_passage, response["journey_id"]),
            )
            expired = main_server.expire_stale_journeys(
                connection, self.client
            )
            connection.commit()
            status = connection.execute(
                "SELECT status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]

        self.assertEqual(expired, (0, 1))
        self.assertEqual(status, "EXPIRED")
        release = self.client.payloads(
            main_server.TOPIC_D_JOURNEY_CONTROL
        )[-1]
        self.assertEqual(release["event"], "JOURNEY_RELEASE")
        self.assertEqual(release["action"], "REMOVE")
        self.assertEqual(release["target_node"], "D")
        self.assertEqual(release["status"], "EXPIRED")
        self.assertEqual(release["journey_status"], "EXPIRED")

    def test_duplicate_a_request_reuses_person_and_journey(self) -> None:
        entry = load_fixture("a_entry.json")
        # Candidate TTL is evaluated against wall clock immediately before
        # every publish.  Keep this idempotency fixture live; stale behavior is
        # covered independently by the 300-second recovery regression.
        entry["timestamp"] = main_server.now_iso()

        main_server.handle_a_entry(self.client, entry)
        first_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        main_server.handle_a_entry(self.client, entry)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(
            second_response["journey_id"],
            first_response["journey_id"],
        )
        self.assertEqual(
            second_response["person_uid"],
            first_response["person_uid"],
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_B)),
            2,
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)),
            0,
        )

        with closing(self.connect()) as connection:
            journey_count = connection.execute(
                "SELECT COUNT(*) FROM journeys"
            ).fetchone()[0]
            person_count = connection.execute(
                "SELECT COUNT(*) FROM persons"
            ).fetchone()[0]
            event_count = connection.execute(
                "SELECT COUNT(*) FROM journey_events"
            ).fetchone()[0]
            indexed = connection.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'index'
                  AND name = 'idx_journeys_request_id'
                """
            ).fetchone()[0]

        self.assertEqual(journey_count, 1)
        self.assertEqual(person_count, 1)
        self.assertEqual(event_count, 1)
        self.assertEqual(indexed, 1)

    def test_live_topology_subscribes_and_recovers_b_with_c_disabled(self) -> None:
        entry = load_fixture("a_entry.json")
        entry["timestamp"] = main_server.now_iso()
        main_server.handle_a_entry(self.client, entry)
        self.client.messages.clear()

        main_server.on_connect(
            self.client,
            userdata=None,
            flags=None,
            reason_code=0,
            properties=None,
        )

        self.assertEqual(
            self.client.subscriptions,
            [
                (main_server.TOPIC_A_ENTRY, 1),
                (main_server.TOPIC_B_PASSAGE, 1),
                (main_server.TOPIC_D_ARRIVAL, 1),
                (main_server.TOPIC_D_DETECTION, 1),
                (main_server.TOPIC_A_TIMING, 1),
                (main_server.TOPIC_B_TIMING, 1),
                (main_server.TOPIC_C_TIMING, 1),
                (main_server.TOPIC_D_TIMING, 1),
            ],
        )
        self.assertEqual(
            [topic for topic, _, _, _ in self.client.messages],
            [
                main_server.TOPIC_CANDIDATE_B,
            ],
        )
        self.assertTrue(
            all(
                qos == main_server.MQTT_CANDIDATE_QOS
                for _, _, qos, _ in self.client.messages
            )
        )

    def test_stranger_detection_is_idempotent_without_creating_journey(self) -> None:
        payload = {
            "event_id": "D-20260814T150001000000+0900-L81",
            "at": "2026-08-14T15:00:01+09:00",
            "node": "D",
            "kind": "STRANGER_DETECTED",
            "identity_status": "UNREGISTERED",
            "local_track_id": 81,
            "journey_id": None,
            "person_uid": None,
            "canonical_person_uid": None,
        }
        raw = json.dumps(payload).encode("utf-8")

        main_server.process_mqtt_message(
            self.client,
            main_server.TOPIC_D_DETECTION,
            raw,
            received_at="2026-08-14T15:00:02+09:00",
        )
        main_server.process_mqtt_message(
            self.client,
            main_server.TOPIC_D_DETECTION,
            raw,
            duplicate=True,
            received_at="2026-08-14T15:00:03+09:00",
        )

        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM detection_events WHERE event_id = ?",
                (payload["event_id"],),
            ).fetchone()
            detection_count = connection.execute(
                "SELECT COUNT(*) FROM detection_events"
            ).fetchone()[0]
            journey_count = connection.execute(
                "SELECT COUNT(*) FROM journeys"
            ).fetchone()[0]
        self.assertEqual(detection_count, 1)
        self.assertEqual(journey_count, 0)
        self.assertEqual(row["identity_status"], "UNREGISTERED")
        self.assertIsNone(row["journey_id"])
        self.assertIsNone(row["person_uid"])
        self.assertIsNone(row["canonical_person_uid"])

    def test_stranger_detection_requires_timezone_and_null_identity(self) -> None:
        payload = {
            "event_id": "D-20260814T150001000000+0900-L82",
            "at": "2026-08-14T15:00:01",
            "node": "D",
            "kind": "STRANGER_DETECTED",
            "identity_status": "UNREGISTERED",
            "local_track_id": 82,
            "journey_id": None,
            "person_uid": None,
            "canonical_person_uid": None,
        }
        with self.assertRaisesRegex(ValueError, "timezone"):
            main_server.handle_stranger_detection(payload)
        payload["at"] = "2026-08-14T15:00:01+09:00"
        payload["person_uid"] = "P000001"
        with self.assertRaisesRegex(ValueError, "person_uid"):
            main_server.handle_stranger_detection(payload)

    def test_a_c_d_payloads_complete_the_journey(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-C-ROUTE", 501),
        )
        candidate = self.client.payloads(main_server.TOPIC_CANDIDATE_C)[-1]
        self.assertEqual(candidate["stage"], "WAITING_B_OR_C")
        self.assertEqual(candidate["route"], ["A"])
        self.assertEqual(
            candidate["candidate_ttl_seconds"],
            main_server.WAITING_B_OR_C_TIMEOUT_SECONDS,
        )
        self.assertIsNotNone(candidate["entry_timestamp"])
        self.assertIsNotNone(candidate["expires_at"])

        passage = self.passage_for(
            candidate["journey_id"], candidate["person_uid"], "C"
        )
        message = type("PassageMessage", (), {})()
        message.topic = main_server.TOPIC_C_PASSAGE
        message.retain = False
        message.payload = json.dumps(passage).encode("utf-8")
        main_server.on_message(self.client, None, message)
        d_candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_D
        )[-1]
        self.assertEqual(d_candidate["route"], ["A", "C"])
        self.assertEqual(d_candidate["middle_node"], "C")
        self.assertEqual(
            d_candidate["candidate_ttl_seconds"],
            main_server.WAITING_D_TIMEOUT_SECONDS,
        )
        self.assertIsNotNone(d_candidate["expires_at"])
        self.assertEqual(
            [item["node_id"] for item in d_candidate["gallery"]],
            ["A", "C", "C"],
        )
        with closing(self.connect()) as connection:
            waiting = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id = ?",
                (candidate["journey_id"],),
            ).fetchone()
        self.assertEqual(waiting["status"], "WAITING_D")
        self.assertEqual(json.loads(waiting["route_json"]), ["A", "C"])

        main_server.handle_d_arrival(
            self.client,
            self.arrival_for(
                candidate["journey_id"],
                candidate["person_uid"],
            ),
        )
        completed = self.client.payloads(
            main_server.TOPIC_JOURNEY_COMPLETED
        )[-1]
        self.assertEqual(completed["route"], ["A", "C", "D"])
        self.assertEqual(completed["status"], "COMPLETED")
        with closing(self.connect()) as connection:
            final = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id = ?",
                (candidate["journey_id"],),
            ).fetchone()
        self.assertEqual(final["status"], "COMPLETED")
        self.assertEqual(json.loads(final["route_json"]), ["A", "C", "D"])
        for topic in (
            main_server.TOPIC_B_JOURNEY_CONTROL,
            main_server.TOPIC_C_JOURNEY_CONTROL,
            main_server.TOPIC_D_JOURNEY_CONTROL,
        ):
            invalidation = self.client.payloads(topic)[-1]
            self.assertEqual(invalidation["action"], "REMOVE")
            self.assertEqual(invalidation["status"], "COMPLETED")
            self.assertEqual(
                invalidation["journey_id"], candidate["journey_id"]
            )

    def test_c_enabled_subscribes_and_recovers_candidate(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-C-RECOVERY", 502),
        )
        self.client.messages.clear()
        main_server.on_connect(
            self.client,
            userdata=None,
            flags=None,
            reason_code=0,
            properties=None,
        )
        self.assertIn(
            (main_server.TOPIC_C_PASSAGE, main_server.MQTT_QOS),
            self.client.subscriptions,
        )
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)),
            1,
        )

    def test_c_enabled_duplicate_entry_and_passage_are_idempotent(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        entry = self.fresh_entry("REQ-C-IDEMPOTENT", 503)
        main_server.handle_a_entry(self.client, entry)
        main_server.handle_a_entry(self.client, entry)
        candidate = self.client.payloads(main_server.TOPIC_CANDIDATE_C)[-1]
        passage = self.passage_for(
            candidate["journey_id"], candidate["person_uid"], "C"
        )
        main_server.handle_passage(self.client, passage, "C")
        main_server.handle_passage(self.client, passage, "C")
        with closing(self.connect()) as connection:
            counts = {
                "persons": connection.execute(
                    "SELECT COUNT(*) FROM persons"
                ).fetchone()[0],
                "journeys": connection.execute(
                    "SELECT COUNT(*) FROM journeys"
                ).fetchone()[0],
                "passages": connection.execute(
                    "SELECT COUNT(*) FROM journey_events "
                    "WHERE event_type='PASSAGE'"
                ).fetchone()[0],
            }
        self.assertEqual(counts, {"persons": 1, "journeys": 1, "passages": 1})
        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_D)),
            1,
        )

    def test_c_final_low_score_rejects_passage_and_gallery(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-C-FINAL-LOW", 506),
        )
        response = self.client.payloads(main_server.TOPIC_A_ENTRY_RESPONSE)[-1]
        passage = self.passage_for(
            response["journey_id"], response["person_uid"], "C"
        )
        passage["similarity"] = 0.592819
        passage["quality"] = 0.959883

        result = main_server.handle_passage(self.client, passage, "C")

        self.assertFalse(result["accepted"])
        self.assertIn(
            "C_FINAL_SIMILARITY_BELOW_THRESHOLD", result["reason_codes"]
        )
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_D), [])
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status,route_json,passage_at FROM journeys "
                "WHERE journey_id=?",
                (response["journey_id"],),
            ).fetchone()
            c_gallery = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery "
                "WHERE journey_id=? AND node_id='C'",
                (response["journey_id"],),
            ).fetchone()[0]
            rejected = connection.execute(
                "SELECT COUNT(*) FROM journey_events "
                "WHERE journey_id=? AND event_type='PASSAGE_REJECTED'",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(journey["status"], "WAITING_B_OR_C")
        self.assertEqual(json.loads(journey["route_json"]), ["A"])
        self.assertIsNone(journey["passage_at"])
        self.assertEqual(c_gallery, 0)
        self.assertEqual(rejected, 1)

    def test_c_passage_quality_boundary_uses_dedicated_fixture(self) -> None:
        boundary = json.loads(
            (
                Path(__file__).parent
                / "fixtures"
                / "c_passage_quality_boundaries.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(main_server.C_PASSAGE_MIN_QUALITY, 0.74)
        self.assertEqual(boundary["minimum_quality"], 0.74)
        # Other BODY and similarity policies must remain independent/unchanged.
        self.assertEqual(main_server.AUTO_DECISION_MIN_BODY_QUALITY, 0.80)
        self.assertEqual(main_server.PERSON_MATCH_THRESHOLD, 0.75)
        self.assertEqual(main_server.PERSON_TOPK_THRESHOLD, 0.68)
        self.assertEqual(main_server.PERSON_COMBINED_THRESHOLD, 0.72)
        self.assertEqual(main_server.MIN_CONSISTENT_BODY_FRAMES, 2)

        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-C-QUALITY-BOUNDARY", 508),
        )
        response = self.client.payloads(main_server.TOPIC_A_ENTRY_RESPONSE)[-1]
        passage = self.passage_for(
            response["journey_id"], response["person_uid"], "C"
        )
        passage["similarity"] = boundary["final_similarity"]
        passage["quality"] = boundary["accepted_quality"]
        c_items = [item for item in passage["gallery"] if item["node_id"] == "C"]
        self.assertGreaterEqual(len(c_items), 2)
        for item in c_items:
            item["quality"] = boundary["accepted_quality"]
        excluded = json.loads(json.dumps(c_items[0]))
        excluded["quality"] = boundary["excluded_quality"]
        passage["gallery"].append(excluded)

        with closing(self.connect()) as connection:
            accepted = main_server.validate_c_passage_evidence(
                connection, response["journey_id"], passage
            )
        self.assertTrue(accepted["accepted"])
        self.assertEqual(len(accepted["accepted_samples"]), len(c_items))
        self.assertNotIn(
            boundary["excluded_quality"],
            [sample["quality"] for sample in accepted["accepted_samples"]],
        )
        final_quality = next(
            item for item in accepted["predicates"]
            if item["name"] == "final_quality"
        )
        self.assertEqual(final_quality["expected"], ">=0.74")
        self.assertTrue(final_quality["pass"])

        below = json.loads(json.dumps(passage))
        below["quality"] = boundary["excluded_quality"]
        with closing(self.connect()) as connection:
            rejected = main_server.validate_c_passage_evidence(
                connection, response["journey_id"], below
            )
        self.assertFalse(rejected["accepted"])
        self.assertIn("C_FINAL_QUALITY_BELOW_THRESHOLD", rejected["reason_codes"])
        rejected_quality = next(
            item for item in rejected["predicates"]
            if item["name"] == "final_quality"
        )
        self.assertEqual(rejected_quality["actual"], 0.739999)
        self.assertFalse(rejected_quality["pass"])

    def test_recovery_rejects_older_duplicate_active_person_journey(self) -> None:
        first = self.add_consistent_body_frames(
            self.fresh_entry("REQ-LEGACY-DUP-1", 507)
        )
        second = self.add_consistent_body_frames(
            self.fresh_entry("REQ-LEGACY-DUP-2", 508)
        )
        main_server.handle_a_entry(self.client, first)
        first_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        with patch.object(main_server, "find_active_journey", return_value=None):
            main_server.handle_a_entry(self.client, second)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.assertEqual(first_response["person_uid"], second_response["person_uid"])
        self.assertNotEqual(first_response["journey_id"], second_response["journey_id"])
        self.client.messages.clear()

        main_server.recover_active_journeys(self.client)

        with closing(self.connect()) as connection:
            statuses = dict(
                connection.execute(
                    "SELECT journey_id,status FROM journeys ORDER BY journey_id"
                ).fetchall()
            )
        self.assertEqual(statuses[first_response["journey_id"]], "REJECTED")
        self.assertEqual(
            statuses[second_response["journey_id"]], "WAITING_B_OR_C"
        )
        candidates = self.client.payloads(main_server.TOPIC_CANDIDATE_B)
        self.assertEqual([item["journey_id"] for item in candidates], [
            second_response["journey_id"]
        ])
        invalidation = self.client.payloads(
            main_server.TOPIC_B_JOURNEY_CONTROL
        )[-1]
        self.assertEqual(invalidation["status"], "REJECTED")
        self.assertEqual(
            invalidation["journey_id"], first_response["journey_id"]
        )

    def test_network_callback_only_enqueues_slow_handler(self) -> None:
        worker = main_server.MqttIngestionWorker(self.client, maxsize=8)
        worker.start()
        handled = threading.Event()
        message = type("EntryMessage", (), {})()
        message.topic = main_server.TOPIC_A_ENTRY
        message.retain = False
        message.payload = json.dumps(
            self.fresh_entry("REQ-ASYNC-CALLBACK", 504)
        ).encode("utf-8")

        def slow_handler(*args, **kwargs) -> None:
            del args, kwargs
            time.sleep(0.25)
            handled.set()

        try:
            with patch.object(
                main_server, "process_mqtt_message", side_effect=slow_handler
            ):
                started = time.monotonic()
                main_server.on_message(self.client, worker, message)
                callback_seconds = time.monotonic() - started
                self.assertLess(callback_seconds, 0.1)
                self.assertTrue(handled.wait(2))
        finally:
            worker.stop()

    def test_recovery_does_not_publish_candidate_older_than_300_seconds(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-STALE-CANDIDATE", 505),
        )
        response = self.client.payloads(main_server.TOPIC_A_ENTRY_RESPONSE)[-1]
        stale_at = (
            datetime.now().astimezone()
            - timedelta(seconds=main_server.WAITING_B_OR_C_TIMEOUT_SECONDS + 124)
        ).isoformat(timespec="seconds")
        with closing(self.connect()) as connection:
            connection.execute(
                "UPDATE journeys SET entry_at=? WHERE journey_id=?",
                (stale_at, response["journey_id"]),
            )
            connection.commit()
        self.client.messages.clear()

        main_server.recover_active_journeys(self.client)

        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_B), [])
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_C), [])
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_D), [])
        with closing(self.connect()) as connection:
            status = connection.execute(
                "SELECT status FROM journeys WHERE journey_id=?",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(status, "EXPIRED")
        for topic in (
            main_server.TOPIC_B_JOURNEY_CONTROL,
            main_server.TOPIC_C_JOURNEY_CONTROL,
            main_server.TOPIC_D_JOURNEY_CONTROL,
        ):
            invalidation = self.client.payloads(topic)[-1]
            self.assertEqual(invalidation["action"], "REMOVE")
            self.assertEqual(invalidation["journey_id"], response["journey_id"])
            self.assertEqual(invalidation["status"], "EXPIRED")

    def test_candidates_use_qos0_and_are_rebuilt_from_active_state(self) -> None:
        main_server.ENABLE_CAMERA_C = True
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-CANDIDATE-QOS0", 507),
        )

        candidate_messages = [
            (topic, payload, qos)
            for topic, payload, qos, _ in self.client.messages
            if topic in {
                main_server.TOPIC_CANDIDATE_B,
                main_server.TOPIC_CANDIDATE_C,
            }
        ]
        self.assertEqual(len(candidate_messages), 2)
        for _, payload, qos in candidate_messages:
            self.assertEqual(qos, 0)
            self.assertEqual(
                payload["margin_scope"], "DISTINCT_IDENTITY_CANDIDATES"
            )
            self.assertEqual(payload["active_journey_policy"], "LATEST_PER_PERSON")
            self.assertIsNotNone(payload["expires_at"])

        response_messages = [
            qos
            for topic, _, qos, _ in self.client.messages
            if topic == main_server.TOPIC_A_ENTRY_RESPONSE
        ]
        self.assertEqual(response_messages, [main_server.MQTT_QOS])

    def test_c_first_ignores_late_b_passage(self) -> None:
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-C-FIRST", 601),
        )
        candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_B
        )[-1]
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_C), [])
        journey_id = candidate["journey_id"]
        person_uid = candidate["person_uid"]

        main_server.handle_passage(
            self.client,
            self.passage_for(journey_id, person_uid, "C"),
            "C",
        )
        main_server.handle_passage(
            self.client,
            self.passage_for(journey_id, person_uid, "B"),
            "B",
        )

        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_D)),
            1,
        )
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status, route_json FROM journeys WHERE journey_id = ?",
                (journey_id,),
            ).fetchone()
            middle_events = connection.execute(
                "SELECT node_id FROM journey_events "
                "WHERE journey_id = ? AND event_type = 'PASSAGE'",
                (journey_id,),
            ).fetchall()
            gallery_nodes = connection.execute(
                "SELECT node_id FROM journey_gallery WHERE journey_id = ?",
                (journey_id,),
            ).fetchall()

        self.assertEqual(journey["status"], "WAITING_D")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "C"])
        self.assertEqual([row["node_id"] for row in middle_events], ["C"])
        self.assertNotIn("B", [row["node_id"] for row in gallery_nodes])

        main_server.handle_d_arrival(
            self.client,
            self.arrival_for(journey_id, person_uid),
        )
        self.assertEqual(
            self.client.payloads(main_server.TOPIC_JOURNEY_COMPLETED)[-1][
                "route"
            ],
            ["A", "C", "D"],
        )

    def test_b_first_ignores_late_c_passage(self) -> None:
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry("REQ-B-FIRST", 701),
        )
        candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_B
        )[-1]
        journey_id = candidate["journey_id"]
        person_uid = candidate["person_uid"]

        main_server.handle_passage(
            self.client,
            self.passage_for(journey_id, person_uid, "B"),
            "B",
        )
        main_server.handle_passage(
            self.client,
            self.passage_for(journey_id, person_uid, "C"),
            "C",
        )
        main_server.handle_d_arrival(
            self.client,
            self.arrival_for(journey_id, person_uid),
        )

        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_D)),
            1,
        )
        self.assertEqual(
            self.client.payloads(main_server.TOPIC_JOURNEY_COMPLETED)[-1][
                "route"
            ],
            ["A", "B", "D"],
        )

    def test_duplicate_b_passage_does_not_republish_d_candidate(self) -> None:
        self._assert_duplicate_passage_is_idempotent("B", 801)

    def test_duplicate_c_passage_does_not_republish_d_candidate(self) -> None:
        self._assert_duplicate_passage_is_idempotent("C", 901)

    def _assert_duplicate_passage_is_idempotent(
        self,
        node_id: str,
        local_track_id: int,
    ) -> None:
        main_server.handle_a_entry(
            self.client,
            self.fresh_entry(f"REQ-DUP-{node_id}", local_track_id),
        )
        candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_B
        )[-1]
        passage = self.passage_for(
            candidate["journey_id"],
            candidate["person_uid"],
            node_id,
        )

        main_server.handle_passage(self.client, passage, node_id)
        main_server.handle_passage(self.client, passage, node_id)

        self.assertEqual(
            len(self.client.payloads(main_server.TOPIC_CANDIDATE_D)),
            1,
        )
        with closing(self.connect()) as connection:
            event_count = connection.execute(
                "SELECT COUNT(*) FROM journey_events "
                "WHERE journey_id = ? AND event_type = 'PASSAGE'",
                (candidate["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(event_count, 1)

    def test_legacy_entry_parser_returns_one_body_sample(self) -> None:
        parsed = main_server.parse_a_entry_samples(
            load_fixture("a_entry.json")
        )
        self.assertEqual(len(parsed["body_samples"]), 1)
        self.assertEqual(len(parsed["face_samples"]), 0)
        self.assertEqual(parsed["body_samples"][0]["embedding"].size, 512)

    def test_body_top3_and_face_top3_parser(self) -> None:
        entry = self.multimodal_entry("REQ-PARSER", 1001)
        parsed = main_server.parse_a_entry_samples(entry)

        self.assertEqual(len(parsed["body_samples"]), 3)
        self.assertEqual(len(parsed["face_samples"]), 3)
        self.assertTrue(
            all(sample["embedding"].size == 512 for sample in parsed["body_samples"])
        )
        self.assertTrue(
            all(sample["embedding"].size == 128 for sample in parsed["face_samples"])
        )
        self.assertEqual(parsed["body_samples"][1]["frame_index"], 20)
        self.assertEqual(parsed["face_samples"][0]["frontal_score"], 0.9)
        self.assertEqual(parsed["face_samples"][0]["sharpness"], 120.0)

    def test_invalid_body_dimension_rejects_entry(self) -> None:
        entry = self.multimodal_entry("REQ-BAD-BODY", 1002)
        entry["body_embedding_dim"] = 256
        with self.assertRaises(ValueError):
            main_server.parse_a_entry_samples(entry)

    def test_invalid_body_sample_does_not_discard_valid_samples(self) -> None:
        entry = self.multimodal_entry("REQ-PARTIAL-BODY", 1005)
        entry["body_embeddings"][1] = [0.0] * 512
        parsed = main_server.parse_a_entry_samples(entry)
        self.assertEqual(len(parsed["body_samples"]), 2)

    def test_invalid_face_dimension_drops_face_and_keeps_body(self) -> None:
        entry = self.multimodal_entry("REQ-BAD-FACE", 1003)
        entry["face_embedding_dim"] = 256
        parsed = main_server.parse_a_entry_samples(entry)
        self.assertEqual(len(parsed["body_samples"]), 3)
        self.assertEqual(len(parsed["face_samples"]), 0)

    def test_face_unavailable_uses_body_only_fallback(self) -> None:
        entry = self.multimodal_entry(
            "REQ-NO-FACE",
            1004,
            face_available=False,
        )
        parsed = main_server.parse_a_entry_samples(entry)
        self.assertEqual(len(parsed["body_samples"]), 3)
        self.assertEqual(len(parsed["face_samples"]), 0)

    def test_multimodal_gallery_is_split_and_b_candidate_is_body_only(
        self,
    ) -> None:
        main_server.handle_a_entry(
            self.client,
            self.multimodal_entry("REQ-GALLERY-SPLIT", 1101),
        )
        response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        b_candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_B
        )[-1]
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_C), [])

        with closing(self.connect()) as connection:
            counts = {
                row["modality"]: row["count"]
                for row in connection.execute(
                    "SELECT modality, COUNT(*) AS count FROM journey_gallery "
                    "WHERE journey_id = ? GROUP BY modality",
                    (response["journey_id"],),
                )
            }

        self.assertEqual(counts, {"BODY": 3, "FACE": 3})
        self.assertEqual(b_candidate["gallery_count"], 3)
        self.assertTrue(
            all(item["modality"] == "BODY" for item in b_candidate["gallery"])
        )
        self.assertTrue(
            all(item["embedding_dim"] == 512 for item in b_candidate["gallery"])
        )

    def test_d_candidate_contains_only_a_and_selected_c_body_gallery(self) -> None:
        main_server.handle_a_entry(
            self.client,
            self.multimodal_entry("REQ-D-BODY-ONLY", 1201),
        )
        candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_B
        )[-1]
        self.assertEqual(self.client.payloads(main_server.TOPIC_CANDIDATE_C), [])
        main_server.handle_passage(
            self.client,
            self.passage_for(
                candidate["journey_id"],
                candidate["person_uid"],
                "C",
            ),
            "C",
        )
        d_candidate = self.client.payloads(
            main_server.TOPIC_CANDIDATE_D
        )[-1]

        self.assertEqual(d_candidate["route"], ["A", "C"])
        self.assertEqual(
            [item["node_id"] for item in d_candidate["gallery"]],
            ["A", "A", "A", "C", "C"],
        )
        self.assertTrue(
            all(item["modality"] == "BODY" for item in d_candidate["gallery"])
        )

    def test_multimodal_active_redetection_reuses_journey(
        self,
    ) -> None:
        first = self.multimodal_entry("REQ-MULTI-ACTIVE-1", 1301)
        second = self.multimodal_entry("REQ-MULTI-ACTIVE-2", 1302)
        main_server.handle_a_entry(self.client, first)
        first_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        main_server.handle_a_entry(self.client, second)
        second_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(
            second_response["journey_id"],
            first_response["journey_id"],
        )
        self.assertEqual(second_response["person_uid"], first_response["person_uid"])
        self.assertEqual(len(self.client.payloads(main_server.TOPIC_CANDIDATE_B)), 2)
        self.assertEqual(len(self.client.payloads(main_server.TOPIC_CANDIDATE_C)), 0)
        with closing(self.connect()) as connection:
            journey_count = connection.execute(
                "SELECT COUNT(*) FROM journeys"
            ).fetchone()[0]
            visit_count = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (first_response["person_uid"],),
            ).fetchone()[0]
            request_count = connection.execute(
                "SELECT COUNT(*) FROM a_entry_requests"
            ).fetchone()[0]
        self.assertEqual(journey_count, 1)
        self.assertEqual(visit_count, 1)
        self.assertEqual(request_count, 2)

    def test_review_status_and_gallery_are_preserved_after_completion(self) -> None:
        known_entry = self.multimodal_entry("REQ-KNOWN-FACE", 1401)
        main_server.handle_a_entry(self.client, known_entry)
        known_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.complete_journey(
            known_response["journey_id"],
            known_response["person_uid"],
        )

        review_entry = self.multimodal_entry(
            "REQ-REVIEW-FACE",
            1402,
            body_embeddings=[
                self.axis_embedding(512, 100)
                for _ in range(3)
            ],
            face_embeddings=[self.axis_embedding(128, 0)],
        )
        main_server.handle_a_entry(self.client, review_entry)
        review_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]

        self.assertEqual(review_response["person_status"], "IDENTITY_PENDING")
        self.assertEqual(
            review_response["candidate_person_uid"],
            known_response["person_uid"],
        )

        review_redetection = dict(review_entry)
        review_redetection["request_id"] = "REQ-REVIEW-FACE-REDETECT"
        review_redetection["local_track_id"] = 1403
        main_server.handle_a_entry(self.client, review_redetection)
        replay_response = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.assertEqual(
            replay_response["journey_id"],
            review_response["journey_id"],
        )
        self.assertEqual(
            replay_response["person_status"],
            "IDENTITY_PENDING",
        )
        self.complete_pending_as_manual(review_response)

        with closing(self.connect()) as connection:
            person_row = connection.execute(
                "SELECT status, visit_count FROM persons WHERE person_uid = ?",
                (review_response["person_uid"],),
            ).fetchone()
            review_journey = connection.execute(
                "SELECT identity_result, review_status FROM journeys "
                "WHERE journey_id = ?",
                (review_response["journey_id"],),
            ).fetchone()
            temporary_count = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                (review_response["journey_id"],),
            ).fetchone()[0]
            permanent_count = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                (review_response["person_uid"],),
            ).fetchone()[0]

        completed = self.client.payloads(
            main_server.TOPIC_JOURNEY_COMPLETED
        )[-1]
        self.assertEqual(person_row["status"], "ACTIVE")
        self.assertEqual(person_row["visit_count"], 1)
        self.assertEqual(review_journey["identity_result"], "UNKNOWN")
        self.assertEqual(review_journey["review_status"], "PENDING")
        self.assertGreater(temporary_count, 0)
        self.assertGreater(permanent_count, 0)
        self.assertFalse(completed["gallery_promoted"])
        self.assertEqual(
            completed["gallery_promotion_reason"],
            "BLOCK_REVIEW_REQUIRED",
        )

    def test_review_case_is_persisted_once_and_backfilled_idempotently(
        self,
    ) -> None:
        known, review, review_case = self.create_review_scenario(
            "REQ-CASE",
            complete_review=False,
        )
        self.assertEqual(review_case["review_id"], "R000001")
        self.assertEqual(review_case["status"], "PENDING")
        self.assertIsNone(review_case["action"])
        self.assertEqual(
            review_case["provisional_person_uid"],
            review["person_uid"],
        )
        self.assertEqual(
            review_case["candidate_person_uid"],
            known["person_uid"],
        )

        with closing(self.connect()) as connection:
            connection.execute(
                "DELETE FROM review_cases WHERE journey_id = ?",
                (review["journey_id"],),
            )
            connection.commit()
        main_server.initialize_database(repair_legacy_rows=True)
        main_server.initialize_database(repair_legacy_rows=True)

        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM review_cases WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "PENDING")
        self.assertEqual(
            rows[0]["provisional_person_uid"],
            review["person_uid"],
        )

    def test_confirm_new_promotes_body_and_face_once(self) -> None:
        known, review, review_case = self.create_review_scenario(
            "REQ-CONFIRM"
        )
        del known
        with closing(self.connect()) as connection:
            temporary_modalities = dict(
                connection.execute(
                    "SELECT modality, COUNT(*) FROM journey_gallery "
                    "WHERE journey_id = ? GROUP BY modality",
                    (review["journey_id"],),
                ).fetchall()
            )
        self.assertGreater(temporary_modalities["BODY"], 0)
        self.assertGreater(temporary_modalities["FACE"], 0)

        result = main_server.resolve_review_confirm_new(
            review_case["review_id"]
        )
        self.assertEqual(result["outcome"], "RESOLVED")
        self.assertEqual(result["action"], "CONFIRM_NEW")
        target_uid = result["target_person_uid"]
        self.assertNotEqual(target_uid, review["person_uid"])

        with closing(self.connect()) as connection:
            person = connection.execute(
                "SELECT status, merged_into_person_uid, visit_count "
                "FROM persons WHERE person_uid = ?",
                (target_uid,),
            ).fetchone()
            case = connection.execute(
                "SELECT * FROM review_cases WHERE review_id = ?",
                (review_case["review_id"],),
            ).fetchone()
            permanent = dict(
                connection.execute(
                    "SELECT modality, COUNT(*) FROM person_embeddings "
                    "WHERE person_uid = ? GROUP BY modality",
                    (target_uid,),
                ).fetchall()
            )
            temporary_count = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(person["status"], "ACTIVE")
        self.assertIsNone(person["merged_into_person_uid"])
        self.assertEqual(person["visit_count"], 1)
        self.assertEqual(case["status"], "RESOLVED")
        self.assertEqual(case["action"], "CONFIRM_NEW")
        self.assertEqual(case["target_person_uid"], target_uid)
        self.assertIsNotNone(case["resolved_at"])
        self.assertGreater(permanent["BODY"], 0)
        self.assertEqual(permanent.get("FACE", 0), 0)
        self.assertEqual(temporary_count, 0)

        repeated = main_server.resolve_review_confirm_new(
            review_case["review_id"]
        )
        self.assertEqual(repeated["outcome"], "ALREADY_RESOLVED")
        conflict = main_server.resolve_review_merge_existing(
            review_case["review_id"],
            case["candidate_person_uid"],
        )
        self.assertEqual(conflict["outcome"], "CONFLICT")
        self.assertEqual(conflict["reason"], "REVIEW_ALREADY_RESOLVED")
        with closing(self.connect()) as connection:
            repeated_counts = dict(
                connection.execute(
                    "SELECT modality, COUNT(*) FROM person_embeddings "
                    "WHERE person_uid = ? GROUP BY modality",
                    (target_uid,),
                ).fetchall()
            )
        self.assertEqual(repeated_counts, permanent)

    def test_merge_existing_updates_canonical_links_and_visit_once(self) -> None:
        known, review, review_case = self.create_review_scenario(
            "REQ-MERGE"
        )
        target_uid = known["person_uid"]
        provisional_uid = review["person_uid"]
        with closing(self.connect()) as connection:
            before_visit = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (target_uid,),
            ).fetchone()[0]
            before_gallery = dict(
                connection.execute(
                    "SELECT modality, COUNT(*) FROM person_embeddings "
                    "WHERE person_uid = ? GROUP BY modality",
                    (target_uid,),
                ).fetchall()
            )
            event_count = connection.execute(
                "SELECT COUNT(*) FROM journey_events WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()[0]
            capture_count = connection.execute(
                "SELECT COUNT(*) FROM journey_captures WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()[0]

        result = main_server.resolve_review_merge_existing(
            review_case["review_id"],
            target_uid,
        )
        self.assertEqual(result["outcome"], "RESOLVED")
        self.assertEqual(result["target_visit_count"], before_visit + 1)

        with closing(self.connect()) as connection:
            provisional = connection.execute(
                "SELECT status, merged_into_person_uid, visit_count "
                "FROM persons WHERE person_uid = ?",
                (provisional_uid,),
            ).fetchone()
            target = connection.execute(
                "SELECT status, visit_count FROM persons WHERE person_uid = ?",
                (target_uid,),
            ).fetchone()
            journey = connection.execute(
                "SELECT person_uid, visit_no, person_status FROM journeys "
                "WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()
            case = connection.execute(
                "SELECT * FROM review_cases WHERE review_id = ?",
                (review_case["review_id"],),
            ).fetchone()
            capture_people = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT person_uid FROM journey_captures "
                    "WHERE journey_id = ?",
                    (review["journey_id"],),
                )
            }
            canonical_event_count = connection.execute(
                "SELECT COUNT(*) FROM journey_events AS events "
                "JOIN journeys ON journeys.journey_id = events.journey_id "
                "WHERE events.journey_id = ? AND journeys.person_uid = ?",
                (review["journey_id"], target_uid),
            ).fetchone()[0]
            after_gallery = dict(
                connection.execute(
                    "SELECT modality, COUNT(*) FROM person_embeddings "
                    "WHERE person_uid = ? GROUP BY modality",
                    (target_uid,),
                ).fetchall()
            )
        self.assertEqual(provisional_uid, target_uid)
        self.assertEqual(provisional["status"], "ACTIVE")
        self.assertIsNone(provisional["merged_into_person_uid"])
        self.assertEqual(provisional["visit_count"], before_visit + 1)
        self.assertEqual(target["status"], "ACTIVE")
        self.assertEqual(target["visit_count"], before_visit + 1)
        self.assertEqual(journey["person_uid"], target_uid)
        self.assertEqual(journey["visit_no"], before_visit + 1)
        self.assertEqual(journey["person_status"], "RETURNING")
        self.assertEqual(capture_people, {target_uid})
        self.assertEqual(canonical_event_count, event_count)
        self.assertGreater(capture_count, 0)
        self.assertEqual(case["status"], "RESOLVED")
        self.assertEqual(case["action"], "MERGE_EXISTING")
        self.assertEqual(case["target_person_uid"], target_uid)
        self.assertEqual(after_gallery, before_gallery)

        repeated = main_server.resolve_review_merge_existing(
            review_case["review_id"],
            target_uid,
        )
        self.assertEqual(repeated["outcome"], "ALREADY_RESOLVED")
        wrong_action = main_server.resolve_review_confirm_new(
            review_case["review_id"]
        )
        self.assertEqual(wrong_action["outcome"], "CONFLICT")
        with closing(self.connect()) as connection:
            final_visit = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (target_uid,),
            ).fetchone()[0]
            final_gallery = dict(
                connection.execute(
                    "SELECT modality, COUNT(*) FROM person_embeddings "
                    "WHERE person_uid = ? GROUP BY modality",
                    (target_uid,),
                ).fetchall()
            )
        self.assertEqual(final_visit, before_visit + 1)
        self.assertEqual(final_gallery, after_gallery)

    def test_merge_active_target_allows_overlapping_journeys(self) -> None:
        known, review, review_case = self.create_review_scenario(
            "REQ-CONFLICT"
        )
        target_entry = self.multimodal_entry(
            "REQ-CONFLICT-TARGET-ACTIVE",
            8100,
        )
        main_server.handle_a_entry(self.client, target_entry)
        target_active = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.assertEqual(target_active["person_uid"], known["person_uid"])

        with closing(self.connect()) as connection:
            before = {
                "visit": connection.execute(
                    "SELECT visit_count FROM persons WHERE person_uid = ?",
                    (known["person_uid"],),
                ).fetchone()[0],
                "permanent": connection.execute(
                    "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                    (known["person_uid"],),
                ).fetchone()[0],
                "temporary": connection.execute(
                    "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                    (review["journey_id"],),
                ).fetchone()[0],
            }
        result = main_server.resolve_review_merge_existing(
            review_case["review_id"],
            known["person_uid"],
        )
        self.assertEqual(result["outcome"], "RESOLVED")

        with closing(self.connect()) as connection:
            case = connection.execute(
                "SELECT status, action FROM review_cases WHERE review_id = ?",
                (review_case["review_id"],),
            ).fetchone()
            provisional = connection.execute(
                "SELECT status, merged_into_person_uid FROM persons "
                "WHERE person_uid = ?",
                (review["person_uid"],),
            ).fetchone()
            journey_owner = connection.execute(
                "SELECT person_uid FROM journeys WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()[0]
            after = {
                "visit": connection.execute(
                    "SELECT visit_count FROM persons WHERE person_uid = ?",
                    (known["person_uid"],),
                ).fetchone()[0],
                "permanent": connection.execute(
                    "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                    (known["person_uid"],),
                ).fetchone()[0],
                "temporary": connection.execute(
                    "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                    (review["journey_id"],),
                ).fetchone()[0],
            }
        self.assertEqual(case["status"], "RESOLVED")
        self.assertEqual(case["action"], "MERGE_EXISTING")
        self.assertEqual(provisional["status"], "ACTIVE")
        self.assertIsNone(provisional["merged_into_person_uid"])
        self.assertEqual(journey_owner, known["person_uid"])
        self.assertEqual(after["visit"], before["visit"] + 1)
        self.assertEqual(after["permanent"], before["permanent"])
        self.assertEqual(after["temporary"], 0)

    def test_resolution_is_allowed_while_journey_is_in_progress(
        self,
    ) -> None:
        _, review, review_case = self.create_review_scenario(
            "REQ-IN-PROGRESS",
            complete_review=False,
        )
        resolved = main_server.resolve_review_confirm_new(
            review["journey_id"]
        )
        self.assertEqual(resolved["outcome"], "RESOLVED")
        self.assertNotEqual(resolved["target_person_uid"], review["person_uid"])
        repeated = main_server.resolve_review_confirm_new(review_case["review_id"])
        self.assertEqual(repeated["outcome"], "ALREADY_RESOLVED")

    def test_confirm_new_rolls_back_after_gallery_promotion_failure(self) -> None:
        _, review, review_case = self.create_review_scenario(
            "REQ-ROLLBACK"
        )
        with closing(self.connect()) as connection:
            before_temporary = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()[0]
            before_person_count = connection.execute(
                "SELECT COUNT(*) FROM persons"
            ).fetchone()[0]
            before_permanent = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                (review["person_uid"],),
            ).fetchone()[0]

        original_promote = main_server.promote_journey_gallery

        def fail_after_promotion(*args: Any, **kwargs: Any) -> int:
            original_promote(*args, **kwargs)
            raise RuntimeError("injected resolution failure")

        with patch.object(
            main_server,
            "promote_journey_gallery",
            side_effect=fail_after_promotion,
        ):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                main_server.resolve_review_confirm_new(
                    review_case["review_id"]
                )

        with closing(self.connect()) as connection:
            case = connection.execute(
                "SELECT status, action, resolved_at FROM review_cases "
                "WHERE review_id = ?",
                (review_case["review_id"],),
            ).fetchone()
            person_status = connection.execute(
                "SELECT status FROM persons WHERE person_uid = ?",
                (review["person_uid"],),
            ).fetchone()[0]
            permanent_count = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                (review["person_uid"],),
            ).fetchone()[0]
            person_count = connection.execute(
                "SELECT COUNT(*) FROM persons"
            ).fetchone()[0]
            temporary_count = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                (review["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(case["status"], "PENDING")
        self.assertIsNone(case["action"])
        self.assertIsNone(case["resolved_at"])
        self.assertEqual(person_status, "ACTIVE")
        self.assertEqual(person_count, before_person_count)
        self.assertEqual(permanent_count, before_permanent)
        self.assertEqual(temporary_count, before_temporary)

    def test_clear_returning_skips_final_identity_review(self) -> None:
        known_entry = self.multimodal_entry("REQ-FINAL-CLEAR-KNOWN", 8301)
        main_server.handle_a_entry(self.client, known_entry)
        known = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.complete_journey(known["journey_id"], known["person_uid"])

        returning_entry = self.multimodal_entry(
            "REQ-FINAL-CLEAR-RETURNING",
            8302,
        )
        main_server.handle_a_entry(self.client, returning_entry)
        returning = self.client.payloads(
            main_server.TOPIC_A_ENTRY_RESPONSE
        )[-1]
        self.assertEqual(returning["person_status"], "RETURNING")
        self.complete_journey(
            returning["journey_id"],
            returning["person_uid"],
        )

        with closing(self.connect()) as connection:
            review_count = connection.execute(
                "SELECT COUNT(*) FROM review_cases WHERE journey_id = ?",
                (returning["journey_id"],),
            ).fetchone()[0]
        completed = self.client.payloads(
            main_server.TOPIC_JOURNEY_COMPLETED
        )[-1]
        self.assertEqual(review_count, 0)
        self.assertIsNone(completed["final_review_result"])

    def test_final_revisit_merges_once_and_preserves_timing(self) -> None:
        known, pending = self.create_pending_identity_scenario(
            "REQ-FINAL-REVISIT"
        )
        self.assertEqual(pending["person_status"], "IDENTITY_PENDING")
        self.assertEqual(
            pending["candidate_person_uid"],
            known["person_uid"],
        )
        self.assertIsNone(pending["canonical_person_uid"])
        self.assertFalse(pending["identity_confirmed"])
        self.assertFalse(pending["gallery_promotion_allowed"])
        with closing(self.connect()) as connection:
            before_visit = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (known["person_uid"],),
            ).fetchone()[0]

        main_server.handle_node_timing(
            self.timing_payload(
                pending,
                "A",
                "2026-08-11T13:00:00+09:00",
                None,
                "2026-08-11T13:00:05+09:00",
            )
        )
        evidence = self.axis_embedding(512, 0)
        passage = self.passage_for(
            pending["journey_id"],
            pending["person_uid"],
            "B",
        )
        for item in passage["gallery"]:
            item["embedding"] = evidence
        main_server.handle_passage(self.client, passage, "B")
        main_server.handle_node_timing(
            self.timing_payload(
                pending,
                "B",
                "2026-08-11T13:00:10+09:00",
                None,
                "2026-08-11T13:00:20+09:00",
            )
        )
        arrival = self.arrival_for(
            pending["journey_id"],
            pending["person_uid"],
        )
        arrival["embedding"] = evidence
        arrival["embedding_dim"] = 512
        with patch("builtins.print") as print_mock:
            main_server.handle_d_arrival(self.client, arrival)
            main_server.handle_d_arrival(self.client, arrival)
        final_logs = [
            call
            for call in print_mock.call_args_list
            if call.args and "REVIEW RESULT" in str(call.args[0])
        ]

        d_timing = main_server.handle_node_timing(
            self.timing_payload(
                pending,
                "D",
                "2026-08-11T13:00:50+09:00",
                None,
                "2026-08-11T13:01:00+09:00",
            )
        )
        timeline = main_server.get_journey_timeline(pending["journey_id"])
        with closing(self.connect()) as connection:
            target_visit = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (known["person_uid"],),
            ).fetchone()[0]
            provisional = connection.execute(
                "SELECT status,merged_into_person_uid,visit_count FROM persons "
                "WHERE person_uid = ?",
                (pending["person_uid"],),
            ).fetchone()
            journey = connection.execute(
                "SELECT person_uid,person_status,route_json FROM journeys "
                "WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()
            review = connection.execute(
                "SELECT * FROM review_cases WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()
            timing_people = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT person_uid FROM journey_node_visits "
                    "WHERE journey_id = ?",
                    (pending["journey_id"],),
                )
            }
        self.assertEqual(target_visit, before_visit + 1)
        self.assertEqual(pending["person_uid"], known["person_uid"])
        self.assertEqual(provisional["status"], "ACTIVE")
        self.assertIsNone(provisional["merged_into_person_uid"])
        self.assertEqual(provisional["visit_count"], before_visit + 1)
        self.assertEqual(journey["person_uid"], known["person_uid"])
        self.assertEqual(journey["person_status"], "RETURNING")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "B", "D"])
        self.assertEqual(review["final_review_result"], "REVISIT")
        self.assertEqual(review["canonical_person_uid"], known["person_uid"])
        self.assertEqual(review["resolution_source"], "FINAL_ROUTE_IDENTITY")
        self.assertEqual(timing_people, {known["person_uid"]})
        self.assertEqual(d_timing["person_uid"], known["person_uid"])
        self.assertEqual(timeline["person_uid"], known["person_uid"])
        self.assertEqual(timeline["journey_elapsed_seconds"], 60.0)
        self.assertEqual(len(final_logs), 1)
        completed = self.client.payloads(
            main_server.TOPIC_JOURNEY_COMPLETED
        )[-1]
        self.assertEqual(completed["final_review_result"], "REVISIT")
        self.assertEqual(
            completed["canonical_person_uid"], known["person_uid"]
        )
        self.assertTrue(completed["identity_confirmed"])

    def test_final_new_confirms_temporary_uid_and_promotes_gallery(self) -> None:
        known, pending = self.create_pending_identity_scenario(
            "REQ-FINAL-NEW"
        )
        evidence = self.axis_embedding(512, 300)
        self.complete_pending_with_evidence(pending, evidence, "B")

        completed = self.client.payloads(
            main_server.TOPIC_JOURNEY_COMPLETED
        )[-1]
        with closing(self.connect()) as connection:
            review = connection.execute(
                "SELECT * FROM review_cases WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()
            canonical_uid = review["canonical_person_uid"]
            person = connection.execute(
                "SELECT status,visit_count FROM persons WHERE person_uid = ?",
                (canonical_uid,),
            ).fetchone()
            journey = connection.execute(
                "SELECT person_uid,person_status,route_json FROM journeys "
                "WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()
            permanent_count = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                (canonical_uid,),
            ).fetchone()[0]
            temporary_count = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(pending["person_uid"], known["person_uid"])
        self.assertNotEqual(canonical_uid, known["person_uid"])
        self.assertEqual(person["status"], "ACTIVE")
        self.assertEqual(person["visit_count"], 1)
        self.assertEqual(journey["person_uid"], canonical_uid)
        self.assertEqual(journey["person_status"], "NEW")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "B", "D"])
        self.assertEqual(review["final_review_result"], "NEW")
        self.assertEqual(review["action"], "CONFIRM_NEW")
        self.assertEqual(review["canonical_person_uid"], canonical_uid)
        self.assertGreater(permanent_count, 0)
        self.assertEqual(temporary_count, 0)
        self.assertEqual(completed["final_review_result"], "NEW")
        self.assertEqual(completed["person_uid"], canonical_uid)

    def test_final_manual_review_preserves_gallery_and_metadata(self) -> None:
        _, pending = self.create_pending_identity_scenario(
            "REQ-FINAL-MANUAL"
        )
        with patch.object(
            main_server,
            "resolve_final_route_identity",
            wraps=main_server.resolve_final_route_identity,
        ) as resolver:
            self.complete_pending_as_manual(pending, "C")
            duplicate = self.arrival_for(
                pending["journey_id"],
                pending["person_uid"],
            )
            main_server.handle_d_arrival(self.client, duplicate)
        with closing(self.connect()) as connection:
            person_status = connection.execute(
                "SELECT status FROM persons WHERE person_uid = ?",
                (pending["person_uid"],),
            ).fetchone()[0]
            journey = connection.execute(
                "SELECT status,person_status,route_json FROM journeys "
                "WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()
            review = connection.execute(
                "SELECT * FROM review_cases WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()
            temporary_count = connection.execute(
                "SELECT COUNT(*) FROM journey_gallery WHERE journey_id = ?",
                (pending["journey_id"],),
            ).fetchone()[0]
            permanent_count = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = ?",
                (pending["person_uid"],),
            ).fetchone()[0]
        self.assertEqual(resolver.call_count, 1)
        self.assertEqual(person_status, "ACTIVE")
        self.assertEqual(journey["status"], "COMPLETED")
        self.assertEqual(journey["person_status"], "REVIEW_REQUIRED")
        self.assertEqual(json.loads(journey["route_json"]), ["A", "C", "D"])
        self.assertEqual(review["status"], "PENDING")
        self.assertIsNone(review["action"])
        self.assertEqual(
            review["final_review_result"],
            "MANUAL_REVIEW_REQUIRED",
        )
        self.assertEqual(review["resolution_source"], "FINAL_ROUTE_IDENTITY")
        self.assertIsNotNone(review["initial_scores_json"])
        self.assertIsNotNone(review["final_scores_json"])
        self.assertGreater(temporary_count, 0)
        self.assertGreater(permanent_count, 0)

    def test_a_timing_is_saved(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-A")
        result = main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T13:00:00.000+09:00",
                "2026-08-11T13:00:01.000+09:00",
                "2026-08-11T13:00:05.000+09:00",
            )
        )
        with closing(self.connect()) as connection:
            row = connection.execute(
                "SELECT * FROM journey_node_visits "
                "WHERE journey_id = ? AND node_id = 'A'",
                (response["journey_id"],),
            ).fetchone()
        self.assertTrue(result["created"])
        self.assertEqual(row["person_uid"], response["person_uid"])
        self.assertEqual(row["local_track_id"], 52)
        self.assertEqual(row["dwell_seconds"], 5.0)
        self.assertEqual(row["exit_reason"], "TRACK_LOST")

    def test_a_to_c_timing_is_saved(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-A-C")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T13:00:00+09:00",
                "2026-08-11T13:00:01+09:00",
                "2026-08-11T13:00:05+09:00",
            )
        )
        self.advance_timing_journey(response, "C")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "C",
                "2026-08-11T13:00:12+09:00",
                "2026-08-11T13:00:13+09:00",
                "2026-08-11T13:00:20+09:00",
            )
        )
        with closing(self.connect()) as connection:
            nodes = [
                row[0]
                for row in connection.execute(
                    "SELECT node_id FROM journey_node_visits "
                    "WHERE journey_id = ? ORDER BY node_id",
                    (response["journey_id"],),
                )
            ]
        self.assertEqual(nodes, ["A", "C"])

    def test_c_timing_after_passage_is_saved_while_waiting_d(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-C-WAITING-D")
        self.advance_timing_journey(response, "C")
        result = main_server.handle_node_timing(
            self.timing_payload(
                response,
                "C",
                "2026-08-11T13:03:53.100+09:00",
                "2026-08-11T13:03:56.020+09:00",
                "2026-08-11T13:04:02.340+09:00",
            )
        )
        with closing(self.connect()) as connection:
            status = connection.execute(
                "SELECT status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(result["dwell_seconds"], 9.24)
        self.assertEqual(status, "WAITING_D")

    def test_d_timing_after_completed_journey_is_saved(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-D-COMPLETED")
        self.advance_timing_journey(response, "B")
        self.complete_timing_journey(response)
        result = main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:05:00+09:00",
                "2026-08-11T13:05:01+09:00",
                "2026-08-11T13:05:07+09:00",
            )
        )
        with closing(self.connect()) as connection:
            status = connection.execute(
                "SELECT status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertEqual(result["dwell_seconds"], 7.0)
        self.assertEqual(status, "COMPLETED")

    def test_a_c_d_timeline_calculation(self) -> None:
        response = self.start_timing_journey(
            "REQ-TIMELINE-A-C-D",
            "2026-08-11T12:59:59+09:00",
        )
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T13:00:00.000+09:00",
                "2026-08-11T13:00:01.000+09:00",
                "2026-08-11T13:00:05.000+09:00",
            )
        )
        self.advance_timing_journey(response, "C")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "C",
                "2026-08-11T13:00:12.280+09:00",
                "2026-08-11T13:00:13.000+09:00",
                "2026-08-11T13:00:20.000+09:00",
            )
        )
        self.complete_timing_journey(response)
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:00:26.510+09:00",
                "2026-08-11T13:00:27.000+09:00",
                "2026-08-11T13:00:34.290+09:00",
            )
        )
        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertEqual(timeline["route"], ["A", "C", "D"])
        self.assertEqual(
            [node["node_id"] for node in timeline["nodes"]],
            ["A", "C", "D"],
        )
        self.assertEqual(timeline["segments"]["A_to_C_seconds"], 7.28)
        self.assertEqual(timeline["segments"]["C_to_D_seconds"], 6.51)
        self.assertEqual(timeline["total_route_seconds"], 34.29)
        self.assertEqual(timeline["journey_elapsed_seconds"], 35.29)
        self.assertEqual(timeline["validation_warnings"], [])

    def test_a_b_d_timeline_calculation(self) -> None:
        response = self.start_timing_journey(
            "REQ-TIMELINE-A-B-D",
            "2026-08-11T13:59:59+09:00",
        )
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T14:00:00+09:00",
                "2026-08-11T14:00:01+09:00",
                "2026-08-11T14:00:05+09:00",
            )
        )
        self.advance_timing_journey(response, "B")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "B",
                "2026-08-11T14:00:10+09:00",
                "2026-08-11T14:00:11+09:00",
                "2026-08-11T14:00:20+09:00",
            )
        )
        self.complete_timing_journey(response)
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T14:00:25+09:00",
                "2026-08-11T14:00:26+09:00",
                "2026-08-11T14:00:35+09:00",
            )
        )
        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertEqual(timeline["route"], ["A", "B", "D"])
        self.assertEqual(timeline["segments"]["A_to_B_seconds"], 5.0)
        self.assertEqual(timeline["segments"]["B_to_D_seconds"], 5.0)
        self.assertEqual(timeline["total_route_seconds"], 35.0)
        self.assertEqual(timeline["journey_elapsed_seconds"], 36.0)

    def test_journey_elapsed_uses_entry_line_and_d_exit_without_middle_timing(
        self,
    ) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-A-C-D",
            "2026-08-11T13:40:10.250+09:00",
        )
        self.advance_timing_journey(response, "C")
        self.complete_timing_journey(response)
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:40:42.000+09:00",
                "2026-08-11T13:40:43.000+09:00",
                "2026-08-11T13:40:48.730+09:00",
            )
        )

        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertEqual(timeline["route"], ["A", "C", "D"])
        self.assertEqual(
            timeline["a_departure_at"],
            "2026-08-11T13:40:10.250+09:00",
        )
        self.assertEqual(
            timeline["d_exit_at"],
            "2026-08-11T13:40:48.730+09:00",
        )
        self.assertEqual(timeline["journey_elapsed_seconds"], 38.48)
        self.assertIsNone(timeline["total_route_seconds"])
        self.assertIsNone(timeline["segments"]["A_to_C_seconds"])
        self.assertIsNone(timeline["segments"]["C_to_D_seconds"])

    def test_journey_elapsed_a_b_d_keeps_total_route_meaning(self) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-A-B-D",
            "2026-08-11T13:40:10.250+09:00",
        )
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T13:40:12.000+09:00",
                None,
                "2026-08-11T13:40:14.000+09:00",
            )
        )
        self.advance_timing_journey(response, "B")
        self.complete_timing_journey(response)
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:40:42.000+09:00",
                None,
                "2026-08-11T13:40:48.730+09:00",
            )
        )

        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertEqual(timeline["route"], ["A", "B", "D"])
        self.assertEqual(timeline["journey_elapsed_seconds"], 38.48)
        self.assertEqual(timeline["total_route_seconds"], 36.73)

    def test_journey_elapsed_without_d_timing_is_null(self) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-NO-D",
            "2026-08-11T13:40:10.250+09:00",
        )
        self.advance_timing_journey(response, "B")
        self.complete_timing_journey(response)

        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertEqual(
            timeline["a_departure_at"],
            "2026-08-11T13:40:10.250+09:00",
        )
        self.assertIsNone(timeline["d_exit_at"])
        self.assertIsNone(timeline["journey_elapsed_seconds"])
        self.assertTrue(
            any(
                "D exited_at 없음" in warning
                for warning in timeline["validation_warnings"]
            )
        )

    def test_journey_elapsed_without_a_entry_timestamp_is_null(self) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-NO-A",
            "2026-08-11T13:40:10.250+09:00",
        )
        self.advance_timing_journey(response, "B")
        self.complete_timing_journey(response)
        with closing(self.connect()) as connection:
            connection.execute(
                "UPDATE journeys SET entry_at = '' WHERE journey_id = ?",
                (response["journey_id"],),
            )
            connection.commit()
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:40:42+09:00",
                None,
                "2026-08-11T13:40:48.730+09:00",
            )
        )

        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertIsNone(timeline["a_departure_at"])
        self.assertIsNone(timeline["journey_elapsed_seconds"])
        self.assertTrue(
            any(
                "A ENTRY timestamp 없음" in warning
                for warning in timeline["validation_warnings"]
            )
        )

    def test_journey_elapsed_negative_is_null_with_warning(self) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-NEGATIVE",
            "2026-08-11T13:40:50.000+09:00",
        )
        self.advance_timing_journey(response, "C")
        self.complete_timing_journey(response)
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:40:42+09:00",
                None,
                "2026-08-11T13:40:48.730+09:00",
            )
        )

        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertIsNone(timeline["journey_elapsed_seconds"])
        self.assertTrue(
            any(
                "journey_elapsed_seconds: 음수 시간" in warning
                for warning in timeline["validation_warnings"]
            )
        )

    def test_journey_elapsed_parse_failure_is_null_with_warning(self) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-BAD-A",
            "2026-08-11T13:40:10.250+09:00",
        )
        self.advance_timing_journey(response, "B")
        self.complete_timing_journey(response)
        with closing(self.connect()) as connection:
            connection.execute(
                "UPDATE journeys SET entry_at = 'not-a-timestamp' "
                "WHERE journey_id = ?",
                (response["journey_id"],),
            )
            connection.commit()
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "D",
                "2026-08-11T13:40:42+09:00",
                None,
                "2026-08-11T13:40:48.730+09:00",
            )
        )

        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertIsNone(timeline["journey_elapsed_seconds"])
        self.assertTrue(
            any(
                "journey_elapsed_seconds: timestamp 형식 오류" in warning
                for warning in timeline["validation_warnings"]
            )
        )

    def test_duplicate_d_timing_keeps_elapsed_and_logs_once(self) -> None:
        response = self.start_timing_journey(
            "REQ-JOURNEY-ELAPSED-DUP",
            "2026-08-11T13:40:10.250+09:00",
        )
        self.advance_timing_journey(response, "B")
        self.complete_timing_journey(response)
        payload = self.timing_payload(
            response,
            "D",
            "2026-08-11T13:40:42+09:00",
            None,
            "2026-08-11T13:40:48.730+09:00",
        )

        with patch("builtins.print") as print_mock:
            first = main_server.handle_node_timing(payload)
            second = main_server.handle_node_timing(payload)
        completion_logs = [
            call
            for call in print_mock.call_args_list
            if call.args
            and "JOURNEY TIME COMPLETED" in str(call.args[0])
        ]
        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        with closing(self.connect()) as connection:
            d_count = connection.execute(
                "SELECT COUNT(*) FROM journey_node_visits "
                "WHERE journey_id = ? AND node_id = 'D'",
                (response["journey_id"],),
            ).fetchone()[0]

        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(d_count, 1)
        self.assertEqual(timeline["journey_elapsed_seconds"], 38.48)
        self.assertEqual(len(completion_logs), 1)

    def test_duplicate_timing_keeps_one_row(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-DUP")
        payload = self.timing_payload(
            response,
            "A",
            "2026-08-11T15:00:00+09:00",
            "2026-08-11T15:00:01+09:00",
            "2026-08-11T15:00:05+09:00",
        )
        first = main_server.handle_node_timing(payload)
        second = main_server.handle_node_timing(payload)
        with closing(self.connect()) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM journey_node_visits "
                "WHERE journey_id = ? AND node_id = 'A'",
                (response["journey_id"],),
            ).fetchone()[0]
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(count, 1)

    def test_earlier_entered_at_is_kept(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-EARLIEST")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T15:00:03+09:00",
                "2026-08-11T15:00:04+09:00",
                "2026-08-11T15:00:10+09:00",
            )
        )
        result = main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T15:00:00+09:00",
                "2026-08-11T15:00:02+09:00",
                "2026-08-11T15:00:08+09:00",
            )
        )
        self.assertEqual(result["entered_at"], "2026-08-11T15:00:00+09:00")
        self.assertEqual(result["matched_at"], "2026-08-11T15:00:02+09:00")
        self.assertEqual(result["exited_at"], "2026-08-11T15:00:10+09:00")
        self.assertEqual(result["dwell_seconds"], 10.0)

    def test_later_exited_at_is_kept(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-LATEST")
        first = self.timing_payload(
            response,
            "A",
            "2026-08-11T15:00:00+09:00",
            "2026-08-11T15:00:01+09:00",
            "2026-08-11T15:00:05+09:00",
        )
        second = dict(first)
        second["exited_at"] = "2026-08-11T15:00:09+09:00"
        main_server.handle_node_timing(first)
        result = main_server.handle_node_timing(second)
        self.assertEqual(result["exited_at"], second["exited_at"])
        self.assertEqual(result["dwell_seconds"], 9.0)

    def test_timing_person_uid_mismatch_is_rejected(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-PERSON-MISMATCH")
        payload = self.timing_payload(
            response,
            "A",
            "2026-08-11T15:00:00+09:00",
            None,
            None,
        )
        payload["person_uid"] = "P999999"
        payload["global_person_id"] = "P999999"
        with self.assertRaisesRegex(ValueError, "canonical person_uid"):
            main_server.handle_node_timing(payload)
        with closing(self.connect()) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM journey_node_visits"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_losing_middle_timing_does_not_change_route(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-LOSER")
        self.advance_timing_journey(response, "C")
        payload = self.timing_payload(
            response,
            "B",
            "2026-08-11T15:00:00+09:00",
            "2026-08-11T15:00:01+09:00",
            "2026-08-11T15:00:05+09:00",
        )
        with self.assertRaisesRegex(ValueError, "losing middle node"):
            main_server.handle_node_timing(payload)
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT route_json,status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
            timing_count = connection.execute(
                "SELECT COUNT(*) FROM journey_node_visits",
            ).fetchone()[0]
        self.assertEqual(json.loads(journey["route_json"]), ["A", "C"])
        self.assertEqual(journey["status"], "WAITING_D")
        self.assertEqual(timing_count, 0)

    def test_timing_does_not_change_visit_count(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-VISIT")
        with closing(self.connect()) as connection:
            before = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (response["person_uid"],),
            ).fetchone()[0]
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T15:00:00+09:00",
                None,
                None,
            )
        )
        with closing(self.connect()) as connection:
            after = connection.execute(
                "SELECT visit_count FROM persons WHERE person_uid = ?",
                (response["person_uid"],),
            ).fetchone()[0]
        self.assertEqual(after, before)

    def test_timing_does_not_change_journey_status(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-STATUS")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T15:00:00+09:00",
                None,
                None,
            )
        )
        with closing(self.connect()) as connection:
            journey = connection.execute(
                "SELECT status,route_json FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
        self.assertEqual(journey["status"], "WAITING_B_OR_C")
        self.assertEqual(json.loads(journey["route_json"]), ["A"])

    def test_timing_mqtt_dispatch_rejects_retained_message(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-MQTT")
        payload = self.timing_payload(
            response,
            "A",
            "2026-08-11T15:00:00+09:00",
            None,
            None,
        )

        message = type("TimingMessage", (), {})()
        message.topic = main_server.TOPIC_A_TIMING
        message.retain = True
        message.payload = json.dumps(payload).encode("utf-8")

        main_server.on_message(self.client, None, message)
        with closing(self.connect()) as connection:
            retained_count = connection.execute(
                "SELECT COUNT(*) FROM journey_node_visits"
            ).fetchone()[0]
        self.assertEqual(retained_count, 0)

        message.retain = False
        main_server.on_message(self.client, None, message)
        with closing(self.connect()) as connection:
            accepted_count = connection.execute(
                "SELECT COUNT(*) FROM journey_node_visits"
            ).fetchone()[0]
        self.assertEqual(accepted_count, 1)

    def test_timeline_missing_timestamps_returns_null_metrics(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-MISSING")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T15:00:00+09:00",
                None,
                None,
            )
        )
        self.advance_timing_journey(response, "B")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "B",
                "2026-08-11T15:00:10+09:00",
                None,
                None,
            )
        )
        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertIsNone(timeline["segments"]["A_to_B_seconds"])
        self.assertIsNone(timeline["segments"]["B_to_D_seconds"])
        self.assertIsNone(timeline["total_route_seconds"])
        self.assertTrue(
            all(node["dwell_seconds"] is None for node in timeline["nodes"])
        )

    def test_negative_timeline_metric_is_null_with_warning(self) -> None:
        response = self.start_timing_journey("REQ-TIMING-NEGATIVE")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "A",
                "2026-08-11T15:00:00+09:00",
                None,
                "2026-08-11T15:00:10+09:00",
            )
        )
        self.advance_timing_journey(response, "B")
        main_server.handle_node_timing(
            self.timing_payload(
                response,
                "B",
                "2026-08-11T15:00:05+09:00",
                None,
                "2026-08-11T15:00:15+09:00",
            )
        )
        timeline = main_server.get_journey_timeline(
            response["journey_id"]
        )
        self.assertIsNone(timeline["segments"]["A_to_B_seconds"])
        self.assertTrue(timeline["validation_warnings"])

    def test_additive_multimodal_migration_preserves_legacy_rows(self) -> None:
        original_path = main_server.DB_PATH
        legacy_path = Path(self.temp_dir.name) / "legacy-main-server.db"
        embedding_blob = main_server.embedding_to_blob(
            main_server.normalize_embedding(
                load_fixture("a_entry.json")["embedding"]
            )
        )
        with closing(sqlite3.connect(legacy_path)) as connection:
            connection.executescript(
                """
                CREATE TABLE persons (
                    person_uid TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                CREATE TABLE person_embeddings (
                    embedding_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    person_uid TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    quality REAL NOT NULL,
                    embedding BLOB NOT NULL
                );
                CREATE TABLE journeys (
                    journey_id TEXT PRIMARY KEY,
                    person_uid TEXT NOT NULL,
                    status TEXT NOT NULL,
                    route_json TEXT NOT NULL,
                    entry_at TEXT NOT NULL
                );
                CREATE TABLE journey_gallery (
                    gallery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    journey_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    quality REAL NOT NULL,
                    embedding BLOB NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT INTO persons VALUES (?, ?, ?, ?)",
                ("P000001", main_server.now_iso(), main_server.now_iso(), "ACTIVE"),
            )
            connection.execute(
                "INSERT INTO journeys VALUES (?, ?, ?, ?, ?)",
                ("J000001", "P000001", "COMPLETED", '["A","B","D"]', main_server.now_iso()),
            )
            connection.execute(
                "INSERT INTO person_embeddings "
                "(person_uid,node_id,captured_at,quality,embedding) VALUES (?,?,?,?,?)",
                ("P000001", "A", main_server.now_iso(), 1.0, embedding_blob),
            )
            connection.execute(
                "INSERT INTO journey_gallery "
                "(journey_id,node_id,captured_at,quality,embedding) VALUES (?,?,?,?,?)",
                ("J000001", "A", main_server.now_iso(), 1.0, embedding_blob),
            )
            connection.commit()

        try:
            main_server.DB_PATH = legacy_path
            main_server.initialize_database()
            main_server.initialize_database()
            with closing(sqlite3.connect(legacy_path)) as connection:
                person_row = connection.execute(
                    "SELECT modality, embedding_dim FROM person_embeddings"
                ).fetchone()
                journey_row = connection.execute(
                    "SELECT modality, embedding_dim FROM journey_gallery"
                ).fetchone()
                indexes = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='index'"
                    )
                }
                person_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(persons)"
                    )
                }
                review_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(review_cases)"
                    )
                }
                timing_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(journey_node_visits)"
                    )
                }
                capture_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(captures)"
                    )
                }
                counts = (
                    connection.execute(
                        "SELECT COUNT(*) FROM person_embeddings"
                    ).fetchone()[0],
                    connection.execute(
                        "SELECT COUNT(*) FROM journey_gallery"
                    ).fetchone()[0],
                )
        finally:
            main_server.DB_PATH = original_path

        self.assertEqual(person_row, ("BODY", 512))
        self.assertEqual(journey_row, ("BODY", 512))
        self.assertEqual(counts, (1, 1))
        self.assertIn("idx_person_embeddings_uid_modality", indexes)
        self.assertIn("idx_journey_gallery_id_modality", indexes)
        self.assertNotIn("idx_one_active_journey_per_person", indexes)
        self.assertIn("idx_persons_merged_into", indexes)
        self.assertIn("idx_review_cases_status", indexes)
        self.assertIn("idx_journey_node_visits_journey", indexes)
        self.assertIn("idx_journey_node_visits_person", indexes)
        self.assertIn("idx_journey_node_visits_node", indexes)
        self.assertIn("merged_into_person_uid", person_columns)
        self.assertTrue(
            {
                "representative_capture_id",
                "representative_source",
                "representative_updated_at",
            }.issubset(person_columns)
        )
        self.assertEqual(
            review_columns,
            {
                "review_id",
                "journey_id",
                "provisional_person_uid",
                "candidate_person_uid",
                "initial_decision",
                "initial_scores_json",
                "status",
                "action",
                "target_person_uid",
                "final_review_result",
                "final_candidate_person_uid",
                "canonical_person_uid",
                "final_scores_json",
                "route_json",
                "resolution_source",
                "final_reviewed_at",
                "pending_person_created",
                "created_at",
                "resolved_at",
            },
        )
        self.assertEqual(
            capture_columns,
            {
                "capture_id",
                "capture_key",
                "request_id",
                "journey_id",
                "person_uid",
                "camera_id",
                "capture_type",
                "source_url",
                "stored_path",
                "quality_score",
                "sha256",
                "mime_type",
                "captured_at",
                "cache_status",
                "cache_error",
                "created_at",
            },
        )
        self.assertEqual(
            timing_columns,
            {
                "journey_id",
                "person_uid",
                "node_id",
                "local_track_id",
                "entered_at",
                "matched_at",
                "exited_at",
                "dwell_seconds",
                "exit_reason",
                "created_at",
                "updated_at",
            },
        )


if __name__ == "__main__":
    unittest.main()
