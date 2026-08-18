from __future__ import annotations

import gc
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from cctv_main import api_server, main_server


FIXTURE = Path(__file__).parent / "fixtures" / "team_a" / "a_entry.json"


class PublishResult:
    rc = 0


class Client:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []
        self.lock = threading.Lock()

    def publish(self, topic: str, payload: str, **_: Any) -> PublishResult:
        with self.lock:
            self.messages.append((topic, json.loads(payload)))
        return PublishResult()

    def responses(self) -> list[dict[str, Any]]:
        return [
            payload
            for topic, payload in self.messages
            if topic == main_server.TOPIC_A_ENTRY_RESPONSE
        ]


def axis(index: int, dimension: int = 512) -> list[float]:
    result = [0.0] * dimension
    result[index] = 1.0
    return result


def vector_with_similarity(similarity: float, other_index: int) -> list[float]:
    result = [0.0] * 512
    result[0] = similarity
    result[other_index] = (1.0 - similarity**2) ** 0.5
    return result


class IdentityDecisionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.original_db = main_server.DB_PATH
        self.original_capture_settings = main_server.CAPTURE_CACHE_SETTINGS
        main_server.CAPTURE_CACHE_SETTINGS = replace(
            self.original_capture_settings, enabled=False
        )
        main_server.DB_PATH = Path(self.temp.name) / "identity.db"
        main_server.initialize_database()
        self.client = Client()

    def tearDown(self) -> None:
        main_server.DB_PATH = self.original_db
        main_server.CAPTURE_CACHE_SETTINGS = self.original_capture_settings
        gc.collect()
        self.temp.cleanup()

    def connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(main_server.DB_PATH)
        connection.row_factory = sqlite3.Row
        return connection

    def entry(
        self,
        request_id: str,
        embedding: list[float] | None = None,
    ) -> dict[str, Any]:
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        payload["request_id"] = request_id
        payload["timestamp"] = main_server.now_iso()
        payload["face_available"] = False
        if embedding is not None:
            payload["embedding"] = embedding
            payload["embedding_dim"] = 512
        return payload

    def seed_person(self, person_uid: str, embedding: list[float]) -> None:
        timestamp = main_server.now_iso()
        with closing(self.connection()) as connection:
            connection.execute(
                """
                INSERT INTO persons (
                    person_uid, created_at, last_seen_at, status,
                    visit_count, merged_into_person_uid
                ) VALUES (?, ?, ?, 'ACTIVE', 1, NULL)
                """,
                (person_uid, timestamp, timestamp),
            )
            connection.execute(
                """
                INSERT INTO person_embeddings (
                    person_uid,node_id,captured_at,quality,
                    modality,embedding_dim,embedding
                ) VALUES (?, 'A', ?, 1.0, 'BODY', 512, ?)
                """,
                (
                    person_uid,
                    timestamp,
                    main_server.embedding_to_blob(
                        main_server.normalize_embedding(embedding)
                    ),
                ),
            )
            connection.commit()

    def seed_embedding(
        self,
        person_uid: str,
        embedding: list[float],
        modality: str,
        quality: float = 1.0,
    ) -> None:
        dimension = 128 if modality == "FACE" else 512
        with closing(self.connection()) as connection:
            connection.execute(
                """
                INSERT INTO person_embeddings (
                    person_uid,node_id,captured_at,quality,
                    modality,embedding_dim,embedding
                ) VALUES (?, 'A', ?, ?, ?, ?, ?)
                """,
                (
                    person_uid,
                    main_server.now_iso(),
                    quality,
                    modality,
                    dimension,
                    main_server.embedding_to_blob(
                        main_server.normalize_embedding(embedding, dimension),
                        dimension,
                    ),
                ),
            )
            connection.commit()

    def body_payload(
        self,
        request_id: str,
        embeddings: list[list[float]],
        qualities: list[float] | None = None,
    ) -> dict[str, Any]:
        payload = self.entry(request_id)
        payload.update(
            {
                "body_count": len(embeddings),
                "body_embedding_dim": 512,
                "body_embeddings": embeddings,
                "body_qualities": qualities or [0.95] * len(embeddings),
                "body_capture_paths": [
                    f"/tmp/{request_id}-{index}.jpg"
                    for index in range(len(embeddings))
                ],
            }
        )
        return payload

    def test_same_person_three_entries_reuse_one_active_journey(self) -> None:
        for index in range(3):
            payload = self.entry(f"SAME-{index}")
            payload.update(
                {
                    "body_count": 2,
                    "body_embedding_dim": 512,
                    "body_embeddings": [axis(0), axis(0)],
                    "body_qualities": [0.95, 0.94],
                    "body_capture_paths": ["/tmp/a.jpg", "/tmp/b.jpg"],
                }
            )
            main_server.handle_a_entry(
                self.client,
                payload,
            )
        responses = self.client.responses()
        self.assertEqual(len({item["journey_id"] for item in responses}), 1)
        self.assertEqual(len({item["person_uid"] for item in responses}), 1)
        self.assertEqual(
            [item["identity_result"] for item in responses],
            ["NEW", "NEW", "NEW"],
        )
        with closing(self.connection()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM journeys").fetchone()[0], 1)

    def test_different_people_do_not_auto_merge(self) -> None:
        for index in range(3):
            main_server.handle_a_entry(
                self.client,
                self.entry(f"DIFFERENT-{index}", axis(index)),
            )
        responses = self.client.responses()
        self.assertEqual(len({item["person_uid"] for item in responses}), 3)
        self.assertTrue(all(item["identity_result"] == "NEW" for item in responses))

    def test_single_body_frame_without_face_requires_review(self) -> None:
        self.seed_person("P000001", axis(0))
        main_server.handle_a_entry(
            self.client,
            self.body_payload("ONE-FRAME", [axis(0)]),
        )
        response = self.client.responses()[-1]
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertEqual(response["review_status"], "PENDING")
        self.assertEqual(
            response["decision_reason"],
            "INSUFFICIENT_MULTIFRAME_CONSISTENCY",
        )

    def test_two_consistent_body_frames_are_normal_returning(self) -> None:
        self.seed_person("P000001", axis(0))
        main_server.handle_a_entry(
            self.client,
            self.body_payload("NORMAL-RETURN", [axis(0), axis(0)]),
        )
        response = self.client.responses()[-1]
        self.assertEqual(response["identity_result"], "RETURNING")
        self.assertEqual(response["person_uid"], "P000001")
        self.assertEqual(
            response["decision_reason"],
            "AUTO_MATCH_BODY_THRESHOLDS_MARGIN_AND_CONSISTENCY",
        )

    def test_p000005_body_only_two_frames_with_margin_is_returning(self) -> None:
        self.seed_person("P000005", axis(0))
        self.seed_person("P000006", axis(1))

        main_server.handle_a_entry(
            self.client,
            self.body_payload("P5-BODY-RETURN", [axis(0), axis(0)]),
        )

        response = self.client.responses()[-1]
        self.assertEqual(response["person_status"], "RETURNING")
        self.assertEqual(response["identity_result"], "RETURNING")
        self.assertEqual(response["person_uid"], "P000005")
        self.assertEqual(response["candidate_person_uid"], "P000005")
        self.assertEqual(response["canonical_person_uid"], "P000005")
        self.assertTrue(response["identity_confirmed"])
        self.assertTrue(response["body_multiframe_consistent"])
        self.assertEqual(response["body_consistent_match_count"], 2)

    def test_p000005_top1_with_insufficient_margin_is_pending(self) -> None:
        self.seed_person("P000005", vector_with_similarity(0.82, 10))
        self.seed_person("P000006", vector_with_similarity(0.80, 11))

        main_server.handle_a_entry(
            self.client,
            self.body_payload("P5-CLOSE-MARGIN", [axis(0), axis(0)]),
        )

        response = self.client.responses()[-1]
        self.assertEqual(response["person_status"], "IDENTITY_PENDING")
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertEqual(response["person_uid"], "P000005")
        self.assertEqual(response["tracking_person_uid"], "P000005")
        self.assertEqual(response["candidate_person_uid"], "P000005")
        self.assertIsNone(response["canonical_person_uid"])
        self.assertFalse(response["identity_confirmed"])
        self.assertEqual(response["decision_reason"], "INSUFFICIENT_MARGIN")

    def test_two_body_frames_with_different_candidates_are_pending(self) -> None:
        self.seed_person("P000005", axis(0))
        self.seed_person("P000006", axis(1))

        main_server.handle_a_entry(
            self.client,
            self.body_payload("P5-FRAME-CONFLICT", [axis(0), axis(1)]),
        )

        response = self.client.responses()[-1]
        self.assertEqual(response["person_status"], "IDENTITY_PENDING")
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertFalse(response["identity_confirmed"])
        self.assertTrue(response["body_frame_candidate_conflict"])
        self.assertEqual(
            response["body_frame_candidate_person_uids"],
            ["P000005", "P000006"],
        )
        self.assertEqual(
            response["decision_reason"], "BODY_FRAME_CANDIDATE_CONFLICT"
        )

    def test_face_and_body_candidate_conflict_is_pending(self) -> None:
        self.seed_person("P000005", axis(0))
        self.seed_person("P000006", axis(1))
        self.seed_embedding("P000005", axis(0, 128), "FACE")
        self.seed_embedding("P000006", axis(1, 128), "FACE")
        payload = self.body_payload(
            "P5-BODY-FACE-CONFLICT", [axis(0), axis(0)]
        )
        payload.update(
            {
                "face_available": True,
                "face_count": 2,
                "face_embedding_dim": 128,
                "face_embeddings": [axis(1, 128), axis(1, 128)],
                "face_qualities": [0.95, 0.94],
                "face_capture_paths": ["/tmp/f1.jpg", "/tmp/f2.jpg"],
            }
        )

        main_server.handle_a_entry(self.client, payload)

        response = self.client.responses()[-1]
        self.assertEqual(response["person_status"], "IDENTITY_PENDING")
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertFalse(response["identity_confirmed"])
        self.assertEqual(
            response["decision_reason"], "BODY_FACE_CANDIDATE_CONFLICT"
        )

    def test_similar_clothes_with_close_top_two_requires_review(self) -> None:
        self.seed_person("P000001", vector_with_similarity(0.82, 10))
        self.seed_person("P000002", vector_with_similarity(0.80, 11))
        main_server.handle_a_entry(
            self.client,
            self.body_payload("CLOSE-TOP2", [axis(0), axis(0)]),
        )
        response = self.client.responses()[-1]
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertEqual(response["review_status"], "PENDING")
        self.assertLess(response["match_margin"], main_server.PERSON_MATCH_MARGIN)

    def test_active_only_redetection_reuses_existing_journey(self) -> None:
        main_server.handle_a_entry(
            self.client,
            self.body_payload("ACTIVE-SEED", [axis(0), axis(0)]),
        )
        borderline = vector_with_similarity(0.80, 20)
        main_server.handle_a_entry(
            self.client,
            self.body_payload("ACTIVE-BORDERLINE", [borderline, borderline]),
        )
        responses = self.client.responses()
        response = responses[-1]
        self.assertEqual(response["journey_id"], responses[0]["journey_id"])
        self.assertEqual(response["identity_result"], "NEW")
        self.assertTrue(response["identity_confirmed"])

    def test_high_best_but_weak_topk_is_not_auto_returning(self) -> None:
        self.seed_person("P000001", axis(0))
        self.seed_embedding("P000001", axis(1), "BODY")
        self.seed_embedding("P000001", axis(2), "BODY")
        main_server.handle_a_entry(
            self.client,
            self.body_payload("WEAK-TOPK", [axis(0), axis(0)]),
        )
        response = self.client.responses()[-1]
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertGreaterEqual(response["person_best_score"], 0.75)
        self.assertLess(response["person_topk_score"], 0.68)

    def test_low_quality_match_requires_review_and_blocks_promotion(self) -> None:
        self.seed_person("P000001", axis(0))
        main_server.handle_a_entry(
            self.client,
            self.body_payload(
                "LOW-QUALITY",
                [axis(0), axis(0)],
                qualities=[0.30, 0.40],
            ),
        )
        response = self.client.responses()[-1]
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertEqual(response["decision_reason"], "INSUFFICIENT_QUALITY")
        self.assertFalse(response["gallery_promotion_allowed"])

    def test_strong_consistent_face_can_corroborate_one_body_frame(self) -> None:
        self.seed_person("P000001", axis(0))
        self.seed_embedding("P000001", axis(0, 128), "FACE")
        payload = self.body_payload("FACE-SUPPORT", [axis(0)])
        payload.update(
            {
                "face_available": True,
                "face_count": 2,
                "face_embedding_dim": 128,
                "face_embeddings": [axis(0, 128), axis(0, 128)],
                "face_qualities": [0.95, 0.94],
                "face_capture_paths": ["/tmp/f1.jpg", "/tmp/f2.jpg"],
            }
        )
        main_server.handle_a_entry(self.client, payload)
        response = self.client.responses()[-1]
        self.assertEqual(response["identity_result"], "RETURNING")
        self.assertEqual(response["person_uid"], "P000001")

    def test_low_quality_face_is_not_promoted_into_existing_person(self) -> None:
        self.seed_person("P000001", axis(0))
        self.seed_embedding("P000001", axis(0, 128), "FACE")
        with closing(self.connection()) as connection:
            timestamp = main_server.now_iso()
            connection.execute(
                """
                INSERT INTO journeys (
                    journey_id,request_id,person_uid,status,route_json,entry_at,
                    visit_no,
                    person_status,identity_result,review_status,
                    canonical_person_uid,gallery_promotion_allowed
                ) VALUES ('J000001','PROMOTE','P000001','COMPLETED','["A","B","D"]',?,
                          1,'RETURNING','RETURNING','NOT_REQUIRED','P000001',1)
                """,
                (timestamp,),
            )
            for index in range(2):
                connection.execute(
                    """
                    INSERT INTO journey_gallery (
                        journey_id,node_id,captured_at,quality,modality,
                        embedding_dim,embedding
                    ) VALUES ('J000001','A',?,0.95,'BODY',512,?)
                    """,
                    (
                        timestamp,
                        main_server.embedding_to_blob(
                            main_server.normalize_embedding(axis(0))
                        ),
                    ),
                )
            connection.execute(
                """
                INSERT INTO journey_gallery (
                    journey_id,node_id,captured_at,quality,modality,
                    embedding_dim,embedding
                ) VALUES ('J000001','A',?,0.60,'FACE',128,?)
                """,
                (
                    timestamp,
                    main_server.embedding_to_blob(
                        main_server.normalize_embedding(axis(1, 128), 128),
                        128,
                    ),
                ),
            )
            before = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid='P000001' AND modality='FACE'"
            ).fetchone()[0]
            main_server.promote_journey_gallery(connection, "J000001", "P000001")
            after = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid='P000001' AND modality='FACE'"
            ).fetchone()[0]
        self.assertEqual(after, before)

    def create_ambiguous_review(self, request_id: str = "AMBIGUOUS") -> dict[str, Any]:
        self.seed_person("P000001", vector_with_similarity(0.75, 10))
        self.seed_person("P000002", vector_with_similarity(0.74, 11))
        main_server.handle_a_entry(self.client, self.entry(request_id, axis(0)))
        return self.client.responses()[-1]

    def test_ambiguous_margin_persists_top_k_without_new_person(self) -> None:
        response = self.create_ambiguous_review()
        self.assertEqual(response["identity_result"], "UNKNOWN")
        self.assertEqual(response["review_status"], "PENDING")
        with closing(self.connection()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 2)
            review = connection.execute(
                "SELECT * FROM review_cases WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
            candidates = connection.execute(
                """
                SELECT candidate_person_uid, rank, fused_similarity
                FROM identity_review_candidates
                WHERE review_id = ? ORDER BY rank
                """,
                (review["review_id"],),
            ).fetchall()
        self.assertEqual([row["candidate_person_uid"] for row in candidates], ["P000001", "P000002"])
        self.assertLess(float(candidates[0]["fused_similarity"]) - float(candidates[1]["fused_similarity"]), main_server.MARGIN_THRESHOLD)

    def test_admin_existing_resolution_is_idempotent_and_promotes(self) -> None:
        response = self.create_ambiguous_review("EXISTING")
        with closing(self.connection()) as connection:
            review_id = connection.execute(
                "SELECT review_id FROM review_cases WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        resolved = main_server.resolve_review_merge_existing(review_id, "P000001")
        repeated = main_server.resolve_review_merge_existing(review_id, "P000001")
        self.assertEqual(resolved["outcome"], "RESOLVED")
        self.assertEqual(repeated["outcome"], "ALREADY_RESOLVED")
        with closing(self.connection()) as connection:
            journey = connection.execute(
                "SELECT person_uid,identity_result,review_status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
            promoted = connection.execute(
                "SELECT COUNT(*) FROM person_embeddings WHERE person_uid = 'P000001'"
            ).fetchone()[0]
        self.assertEqual(tuple(journey), ("P000001", "RETURNING", "RESOLVED"))
        # A single ambiguous query may be linked manually, but it is not
        # automatically promoted into an established gallery.
        self.assertEqual(promoted, 1)

    def test_admin_new_resolution_creates_exactly_one_person(self) -> None:
        response = self.create_ambiguous_review("CONFIRM-NEW")
        with closing(self.connection()) as connection:
            review_id = connection.execute(
                "SELECT review_id FROM review_cases WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        resolved = main_server.resolve_review_confirm_new(review_id)
        repeated = main_server.resolve_review_confirm_new(review_id)
        self.assertEqual(resolved["outcome"], "RESOLVED")
        self.assertEqual(repeated["outcome"], "ALREADY_RESOLVED")
        with closing(self.connection()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 3)
            journey = connection.execute(
                "SELECT person_uid,identity_result,review_status FROM journeys WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
        self.assertEqual(journey["person_uid"], resolved["target_person_uid"])
        self.assertEqual((journey["identity_result"], journey["review_status"]), ("NEW", "RESOLVED"))

    def test_duplicate_request_is_idempotent(self) -> None:
        self.seed_person("P000005", axis(0))
        payload = self.body_payload(
            "DUPLICATE-P5", [axis(0), axis(0)]
        )
        main_server.handle_a_entry(self.client, payload)
        main_server.handle_a_entry(self.client, payload)
        responses = self.client.responses()
        self.assertEqual(responses[0]["journey_id"], responses[1]["journey_id"])
        self.assertEqual(responses[0]["canonical_person_uid"], "P000005")
        self.assertEqual(responses[1]["canonical_person_uid"], "P000005")
        self.assertTrue(responses[0]["identity_confirmed"])
        self.assertTrue(responses[1]["identity_confirmed"])
        with closing(self.connection()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM journeys").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 1)

    def test_concurrent_similar_entries_share_person_and_active_journey(self) -> None:
        def consistent_entry(index: int) -> dict[str, Any]:
            payload = self.entry(f"CONCURRENT-{index}")
            payload.update(
                {
                    "body_count": 2,
                    "body_embedding_dim": 512,
                    "body_embeddings": [axis(0), axis(0)],
                    "body_qualities": [0.95, 0.94],
                    "body_capture_paths": ["/tmp/a.jpg", "/tmp/b.jpg"],
                }
            )
            return payload

        threads = [
            threading.Thread(
                target=main_server.handle_a_entry,
                args=(self.client, consistent_entry(index)),
            )
            for index in range(3)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        with closing(self.connection()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM journeys").fetchone()[0], 1)

    def test_invalid_embeddings_are_rejected_with_reason(self) -> None:
        for embedding, reason in (
            ([0.0] * 511, "INVALID_DIM"),
            ([0.0] * 512, "ZERO_NORM"),
            ([float("nan")] + [0.0] * 511, "NAN_OR_INF"),
        ):
            with self.assertRaisesRegex(ValueError, reason):
                main_server.parse_a_entry_samples(self.entry(reason, embedding))

    def test_body_gallery_sizes_one_two_three_and_empty(self) -> None:
        for count in (1, 2, 3):
            payload = self.entry(f"GALLERY-{count}")
            payload.update(
                {
                    "body_count": count,
                    "body_embedding_dim": 512,
                    "body_embeddings": [axis(index) for index in range(count)],
                    "body_qualities": [1.0] * count,
                    "body_capture_paths": [f"/tmp/{index}.jpg" for index in range(count)],
                }
            )
            parsed = main_server.parse_a_entry_samples(payload)
            self.assertEqual(len(parsed["body_samples"]), count)
        empty = self.entry("EMPTY")
        empty.pop("embedding", None)
        empty["body_embeddings"] = []
        with self.assertRaisesRegex(ValueError, "NO_GALLERY"):
            main_server.parse_a_entry_samples(empty)

    def test_review_and_candidates_survive_reinitialization(self) -> None:
        response = self.create_ambiguous_review("RESTART")
        main_server.initialize_database()
        main_server.initialize_database()
        with closing(self.connection()) as connection:
            review = connection.execute(
                "SELECT review_id,status FROM review_cases WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()
            candidate_count = connection.execute(
                "SELECT COUNT(*) FROM identity_review_candidates WHERE review_id = ?",
                (review["review_id"],),
            ).fetchone()[0]
        self.assertEqual(review["status"], "PENDING")
        self.assertEqual(candidate_count, 2)

    def test_identity_review_api_get_and_idempotent_post(self) -> None:
        response = self.create_ambiguous_review("API-REVIEW")
        with closing(self.connection()) as connection:
            review_id = connection.execute(
                "SELECT review_id FROM review_cases WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        server = api_server.create_server(
            host="127.0.0.1",
            port=0,
            db_path=main_server.DB_PATH,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            with urlopen(f"{base}/api/identity-reviews?status=PENDING") as raw:
                listing = json.load(raw)
            self.assertEqual(listing["items"][0]["review_id"], review_id)
            with urlopen(f"{base}/api/identity-reviews/{review_id}") as raw:
                detail = json.load(raw)
            self.assertEqual(len(detail["candidates"]), 2)

            body = json.dumps(
                {
                    "action": "SELECT_EXISTING",
                    "selected_person_uid": "P000001",
                }
            ).encode("utf-8")
            for expected in ("RESOLVED", "ALREADY_RESOLVED"):
                request = Request(
                    f"{base}/api/identity-reviews/{review_id}/resolve",
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as raw:
                    result = json.load(raw)
                self.assertEqual(result["outcome"], expected)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_case_a_good_body_fail_and_face_pass_blocks_auto_returning(self) -> None:
        # Case A (J000012 reproduction): Body fails (<0.72), Face passes (0.6282 >= 0.363)
        self.seed_person("P000001", axis(0, 512))
        self.seed_embedding("P000001", axis(0, 128), "FACE", 0.95)
        face_emb = [0.0] * 128
        face_emb[0] = 0.6282
        face_emb[1] = (1.0 - 0.6282**2) ** 0.5

        payload = self.entry("REPRO-J000012")
        payload.update(
            {
                "body_count": 1,
                "body_embedding_dim": 512,
                "body_embeddings": [vector_with_similarity(0.6746, 1)],
                "body_qualities": [0.90],
                "face_available": True,
                "face_count": 2,
                "face_embedding_dim": 128,
                "face_embeddings": [face_emb, face_emb],
                "face_qualities": [0.90, 0.90],
            }
        )
        main_server.handle_a_entry(self.client, payload)
        responses = self.client.responses()
        self.assertEqual(len(responses), 1)
        resp = responses[0]
        self.assertNotEqual(resp["identity_result"], "RETURNING")
        self.assertEqual(resp["person_status"], "IDENTITY_PENDING")
        self.assertEqual(resp["decision_reason"], "BODY_EVIDENCE_REJECTS_FACE_OVERRIDE")

    def test_case_b_body_pass_and_face_pass_auto_returning(self) -> None:
        # Case B: Good body passes (>0.75) and face passes
        self.seed_person("P000001", axis(0, 512))
        self.seed_embedding("P000001", axis(0, 128), "FACE", 0.95)
        face_emb = [0.0] * 128
        face_emb[0] = 0.65
        face_emb[1] = (1.0 - 0.65**2) ** 0.5

        payload = self.entry("BODY-PASS-FACE-PASS")
        payload.update(
            {
                "body_count": 2,
                "body_embedding_dim": 512,
                "body_embeddings": [vector_with_similarity(0.85, 1), vector_with_similarity(0.85, 1)],
                "body_qualities": [0.90, 0.90],
                "face_available": True,
                "face_count": 2,
                "face_embedding_dim": 128,
                "face_embeddings": [face_emb, face_emb],
                "face_qualities": [0.90, 0.90],
            }
        )
        main_server.handle_a_entry(self.client, payload)
        responses = self.client.responses()
        self.assertEqual(len(responses), 1)
        resp = responses[0]
        self.assertEqual(resp["identity_result"], "RETURNING")
        self.assertEqual(resp["decision_reason"], "AUTO_MATCH_BODY_THRESHOLDS_MARGIN_AND_CONSISTENCY")

    def test_case_c_body_strong_match_without_face_auto_returning(self) -> None:
        # Case C / D: Body strong match, no face provided
        self.seed_person("P000001", axis(0, 512))
        payload = self.entry("BODY-ONLY-PASS")
        payload.update(
            {
                "body_count": 2,
                "body_embedding_dim": 512,
                "body_embeddings": [vector_with_similarity(0.88, 1), vector_with_similarity(0.88, 1)],
                "body_qualities": [0.90, 0.90],
                "face_available": False,
                "face_count": 0,
                "face_embeddings": [],
            }
        )
        main_server.handle_a_entry(self.client, payload)
        responses = self.client.responses()
        self.assertEqual(len(responses), 1)
        resp = responses[0]
        self.assertEqual(resp["identity_result"], "RETURNING")
        self.assertEqual(resp["decision_reason"], "AUTO_MATCH_BODY_THRESHOLDS_MARGIN_AND_CONSISTENCY")

    def test_case_d_face_fallback_when_body_low_quality(self) -> None:
        # Case E: Body sample quality is below minimum threshold (<0.65) and strong face is provided
        self.seed_person("P000001", axis(0, 512))
        self.seed_embedding("P000001", axis(0, 128), "FACE", 0.95)
        face_emb = [0.0] * 128
        face_emb[0] = 0.80
        face_emb[1] = (1.0 - 0.80**2) ** 0.5

        payload = self.entry("LOW-QUALITY-BODY-FACE-FALLBACK")
        payload.update(
            {
                "body_count": 1,
                "body_embedding_dim": 512,
                "body_embeddings": [vector_with_similarity(0.40, 1)],
                "body_qualities": [0.30],  # below AUTO_DECISION_MIN_BODY_QUALITY (0.65)
                "face_available": True,
                "face_count": 2,
                "face_embedding_dim": 128,
                "face_embeddings": [face_emb, face_emb],
                "face_qualities": [0.90, 0.90],
            }
        )
        main_server.handle_a_entry(self.client, payload)
        responses = self.client.responses()
        self.assertEqual(len(responses), 1)
        resp = responses[0]
        self.assertEqual(resp["identity_result"], "RETURNING")
        self.assertEqual(resp["decision_reason"], "AUTO_MATCH_FACE_THRESHOLD_MARGIN_AND_CONSISTENCY")


if __name__ == "__main__":
    unittest.main()
