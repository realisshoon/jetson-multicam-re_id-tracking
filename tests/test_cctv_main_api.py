from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from cctv_main.api_server import create_server


INITIAL_103 = {
    "body": {
        "best_score": 0.695,
        "combined_score": 0.661,
        "match_margin": 0.027,
    },
    "face": {"candidate_person_uid": "P000045"},
}
FINAL_103 = {
    "body_all": {
        "person_uid": "P000002",
        "combined_score": 0.850,
        "match_margin": 0.167,
        "sample_count": 32,
    },
    "face": {"person_uid": "P000045", "combined_score": 0.797},
}
FINAL_104 = {
    "body_all": {
        "person_uid": "P000006",
        "combined_score": 0.798,
        "match_margin": 0.072,
        "sample_count": 24,
    },
    "face": {"person_uid": "P000045", "combined_score": 0.062},
}


def create_fixture_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            PRAGMA journal_mode = WAL;
            CREATE TABLE persons (
                person_uid TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                status TEXT NOT NULL,
                visit_count INTEGER NOT NULL DEFAULT 0,
                merged_into_person_uid TEXT
            );
            CREATE TABLE journeys (
                journey_id TEXT PRIMARY KEY,
                request_id TEXT,
                person_uid TEXT NOT NULL,
                status TEXT NOT NULL,
                route_json TEXT NOT NULL,
                entry_at TEXT NOT NULL,
                passage_at TEXT,
                arrival_at TEXT,
                completed_at TEXT,
                person_match_score REAL,
                second_match_score REAL,
                person_best_score REAL,
                person_topk_score REAL,
                person_combined_score REAL,
                second_person_score REAL,
                match_source TEXT,
                gallery_promotion_allowed INTEGER NOT NULL DEFAULT 0,
                person_status TEXT NOT NULL DEFAULT 'NEW',
                candidate_person_uid TEXT,
                entry_local_track_id TEXT,
                visit_no INTEGER,
                identity_result TEXT NOT NULL DEFAULT 'UNKNOWN',
                review_status TEXT NOT NULL DEFAULT 'NOT_REQUIRED',
                canonical_person_uid TEXT
            );
            CREATE TABLE review_cases (
                review_id TEXT PRIMARY KEY,
                journey_id TEXT NOT NULL UNIQUE,
                provisional_person_uid TEXT NOT NULL,
                candidate_person_uid TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                action TEXT,
                target_person_uid TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                initial_decision TEXT,
                initial_scores_json TEXT,
                final_review_result TEXT,
                final_candidate_person_uid TEXT,
                canonical_person_uid TEXT,
                final_scores_json TEXT,
                route_json TEXT,
                resolution_source TEXT,
                final_reviewed_at TEXT
            );
            CREATE TABLE journey_node_visits (
                journey_id TEXT NOT NULL,
                person_uid TEXT NOT NULL,
                node_id TEXT NOT NULL,
                local_track_id INTEGER,
                entered_at TEXT NOT NULL,
                matched_at TEXT,
                exited_at TEXT,
                dwell_seconds REAL,
                exit_reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (journey_id, node_id)
            );
            CREATE TABLE journey_captures (
                capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
                journey_id TEXT NOT NULL,
                person_uid TEXT NOT NULL,
                node_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                image_path TEXT NOT NULL,
                similarity REAL,
                quality REAL NOT NULL DEFAULT 1.0,
                verification_status TEXT NOT NULL DEFAULT 'AUTO_MATCHED',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE journey_gallery (
                gallery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                journey_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                quality REAL NOT NULL,
                embedding BLOB NOT NULL,
                modality TEXT NOT NULL DEFAULT 'BODY',
                embedding_dim INTEGER NOT NULL DEFAULT 512
            );
            CREATE TABLE journey_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                journey_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE detection_events (
                event_id TEXT PRIMARY KEY,
                event_at TEXT NOT NULL,
                node_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                identity_status TEXT NOT NULL,
                local_track_id INTEGER NOT NULL,
                journey_id TEXT,
                person_uid TEXT,
                canonical_person_uid TEXT,
                payload_json TEXT NOT NULL,
                received_at TEXT NOT NULL
            );
            """
        )
        people = [
            ("P000002", "ACTIVE", 8, None),
            ("P000006", "ACTIVE", 12, None),
            ("P000045", "ACTIVE", 2, None),
            ("P000071", "REVIEW_REQUIRED", 1, None),
            ("P000072", "MERGED", 1, "P000006"),
        ]
        connection.executemany(
            """
            INSERT INTO persons
              (person_uid, created_at, last_seen_at, status, visit_count,
               merged_into_person_uid)
            VALUES (?, '2026-08-11T15:00:00+09:00',
                    '2026-08-11T15:30:00+09:00', ?, ?, ?)
            """,
            people,
        )
        journeys = [
            (
                "J000103",
                "P000071",
                "COMPLETED",
                '["A","C","D"]',
                "2026-08-11T15:20:27+09:00",
                "REVIEW_REQUIRED",
                "P000045",
                1,
            ),
            (
                "J000104",
                "P000006",
                "COMPLETED",
                '["A","C","D"]',
                "2026-08-11T15:21:43+09:00",
                "MERGED",
                "P000006",
                12,
            ),
        ]
        connection.executemany(
            """
            INSERT INTO journeys
              (journey_id, person_uid, status, route_json, entry_at,
               passage_at, arrival_at, completed_at, person_status,
               candidate_person_uid, visit_no)
            VALUES (?, ?, ?, ?, ?,
                    '2026-08-11T15:21:49+09:00',
                    '2026-08-11T15:21:52+09:00',
                    '2026-08-11T15:21:52+09:00', ?, ?, ?)
            """,
            journeys,
        )
        connection.execute(
            """
            INSERT INTO review_cases
              (review_id, journey_id, provisional_person_uid,
               candidate_person_uid, status, created_at, initial_decision,
               initial_scores_json, final_review_result,
               final_candidate_person_uid, canonical_person_uid,
               final_scores_json, route_json, resolution_source,
               final_reviewed_at)
            VALUES ('R000017', 'J000103', 'P000071', 'P000045', 'PENDING',
                    '2026-08-11T15:20:27+09:00', 'IDENTITY_PENDING', ?,
                    'MANUAL_REVIEW_REQUIRED', 'P000002', NULL, ?,
                    '["A","C","D"]', 'FINAL_ROUTE_IDENTITY',
                    '2026-08-11T15:20:32+09:00')
            """,
            (json.dumps(INITIAL_103), json.dumps(FINAL_103)),
        )
        connection.execute(
            """
            INSERT INTO review_cases
              (review_id, journey_id, provisional_person_uid,
               candidate_person_uid, status, action, target_person_uid,
               created_at, resolved_at, initial_decision,
               initial_scores_json, final_review_result,
               final_candidate_person_uid, canonical_person_uid,
               final_scores_json, route_json, resolution_source,
               final_reviewed_at)
            VALUES ('R000018', 'J000104', 'P000072', 'P000006', 'RESOLVED',
                    'MERGE_EXISTING', 'P000006',
                    '2026-08-11T15:21:43+09:00',
                    '2026-08-11T15:21:52+09:00', 'IDENTITY_PENDING', '{}',
                    'REVISIT', 'P000006', 'P000006', ?,
                    '["A","C","D"]', 'FINAL_ROUTE_IDENTITY',
                    '2026-08-11T15:21:52+09:00')
            """,
            (json.dumps(FINAL_104),),
        )
        nodes = [
            (
                "J000103",
                "P000071",
                "A",
                5,
                "2026-08-11T15:20:25.893+09:00",
                "2026-08-11T15:20:27.583+09:00",
                "2026-08-11T15:20:37.683+09:00",
                11.790,
            ),
            (
                "J000103",
                "P000071",
                "D",
                4,
                "2026-08-11T15:20:31.928+09:00",
                "2026-08-11T15:20:33.206+09:00",
                "2026-08-11T15:20:40.880+09:00",
                8.952,
            ),
            (
                "J000104",
                "P000006",
                "A",
                13,
                "2026-08-11T15:21:35.702+09:00",
                "2026-08-11T15:21:43.380+09:00",
                "2026-08-11T15:21:55.558+09:00",
                19.856,
            ),
            (
                "J000104",
                "P000006",
                "D",
                13,
                "2026-08-11T15:21:51.745+09:00",
                "2026-08-11T15:21:52.878+09:00",
                "2026-08-11T15:22:01.141+09:00",
                9.396,
            ),
        ]
        connection.executemany(
            """
            INSERT INTO journey_node_visits
              (journey_id, person_uid, node_id, local_track_id, entered_at,
               matched_at, exited_at, dwell_seconds, exit_reason,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'TRACK_LOST',
                    '2026-08-11T15:30:00+09:00',
                    '2026-08-11T15:30:00+09:00')
            """,
            nodes,
        )
        connection.execute(
            """
            INSERT INTO journey_captures
              (journey_id, person_uid, node_id, captured_at, image_path,
               similarity, quality, metadata_json)
            VALUES ('J000104', 'P000006', 'D',
                    '2026-08-11T15:21:52+09:00',
                    '/home/aidl/work/captures/D/J000104.jpg',
                    0.759, 0.759, '{"local_track_id":13}')
            """
        )
        connection.execute(
            """
            INSERT INTO journey_gallery
              (journey_id, node_id, captured_at, quality, embedding,
               modality, embedding_dim)
            VALUES ('J000104', 'D', '2026-08-11T15:21:52+09:00',
                    0.759, ?, 'BODY', 512)
            """,
            (b"SECRET_EMBEDDING_VECTOR",),
        )
        entry_payload = {
            "body_capture_paths": [
                "/home/aidl/work/pj/outputs/captures/A/20260811/body 1.jpg",
                "/home/aidl/work/pj/outputs/captures/A/20260811/body_2.jpg",
                "/home/aidl/work/pj/outputs/captures/A/20260811/body_3.jpg",
                "/home/aidl/work/pj/outputs/captures/A/20260811/body_4.jpg",
            ],
            "body_qualities": [0.91, 0.82, 0.73, 0.64],
            "face_capture_paths": [
                "/home/aidl/work/pj/outputs/captures/A_face/20260811/face_1.jpg",
                "/home/aidl/work/pj/outputs/captures/A_face/20260811/face_2.jpg",
                "/home/aidl/work/pj/outputs/captures/A_face/20260811/face_3.jpg",
            ],
            "face_qualities": [0.88, 0.77, 0.66],
            "body_embeddings": [[1.0, 2.0]],
            "face_embeddings": [[3.0, 4.0]],
        }
        connection.execute(
            """
            INSERT INTO journey_events
              (journey_id, node_id, event_type, event_at, payload_json)
            VALUES ('J000104', 'A', 'ENTRY',
                    '2026-08-11T15:21:43+09:00', ?)
            """,
            (json.dumps(entry_payload),),
        )
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "web_api_j000061_j000062.json"
        )
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        connection.executemany(
            """
            INSERT INTO persons
              (person_uid, created_at, last_seen_at, status, visit_count)
            VALUES (?, '2026-08-14T13:47:00+09:00',
                    '2026-08-14T13:48:00+09:00', ?, ?)
            """,
            [
                (row["person_uid"], row["status"], row["visit_count"])
                for row in fixture["persons"]
            ],
        )
        connection.executemany(
            """
            INSERT INTO journeys
              (journey_id, request_id, person_uid, status, route_json,
               entry_at, passage_at, arrival_at, completed_at,
               person_status, candidate_person_uid, visit_no,
               identity_result, review_status, canonical_person_uid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            [
                (
                    row["journey_id"],
                    row["request_id"],
                    row["person_uid"],
                    row["status"],
                    json.dumps(row["route"]),
                    row["entry_at"],
                    row["passage_at"],
                    row["arrival_at"],
                    row["completed_at"],
                    row["person_status"],
                    row["candidate_person_uid"],
                    row["identity_result"],
                    row["review_status"],
                    row["canonical_person_uid"],
                )
                for row in fixture["journeys"]
            ],
        )
        connection.executemany(
            """
            INSERT INTO journey_events
              (journey_id, node_id, event_type, event_at, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row["journey_id"],
                    row["node"],
                    row["kind"],
                    row["at"],
                    json.dumps(row["payload"]),
                )
                for row in fixture["events"]
            ],
        )
        connection.executemany(
            """
            INSERT INTO journey_captures
              (journey_id, person_uid, node_id, captured_at, image_path,
               quality)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["journey_id"],
                    row["person_uid"],
                    row["node"],
                    row["at"],
                    row["path"],
                    row["quality"],
                )
                for row in fixture["captures"]
            ],
        )
        connection.commit()
    finally:
        connection.close()


def database_fingerprint(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        dump = "\n".join(connection.iterdump()).encode("utf-8")
    finally:
        connection.close()
    return hashlib.sha256(dump).hexdigest()


class CctvMainApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "main_server.db"
        create_fixture_database(self.db_path)
        self.server = create_server(
            "127.0.0.1",
            0,
            self.db_path,
            camera_a_image_base_url="http://camera-a.test:8000",
        )
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temp_dir.cleanup()

    def request(
        self,
        path: str,
        method: str = "GET",
    ) -> tuple[int, dict[str, Any], Any, bytes]:
        request = Request(self.base_url + path, method=method)
        try:
            with urlopen(request, timeout=3) as response:
                body = response.read()
                return (
                    response.status,
                    json.loads(body),
                    response.headers,
                    body,
                )
        except HTTPError as error:
            body = error.read()
            return error.code, json.loads(body), error.headers, body

    def readonly_get(
        self,
        path: str,
        method: str = "GET",
    ) -> tuple[int, dict[str, Any], Any, bytes]:
        before = database_fingerprint(self.db_path)
        result = self.request(path, method=method)
        after = database_fingerprint(self.db_path)
        self.assertEqual(before, after)
        return result

    def test_health_and_cors(self) -> None:
        status, payload, headers, _ = self.readonly_get("/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["database"], "ok")
        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")

        status, payload, _, _ = self.readonly_get("/api/status")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["database"], "ok")

    def test_dashboard_summary(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/dashboard/summary"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["persons_total"], 6)
        self.assertEqual(payload["persons_total_including_merged"], 7)
        self.assertEqual(payload["journeys_total"], 4)
        self.assertEqual(payload["pending_reviews"], 1)

    def test_journeys_list_and_filters(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/journeys?limit=1&status=COMPLETED&person_uid=P000006"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["journey_id"], "J000104")
        self.assertEqual(payload["items"][0]["person_uid"], "P000006")

        status, payload, _, _ = self.readonly_get(
            "/api/journeys?final_review_result=MANUAL_REVIEW_REQUIRED"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["items"][0]["journey_id"], "J000103")
        self.assertEqual(payload["items"][0]["journey_status"], "COMPLETED")
        self.assertEqual(payload["items"][0]["identity_result"], "UNKNOWN")
        self.assertEqual(payload["items"][0]["review_status"], "PENDING")
        self.assertFalse(payload["items"][0]["identity_confirmed"])
        self.assertIsNone(payload["items"][0]["person_uid"])
        self.assertEqual(
            payload["items"][0]["candidate_person_uid"], "P000045"
        )
        self.assertEqual(
            payload["items"][0]["tracking_person_uid"], "P000071"
        )
        self.assertIsNone(payload["items"][0]["canonical_person_uid"])

    def test_j000103_manual_review_is_not_canonicalized(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/journeys/J000103"
        )
        self.assertEqual(status, 200)
        self.assertIsNone(payload["person"]["person_uid"])
        self.assertEqual(
            payload["person"]["candidate_person_uid"], "P000045"
        )
        self.assertEqual(
            payload["person"]["tracking_person_uid"], "P000071"
        )
        self.assertFalse(payload["person"]["identity_confirmed"])
        self.assertEqual(payload["person"]["status"], "REVIEW_REQUIRED")
        self.assertEqual(payload["journey_status"], "COMPLETED")
        self.assertEqual(payload["identity_result"], "UNKNOWN")
        self.assertEqual(payload["review_status"], "PENDING")
        self.assertFalse(payload["identity_confirmed"])
        self.assertIsNone(payload["canonical_person_uid"])
        self.assertEqual(
            payload["final_review_result"], "MANUAL_REVIEW_REQUIRED"
        )
        identity = payload["identity"]
        self.assertEqual(identity["temporary_person_uid"], "P000071")
        self.assertEqual(identity["final_candidate_person_uid"], "P000002")
        self.assertIsNone(identity["canonical_person_uid"])
        self.assertEqual(identity["final_result"], "MANUAL_REVIEW_REQUIRED")
        self.assertAlmostEqual(identity["final_score"], 0.850)
        self.assertAlmostEqual(identity["final_margin"], 0.167)

    def test_j000104_revisit_uses_canonical_uid_and_timing(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/journeys/J000104"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["person"]["person_uid"], "P000006")
        self.assertEqual(payload["identity"]["temporary_person_uid"], "P000072")
        self.assertEqual(payload["identity"]["canonical_person_uid"], "P000006")
        self.assertEqual(payload["identity"]["final_result"], "REVISIT")
        self.assertEqual(payload["identity_result"], "RETURNING")
        self.assertEqual(payload["review_status"], "RESOLVED")
        self.assertTrue(payload["identity_confirmed"])
        self.assertEqual(payload["canonical_person_uid"], "P000006")
        self.assertAlmostEqual(payload["timing"]["elapsed_seconds"], 18.141)
        self.assertEqual(len(payload["nodes"]), 3)
        self.assertEqual(payload["nodes"][0]["local_track_id"], 13)

    def test_j000061_and_j000062_web_contract(self) -> None:
        for journey_id, canonical_uid in (
            ("J000061", "P000048"),
            ("J000062", "P000049"),
        ):
            status, payload, _, _ = self.readonly_get(
                f"/api/journeys/{journey_id}"
            )
            self.assertEqual(status, 200)
            self.assertEqual(payload["journey_status"], "COMPLETED")
            self.assertEqual(payload["route"], ["A", "C", "D"])
            self.assertEqual(
                [node["node_id"] for node in payload["nodes"]],
                ["A", "C", "D"],
            )
            self.assertEqual(
                [capture["node_id"] for capture in payload["captures"]],
                ["A", "C", "D"],
            )
            self.assertEqual(payload["canonical_person_uid"], canonical_uid)
            self.assertEqual(
                payload["identity"]["canonical_person_uid"], canonical_uid
            )
            self.assertEqual(payload["person_status"], "NEW")
            self.assertIsNotNone(payload["arrival_at"])
            self.assertIsNotNone(payload["completed_at"])
            self.assertGreater(
                payload["timing"]["completion_duration_seconds"], 0
            )

        status, listing, _, _ = self.readonly_get("/api/journeys?limit=20")
        self.assertEqual(status, 200)
        by_id = {item["journey_id"]: item for item in listing["items"]}
        for journey_id in ("J000061", "J000062"):
            item = by_id[journey_id]
            self.assertEqual(item["route"], ["A", "C", "D"])
            self.assertEqual(
                [node["node_id"] for node in item["nodes"]],
                ["A", "C", "D"],
            )
            self.assertEqual(
                [capture["node_id"] for capture in item["captures"]],
                ["A", "C", "D"],
            )

    def test_events_since_contract_is_ordered_unique_and_canonical_only(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/events?since=2026-08-14T13%3A47%3A00%2B09%3A00"
        )
        self.assertEqual(status, 200)
        items = payload["items"]
        self.assertEqual(len(items), 6)
        self.assertEqual(len({item["event_id"] for item in items}), 6)
        self.assertEqual(
            [(item["node"], item["kind"]) for item in items[:3]],
            [("A", "ENTRY"), ("C", "PASSAGE"), ("D", "ARRIVAL")],
        )
        self.assertEqual(items[0]["canonical_person_uid"], "P000048")
        self.assertEqual(items[-1]["canonical_person_uid"], "P000049")
        self.assertNotEqual(items[-1]["canonical_person_uid"], "P000048")
        self.assertEqual(items[-1]["identity_status"], "NEW")

    def test_events_rejects_missing_invalid_or_naive_since(self) -> None:
        for path in (
            "/api/events",
            "/api/events?since=not-a-time",
            "/api/events?since=2026-08-14T13%3A47%3A00",
        ):
            status, payload, _, _ = self.readonly_get(path)
            self.assertEqual(status, 400)
            self.assertEqual(payload["error"], "invalid_query")

    def test_events_never_exposes_pending_tracking_uid_as_canonical(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                INSERT INTO journey_events
                  (journey_id, node_id, event_type, event_at, payload_json)
                VALUES ('J000103', 'A', 'ENTRY',
                        '2026-08-11T15:20:27+09:00', '{}')
                """
            )
            connection.commit()
        finally:
            connection.close()
        status, payload, _, _ = self.readonly_get(
            "/api/events?since=2026-08-11T15%3A20%3A26%2B09%3A00"
        )
        self.assertEqual(status, 200)
        event = next(
            item for item in payload["items"] if item["journey_id"] == "J000103"
        )
        self.assertIsNone(event["person_uid"])
        self.assertIsNone(event["canonical_person_uid"])
        self.assertEqual(event["identity_status"], "MANUAL_REVIEW_REQUIRED")

    def test_stranger_detection_event_is_returned_without_a_journey(self) -> None:
        event_id = "D-20260814T134720000000+0900-L77"
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                INSERT INTO detection_events (
                    event_id, event_at, node_id, event_type,
                    identity_status, local_track_id, journey_id,
                    person_uid, canonical_person_uid, payload_json,
                    received_at
                ) VALUES (?, '2026-08-14T13:47:20+09:00', 'D',
                          'STRANGER_DETECTED', 'UNREGISTERED', 77,
                          NULL, NULL, NULL, '{}',
                          '2026-08-14T13:47:20+09:00')
                """,
                (event_id,),
            )
            connection.commit()
        finally:
            connection.close()

        status, payload, _, _ = self.readonly_get(
            "/api/events?since=2026-08-14T13%3A47%3A19%2B09%3A00"
        )
        self.assertEqual(status, 200)
        event = next(item for item in payload["items"] if item["event_id"] == event_id)
        self.assertEqual(
            event,
            {
                "event_id": event_id,
                "at": "2026-08-14T13:47:20+09:00",
                "journey_id": None,
                "node": "D",
                "kind": "STRANGER_DETECTED",
                "person_uid": None,
                "canonical_person_uid": None,
                "identity_status": "UNREGISTERED",
            },
        )

    def test_capture_groups_preserve_flat_captures_and_return_top3(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/journeys/J000104"
        )
        self.assertEqual(status, 200)
        self.assertIsInstance(payload["captures"], list)
        self.assertEqual(len(payload["captures"]), 1)
        self.assertEqual(payload["captures"][0]["node_id"], "D")

        groups = payload["capture_groups"]["A"]
        self.assertEqual([item["rank"] for item in groups["body"]], [1, 2, 3])
        self.assertEqual([item["quality"] for item in groups["body"]], [0.91, 0.82, 0.73])
        self.assertEqual(len(groups["face"]), 3)
        self.assertEqual(
            groups["body"][0]["url"],
            "http://camera-a.test:8000/captures/body/20260811/body%201.jpg",
        )
        self.assertEqual(
            groups["face"][0]["url"],
            "http://camera-a.test:8000/captures/face/20260811/face_1.jpg",
        )

    def test_capture_group_rejects_outside_root_without_hiding_row(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            row = connection.execute(
                "SELECT payload_json FROM journey_events WHERE journey_id='J000104'"
            ).fetchone()
            event = json.loads(row[0])
            event["body_capture_paths"][1] = "/tmp/not-a-camera-capture.jpg"
            connection.execute(
                "UPDATE journey_events SET payload_json=? WHERE journey_id='J000104'",
                (json.dumps(event),),
            )
            connection.commit()
        finally:
            connection.close()
        status, payload, _, _ = self.readonly_get("/api/journeys/J000104")
        self.assertEqual(status, 200)
        self.assertIsNone(payload["capture_groups"]["A"]["body"][1]["url"])
        self.assertTrue(
            any("outside allowed root" in item for item in payload["validation_warnings"])
        )

    def test_capture_group_malformed_entry_payload_is_safe(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "UPDATE journey_events SET payload_json='{bad' WHERE journey_id='J000104'"
            )
            connection.commit()
        finally:
            connection.close()
        status, payload, _, _ = self.readonly_get("/api/journeys/J000104")
        self.assertEqual(status, 200)
        self.assertEqual(payload["capture_groups"], {"A": {"body": [], "face": []}})
        self.assertTrue(payload["validation_warnings"])

    def test_unknown_journey_and_invalid_query(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/journeys/J999999"
        )
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "journey_not_found")
        status, payload, _, _ = self.readonly_get(
            "/api/journeys?limit=999"
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_query")

    def test_persons_list_hides_merged_by_default(self) -> None:
        status, payload, _, _ = self.readonly_get("/api/persons")
        self.assertEqual(status, 200)
        uids = {item["person_uid"] for item in payload["items"]}
        self.assertNotIn("P000072", uids)
        status, payload, _, _ = self.readonly_get(
            "/api/persons?include_merged=true"
        )
        self.assertEqual(status, 200)
        uids = {item["person_uid"] for item in payload["items"]}
        self.assertIn("P000072", uids)

    def test_p000006_person_detail_includes_revisit(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/persons/P000006"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["visit_count"], 12)
        self.assertEqual(payload["journeys"][0]["journey_id"], "J000104")
        self.assertAlmostEqual(
            payload["journeys"][0]["elapsed_seconds"], 18.141
        )

    def test_pending_candidate_is_not_listed_as_confirmed_person_visit(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/persons/P000071"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["journeys"], [])

        status, payload, _, _ = self.readonly_get(
            "/api/journeys?person_uid=P000071"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["items"], [])

    def test_reviews_list_and_manual_review_detail(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/reviews?status=PENDING"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["items"][0]["journey_id"], "J000103")
        status, payload, _, _ = self.readonly_get(
            "/api/reviews/J000103"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            payload["identity"]["final_scores"]["body_all"]["sample_count"],
            32,
        )

    def test_malformed_json_is_safe_and_read_only(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                "UPDATE journeys SET route_json = '{bad' WHERE journey_id = 'J000103'"
            )
            connection.execute(
                "UPDATE review_cases SET final_scores_json = '{bad' WHERE journey_id = 'J000103'"
            )
            connection.commit()
        finally:
            connection.close()
        status, payload, _, _ = self.readonly_get(
            "/api/journeys/J000103"
        )
        self.assertEqual(status, 200)
        self.assertIsNone(payload["route"])
        self.assertIsNone(payload["identity"]["final_scores"])
        self.assertTrue(payload["validation_warnings"])
        self.assertEqual(payload["identity"]["final_scores_raw"], "{bad")

    def test_incompatible_timestamp_timezone_is_safe(self) -> None:
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                UPDATE journey_node_visits
                SET exited_at = '2026-08-11T15:22:01.141'
                WHERE journey_id = 'J000104' AND node_id = 'D'
                """
            )
            connection.commit()
        finally:
            connection.close()
        status, payload, _, _ = self.readonly_get(
            "/api/journeys/J000104"
        )
        self.assertEqual(status, 200)
        self.assertIsNone(payload["timing"]["elapsed_seconds"])
        self.assertTrue(payload["validation_warnings"])

    def test_embedding_vectors_are_never_exposed(self) -> None:
        status, payload, _, body = self.readonly_get(
            "/api/journeys/J000104"
        )
        self.assertEqual(status, 200)
        self.assertNotIn(b"SECRET_EMBEDDING_VECTOR", body)

        def keys(value: Any) -> set[str]:
            if isinstance(value, dict):
                result = set(value)
                for child in value.values():
                    result.update(keys(child))
                return result
            if isinstance(value, list):
                result: set[str] = set()
                for child in value:
                    result.update(keys(child))
                return result
            return set()

        self.assertNotIn("embedding", keys(payload))
        self.assertNotIn("embeddings", keys(payload))
        self.assertEqual(payload["gallery_summary"][0]["embedding_dim"], 512)

    def test_main_style_writer_can_commit_while_api_is_running(self) -> None:
        before_status, _, _, _ = self.request("/api/health")
        self.assertEqual(before_status, 200)
        writer = sqlite3.connect(self.db_path, timeout=3)
        try:
            writer.execute(
                """
                INSERT INTO persons
                  (person_uid, created_at, last_seen_at, status, visit_count)
                VALUES ('P000099', '2026-08-11T16:00:00+09:00',
                        '2026-08-11T16:00:00+09:00', 'ACTIVE', 1)
                """
            )
            writer.commit()
        finally:
            writer.close()
        status, payload, _, _ = self.request(
            "/api/persons?include_merged=true"
        )
        self.assertEqual(status, 200)
        self.assertIn(
            "P000099", {item["person_uid"] for item in payload["items"]}
        )

    def test_post_is_not_available(self) -> None:
        status, payload, _, _ = self.readonly_get(
            "/api/reviews/J000103", method="POST"
        )
        self.assertEqual(status, 405)
        self.assertEqual(payload["error"], "method_not_allowed")

    def test_database_unavailable_returns_503(self) -> None:
        missing = Path(self.temp_dir.name) / "missing.db"
        server = create_server("127.0.0.1", 0, missing)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            request = Request(
                f"http://127.0.0.1:{server.server_port}/api/health"
            )
            with self.assertRaises(HTTPError) as context:
                urlopen(request, timeout=3)
            self.assertEqual(context.exception.code, 503)
            payload = json.loads(context.exception.read())
            self.assertEqual(payload["error"], "database_unavailable")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
