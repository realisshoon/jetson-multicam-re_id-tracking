from __future__ import annotations

import base64
import gc
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from cctv_main import api_server, main_server


FIXTURE = Path(__file__).parent / "fixtures" / "team_a" / "a_entry.json"
FIXTURE_ROOT = FIXTURE.parent
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)
JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAMCAgMCAgMDAwMEAwMEBQgFBQQEBQoH"
    "BwYIDAoMDAsKCwsNDhIQDQ4RDgsLEBYQERMUFRUVDA8XGBYUGBIUFRT/2wBDAQME"
    "BAUEBQkFBQkUDQsNFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU"
    "FBQUFBQUFBQUFBQUFBT/wAARCAACAAIDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEA"
    "AAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIh"
    "MUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6"
    "Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZ"
    "mqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx"
    "8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREA"
    "AgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAV"
    "YnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hp"
    "anN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPE"
    "xcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwD8"
    "rZ55LmaSaaRpZZGLvI7EszE5JJPUmiiis6fwL0OvF/7xU/xP8z//2Q=="
)


class ImageHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/captures/slow.png":
            time.sleep(0.3)
        if self.path == "/captures/404.png":
            self.send_error(404)
            return
        is_jpeg = self.path in {
            "/captures/body/CAMERA-A/body_01.jpg",
            "/captures/face/CAMERA-A/face_01.jpg",
        }
        body = b"not an image" if self.path == "/captures/text" else (
            JPEG if is_jpeg else PNG
        )
        if self.path == "/captures/large.png":
            body = PNG * 100
        self.send_response(200)
        self.send_header(
            "Content-Type",
            "text/plain"
            if self.path == "/captures/text"
            else ("image/jpeg" if is_jpeg else "image/png"),
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_: Any) -> None:
        pass


class Result:
    rc = 0


class Client:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []

    def publish(self, topic: str, payload: str, **_: Any) -> Result:
        self.messages.append((topic, json.loads(payload)))
        return Result()

    def latest(self) -> dict[str, Any]:
        return [
            payload
            for topic, payload in self.messages
            if topic == main_server.TOPIC_A_ENTRY_RESPONSE
        ][-1]


def axis(index: int) -> list[float]:
    result = [0.0] * 512
    result[index] = 1.0
    return result


def vector_with_similarity(similarity: float, other_index: int) -> list[float]:
    result = [0.0] * 512
    result[0] = similarity
    result[other_index] = (1.0 - similarity**2) ** 0.5
    return result


class CaptureCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.original_db = main_server.DB_PATH
        self.original_settings = main_server.CAPTURE_CACHE_SETTINGS
        self.original_enable_camera_c = main_server.ENABLE_CAMERA_C
        main_server.ENABLE_CAMERA_C = False
        main_server.DB_PATH = Path(self.temp.name) / "main.db"
        self.image_server = ThreadingHTTPServer(("127.0.0.1", 0), ImageHandler)
        self.image_thread = threading.Thread(
            target=self.image_server.serve_forever, daemon=True
        )
        self.image_thread.start()
        self.image_server_stopped = False
        port = self.image_server.server_port
        main_server.CAPTURE_CACHE_SETTINGS = replace(
            self.original_settings,
            enabled=True,
            storage_root=Path(self.temp.name) / "captures",
            jetson_a_base_url=f"http://127.0.0.1:{port}",
            allowed_host="127.0.0.1",
            allowed_port=port,
            allowed_url_prefix="/captures/",
            body_public_prefix="/captures/",
            face_public_prefix="/captures/",
            connect_timeout_seconds=0.1,
            read_timeout_seconds=0.1,
            max_file_bytes=1024,
        )
        main_server.initialize_database()
        self.client = Client()

    def tearDown(self) -> None:
        if not self.image_server_stopped:
            self.image_server.shutdown()
            self.image_server.server_close()
            self.image_thread.join(timeout=2)
        main_server.DB_PATH = self.original_db
        main_server.CAPTURE_CACHE_SETTINGS = self.original_settings
        main_server.ENABLE_CAMERA_C = self.original_enable_camera_c
        gc.collect()
        self.temp.cleanup()

    def entry(
        self,
        request_id: str,
        embedding: list[float],
        path: str = "/captures/ok.png",
        capture_key: str | None = None,
    ) -> dict[str, Any]:
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        source_url = main_server.CAPTURE_CACHE_SETTINGS.jetson_a_base_url + path
        payload.update(
            {
                "request_id": request_id,
                "timestamp": main_server.now_iso(),
                "body_embeddings": [embedding],
                "body_embedding_dim": 512,
                "body_count": 1,
                "body_qualities": [0.8],
                "body_capture_paths": [source_url],
                "face_available": False,
                "capture_path": source_url,
            }
        )
        if capture_key:
            payload["captures"] = [
                {
                    "capture_key": capture_key,
                    "capture_type": "BODY",
                    "source_url": source_url,
                    "quality_score": 0.8,
                }
            ]
            payload["body_capture_paths"] = []
            payload.pop("capture_path", None)
        return payload

    def rows(self) -> list[sqlite3.Row]:
        connection = sqlite3.connect(main_server.DB_PATH)
        connection.row_factory = sqlite3.Row
        return connection.execute("SELECT * FROM captures ORDER BY capture_id").fetchall()

    def finalize_latest_for_matching(self) -> None:
        response = self.client.latest()
        with closing(sqlite3.connect(main_server.DB_PATH)) as connection:
            connection.row_factory = sqlite3.Row
            # This fixture uses one BODY frame, while production promotion
            # intentionally requires a multi-frame cluster. Seed the permanent
            # gallery directly so this capture-specific test can create two
            # known identities without weakening the production rule.
            connection.execute(
                """
                INSERT INTO person_embeddings (
                    person_uid, node_id, captured_at, quality,
                    modality, embedding_dim, embedding
                )
                SELECT ?, node_id, captured_at, quality,
                       modality, embedding_dim, embedding
                FROM journey_gallery
                WHERE journey_id = ?
                """,
                (response["person_uid"], response["journey_id"]),
            )
            connection.execute(
                "UPDATE journeys SET status='COMPLETED',route_json='[\"A\",\"B\",\"D\"]' "
                "WHERE journey_id=?",
                (response["journey_id"],),
            )
            connection.commit()

    def test_new_returning_and_duplicate_request_link_cached_capture(self) -> None:
        main_server.handle_a_entry(self.client, self.entry("IMG-NEW", axis(0)))
        new_response = self.client.latest()
        returning_payload = self.entry("IMG-RETURN", axis(0))
        returning_payload.update(
            {
                "body_count": 2,
                "body_embedding_dim": 512,
                "body_embeddings": [axis(0), axis(0)],
                "body_qualities": [0.95, 0.94],
                "body_confidences": [0.99, 0.98],
                "body_frame_indices": [1, 2],
            }
        )
        main_server.handle_a_entry(self.client, returning_payload)
        returning_response = self.client.latest()
        main_server.handle_a_entry(self.client, returning_payload)
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["cache_status"] == "CACHED" for row in rows))
        self.assertEqual(rows[0]["person_uid"], new_response["person_uid"])
        self.assertEqual(rows[1]["person_uid"], returning_response["person_uid"])
        self.assertEqual(new_response["person_uid"], returning_response["person_uid"])
        self.assertEqual(
            len(list(main_server.CAPTURE_CACHE_SETTINGS.storage_root.rglob("*.png"))),
            1,
        )

    def test_additive_migration_preserves_legacy_rows(self) -> None:
        legacy = Path(self.temp.name) / "legacy.db"
        with closing(sqlite3.connect(legacy)) as connection:
            connection.executescript(
                """
                CREATE TABLE persons (
                    person_uid TEXT PRIMARY KEY, created_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL, status TEXT NOT NULL
                );
                CREATE TABLE journeys (
                    journey_id TEXT PRIMARY KEY, person_uid TEXT NOT NULL,
                    status TEXT NOT NULL, route_json TEXT NOT NULL,
                    entry_at TEXT NOT NULL
                );
                CREATE TABLE person_embeddings (
                    embedding_id INTEGER PRIMARY KEY, person_uid TEXT NOT NULL,
                    node_id TEXT NOT NULL, captured_at TEXT NOT NULL,
                    quality REAL NOT NULL, embedding BLOB NOT NULL
                );
                CREATE TABLE journey_gallery (
                    gallery_id INTEGER PRIMARY KEY, journey_id TEXT NOT NULL,
                    node_id TEXT NOT NULL, captured_at TEXT NOT NULL,
                    quality REAL NOT NULL, embedding BLOB NOT NULL
                );
                """
            )
            now = main_server.now_iso()
            connection.execute(
                "INSERT INTO persons VALUES ('P000001', ?, ?, 'ACTIVE')", (now, now)
            )
            connection.execute(
                "INSERT INTO journeys VALUES ('J000001','P000001','COMPLETED','[\"A\",\"B\",\"D\"]',?)",
                (now,),
            )
            connection.commit()
        current = main_server.DB_PATH
        try:
            main_server.DB_PATH = legacy
            main_server.initialize_database()
            main_server.initialize_database()
        finally:
            main_server.DB_PATH = current
        with closing(sqlite3.connect(legacy)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM journeys").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 0)
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(persons)")
            }
            self.assertTrue(
                {
                    "representative_capture_id",
                    "representative_source",
                    "representative_updated_at",
                }.issubset(columns)
            )
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_pending_capture_stays_journey_only_until_admin_resolution(self) -> None:
        main_server.handle_a_entry(
            self.client, self.entry("KNOWN-1", vector_with_similarity(0.75, 10))
        )
        self.finalize_latest_for_matching()
        main_server.handle_a_entry(
            self.client, self.entry("KNOWN-2", vector_with_similarity(0.74, 11))
        )
        self.finalize_latest_for_matching()
        main_server.handle_a_entry(self.client, self.entry("PENDING", axis(0)))
        response = self.client.latest()
        self.assertEqual(response["identity_result"], "UNKNOWN")
        pending = self.rows()[-1]
        self.assertIsNone(pending["person_uid"])
        with closing(sqlite3.connect(main_server.DB_PATH)) as connection:
            review_id = connection.execute(
                "SELECT review_id FROM review_cases WHERE journey_id = ?",
                (response["journey_id"],),
            ).fetchone()[0]
        result = main_server.resolve_review_merge_existing(
            review_id, self.rows()[0]["person_uid"]
        )
        self.assertEqual(result["outcome"], "RESOLVED")
        self.assertEqual(self.rows()[-1]["person_uid"], self.rows()[0]["person_uid"])

    def test_same_capture_key_is_idempotent(self) -> None:
        payload = self.entry("SAME-KEY", axis(0), capture_key="camera-a-key-1")
        main_server.handle_a_entry(self.client, payload)
        main_server.handle_a_entry(self.client, payload)
        self.assertEqual(len(self.rows()), 1)

    def test_camera_a_source_path_contract_is_cached_and_served_offline(self) -> None:
        request_id = "CAMERA-A-SOURCE-PATH"
        payload = self.entry(request_id, axis(0))
        payload["captures"] = [
            {
                "capture_key": f"{request_id}-BODY-01",
                "capture_type": "BODY",
                "source_path": "/captures/body/CAMERA-A/body_01.jpg",
                "capture_path": "/legacy/body/path/body_01.jpg",
                "quality_score": 0.91,
            },
            {
                "capture_key": f"{request_id}-FACE-01",
                "capture_type": "FACE",
                "source_path": "/captures/face/CAMERA-A/face_01.jpg",
                "capture_path": "/legacy/face/path/face_01.jpg",
                "quality_score": 0.82,
            },
        ]
        payload["capture_errors"] = [
            {
                "capture_key": f"{request_id}-BODY-02",
                "capture_type": "BODY",
                "reason": "JPEG_SAVE_FAILED",
            }
        ]
        payload["body_capture_paths"] = [
            "/captures/body/CAMERA-A/body_01.jpg"
        ]
        payload["face_capture_paths"] = [
            "/captures/face/CAMERA-A/face_01.jpg"
        ]
        payload["capture_path"] = payload["body_capture_paths"][0]

        main_server.handle_a_entry(self.client, payload)
        main_server.handle_a_entry(self.client, payload)
        response = self.client.latest()
        rows = self.rows()

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {row["capture_key"] for row in rows},
            {f"{request_id}-BODY-01", f"{request_id}-FACE-01"},
        )
        self.assertTrue(all(row["cache_status"] == "CACHED" for row in rows))
        self.assertTrue(all(row["request_id"] == request_id for row in rows))
        self.assertTrue(
            all(row["journey_id"] == response["journey_id"] for row in rows)
        )
        self.assertTrue(
            all(row["person_uid"] == response["person_uid"] for row in rows)
        )
        self.assertEqual(
            {row["source_url"] for row in rows},
            {
                main_server.CAPTURE_CACHE_SETTINGS.jetson_a_base_url
                + "/captures/body/CAMERA-A/body_01.jpg",
                main_server.CAPTURE_CACHE_SETTINGS.jetson_a_base_url
                + "/captures/face/CAMERA-A/face_01.jpg",
            },
        )
        stored_files = list(
            main_server.CAPTURE_CACHE_SETTINGS.storage_root.rglob("*.jpg")
        )
        self.assertEqual(len(stored_files), 1)
        self.assertEqual(stored_files[0].read_bytes(), JPEG)

        face = next(row for row in rows if row["capture_type"] == "FACE")
        self.image_server.shutdown()
        self.image_server.server_close()
        self.image_thread.join(timeout=2)
        self.image_server_stopped = True

        api = api_server.create_server(
            "127.0.0.1",
            0,
            main_server.DB_PATH,
            "*",
            capture_storage_root=main_server.CAPTURE_CACHE_SETTINGS.storage_root,
        )
        thread = threading.Thread(target=api.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{api.server_port}"
        try:
            with urlopen(
                f"{base}/api/captures/{face['capture_id']}/image"
            ) as image_response:
                self.assertEqual(image_response.status, 200)
                self.assertEqual(image_response.headers.get_content_type(), "image/jpeg")
                self.assertEqual(image_response.read(), JPEG)
            with urlopen(f"{base}/api/persons/{response['person_uid']}") as person_response:
                person = json.load(person_response)
            self.assertEqual(
                person["representative_image_url"],
                f"/api/captures/{face['capture_id']}/image",
            )
        finally:
            api.shutdown()
            api.server_close()
            thread.join(timeout=2)

    def test_automatic_representative_prefers_face_then_quality(self) -> None:
        payload = self.entry("FACE-REP", axis(0))
        base = main_server.CAPTURE_CACHE_SETTINGS.jetson_a_base_url
        payload["captures"] = [
            {
                "capture_key": "face-rep-body",
                "capture_type": "BODY",
                "source_url": base + "/captures/ok.png",
                "quality_score": 0.99,
            },
            {
                "capture_key": "face-rep-face",
                "capture_type": "FACE",
                "source_url": base + "/captures/ok.png",
                "quality_score": 0.7,
            },
        ]
        payload["body_capture_paths"] = []
        payload.pop("capture_path", None)
        main_server.handle_a_entry(self.client, payload)
        with closing(sqlite3.connect(main_server.DB_PATH)) as connection:
            capture_type = connection.execute(
                """
                SELECT c.capture_type FROM persons p
                JOIN captures c ON c.capture_id = p.representative_capture_id
                WHERE p.person_uid = ?
                """,
                (self.client.latest()["person_uid"],),
            ).fetchone()[0]
        self.assertEqual(capture_type, "FACE")

    def test_download_failures_do_not_abort_entry(self) -> None:
        paths = (
            "/captures/404.png",
            "/captures/text",
            "/captures/large.png",
            "/captures/slow.png",
        )
        for index, path in enumerate(paths):
            main_server.handle_a_entry(
                self.client, self.entry(f"FAIL-{index}", axis(index), path)
            )
        self.assertEqual(len(self.client.messages) > 0, True)
        self.assertTrue(all(row["cache_status"] == "FAILED" for row in self.rows()))
        reasons = " ".join(str(row["cache_error"]) for row in self.rows())
        self.assertIn("HTTP_404", reasons)
        self.assertIn("IMAGE_", reasons)
        self.assertIn("IMAGE_TOO_LARGE", reasons)
        self.assertIn("TIMEOUT", reasons)

    def test_invalid_external_url_is_recorded_failed(self) -> None:
        payload = self.entry("BAD-URL", axis(0))
        payload["body_capture_paths"] = ["http://example.com/captures/a.jpg"]
        payload["capture_path"] = payload["body_capture_paths"][0]
        main_server.handle_a_entry(self.client, payload)
        row = self.rows()[0]
        self.assertEqual(row["cache_status"], "FAILED")
        self.assertEqual(row["cache_error"], "URL_HOST_NOT_ALLOWED")

    def test_image_api_and_manual_representative_survive_restart(self) -> None:
        main_server.handle_a_entry(self.client, self.entry("API-IMAGE", axis(0)))
        capture = self.rows()[0]
        person_uid = str(capture["person_uid"])
        self.image_server.shutdown()
        self.image_server.server_close()
        self.image_thread.join(timeout=2)
        self.image_server_stopped = True
        main_server.initialize_database()
        api = api_server.create_server(
            "127.0.0.1",
            0,
            main_server.DB_PATH,
            "*",
            capture_storage_root=main_server.CAPTURE_CACHE_SETTINGS.storage_root,
        )
        thread = threading.Thread(target=api.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{api.server_port}"
        try:
            with urlopen(f"{base}/api/captures/{capture['capture_id']}/image") as response:
                self.assertEqual(response.read(), PNG)
                self.assertEqual(response.headers.get_content_type(), "image/png")
            with urlopen(f"{base}/api/persons/{person_uid}/captures") as response:
                body = json.load(response)
                self.assertEqual(body["items"][0]["capture_id"], capture["capture_id"])
            request = Request(
                f"{base}/api/persons/{person_uid}/representative-capture",
                data=json.dumps({"capture_id": capture["capture_id"]}).encode(),
                headers={"Content-Type": "application/json"},
                method="PATCH",
            )
            with urlopen(request) as response:
                body = json.load(response)
                self.assertEqual(body["representative_source"], "MANUAL")
            main_server.handle_a_entry(
                self.client, self.entry("API-IMAGE-RETURN", axis(0))
            )
            with closing(sqlite3.connect(main_server.DB_PATH)) as connection:
                unchanged = connection.execute(
                    "SELECT representative_capture_id, representative_source "
                    "FROM persons WHERE person_uid = ?",
                    (person_uid,),
                ).fetchone()
            self.assertEqual(unchanged, (capture["capture_id"], "MANUAL"))
        finally:
            api.shutdown()
            api.server_close()
            thread.join(timeout=2)
        main_server.initialize_database()
        with closing(sqlite3.connect(main_server.DB_PATH)) as connection:
            representative = connection.execute(
                "SELECT representative_capture_id, representative_source "
                "FROM persons WHERE person_uid = ?",
                (person_uid,),
            ).fetchone()
        self.assertEqual(representative, (capture["capture_id"], "MANUAL"))

    def test_pending_capture_cannot_be_manual_representative(self) -> None:
        main_server.handle_a_entry(
            self.client, self.entry("P1", vector_with_similarity(0.75, 10))
        )
        first = self.rows()[0]
        self.finalize_latest_for_matching()
        main_server.handle_a_entry(
            self.client, self.entry("P2", vector_with_similarity(0.74, 11))
        )
        self.finalize_latest_for_matching()
        main_server.handle_a_entry(self.client, self.entry("P3", axis(0)))
        pending = self.rows()[-1]
        repository = api_server.ReadOnlyRepository(
            main_server.DB_PATH,
            capture_storage_root=main_server.CAPTURE_CACHE_SETTINGS.storage_root,
        )
        with self.assertRaises(api_server.ApiError) as context:
            repository.set_representative_capture(
                str(first["person_uid"]), {"capture_id": pending["capture_id"]}
            )
        self.assertEqual(context.exception.status, 409)

    def test_existing_a_b_d_flow_remains_completed_with_camera_c_disabled(self) -> None:
        main_server.handle_a_entry(self.client, self.entry("A-B-D", axis(0)))
        entry_response = self.client.latest()
        passage = json.loads((FIXTURE_ROOT / "b_passage.json").read_text(encoding="utf-8"))
        passage["journey_id"] = entry_response["journey_id"]
        passage["person_uid"] = entry_response["person_uid"]
        passage["b_passage_timestamp"] = main_server.now_iso()
        main_server.handle_passage(self.client, passage, "B")
        arrival = json.loads((FIXTURE_ROOT / "d_arrival.json").read_text(encoding="utf-8"))
        arrival["journey_id"] = entry_response["journey_id"]
        arrival["person_uid"] = entry_response["person_uid"]
        arrival["global_person_id"] = entry_response["person_uid"]
        passage_at = datetime.fromisoformat(passage["b_passage_timestamp"])
        arrival.update(
            {
                "passage_timestamp": passage_at.isoformat(timespec="seconds"),
                "candidate_received_at": (
                    passage_at + timedelta(seconds=1)
                ).isoformat(timespec="seconds"),
                "d_track_first_seen_at": (
                    passage_at + timedelta(seconds=2)
                ).isoformat(timespec="seconds"),
                "d_arrival_timestamp": (
                    passage_at + timedelta(seconds=10)
                ).isoformat(timespec="seconds"),
                "passage_to_d_duration_seconds": 10,
            }
        )
        main_server.handle_d_arrival(self.client, arrival)
        completed = [
            payload
            for topic, payload in self.client.messages
            if topic == main_server.TOPIC_JOURNEY_COMPLETED
        ][-1]
        self.assertEqual(completed["route"], ["A", "B", "D"])
        self.assertEqual(completed["status"], "COMPLETED")
        self.assertFalse(main_server.ENABLE_CAMERA_C)


if __name__ == "__main__":
    unittest.main()
