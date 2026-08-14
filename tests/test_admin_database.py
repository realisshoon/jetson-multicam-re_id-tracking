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
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from cctv_main import main_server
from cctv_main.admin_control import (
    BUSINESS_TABLES,
    DATABASE_SCHEMA_VERSION,
    AdminControlError,
    DatabaseAdminController,
    IngestionCoordinator,
    MainAdminControlServer,
)
from cctv_main.api_server import ApiDatabaseGate, ApiError, create_server


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "team_a"


class FakePublishResult:
    rc = 0


class FakeMqttClient:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []

    def publish(self, topic: str, payload: str, **_: Any) -> FakePublishResult:
        self.messages.append((topic, json.loads(payload)))
        return FakePublishResult()


class FakeAdminClient:
    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        del method, payload
        if path == "/api/admin/database/status":
            return 200, {"database_status": "READY", "schema_version": 62}
        raise AssertionError(path)


def http_json(
    url: str,
    *,
    method: str = "GET",
    token: str | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    headers: dict[str, str] = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


class AdminApiAuthenticationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "api.db"
        sqlite3.connect(self.db_path).close()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _serve(self, token: str | None):
        server = create_server(
            "127.0.0.1",
            0,
            self.db_path,
            admin_token=token or "",
            admin_client=FakeAdminClient() if token else None,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def cleanup() -> None:
            server.shutdown()
            server.server_close()
            thread.join(5)
        self.addCleanup(cleanup)
        return f"http://127.0.0.1:{server.server_port}"

    def test_admin_api_disabled_when_token_unset(self) -> None:
        base = self._serve(None)
        status, payload = http_json(base + "/api/admin/database/status")
        self.assertEqual(status, 503)
        self.assertEqual(payload, {"error": "ADMIN_API_DISABLED"})

    def test_missing_and_mismatched_tokens_are_rejected(self) -> None:
        base = self._serve("secret-token")
        status, payload = http_json(base + "/api/admin/database/status")
        self.assertEqual((status, payload["error"]), (401, "ADMIN_AUTH_REQUIRED"))
        status, payload = http_json(
            base + "/api/admin/database/status", token="wrong-token"
        )
        self.assertEqual((status, payload["error"]), (403, "ADMIN_FORBIDDEN"))
        status, payload = http_json(
            base + "/api/admin/database/status", token="secret-token"
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["schema_version"], 62)


class ApiDatabaseGateTest(unittest.TestCase):
    def test_pause_drains_existing_connection_and_rejects_new_db_work(self) -> None:
        gate = ApiDatabaseGate()
        entered = threading.Event()
        release = threading.Event()

        def existing_request() -> None:
            with gate.work():
                entered.set()
                release.wait(5)

        request_thread = threading.Thread(target=existing_request)
        request_thread.start()
        self.assertTrue(entered.wait(2))
        paused = threading.Event()

        def pause() -> None:
            gate.pause_and_wait()
            paused.set()

        pause_thread = threading.Thread(target=pause)
        pause_thread.start()
        time.sleep(0.05)
        self.assertFalse(paused.is_set())
        release.set()
        self.assertTrue(paused.wait(2))
        with self.assertRaises(ApiError) as caught:
            with gate.work():
                pass
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(caught.exception.payload["error"], "DATABASE_MAINTENANCE")
        gate.resume()
        with gate.work():
            pass
        request_thread.join(2)
        pause_thread.join(2)


class DatabaseAdminControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "data" / "main.db"
        self.capture_root = self.root / "data" / "captures"
        self.backup_root = self.root / "backups"
        self.capture_root.mkdir(parents=True)
        self.original_db_path = main_server.DB_PATH
        self.original_capture_settings = main_server.CAPTURE_CACHE_SETTINGS
        main_server.DB_PATH = self.db_path
        main_server.CAPTURE_CACHE_SETTINGS = replace(
            self.original_capture_settings,
            enabled=False,
            storage_root=self.capture_root,
        )
        main_server.initialize_database()
        self.runtime_gallery = {"P999999": [1, 2, 3]}
        self.ingestion = IngestionCoordinator()
        self.controller = DatabaseAdminController(
            self.db_path,
            self.capture_root,
            self.backup_root,
            main_server.initialize_database,
            self.ingestion,
            self.runtime_gallery.clear,
            confirmation_ttl_seconds=30,
        )

    def tearDown(self) -> None:
        main_server.DB_PATH = self.original_db_path
        main_server.CAPTURE_CACHE_SETTINGS = self.original_capture_settings
        gc.collect()
        self.temp_dir.cleanup()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def seed_person_and_journey(self, status: str = "COMPLETED") -> None:
        now = datetime.now(timezone.utc).isoformat()
        with closing(self.connect()) as connection:
            connection.execute(
                "INSERT INTO persons(person_uid,created_at,last_seen_at,status,visit_count) "
                "VALUES('P000001',?,?, 'ACTIVE',1)",
                (now, now),
            )
            connection.execute(
                """
                INSERT INTO journeys(
                    journey_id,request_id,person_uid,visit_no,status,route_json,
                    entry_at,person_status,identity_result,review_status,
                    canonical_person_uid
                ) VALUES('J000001','REQ-1','P000001',1,?,'["A"]',?,
                         'NEW','NEW','NOT_REQUIRED','P000001')
                """,
                (status, now),
            )
            connection.execute(
                "INSERT INTO person_embeddings(person_uid,node_id,captured_at,quality,"
                "modality,embedding_dim,embedding) VALUES('P000001','D',?,0.9,'BODY',4,?)",
                (now, sqlite3.Binary(b"1234")),
            )
            connection.execute(
                "INSERT INTO journey_gallery(journey_id,node_id,captured_at,quality,"
                "modality,embedding_dim,embedding) VALUES('J000001','A',?,0.9,'BODY',4,?)",
                (now, sqlite3.Binary(b"1234")),
            )
            connection.commit()

    @staticmethod
    def wait_for_job(
        controller: DatabaseAdminController,
        job_id: str,
        timeout: float = 10,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = controller.job(job_id)
            if job["status"] in {"COMPLETED", "FAILED"}:
                return job
            time.sleep(0.02)
        raise AssertionError(f"job did not finish: {controller.job(job_id)}")

    def reset_request(self, confirmation_id: str) -> dict[str, Any]:
        return {
            "confirmation_id": confirmation_id,
            "confirmation_text": "전체 데이터 초기화",
            "capture_policy": "ARCHIVE",
            "force": False,
        }

    def test_status_and_preview_report_actual_counts_without_mutation(self) -> None:
        self.seed_person_and_journey()
        before = hashlib.sha256(self.db_path.read_bytes()).hexdigest()
        status = self.controller.status()
        preview = self.controller.preview_reset()
        self.assertEqual(status["schema_version"], DATABASE_SCHEMA_VERSION)
        self.assertEqual(status["integrity_check"], "ok")
        self.assertEqual(status["gallery_count"], 2)
        self.assertEqual(preview["person_count"], 1)
        self.assertEqual(preview["journey_count"], 1)
        self.assertTrue(preview["can_reset"])
        self.assertTrue(preview["confirmation_id"].startswith("reset_"))
        self.assertEqual(before, hashlib.sha256(self.db_path.read_bytes()).hexdigest())

    def test_active_journey_blocks_reset(self) -> None:
        confirmation = self.controller.preview_reset()["confirmation_id"]
        self.seed_person_and_journey("WAITING_D")
        preview = self.controller.preview_reset()
        self.assertFalse(preview["can_reset"])
        self.assertEqual(preview["blocking_reason"], "ACTIVE_JOURNEYS_EXIST")
        self.assertIsNone(preview["confirmation_id"])
        with self.assertRaises(AdminControlError) as caught:
            self.controller.execute_reset(self.reset_request(confirmation))
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(caught.exception.payload["error"], "ACTIVE_JOURNEYS_EXIST")

    def test_expired_confirmation_is_rejected(self) -> None:
        current = [datetime(2026, 8, 13, 15, 0, tzinfo=timezone.utc)]
        controller = DatabaseAdminController(
            self.db_path,
            self.capture_root,
            self.backup_root,
            main_server.initialize_database,
            self.ingestion,
            clock=lambda: current[0],
            confirmation_ttl_seconds=30,
        )
        confirmation = controller.preview_reset()["confirmation_id"]
        current[0] += timedelta(seconds=31)
        with self.assertRaises(AdminControlError) as caught:
            controller.execute_reset(self.reset_request(confirmation))
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(caught.exception.payload["error"], "INVALID_OR_EXPIRED_CONFIRMATION")

    def test_duplicate_reset_is_rejected(self) -> None:
        first = self.controller.preview_reset()["confirmation_id"]
        second = self.controller.preview_reset()["confirmation_id"]
        work = self.ingestion.work()
        work.__enter__()
        try:
            accepted = self.controller.execute_reset(self.reset_request(first))
            deadline = time.monotonic() + 5
            while self.controller.job(accepted["job_id"])["status"] == "PREPARING":
                if time.monotonic() >= deadline:
                    self.fail("reset worker did not start")
                time.sleep(0.01)
            with self.assertRaises(AdminControlError) as caught:
                self.controller.execute_reset(self.reset_request(second))
            self.assertEqual(caught.exception.status, 409)
            self.assertEqual(caught.exception.payload["error"], "DATABASE_JOB_IN_PROGRESS")
        finally:
            work.__exit__(None, None, None)
        self.assertEqual(
            self.wait_for_job(self.controller, accepted["job_id"])["status"],
            "COMPLETED",
        )

    def test_backup_failure_preserves_original_database(self) -> None:
        self.seed_person_and_journey()
        confirmation = self.controller.preview_reset()["confirmation_id"]
        with patch.object(
            self.controller,
            "_create_verified_snapshot",
            side_effect=OSError("simulated backup failure"),
        ):
            accepted = self.controller.execute_reset(self.reset_request(confirmation))
            job = self.wait_for_job(self.controller, accepted["job_id"])
        self.assertEqual(job["status"], "FAILED")
        self.assertTrue(self.db_path.exists())
        with closing(self.connect()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_initialize_failure_restores_original_database_and_cache(self) -> None:
        self.seed_person_and_journey()
        marker = self.capture_root / "old.jpg"
        marker.write_bytes(b"jpeg")
        controller = DatabaseAdminController(
            self.db_path,
            self.capture_root,
            self.backup_root,
            lambda: (_ for _ in ()).throw(RuntimeError("simulated init failure")),
            self.ingestion,
        )
        confirmation = controller.preview_reset()["confirmation_id"]
        accepted = controller.execute_reset(self.reset_request(confirmation))
        job = self.wait_for_job(controller, accepted["job_id"])
        self.assertEqual(job["status"], "FAILED")
        with closing(self.connect()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM persons").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(marker.read_bytes(), b"jpeg")

    def test_successful_reset_archives_cache_clears_tables_and_accepts_new_entry(self) -> None:
        self.seed_person_and_journey()
        (self.capture_root / "old.jpg").write_bytes(b"jpeg")
        confirmation = self.controller.preview_reset()["confirmation_id"]
        accepted = self.controller.execute_reset(self.reset_request(confirmation))
        job = self.wait_for_job(self.controller, accepted["job_id"])
        self.assertEqual(job["status"], "COMPLETED")
        self.assertEqual(job["integrity_check"], "ok")
        self.assertEqual(self.runtime_gallery, {})
        with closing(self.connect()) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 62)
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            for table in BUSINESS_TABLES:
                self.assertEqual(
                    connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0],
                    0,
                    table,
                )
        archive = self.backup_root / str(job["backup_id"]) / "retired_live" / "captures"
        self.assertEqual((archive / "old.jpg").read_bytes(), b"jpeg")
        self.assertEqual(list(self.capture_root.iterdir()), [])

        payload = json.loads((FIXTURE_ROOT / "a_entry.json").read_text(encoding="utf-8"))
        payload["request_id"] = "RESET-E2E-ENTRY-001"
        client = FakeMqttClient()
        main_server.handle_a_entry(client, payload)
        response = next(
            item
            for topic, item in client.messages
            if topic == main_server.TOPIC_A_ENTRY_RESPONSE
        )
        self.assertEqual(response["person_status"], "NEW")
        self.assertEqual(response["person_uid"], "P000001")
        self.assertEqual(response["journey_id"], "J000001")

    def test_all_public_admin_endpoints_proxy_to_main_control(self) -> None:
        token = "integration-secret"
        control = MainAdminControlServer(
            ("127.0.0.1", 0), self.controller, token
        )
        control_thread = threading.Thread(target=control.serve_forever, daemon=True)
        control_thread.start()
        public = create_server(
            "127.0.0.1",
            0,
            self.db_path,
            admin_token=token,
            admin_control_url=f"http://127.0.0.1:{control.server_port}",
        )
        public_thread = threading.Thread(target=public.serve_forever, daemon=True)
        public_thread.start()
        base = f"http://127.0.0.1:{public.server_port}"
        try:
            status, document = http_json(
                base + "/api/admin/database/status", token=token
            )
            self.assertEqual(status, 200)
            self.assertEqual(document["schema_version"], 62)

            status, document = http_json(
                base + "/api/admin/database/backup",
                method="POST",
                token=token,
                payload={},
            )
            self.assertEqual(status, 200)
            self.assertEqual(document["status"], "COMPLETED")

            status, preview = http_json(
                base + "/api/admin/database/reset/preview",
                method="POST",
                token=token,
                payload={},
            )
            self.assertEqual(status, 200)
            status, accepted = http_json(
                base + "/api/admin/database/reset/execute",
                method="POST",
                token=token,
                payload=self.reset_request(preview["confirmation_id"]),
            )
            self.assertEqual(status, 202)
            deadline = time.monotonic() + 10
            while True:
                status, job = http_json(
                    base + f"/api/admin/database/jobs/{accepted['job_id']}",
                    token=token,
                )
                self.assertEqual(status, 200)
                if job["status"] in {"COMPLETED", "FAILED"}:
                    break
                if time.monotonic() >= deadline:
                    self.fail(f"job timeout: {job}")
                time.sleep(0.02)
            self.assertEqual(job["status"], "COMPLETED")
            self.assertEqual(
                [item["status"] for item in job["history"]],
                [
                    "PREPARING",
                    "PAUSING_INGESTION",
                    "BACKING_UP",
                    "RESETTING",
                    "REOPENING",
                    "VERIFYING",
                    "COMPLETED",
                ],
            )
        finally:
            public.shutdown()
            public.server_close()
            public_thread.join(5)
            control.shutdown()
            control.server_close()
            control_thread.join(5)


if __name__ == "__main__":
    unittest.main()
