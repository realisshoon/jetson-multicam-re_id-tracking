from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RESET_SCRIPT = ROOT / "scripts" / "reset_test_db.ps1"


class MockAdminApiServer(HTTPServer):
    def __init__(self, address: tuple[str, int], handler_cls: type[BaseHTTPRequestHandler]) -> None:
        super().__init__(address, handler_cls)
        self.routes: dict[tuple[str, str], tuple[int, dict[str, Any]]] = {}
        self.request_history: list[dict[str, Any]] = []
        self.lock = threading.Lock()


class MockAdminApiHandler(BaseHTTPRequestHandler):
    server: MockAdminApiServer

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self, method: str) -> None:
        path = self.path.split("?", 1)[0]
        auth = self.headers.get("Authorization", "")
        content_length = int(self.headers.get("Content-Length", "0"))
        body_bytes = self.rfile.read(content_length) if content_length > 0 else b""
        body_json = json.loads(body_bytes.decode("utf-8")) if body_bytes else None

        with self.server.lock:
            self.server.request_history.append(
                {
                    "method": method,
                    "path": path,
                    "auth": auth,
                    "body": body_json,
                }
            )
            handler = self.server.routes.get((method, path))
            if callable(handler):
                status, resp = handler(self, method, path, body_json)
                self._json(status, resp)
                return
            if handler is not None:
                status, resp = handler
                self._json(status, resp)
                return

        self._json(404, {"error": "NOT_FOUND"})

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def log_message(self, format: str, *args: Any) -> None:
        pass


class ResetScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = MockAdminApiServer(("127.0.0.1", 0), MockAdminApiHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.token = "test-admin-secret-token"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def run_script(
        self,
        token: str | None = "test-admin-secret-token",
        base_url: str | None = None,
        ps_executable: str = "powershell.exe",
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        if token is not None:
            env["MAIN_ADMIN_TOKEN"] = token
        else:
            env.pop("MAIN_ADMIN_TOKEN", None)

        args = [
            ps_executable,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(RESET_SCRIPT),
        ]
        if base_url:
            args.extend(["-BaseUrl", base_url])
        else:
            args.extend(["-BaseUrl", self.base_url])

        return subprocess.run(
            args,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

    def test_pwsh_successful_reset_flow(self) -> None:
        try:
            pwsh_check = subprocess.run(["pwsh", "-version"], capture_output=True)
            if pwsh_check.returncode != 0:
                self.skipTest("pwsh is not available")
        except FileNotFoundError:
            self.skipTest("pwsh is not available")

        status_calls = 0

        def get_status(*args: Any) -> tuple[int, dict[str, Any]]:
            nonlocal status_calls
            status_calls += 1
            if status_calls == 1:
                return 200, {
                    "database_status": "READY",
                    "schema_version": 62,
                    "integrity_check": "ok",
                    "person_count": 5,
                    "journey_count": 5,
                    "gallery_count": 5,
                    "permanent_gallery_count": 2,
                    "journey_gallery_count": 3,
                    "capture_count": 5,
                    "active_journey_count": 0,
                    "reset_allowed": True,
                    "blocking_reason": None,
                }
            return 200, {
                "database_status": "READY",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 0,
                "journey_count": 0,
                "gallery_count": 0,
                "permanent_gallery_count": 0,
                "journey_gallery_count": 0,
                "capture_count": 0,
                "active_journey_count": 0,
                "reset_allowed": True,
                "blocking_reason": None,
            }

        self.server.routes[("GET", "/api/admin/database/status")] = get_status
        self.server.routes[("POST", "/api/admin/database/backup")] = (
            200,
            {
                "backup_id": "DBBACKUP-PWSH-001",
                "status": "COMPLETED",
                "integrity_check": "ok",
                "database_bytes": 102400,
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/preview")] = (
            200,
            {
                "person_count": 5,
                "journey_count": 5,
                "gallery_count": 5,
                "capture_count": 5,
                "active_journey_count": 0,
                "can_reset": True,
                "blocking_reason": None,
                "confirmation_id": "reset_pwsh_test",
                "expires_at": "2026-08-15T12:05:00+09:00",
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/execute")] = (
            202,
            {
                "accepted": True,
                "job_id": "DBRESET-PWSH-001",
                "status": "PREPARING",
            },
        )
        self.server.routes[("GET", "/api/admin/database/jobs/DBRESET-PWSH-001")] = (
            200,
            {
                "job_id": "DBRESET-PWSH-001",
                "status": "COMPLETED",
                "integrity_check": "ok",
                "error": None,
            },
        )

        result = self.run_script(token=self.token, ps_executable="pwsh")
        self.assertEqual(result.returncode, 0, f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        self.assertIn("CLEAN NEW TEST READY", result.stdout)
        self.assertIn("READY FOR NEW   : YES", result.stdout)

    def test_token_missing_aborts_immediately(self) -> None:
        result = self.run_script(token=None)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET ABORTED]", result.stdout)
        self.assertIn("MAIN_ADMIN_TOKEN is not set", result.stdout)

    def test_token_is_never_leaked_in_output(self) -> None:
        secret = "SUPER_SECRET_12345_TOKEN"
        self.server.routes[("GET", "/api/admin/database/status")] = (
            403,
            {"error": "ADMIN_FORBIDDEN"},
        )
        result = self.run_script(token=secret)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(secret, result.stdout)
        self.assertNotIn(secret, result.stderr)

    def test_active_journey_blocks_reset(self) -> None:
        self.server.routes[("GET", "/api/admin/database/status")] = (
            200,
            {
                "database_status": "BLOCKED",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 5,
                "journey_count": 10,
                "gallery_count": 20,
                "permanent_gallery_count": 5,
                "journey_gallery_count": 15,
                "capture_count": 30,
                "active_journey_count": 2,
                "reset_allowed": False,
                "blocking_reason": "ACTIVE_JOURNEYS_EXIST",
            },
        )
        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET BLOCKED]", result.stdout)
        self.assertIn("Active Journey : 2", result.stdout)
        self.assertIn("ACTIVE_JOURNEYS_EXIST", result.stdout)
        # Ensure no backup or reset calls were made
        paths = [req["path"] for req in self.server.request_history]
        self.assertNotIn("/api/admin/database/backup", paths)
        self.assertNotIn("/api/admin/database/reset/preview", paths)

    def test_integrity_check_failure_blocks_reset(self) -> None:
        self.server.routes[("GET", "/api/admin/database/status")] = (
            200,
            {
                "database_status": "BLOCKED",
                "schema_version": 62,
                "integrity_check": "malformed database",
                "person_count": 0,
                "journey_count": 0,
                "gallery_count": 0,
                "permanent_gallery_count": 0,
                "journey_gallery_count": 0,
                "capture_count": 0,
                "active_journey_count": 0,
                "reset_allowed": False,
                "blocking_reason": "DATABASE_INTEGRITY_CHECK_FAILED",
            },
        )
        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET BLOCKED]", result.stdout)
        self.assertIn("DATABASE_INTEGRITY_CHECK_FAILED", result.stdout)

    def test_backup_failure_aborts_reset(self) -> None:
        self.server.routes[("GET", "/api/admin/database/status")] = (
            200,
            {
                "database_status": "READY",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 1,
                "journey_count": 1,
                "gallery_count": 1,
                "permanent_gallery_count": 1,
                "journey_gallery_count": 0,
                "capture_count": 1,
                "active_journey_count": 0,
                "reset_allowed": True,
                "blocking_reason": None,
            },
        )
        self.server.routes[("POST", "/api/admin/database/backup")] = (
            500,
            {"error": "BACKUP_FAILED", "detail": "disk full"},
        )
        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET ABORTED]", result.stdout)
        paths = [req["path"] for req in self.server.request_history]
        self.assertNotIn("/api/admin/database/reset/preview", paths)
        self.assertNotIn("/api/admin/database/reset/execute", paths)

    def test_successful_reset_flow(self) -> None:
        status_calls = 0

        def get_status(*args: Any) -> tuple[int, dict[str, Any]]:
            nonlocal status_calls
            status_calls += 1
            if status_calls == 1:
                # Pre-reset status
                return 200, {
                    "database_status": "READY",
                    "schema_version": 62,
                    "integrity_check": "ok",
                    "person_count": 10,
                    "journey_count": 15,
                    "gallery_count": 30,
                    "permanent_gallery_count": 5,
                    "journey_gallery_count": 25,
                    "capture_count": 40,
                    "active_journey_count": 0,
                    "reset_allowed": True,
                    "blocking_reason": None,
                }
            # Post-reset verification status
            return 200, {
                "database_status": "READY",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 0,
                "journey_count": 0,
                "gallery_count": 0,
                "permanent_gallery_count": 0,
                "journey_gallery_count": 0,
                "capture_count": 0,
                "active_journey_count": 0,
                "reset_allowed": True,
                "blocking_reason": None,
            }

        self.server.routes[("GET", "/api/admin/database/status")] = get_status
        self.server.routes[("POST", "/api/admin/database/backup")] = (
            200,
            {
                "backup_id": "DBBACKUP-20260815-120000-abcd",
                "status": "COMPLETED",
                "integrity_check": "ok",
                "database_bytes": 102400,
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/preview")] = (
            200,
            {
                "person_count": 10,
                "journey_count": 15,
                "gallery_count": 30,
                "capture_count": 40,
                "active_journey_count": 0,
                "can_reset": True,
                "blocking_reason": None,
                "confirmation_id": "reset_20260815_test",
                "expires_at": "2026-08-15T12:05:00+09:00",
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/execute")] = (
            202,
            {
                "accepted": True,
                "job_id": "DBRESET-20260815-001",
                "status": "PREPARING",
            },
        )

        job_polls = 0

        def get_job(*args: Any) -> tuple[int, dict[str, Any]]:
            nonlocal job_polls
            job_polls += 1
            if job_polls == 1:
                return 200, {
                    "job_id": "DBRESET-20260815-001",
                    "status": "RESETTING",
                    "integrity_check": None,
                    "error": None,
                }
            return 200, {
                "job_id": "DBRESET-20260815-001",
                "status": "COMPLETED",
                "integrity_check": "ok",
                "error": None,
            }

        self.server.routes[("GET", "/api/admin/database/jobs/DBRESET-20260815-001")] = get_job

        result = self.run_script(token=self.token)
        self.assertEqual(result.returncode, 0, f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        self.assertIn("[1/5] Database status OK", result.stdout)
        self.assertIn("[2/5] Backup completed", result.stdout)
        self.assertIn("Backup ID: DBBACKUP-20260815-120000-abcd", result.stdout)
        self.assertIn("[3/5] Reset preview ready", result.stdout)
        self.assertIn("[4/5] Reset execute accepted", result.stdout)
        self.assertIn("Job ID: DBRESET-20260815-001", result.stdout)
        self.assertIn("CLEAN NEW TEST READY", result.stdout)
        self.assertIn("READY FOR NEW   : YES", result.stdout)

        # Check execute payload
        execute_req = next(req for req in self.server.request_history if req["path"] == "/api/admin/database/reset/execute")
        self.assertEqual(execute_req["body"]["confirmation_id"], "reset_20260815_test")
        self.assertEqual(execute_req["body"]["confirmation_text"], "전체 데이터 초기화")
        self.assertEqual(execute_req["body"]["capture_policy"], "ARCHIVE")
        self.assertFalse(execute_req["body"]["force"])

    def test_failed_job_reports_error_and_exits_nonzero(self) -> None:
        self.server.routes[("GET", "/api/admin/database/status")] = (
            200,
            {
                "database_status": "READY",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 1,
                "journey_count": 1,
                "gallery_count": 1,
                "permanent_gallery_count": 1,
                "journey_gallery_count": 0,
                "capture_count": 1,
                "active_journey_count": 0,
                "reset_allowed": True,
                "blocking_reason": None,
            },
        )
        self.server.routes[("POST", "/api/admin/database/backup")] = (
            200,
            {
                "backup_id": "DBBACKUP-20260815-001",
                "status": "COMPLETED",
                "integrity_check": "ok",
                "database_bytes": 102400,
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/preview")] = (
            200,
            {
                "person_count": 1,
                "journey_count": 1,
                "gallery_count": 1,
                "capture_count": 1,
                "active_journey_count": 0,
                "can_reset": True,
                "blocking_reason": None,
                "confirmation_id": "reset_20260815_fail",
                "expires_at": "2026-08-15T12:05:00+09:00",
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/execute")] = (
            202,
            {
                "accepted": True,
                "job_id": "DBRESET-20260815-002",
                "status": "PREPARING",
            },
        )
        self.server.routes[("GET", "/api/admin/database/jobs/DBRESET-20260815-002")] = (
            200,
            {
                "job_id": "DBRESET-20260815-002",
                "status": "FAILED",
                "error": "simulated reset error in reinit",
                "history": [{"status": "PREPARING"}, {"status": "FAILED"}],
            },
        )

        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET FAILED]", result.stdout)
        self.assertIn("simulated reset error in reinit", result.stdout)

    def test_database_changed_since_preview_aborts_without_auto_retry(self) -> None:
        self.server.routes[("GET", "/api/admin/database/status")] = (
            200,
            {
                "database_status": "READY",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 1,
                "journey_count": 1,
                "gallery_count": 1,
                "permanent_gallery_count": 1,
                "journey_gallery_count": 0,
                "capture_count": 1,
                "active_journey_count": 0,
                "reset_allowed": True,
                "blocking_reason": None,
            },
        )
        self.server.routes[("POST", "/api/admin/database/backup")] = (
            200,
            {
                "backup_id": "DBBACKUP-20260815-001",
                "status": "COMPLETED",
                "integrity_check": "ok",
                "database_bytes": 102400,
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/preview")] = (
            200,
            {
                "person_count": 1,
                "journey_count": 1,
                "gallery_count": 1,
                "capture_count": 1,
                "active_journey_count": 0,
                "can_reset": True,
                "blocking_reason": None,
                "confirmation_id": "reset_20260815_changed",
                "expires_at": "2026-08-15T12:05:00+09:00",
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/execute")] = (
            409,
            {"error": "DATABASE_CHANGED_SINCE_PREVIEW"},
        )

        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET ABORTED]", result.stdout)
        self.assertIn("DATABASE_CHANGED_SINCE_PREVIEW", result.stdout)
        # Verify execute was called only once (no auto retry)
        exec_calls = [req for req in self.server.request_history if req["path"] == "/api/admin/database/reset/execute"]
        self.assertEqual(len(exec_calls), 1)

    def test_post_reset_verification_fails_if_counts_not_zero(self) -> None:
        status_calls = 0

        def get_status(*args: Any) -> tuple[int, dict[str, Any]]:
            nonlocal status_calls
            status_calls += 1
            if status_calls == 1:
                return 200, {
                    "database_status": "READY",
                    "schema_version": 62,
                    "integrity_check": "ok",
                    "person_count": 5,
                    "journey_count": 5,
                    "gallery_count": 10,
                    "permanent_gallery_count": 5,
                    "journey_gallery_count": 5,
                    "capture_count": 5,
                    "active_journey_count": 0,
                    "reset_allowed": True,
                    "blocking_reason": None,
                }
            # Dirty post status
            return 200, {
                "database_status": "READY",
                "schema_version": 62,
                "integrity_check": "ok",
                "person_count": 1,  # NOT ZERO!
                "journey_count": 0,
                "gallery_count": 0,
                "permanent_gallery_count": 0,
                "journey_gallery_count": 0,
                "capture_count": 0,
                "active_journey_count": 0,
                "reset_allowed": True,
                "blocking_reason": None,
            }

        self.server.routes[("GET", "/api/admin/database/status")] = get_status
        self.server.routes[("POST", "/api/admin/database/backup")] = (
            200,
            {"backup_id": "DBBACKUP-1", "status": "COMPLETED", "integrity_check": "ok", "database_bytes": 100},
        )
        self.server.routes[("POST", "/api/admin/database/reset/preview")] = (
            200,
            {
                "person_count": 5,
                "journey_count": 5,
                "gallery_count": 10,
                "capture_count": 5,
                "active_journey_count": 0,
                "can_reset": True,
                "blocking_reason": None,
                "confirmation_id": "reset_1",
                "expires_at": "2026-08-15T12:05:00+09:00",
            },
        )
        self.server.routes[("POST", "/api/admin/database/reset/execute")] = (
            202,
            {"accepted": True, "job_id": "DBRESET-1", "status": "PREPARING"},
        )
        self.server.routes[("GET", "/api/admin/database/jobs/DBRESET-1")] = (
            200,
            {"job_id": "DBRESET-1", "status": "COMPLETED", "integrity_check": "ok", "error": None},
        )

        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("RESET VERIFICATION FAILED", result.stdout)
        self.assertIn("READY FOR NEW   : NO", result.stdout)

    def test_admin_api_disabled_reports_503(self) -> None:
        self.server.routes[("GET", "/api/admin/database/status")] = (
            503,
            {"error": "ADMIN_API_DISABLED"},
        )
        result = self.run_script(token=self.token)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("[RESET ABORTED]", result.stdout)
        self.assertIn("ADMIN_API_DISABLED", result.stdout)


if __name__ == "__main__":
    unittest.main()
