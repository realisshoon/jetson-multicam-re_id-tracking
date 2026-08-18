from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import shutil
import sqlite3
import threading
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DATABASE_SCHEMA_VERSION = 62
ADMIN_CONTROL_DEFAULT_HOST = "127.0.0.1"
ADMIN_CONTROL_DEFAULT_PORT = 8091
ACTIVE_JOURNEY_STATUSES = ("WAITING_B_OR_C", "WAITING_D")
RESET_CONFIRMATION_TEXT = "전체 데이터 초기화"
RESET_TERMINAL_STATUSES = {"COMPLETED", "FAILED"}
RESET_JOB_STATUSES = {
    "PREPARING",
    "PAUSING_INGESTION",
    "BACKING_UP",
    "RESETTING",
    "REOPENING",
    "VERIFYING",
    *RESET_TERMINAL_STATUSES,
}
BUSINESS_TABLES = (
    "a_entry_requests",
    "captures",
    "d_arrival_attempts",
    "detection_events",
    "identity_review_audit",
    "identity_review_candidates",
    "journey_captures",
    "journey_events",
    "journey_gallery",
    "journey_node_visits",
    "journeys",
    "person_embeddings",
    "persons",
    "review_cases",
)


class AdminControlError(Exception):
    def __init__(self, status: int, error: str, **details: Any) -> None:
        super().__init__(error)
        self.status = int(status)
        self.payload = {"error": error, **details}


class IngestionCoordinator:
    """Blocks new MQTT work and waits for in-flight handlers to finish."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._paused = False
        self._active = 0

    @contextmanager
    def work(self) -> Iterator[None]:
        with self._condition:
            while self._paused:
                self._condition.wait()
            self._active += 1
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def pause_and_wait(self) -> None:
        with self._condition:
            self._paused = True
            while self._active:
                self._condition.wait()

    def resume(self) -> None:
        with self._condition:
            self._paused = False
            self._condition.notify_all()

    @property
    def paused(self) -> bool:
        with self._condition:
            return self._paused


def _now() -> datetime:
    return datetime.now().astimezone()


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def _safe_error_message(error: BaseException) -> str:
    # Never include environment values or request headers in job output.
    return f"{type(error).__name__}: {error}"[:500]


class DatabaseAdminController:
    def __init__(
        self,
        db_path: Path | str,
        capture_root: Path | str,
        backup_root: Path | str,
        initialize_database: Callable[[], None],
        ingestion: IngestionCoordinator,
        clear_runtime_state: Callable[[], Any] | None = None,
        before_reset: Callable[[], Any] | None = None,
        confirmation_ttl_seconds: int = 300,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.db_path = Path(db_path).resolve()
        self.capture_root = Path(capture_root).resolve()
        self.backup_root = Path(backup_root).resolve()
        self.initialize_database = initialize_database
        self.ingestion = ingestion
        self.clear_runtime_state = clear_runtime_state or (lambda: None)
        self.before_reset = before_reset or (lambda: None)
        self.confirmation_ttl_seconds = max(30, int(confirmation_ttl_seconds))
        self.clock = clock
        self._lock = threading.RLock()
        self._confirmations: dict[str, dict[str, Any]] = {}
        self._jobs: dict[str, dict[str, Any]] = {}
        self._job_sequence_by_day: dict[str, int] = {}
        self._active_job_id: str | None = None
        self._backup_in_progress = False

    @contextmanager
    def _connect(self, *, readonly: bool = False) -> Iterator[sqlite3.Connection]:
        if readonly:
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=30)
            connection.execute("PRAGMA query_only = ON")
        else:
            connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone() is not None

    def _count(self, connection: sqlite3.Connection, table: str) -> int:
        if not self._table_exists(connection, table):
            return 0
        return int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])

    def _counts(self, connection: sqlite3.Connection) -> dict[str, int]:
        permanent = self._count(connection, "person_embeddings")
        provisional = self._count(connection, "journey_gallery")
        active = 0
        if self._table_exists(connection, "journeys"):
            placeholders = ",".join("?" for _ in ACTIVE_JOURNEY_STATUSES)
            active = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM journeys WHERE status IN ({placeholders})",
                    ACTIVE_JOURNEY_STATUSES,
                ).fetchone()[0]
            )
        return {
            "person_count": self._count(connection, "persons"),
            "journey_count": self._count(connection, "journeys"),
            "gallery_count": permanent + provisional,
            "permanent_gallery_count": permanent,
            "journey_gallery_count": provisional,
            "capture_count": self._count(connection, "captures"),
            "active_journey_count": active,
        }

    def _last_backup_at(self) -> str | None:
        if not self.backup_root.exists():
            return None
        latest: datetime | None = None
        for manifest in self.backup_root.glob("*/manifest.json"):
            try:
                document = json.loads(manifest.read_text(encoding="utf-8"))
                raw = document.get("completed_at") or document.get("created_at")
                value = datetime.fromisoformat(str(raw))
            except (OSError, TypeError, ValueError, json.JSONDecodeError):
                continue
            if latest is None or value > latest:
                latest = value
        return _iso(latest) if latest is not None else None

    def status(self) -> dict[str, Any]:
        try:
            with self._connect(readonly=True) as connection:
                integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
                counts = self._counts(connection)
        except sqlite3.Error as error:
            raise AdminControlError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "DATABASE_UNAVAILABLE",
            ) from error
        with self._lock:
            job_running = self._active_job_id is not None
        reason = None
        if counts["active_journey_count"]:
            reason = "ACTIVE_JOURNEYS_EXIST"
        elif job_running:
            reason = "DATABASE_JOB_IN_PROGRESS"
        elif self._backup_in_progress:
            reason = "DATABASE_BACKUP_IN_PROGRESS"
        elif integrity.lower() != "ok":
            reason = "DATABASE_INTEGRITY_CHECK_FAILED"
        return {
            "database_status": "READY" if reason is None else "BLOCKED",
            "schema_version": version,
            "integrity_check": integrity,
            **counts,
            "last_backup_at": self._last_backup_at(),
            "reset_allowed": reason is None,
            "blocking_reason": reason,
        }

    def preview_reset(self) -> dict[str, Any]:
        with self._connect(readonly=True) as connection:
            counts = self._counts(connection)
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        with self._lock:
            self._purge_confirmations_locked()
            reason = None
            if counts["active_journey_count"]:
                reason = "ACTIVE_JOURNEYS_EXIST"
            elif self._active_job_id is not None:
                reason = "DATABASE_JOB_IN_PROGRESS"
            elif self._backup_in_progress:
                reason = "DATABASE_BACKUP_IN_PROGRESS"
            elif integrity.lower() != "ok":
                reason = "DATABASE_INTEGRITY_CHECK_FAILED"
            confirmation_id = None
            expires_at = None
            if reason is None:
                now = self.clock()
                expires = now + timedelta(seconds=self.confirmation_ttl_seconds)
                confirmation_id = self._new_confirmation_id_locked(now)
                expires_at = _iso(expires)
                self._confirmations[confirmation_id] = {
                    "expires_at": expires,
                    "used": False,
                    "counts": dict(counts),
                }
        return {
            **counts,
            "can_reset": reason is None,
            "blocking_reason": reason,
            "confirmation_id": confirmation_id,
            "expires_at": expires_at,
        }

    def _new_confirmation_id_locked(self, now: datetime) -> str:
        while True:
            candidate = f"reset_{now:%Y%m%d}_{secrets.token_hex(2)}"
            if candidate not in self._confirmations:
                return candidate

    def _purge_confirmations_locked(self) -> None:
        now = self.clock()
        stale = [
            key
            for key, item in self._confirmations.items()
            if item["used"] or item["expires_at"] <= now
        ]
        for key in stale:
            self._confirmations.pop(key, None)

    def _validate_confirmation_locked(self, confirmation_id: str) -> None:
        item = self._confirmations.get(confirmation_id)
        if item is None or item["used"] or item["expires_at"] <= self.clock():
            raise AdminControlError(
                HTTPStatus.CONFLICT,
                "INVALID_OR_EXPIRED_CONFIRMATION",
            )

    def execute_reset(self, request: dict[str, Any]) -> dict[str, Any]:
        confirmation_id = str(request.get("confirmation_id", "")).strip()
        confirmation_text = str(request.get("confirmation_text", ""))
        capture_policy = str(request.get("capture_policy", "ARCHIVE")).upper()
        force = request.get("force", False)
        if not isinstance(force, bool):
            raise AdminControlError(HTTPStatus.BAD_REQUEST, "INVALID_FORCE_VALUE")
        if confirmation_text != RESET_CONFIRMATION_TEXT:
            raise AdminControlError(HTTPStatus.CONFLICT, "CONFIRMATION_TEXT_MISMATCH")
        if capture_policy != "ARCHIVE":
            raise AdminControlError(
                HTTPStatus.BAD_REQUEST,
                "INVALID_CAPTURE_POLICY",
                allowed=["ARCHIVE"],
            )
        with self._lock:
            self._purge_confirmations_locked()
            self._validate_confirmation_locked(confirmation_id)
            if self._active_job_id is not None:
                raise AdminControlError(
                    HTTPStatus.CONFLICT,
                    "DATABASE_JOB_IN_PROGRESS",
                    job_id=self._active_job_id,
                )
            if self._backup_in_progress:
                raise AdminControlError(
                    HTTPStatus.CONFLICT,
                    "DATABASE_BACKUP_IN_PROGRESS",
                )
            with self._connect(readonly=True) as connection:
                current_counts = self._counts(connection)
                active = current_counts["active_journey_count"]
            if active:
                raise AdminControlError(
                    HTTPStatus.CONFLICT,
                    "ACTIVE_JOURNEYS_EXIST",
                    active_journey_count=active,
                )
            preview_counts = self._confirmations[confirmation_id]["counts"]
            comparable = (
                "person_count",
                "journey_count",
                "gallery_count",
                "capture_count",
                "active_journey_count",
            )
            if any(current_counts[key] != preview_counts[key] for key in comparable):
                self._confirmations[confirmation_id]["used"] = True
                raise AdminControlError(
                    HTTPStatus.CONFLICT,
                    "DATABASE_CHANGED_SINCE_PREVIEW",
                )
            self._confirmations[confirmation_id]["used"] = True
            job_id = self._new_job_id_locked()
            now = _iso(self.clock())
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "PREPARING",
                "created_at": now,
                "updated_at": now,
                "completed_at": None,
                "backup_id": None,
                "integrity_check": None,
                "error": None,
                "history": [{"status": "PREPARING", "at": now}],
            }
            self._active_job_id = job_id
        worker = threading.Thread(
            target=self._run_reset,
            args=(job_id, capture_policy),
            name=f"database-reset-{job_id}",
            daemon=True,
        )
        worker.start()
        return {"accepted": True, "job_id": job_id, "status": "PREPARING"}

    def _new_job_id_locked(self) -> str:
        day = self.clock().strftime("%Y%m%d")
        sequence = self._job_sequence_by_day.get(day, 0) + 1
        self._job_sequence_by_day[day] = sequence
        return f"DBRESET-{day}-{sequence:03d}"

    def job(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            value = self._jobs.get(job_id)
            if value is None:
                raise AdminControlError(HTTPStatus.NOT_FOUND, "DATABASE_JOB_NOT_FOUND")
            return dict(value)

    def _set_job(self, job_id: str, status: str, **values: Any) -> None:
        if status not in RESET_JOB_STATUSES:
            raise ValueError(f"unknown reset status: {status}")
        with self._lock:
            job = self._jobs[job_id]
            job.update(values)
            job["status"] = status
            job["updated_at"] = _iso(self.clock())
            job["history"].append({"status": status, "at": job["updated_at"]})
            if status in RESET_TERMINAL_STATUSES:
                job["completed_at"] = job["updated_at"]
                if self._active_job_id == job_id:
                    self._active_job_id = None

    def backup(self) -> dict[str, Any]:
        with self._lock:
            if self._active_job_id is not None:
                raise AdminControlError(
                    HTTPStatus.CONFLICT,
                    "DATABASE_JOB_IN_PROGRESS",
                    job_id=self._active_job_id,
                )
            if self._backup_in_progress:
                raise AdminControlError(
                    HTTPStatus.CONFLICT,
                    "DATABASE_BACKUP_IN_PROGRESS",
                )
            self._backup_in_progress = True
        self.ingestion.pause_and_wait()
        try:
            self._checkpoint_wal()
            backup_dir, manifest = self._create_verified_snapshot("DBBACKUP")
        finally:
            self.ingestion.resume()
            with self._lock:
                self._backup_in_progress = False
        return {
            "backup_id": backup_dir.name,
            "status": "COMPLETED",
            "created_at": manifest["created_at"],
            "integrity_check": manifest["integrity_check"],
            "database_bytes": manifest["database_bytes"],
        }

    def _checkpoint_wal(self) -> tuple[int, int, int]:
        with self._connect() as connection:
            result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return tuple(int(value) for value in result)

    def _create_verified_snapshot(self, prefix: str) -> tuple[Path, dict[str, Any]]:
        now = self.clock()
        backup_id = f"{prefix}-{now:%Y%m%d-%H%M%S}-{secrets.token_hex(2)}"
        backup_dir = self.backup_root / backup_id
        backup_dir.mkdir(parents=True, exist_ok=False)
        snapshot = backup_dir / "database.snapshot.db"
        try:
            source = sqlite3.connect(self.db_path, timeout=30)
            destination = sqlite3.connect(snapshot)
            try:
                source.backup(destination)
            finally:
                destination.close()
                source.close()
            check = sqlite3.connect(f"file:{snapshot.as_posix()}?mode=ro", uri=True)
            try:
                integrity = str(check.execute("PRAGMA integrity_check").fetchone()[0])
            finally:
                check.close()
            if integrity.lower() != "ok":
                raise RuntimeError(f"backup integrity_check failed: {integrity}")
            digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            for suffix in ("-wal", "-shm"):
                related = Path(f"{self.db_path}{suffix}")
                if related.exists():
                    shutil.copy2(related, backup_dir / related.name)
            manifest = {
                "backup_id": backup_id,
                "created_at": _iso(now),
                "completed_at": _iso(self.clock()),
                "integrity_check": integrity,
                "database_bytes": snapshot.stat().st_size,
                "sha256": digest,
                "schema_version": self._schema_version(snapshot),
            }
            (backup_dir / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return backup_dir, manifest
        except Exception:
            (backup_dir / "FAILED").write_text("backup failed\n", encoding="utf-8")
            raise

    @staticmethod
    def _schema_version(path: Path) -> int:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
        try:
            return int(connection.execute("PRAGMA user_version").fetchone()[0])
        finally:
            connection.close()

    def _run_reset(self, job_id: str, capture_policy: str) -> None:
        backup_dir: Path | None = None
        self._set_job(job_id, "PAUSING_INGESTION")
        self.ingestion.pause_and_wait()
        try:
            self.before_reset()
            self._set_job(job_id, "BACKING_UP")
            self._checkpoint_wal()
            backup_dir, _ = self._create_verified_snapshot(job_id)
            self._set_job(job_id, "RESETTING", backup_id=backup_dir.name)
            self._archive_live_captures(backup_dir, capture_policy)
            self._reset_database_in_place()
            self._set_job(job_id, "REOPENING")
            self.initialize_database()
            self.clear_runtime_state()
            self._set_job(job_id, "VERIFYING")
            integrity = self._verify_fresh_database()
            self._set_job(
                job_id,
                "COMPLETED",
                integrity_check=integrity,
            )
        except Exception as error:
            restore_error = None
            if backup_dir is not None:
                try:
                    self._restore_from_snapshot(backup_dir)
                    self._restore_live_captures(backup_dir)
                except Exception as nested:
                    restore_error = _safe_error_message(nested)
            error_message = _safe_error_message(error)
            if restore_error is not None:
                error_message += f"; restore_error={restore_error}"
            self._set_job(job_id, "FAILED", error=error_message)
            traceback.print_exc()
        finally:
            self.ingestion.resume()

    def _archive_live_captures(self, backup_dir: Path, capture_policy: str) -> None:
        if capture_policy != "ARCHIVE":
            raise ValueError("unsupported capture policy")
        retired = backup_dir / "retired_live"
        retired.mkdir(parents=True, exist_ok=True)
        if self.capture_root.exists():
            target = retired / "captures"
            if target.exists():
                shutil.rmtree(target)
            self.capture_root.replace(target)
        self.capture_root.mkdir(parents=True, exist_ok=True)

    _archive_live_files = _archive_live_captures

    def _restore_live_captures(self, backup_dir: Path) -> None:
        retired = backup_dir / "retired_live"
        archived_capture = retired / "captures"
        if archived_capture.exists():
            if self.capture_root.exists():
                shutil.rmtree(self.capture_root)
            archived_capture.replace(self.capture_root)
        else:
            self.capture_root.mkdir(parents=True, exist_ok=True)

    _restore_live_files = _restore_live_captures

    def _reset_database_in_place(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute("PRAGMA foreign_keys = OFF")
                for table in BUSINESS_TABLES:
                    if self._table_exists(connection, table):
                        connection.execute(f'DELETE FROM "{table}"')
                if self._table_exists(connection, "sqlite_sequence"):
                    connection.execute("DELETE FROM sqlite_sequence")
                connection.execute("PRAGMA foreign_keys = ON")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _restore_from_snapshot(self, backup_dir: Path) -> None:
        snapshot = backup_dir / "database.snapshot.db"
        if snapshot.exists() and self.db_path.exists():
            source = sqlite3.connect(f"file:{snapshot.as_posix()}?mode=ro", uri=True)
            destination = sqlite3.connect(self.db_path, timeout=30)
            try:
                source.backup(destination)
            finally:
                destination.close()
                source.close()

    def _database_files(self) -> tuple[Path, Path, Path]:
        return (
            self.db_path,
            Path(f"{self.db_path}-wal"),
            Path(f"{self.db_path}-shm"),
        )

    def _verify_fresh_database(self) -> str:
        with self._connect(readonly=True) as connection:
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            foreign_key_errors = connection.execute("PRAGMA foreign_key_check").fetchall()
            nonempty = {
                table: self._count(connection, table)
                for table in BUSINESS_TABLES
                if self._count(connection, table)
            }
        if integrity.lower() != "ok":
            raise RuntimeError(f"new database integrity_check failed: {integrity}")
        if version != DATABASE_SCHEMA_VERSION:
            raise RuntimeError(
                f"new database schema version {version}, expected {DATABASE_SCHEMA_VERSION}"
            )
        if foreign_key_errors:
            raise RuntimeError("new database foreign_key_check failed")
        if nonempty:
            raise RuntimeError(f"new database is not empty: {nonempty}")
        return integrity


def bearer_authorized(header: str | None, token: str) -> bool:
    if not header or not header.startswith("Bearer "):
        return False
    supplied = header[len("Bearer ") :]
    return bool(supplied) and hmac.compare_digest(supplied, token)


class MainAdminControlServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        controller: DatabaseAdminController,
        token: str,
    ) -> None:
        self.controller = controller
        self.admin_token = token
        super().__init__(address, MainAdminControlHandler)


class MainAdminControlHandler(BaseHTTPRequestHandler):
    server: MainAdminControlServer
    protocol_version = "HTTP/1.1"

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _authenticate(self) -> None:
        if not bearer_authorized(self.headers.get("Authorization"), self.server.admin_token):
            raise AdminControlError(HTTPStatus.FORBIDDEN, "ADMIN_FORBIDDEN")

    def _request_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise AdminControlError(HTTPStatus.BAD_REQUEST, "INVALID_CONTENT_LENGTH") from error
        if length == 0:
            return {}
        if length > 65536:
            raise AdminControlError(HTTPStatus.BAD_REQUEST, "INVALID_REQUEST_SIZE")
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AdminControlError(HTTPStatus.BAD_REQUEST, "INVALID_JSON") from error
        if not isinstance(value, dict):
            raise AdminControlError(HTTPStatus.BAD_REQUEST, "INVALID_JSON_OBJECT")
        return value

    def _dispatch(self, method: str) -> None:
        try:
            self._authenticate()
            path = self.path.split("?", 1)[0]
            controller = self.server.controller
            if method == "GET" and path == "/api/admin/database/status":
                return self._json(HTTPStatus.OK, controller.status())
            if method == "GET" and path.startswith("/api/admin/database/jobs/"):
                job_id = path.rsplit("/", 1)[-1]
                return self._json(HTTPStatus.OK, controller.job(job_id))
            if method == "POST" and path == "/api/admin/database/backup":
                self._request_json()
                return self._json(HTTPStatus.OK, controller.backup())
            if method == "POST" and path == "/api/admin/database/reset/preview":
                self._request_json()
                return self._json(HTTPStatus.OK, controller.preview_reset())
            if method == "POST" and path == "/api/admin/database/reset/execute":
                response = controller.execute_reset(self._request_json())
                return self._json(HTTPStatus.ACCEPTED, response)
            raise AdminControlError(HTTPStatus.NOT_FOUND, "ENDPOINT_NOT_FOUND")
        except AdminControlError as error:
            self._json(error.status, error.payload)
        except Exception:
            traceback.print_exc()
            self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "INTERNAL_SERVER_ERROR"})

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[Main Admin Control] {self.client_address[0]} {format % args}", flush=True)


class AdminControlClient:
    def __init__(self, base_url: str, token: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        body = None
        headers = {"Authorization": f"Bearer {self._token}"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return int(response.status), json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            try:
                payload_value = json.loads(error.read().decode("utf-8"))
            except Exception:
                payload_value = {"error": "MAIN_ADMIN_CONTROL_ERROR"}
            raise AdminControlError(error.code, **payload_value) from error
        except (URLError, TimeoutError, OSError) as error:
            raise AdminControlError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "MAIN_ADMIN_CONTROL_UNAVAILABLE",
            ) from error


def configured_admin_token() -> str | None:
    value = os.environ.get("MAIN_ADMIN_TOKEN", "").strip()
    return value or None
