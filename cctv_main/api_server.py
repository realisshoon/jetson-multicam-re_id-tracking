from __future__ import annotations

import argparse
import hmac
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, Iterator
from urllib.parse import parse_qs, quote, urlsplit

from cctv_main.admin_control import (
    ADMIN_CONTROL_DEFAULT_PORT,
    AdminControlClient,
    AdminControlError,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "main_server.db"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080
DEFAULT_MAIN_SERVER_IP = "10.10.20.33"
DEFAULT_CAMERA_A_IMAGE_BASE_URL = "http://10.10.20.56:8000"
DEFAULT_CAMERA_A_BODY_ROOT = "/home/aidl/work/pj/outputs/captures/A"
DEFAULT_CAMERA_A_FACE_ROOT = "/home/aidl/work/pj/outputs/captures/A_face"
MAX_PAGE_SIZE = 200

PERSON_UID_PATTERN = re.compile(r"^P\d{6}$")
JOURNEY_ID_PATTERN = re.compile(r"^J\d{6}$")
REVIEW_ID_PATTERN = re.compile(r"^R\d{6}$")
CAPTURE_ID_PATTERN = re.compile(r"^\d+$")
JOURNEY_STATUSES = {
    "WAITING_B_OR_C",
    "WAITING_D",
    "COMPLETED",
    "EXPIRED",
}
FINAL_REVIEW_RESULTS = {
    "REVISIT",
    "NEW",
    "MANUAL_REVIEW_REQUIRED",
}
REVIEW_STATUSES = {"PENDING", "RESOLVED"}


class ApiError(Exception):
    def __init__(
        self,
        status: int,
        error: str,
        **details: Any,
    ) -> None:
        super().__init__(error)
        self.status = status
        self.payload = {"error": error, **details}


class ApiDatabaseGate:
    """Drains API SQLite users before Main replaces the database files."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._paused = False
        self._active = 0

    @contextmanager
    def work(self) -> Iterator[None]:
        with self._condition:
            if self._paused:
                raise ApiError(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "DATABASE_MAINTENANCE",
                )
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


def _parse_json_field(
    raw_value: Any,
    field_name: str,
    expected_type: type[Any],
    warnings: list[str],
) -> tuple[Any, str | None]:
    if raw_value is None:
        return None, None
    raw = str(raw_value)
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        warnings.append(f"{field_name}: invalid JSON")
        return None, raw
    if not isinstance(value, expected_type):
        warnings.append(
            f"{field_name}: expected {expected_type.__name__}"
        )
        return None, raw
    return value, None


def _seconds_between(
    start_value: Any,
    end_value: Any,
    metric_name: str,
    warnings: list[str],
) -> float | None:
    if start_value is None or end_value is None:
        return None
    try:
        start = datetime.fromisoformat(str(start_value))
        end = datetime.fromisoformat(str(end_value))
    except (TypeError, ValueError):
        warnings.append(f"{metric_name}: invalid timestamp")
        return None
    try:
        seconds = (end - start).total_seconds()
    except TypeError:
        warnings.append(f"{metric_name}: incompatible timestamp timezone")
        return None
    if seconds < 0:
        warnings.append(f"{metric_name}: end precedes start")
        return None
    return round(seconds, 3)


def _single_query_value(
    query: dict[str, list[str]],
    name: str,
) -> str | None:
    values = query.get(name)
    if values is None:
        return None
    if len(values) != 1 or not values[0].strip():
        raise ApiError(
            HTTPStatus.BAD_REQUEST,
            "invalid_query",
            parameter=name,
        )
    return values[0].strip()


def _pagination(query: dict[str, list[str]]) -> tuple[int, int]:
    raw_limit = _single_query_value(query, "limit")
    raw_offset = _single_query_value(query, "offset")
    try:
        limit = 50 if raw_limit is None else int(raw_limit)
        offset = 0 if raw_offset is None else int(raw_offset)
    except ValueError as error:
        raise ApiError(
            HTTPStatus.BAD_REQUEST,
            "invalid_query",
            detail="limit and offset must be integers",
        ) from error
    if limit < 1 or limit > MAX_PAGE_SIZE:
        raise ApiError(
            HTTPStatus.BAD_REQUEST,
            "invalid_query",
            parameter="limit",
            allowed=f"1..{MAX_PAGE_SIZE}",
        )
    if offset < 0:
        raise ApiError(
            HTTPStatus.BAD_REQUEST,
            "invalid_query",
            parameter="offset",
            allowed=">= 0",
        )
    return limit, offset


def _boolean_query(
    query: dict[str, list[str]],
    name: str,
    default: bool = False,
) -> bool:
    raw = _single_query_value(query, name)
    if raw is None:
        return default
    normalized = raw.lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ApiError(
        HTTPStatus.BAD_REQUEST,
        "invalid_query",
        parameter=name,
        allowed="true or false",
    )


class ReadOnlyRepository:
    resolution_lock = threading.Lock()
    def __init__(
        self,
        db_path: Path | str,
        camera_a_image_base_url: str = DEFAULT_CAMERA_A_IMAGE_BASE_URL,
        camera_a_body_root: str = DEFAULT_CAMERA_A_BODY_ROOT,
        camera_a_face_root: str = DEFAULT_CAMERA_A_FACE_ROOT,
        capture_storage_root: Path | str | None = None,
        database_gate: ApiDatabaseGate | None = None,
    ) -> None:
        self.db_path = Path(db_path).resolve()
        self.camera_a_image_base_url = camera_a_image_base_url.rstrip("/")
        self.camera_a_capture_roots = {
            "body": PurePosixPath(camera_a_body_root),
            "face": PurePosixPath(camera_a_face_root),
        }
        self.capture_storage_root = Path(
            capture_storage_root or (self.db_path.parent / "captures")
        ).resolve()
        self.database_gate = database_gate or ApiDatabaseGate()

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (name,),
        ).fetchone() is not None

    @staticmethod
    def _column_exists(
        connection: sqlite3.Connection, table: str, column: str
    ) -> bool:
        return column in {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})")
        }

    @staticmethod
    def _capture_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "capture_id": row["capture_id"],
            "capture_key": row["capture_key"],
            "request_id": row["request_id"],
            "journey_id": row["journey_id"],
            "person_uid": row["person_uid"],
            "camera_id": row["camera_id"],
            "capture_type": row["capture_type"],
            "source_url": row["source_url"],
            "quality_score": row["quality_score"],
            "sha256": row["sha256"],
            "mime_type": row["mime_type"],
            "captured_at": row["captured_at"],
            "cache_status": row["cache_status"],
            "cache_error": row["cache_error"],
            "image_url": (
                f"/api/captures/{row['capture_id']}/image"
                if row["cache_status"] == "CACHED" and row["stored_path"]
                else None
            ),
        }

    def capture_image(self, capture_id: str) -> tuple[Path, str, str | None]:
        if not CAPTURE_ID_PATTERN.fullmatch(capture_id):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_capture_id")
        with self.connect() as connection:
            if not self._table_exists(connection, "captures"):
                raise ApiError(HTTPStatus.NOT_FOUND, "capture_not_found")
            row = connection.execute(
                "SELECT stored_path, mime_type, sha256, cache_status "
                "FROM captures WHERE capture_id = ?",
                (int(capture_id),),
            ).fetchone()
        if row is None:
            raise ApiError(HTTPStatus.NOT_FOUND, "capture_not_found")
        if row["cache_status"] != "CACHED" or not row["stored_path"]:
            raise ApiError(
                HTTPStatus.CONFLICT,
                "capture_not_cached",
                cache_status=row["cache_status"],
            )
        candidate = (self.capture_storage_root / str(row["stored_path"])).resolve()
        try:
            candidate.relative_to(self.capture_storage_root)
        except ValueError as error:
            raise ApiError(HTTPStatus.NOT_FOUND, "capture_file_not_found") from error
        if not candidate.is_file():
            raise ApiError(HTTPStatus.NOT_FOUND, "capture_file_not_found")
        return candidate, str(row["mime_type"] or "application/octet-stream"), row["sha256"]

    def person_captures(self, person_uid: str) -> dict[str, Any]:
        if not PERSON_UID_PATTERN.fullmatch(person_uid):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_person_uid")
        with self.connect() as connection:
            if self._person(connection, person_uid) is None:
                raise ApiError(HTTPStatus.NOT_FOUND, "person_not_found")
            if not self._table_exists(connection, "captures"):
                return {"person_uid": person_uid, "items": []}
            rows = connection.execute(
                """
                SELECT * FROM captures WHERE person_uid = ?
                ORDER BY captured_at DESC, capture_id DESC
                """,
                (person_uid,),
            ).fetchall()
        return {
            "person_uid": person_uid,
            "items": [self._capture_payload(row) for row in rows],
        }

    def set_representative_capture(
        self, person_uid: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        if not PERSON_UID_PATTERN.fullmatch(person_uid):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_person_uid")
        capture_id = request.get("capture_id")
        if isinstance(capture_id, bool):
            capture_id = None
        try:
            normalized_capture_id = int(capture_id)
        except (TypeError, ValueError) as error:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_capture_id") from error
        with self.connect_write() as connection:
            connection.execute("BEGIN IMMEDIATE")
            person = self._person(connection, person_uid)
            if person is None:
                raise ApiError(HTTPStatus.NOT_FOUND, "person_not_found")
            capture = connection.execute(
                """
                SELECT c.capture_id, c.person_uid, c.cache_status,
                       j.identity_result, j.review_status
                FROM captures c JOIN journeys j ON j.journey_id = c.journey_id
                WHERE c.capture_id = ?
                """,
                (normalized_capture_id,),
            ).fetchone()
            if capture is None:
                raise ApiError(HTTPStatus.NOT_FOUND, "capture_not_found")
            if (
                capture["person_uid"] != person_uid
                or capture["identity_result"] not in {"NEW", "RETURNING"}
                or capture["review_status"] == "PENDING"
            ):
                raise ApiError(HTTPStatus.CONFLICT, "capture_not_linked_to_person")
            if capture["cache_status"] != "CACHED":
                raise ApiError(HTTPStatus.CONFLICT, "capture_not_cached")
            updated_at = datetime.now().astimezone().isoformat(timespec="seconds")
            connection.execute(
                """
                UPDATE persons SET representative_capture_id = ?,
                    representative_source = 'MANUAL', representative_updated_at = ?
                WHERE person_uid = ?
                """,
                (normalized_capture_id, updated_at, person_uid),
            )
            connection.commit()
        return {
            "person_uid": person_uid,
            "representative_capture_id": normalized_capture_id,
            "representative_source": "MANUAL",
            "representative_updated_at": updated_at,
            "image_url": f"/api/captures/{normalized_capture_id}/image",
        }

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        with self.database_gate.work():
            uri = f"file:{self.db_path.as_posix()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            try:
                yield connection
            finally:
                connection.close()

    @contextmanager
    def connect_write(self) -> Iterator[sqlite3.Connection]:
        with self.database_gate.work():
            connection = sqlite3.connect(self.db_path, timeout=30)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 30000")
            try:
                yield connection
            finally:
                connection.close()

    @staticmethod
    def _canonical_person_uid(
        connection: sqlite3.Connection,
        journey: sqlite3.Row,
        review: sqlite3.Row | None,
    ) -> str:
        if (
            review is not None
            and review["final_review_result"] in {"REVISIT", "NEW"}
            and review["canonical_person_uid"]
        ):
            return str(review["canonical_person_uid"])

        current = str(journey["person_uid"])
        visited: set[str] = set()
        while current not in visited:
            visited.add(current)
            person = connection.execute(
                "SELECT merged_into_person_uid FROM persons WHERE person_uid = ?",
                (current,),
            ).fetchone()
            if person is None or person["merged_into_person_uid"] is None:
                return current
            current = str(person["merged_into_person_uid"])
        return str(journey["person_uid"])

    @staticmethod
    def _person(
        connection: sqlite3.Connection,
        person_uid: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM persons WHERE person_uid = ?",
            (person_uid,),
        ).fetchone()

    @staticmethod
    def _review(
        connection: sqlite3.Connection,
        journey_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM review_cases WHERE journey_id = ?",
            (journey_id,),
        ).fetchone()

    @staticmethod
    def _effective_person_status(
        journey: sqlite3.Row,
        review: sqlite3.Row | None,
    ) -> str:
        final_result = review["final_review_result"] if review else None
        if final_result == "REVISIT":
            return "RETURNING"
        if final_result == "NEW":
            return "NEW"
        if final_result == "MANUAL_REVIEW_REQUIRED":
            return "REVIEW_REQUIRED"
        return str(journey["person_status"])

    @staticmethod
    def _identity_state(
        journey: sqlite3.Row,
        review: sqlite3.Row | None,
    ) -> dict[str, Any]:
        final_result = review["final_review_result"] if review else None
        review_status = (
            journey["review_status"]
            if "review_status" in journey.keys()
            else (review["status"] if review else "NOT_REQUIRED")
        )
        identity_result = (
            journey["identity_result"]
            if "identity_result" in journey.keys()
            else "UNKNOWN"
        )
        canonical_uid = (
            journey["canonical_person_uid"]
            if "canonical_person_uid" in journey.keys()
            else None
        )
        candidate_uid = (
            journey["candidate_person_uid"]
            if "candidate_person_uid" in journey.keys()
            else (review["candidate_person_uid"] if review else None)
        )
        if final_result == "REVISIT":
            identity_result = "RETURNING"
            review_status = "RESOLVED"
            canonical_uid = review["canonical_person_uid"]
        elif final_result == "NEW":
            identity_result = "NEW"
            review_status = "RESOLVED"
            canonical_uid = review["canonical_person_uid"]
        elif final_result == "MANUAL_REVIEW_REQUIRED":
            identity_result = "UNKNOWN"
            review_status = "PENDING"
            canonical_uid = None
        elif identity_result == "UNKNOWN" and review is None:
            person_status = str(journey["person_status"])
            if person_status in {"NEW", "RETURNING"}:
                identity_result = person_status
                canonical_uid = canonical_uid or journey["person_uid"]
        confirmed = bool(
            identity_result in {"NEW", "RETURNING"}
            and review_status != "PENDING"
            and canonical_uid is not None
            and final_result != "MANUAL_REVIEW_REQUIRED"
        )
        return {
            "identity_result": identity_result,
            "review_status": review_status,
            "candidate_person_uid": candidate_uid,
            "canonical_person_uid": canonical_uid,
            "tracking_person_uid": journey["person_uid"],
            "final_review_result": final_result,
            "identity_confirmed": confirmed,
        }

    @staticmethod
    def _identity(
        review: sqlite3.Row | None,
        warnings: list[str],
    ) -> dict[str, Any]:
        if review is None:
            return {
                "initial_decision": None,
                "temporary_person_uid": None,
                "initial_candidate_person_uid": None,
                "final_result": None,
                "final_candidate_person_uid": None,
                "canonical_person_uid": None,
                "final_score": None,
                "final_margin": None,
                "initial_scores": None,
                "final_scores": None,
            }

        initial_scores, initial_raw = _parse_json_field(
            review["initial_scores_json"],
            "initial_scores_json",
            dict,
            warnings,
        )
        final_scores, final_raw = _parse_json_field(
            review["final_scores_json"],
            "final_scores_json",
            dict,
            warnings,
        )
        body_all = (
            final_scores.get("body_all")
            if isinstance(final_scores, dict)
            else None
        )
        if not isinstance(body_all, dict):
            body_all = None
        identity = {
            "initial_decision": review["initial_decision"],
            "temporary_person_uid": review["provisional_person_uid"],
            "initial_candidate_person_uid": review["candidate_person_uid"],
            "final_result": review["final_review_result"],
            "final_candidate_person_uid": review[
                "final_candidate_person_uid"
            ],
            "canonical_person_uid": review["canonical_person_uid"],
            "final_score": (
                body_all.get("combined_score") if body_all else None
            ),
            "final_margin": (
                body_all.get("match_margin") if body_all else None
            ),
            "initial_scores": initial_scores,
            "final_scores": final_scores,
        }
        if initial_raw is not None:
            identity["initial_scores_raw"] = initial_raw
        if final_raw is not None:
            identity["final_scores_raw"] = final_raw
        return identity

    @staticmethod
    def _timeline(
        connection: sqlite3.Connection,
        journey: sqlite3.Row,
        warnings: list[str],
    ) -> dict[str, Any]:
        route, route_raw = _parse_json_field(
            journey["route_json"],
            "journeys.route_json",
            list,
            warnings,
        )
        rows = connection.execute(
            """
            SELECT node_id, local_track_id, entered_at, matched_at,
                   exited_at, dwell_seconds, exit_reason
            FROM journey_node_visits
            WHERE journey_id = ?
            """,
            (journey["journey_id"],),
        ).fetchall()
        by_node = {str(row["node_id"]): row for row in rows}
        event_rows = connection.execute(
            """
            SELECT event_id, node_id, event_type, event_at, payload_json
            FROM journey_events
            WHERE journey_id = ?
              AND node_id IN ('A', 'B', 'C', 'D')
              AND event_type IN ('ENTRY', 'PASSAGE', 'ARRIVAL')
            ORDER BY event_at, event_id
            """,
            (journey["journey_id"],),
        ).fetchall()
        event_by_node: dict[str, tuple[sqlite3.Row, dict[str, Any]]] = {}
        for event_row in event_rows:
            node_id = str(event_row["node_id"])
            payload, _ = _parse_json_field(
                event_row["payload_json"],
                f"journey_events.{event_row['event_id']}.payload_json",
                dict,
                warnings,
            )
            event_by_node.setdefault(
                node_id,
                (event_row, payload if isinstance(payload, dict) else {}),
            )

        capture_rows = connection.execute(
            """
            SELECT node_id, MIN(captured_at) captured_at,
                   MIN(capture_id) capture_id
            FROM journey_captures
            WHERE journey_id = ? AND node_id IN ('A', 'B', 'C', 'D')
            GROUP BY node_id
            """,
            (journey["journey_id"],),
        ).fetchall()
        capture_by_node = {str(row["node_id"]): row for row in capture_rows}
        route_nodes = route if isinstance(route, list) else []
        middle_node = next(
            (node for node in route_nodes if node in {"B", "C"}),
            None,
        )
        observed_nodes = set(by_node) | set(event_by_node) | set(capture_by_node)
        preferred_order = ["A"]
        if middle_node:
            preferred_order.append(str(middle_node))
        preferred_order.append("D")
        order = [
            node_id
            for node_id in preferred_order
            if node_id in route_nodes or node_id in observed_nodes
        ]
        order.extend(
            node_id
            for node_id in route_nodes
            if node_id in {"A", "B", "C", "D"} and node_id not in order
        )
        order.extend(sorted(observed_nodes - set(order)))

        nodes: list[dict[str, Any]] = []
        for node_id in order:
            row = by_node.get(node_id)
            if row is None:
                event = event_by_node.get(node_id)
                event_row, payload = event if event else (None, {})
                fallback_at = (
                    event_row["event_at"]
                    if event_row is not None
                    else (
                        capture_by_node[node_id]["captured_at"]
                        if node_id in capture_by_node
                        else (
                            journey["entry_at"]
                            if node_id == "A"
                            else journey["passage_at"]
                            if node_id in {"B", "C"}
                            else journey["arrival_at"]
                        )
                    )
                )
                local_track_id = next(
                    (
                        payload.get(key)
                        for key in (
                            f"{node_id.lower()}_local_track_id",
                            "local_track_id",
                            "d_local_track_id",
                            "c_local_track_id",
                            "b_local_track_id",
                        )
                        if payload.get(key) is not None
                    ),
                    None,
                )
                first_seen_at = next(
                    (
                        payload.get(key)
                        for key in (
                            f"{node_id.lower()}_track_first_seen_at",
                            "track_first_seen_at",
                            "d_track_first_seen_at",
                            "c_track_first_seen_at",
                            "b_track_first_seen_at",
                        )
                        if payload.get(key)
                    ),
                    None,
                )
                nodes.append(
                    {
                        "node_id": node_id,
                        "local_track_id": local_track_id,
                        "entered_at": first_seen_at or fallback_at,
                        "matched_at": fallback_at,
                        "exited_at": None,
                        "dwell_seconds": None,
                        "exit_reason": None,
                    }
                )
                continue
            dwell = _seconds_between(
                row["entered_at"],
                row["exited_at"],
                f"{journey['journey_id']}.{node_id}_dwell_seconds",
                warnings,
            )
            nodes.append(
                {
                    "node_id": node_id,
                    "local_track_id": row["local_track_id"],
                    "entered_at": row["entered_at"],
                    "matched_at": row["matched_at"],
                    "exited_at": row["exited_at"],
                    "dwell_seconds": dwell,
                    "exit_reason": row["exit_reason"],
                }
            )

        d_row = by_node.get("D")
        d_exit_at = d_row["exited_at"] if d_row else None
        elapsed = _seconds_between(
            journey["entry_at"],
            d_exit_at,
            f"{journey['journey_id']}.journey_elapsed_seconds",
            warnings,
        )
        a_row = by_node.get("A")
        total_route = _seconds_between(
            a_row["entered_at"] if a_row else None,
            d_exit_at,
            f"{journey['journey_id']}.total_route_seconds",
            warnings,
        )
        completion_duration = _seconds_between(
            journey["entry_at"],
            journey["completed_at"],
            f"{journey['journey_id']}.completion_duration_seconds",
            warnings,
        )
        segments: dict[str, float | None] = {}
        if middle_node:
            middle_row = by_node.get(str(middle_node))
            segments[f"A_to_{middle_node}_seconds"] = _seconds_between(
                a_row["exited_at"] if a_row else None,
                middle_row["entered_at"] if middle_row else None,
                f"{journey['journey_id']}.A_to_{middle_node}_seconds",
                warnings,
            )
            segments[f"{middle_node}_to_D_seconds"] = _seconds_between(
                middle_row["exited_at"] if middle_row else None,
                d_row["entered_at"] if d_row else None,
                f"{journey['journey_id']}.{middle_node}_to_D_seconds",
                warnings,
            )
        result = {
            "route": route,
            "nodes": nodes,
            "a_start": journey["entry_at"],
            "d_exit": d_exit_at,
            "elapsed_seconds": elapsed,
            "total_route_seconds": total_route,
            "completion_duration_seconds": completion_duration,
            "segments": segments,
        }
        if route_raw is not None:
            result["route_raw"] = route_raw
        return result

    def health(self) -> dict[str, Any]:
        with self.connect() as connection:
            connection.execute("SELECT 1").fetchone()
            schema_version = connection.execute(
                "PRAGMA schema_version"
            ).fetchone()[0]
        return {
            "status": "ok",
            "service": "cctv-main-api",
            "database": "ok",
            "schema_version": schema_version,
            "main_server_ip": DEFAULT_MAIN_SERVER_IP,
        }

    def dashboard_summary(self) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM persons
                   WHERE merged_into_person_uid IS NULL) persons_total,
                  (SELECT COUNT(*) FROM persons) persons_total_including_merged,
                  (SELECT COUNT(*) FROM journeys) journeys_total,
                  (SELECT COUNT(*) FROM journeys
                   WHERE status IN ('WAITING_B_OR_C', 'WAITING_D')) active_journeys,
                  (SELECT COUNT(*) FROM journeys
                   WHERE status = 'COMPLETED') completed_journeys,
                  (SELECT COUNT(*) FROM review_cases
                   WHERE status = 'PENDING') pending_reviews,
                  (SELECT COUNT(*) FROM journeys j
                   LEFT JOIN review_cases r ON r.journey_id = j.journey_id
                   WHERE r.final_review_result = 'REVISIT'
                      OR (r.final_review_result IS NULL
                          AND j.person_status = 'RETURNING')) returning_visits,
                  (SELECT COUNT(*) FROM journeys j
                   LEFT JOIN review_cases r ON r.journey_id = j.journey_id
                   WHERE r.final_review_result = 'NEW'
                      OR (r.final_review_result IS NULL
                          AND j.person_status = 'NEW')) new_visits
                """
            ).fetchone()
        return dict(row)

    def journeys(
        self,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        limit, offset = _pagination(query)
        status = _single_query_value(query, "status")
        person_uid = _single_query_value(query, "person_uid")
        final_result = _single_query_value(query, "final_review_result")
        if status is not None and status not in JOURNEY_STATUSES:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="status",
            )
        if person_uid is not None and not PERSON_UID_PATTERN.fullmatch(
            person_uid
        ):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="person_uid",
            )
        if final_result is not None and final_result not in FINAL_REVIEW_RESULTS:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="final_review_result",
            )

        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("j.status = ?")
            params.append(status)
        if final_result:
            clauses.append("r.final_review_result = ?")
            params.append(final_result)
        if person_uid:
            clauses.append(
                """
                (((j.person_uid = ? OR jp.merged_into_person_uid = ?)
                   AND j.person_status IN ('NEW', 'RETURNING', 'MERGED')
                   AND (r.status IS NULL OR r.status <> 'PENDING'))
                 OR r.canonical_person_uid = ?)
                """
            )
            params.extend([person_uid, person_uid, person_uid])
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        sql = f"""
            SELECT j.*
            FROM journeys j
            LEFT JOIN review_cases r ON r.journey_id = j.journey_id
            LEFT JOIN persons jp ON jp.person_uid = j.person_uid
            {where}
            ORDER BY j.entry_at DESC, j.journey_id DESC
            LIMIT ? OFFSET ?
        """
        with self.connect() as connection:
            rows = connection.execute(sql, (*params, limit, offset)).fetchall()
            items = [self._journey_list_item(connection, row) for row in rows]
        return {"items": items, "limit": limit, "offset": offset}

    def events(self, query: dict[str, list[str]]) -> dict[str, Any]:
        since = _single_query_value(query, "since")
        if since is None:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="since",
                detail="since is required and must be a timezone-aware ISO-8601 timestamp",
            )
        try:
            parsed_since = datetime.fromisoformat(since)
        except ValueError as error:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="since",
                detail="invalid ISO-8601 timestamp",
            ) from error
        if parsed_since.tzinfo is None or parsed_since.utcoffset() is None:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="since",
                detail="timezone offset is required",
            )

        limit, offset = _pagination(query)
        normalized_since = parsed_since.isoformat()
        with self.connect() as connection:
            fetch_limit = limit + offset
            journey_rows = connection.execute(
                """
                SELECT event_id, journey_id, node_id, event_type, event_at
                FROM journey_events
                WHERE node_id IN ('A', 'B', 'C', 'D')
                  AND event_type IN ('ENTRY', 'PASSAGE', 'ARRIVAL')
                  AND julianday(event_at) > julianday(?)
                ORDER BY julianday(event_at), event_id
                LIMIT ?
                """,
                (normalized_since, fetch_limit),
            ).fetchall()
            all_items: list[dict[str, Any]] = []
            for row in journey_rows:
                event_id = int(row["event_id"])
                journey = connection.execute(
                    "SELECT * FROM journeys WHERE journey_id = ?",
                    (row["journey_id"],),
                ).fetchone()
                if journey is None:
                    continue
                review = self._review(connection, str(row["journey_id"]))
                state = self._identity_state(journey, review)
                canonical_uid = (
                    state["canonical_person_uid"]
                    if state["identity_confirmed"]
                    else None
                )
                if state["identity_confirmed"]:
                    identity_status = state["identity_result"]
                elif state["final_review_result"] == "MANUAL_REVIEW_REQUIRED":
                    identity_status = "MANUAL_REVIEW_REQUIRED"
                else:
                    identity_status = "IDENTITY_PENDING"
                all_items.append(
                    {
                        "event_id": event_id,
                        "at": row["event_at"],
                        "journey_id": row["journey_id"],
                        "node": row["node_id"],
                        "kind": row["event_type"],
                        "person_uid": canonical_uid,
                        "canonical_person_uid": canonical_uid,
                        "identity_status": identity_status,
                    }
                )
            if self._table_exists(connection, "detection_events"):
                detection_rows = connection.execute(
                    """
                    SELECT event_id, event_at, node_id, event_type,
                           identity_status, journey_id, person_uid,
                           canonical_person_uid
                    FROM detection_events
                    WHERE julianday(event_at) > julianday(?)
                    ORDER BY julianday(event_at), event_id
                    LIMIT ?
                    """,
                    (normalized_since, fetch_limit),
                ).fetchall()
                all_items.extend(
                    {
                        "event_id": str(row["event_id"]),
                        "at": row["event_at"],
                        "journey_id": row["journey_id"],
                        "node": row["node_id"],
                        "kind": row["event_type"],
                        "person_uid": row["person_uid"],
                        "canonical_person_uid": row[
                            "canonical_person_uid"
                        ],
                        "identity_status": row["identity_status"],
                    }
                    for row in detection_rows
                )

        def event_sort_key(item: dict[str, Any]) -> tuple[float, str]:
            try:
                event_epoch = datetime.fromisoformat(str(item["at"])).timestamp()
            except (TypeError, ValueError):
                event_epoch = float("inf")
            return event_epoch, str(item["event_id"])

        all_items.sort(key=event_sort_key)
        unique_items: list[dict[str, Any]] = []
        seen_event_ids: set[str] = set()
        for item in all_items:
            event_key = str(item["event_id"])
            if event_key in seen_event_ids:
                continue
            seen_event_ids.add(event_key)
            unique_items.append(item)
        items = unique_items[offset : offset + limit]
        return {
            "items": items,
            "since": normalized_since,
            "next_since": items[-1]["at"] if items else normalized_since,
            "limit": limit,
            "offset": offset,
        }

    def _journey_list_item(
        self,
        connection: sqlite3.Connection,
        journey: sqlite3.Row,
    ) -> dict[str, Any]:
        warnings: list[str] = []
        review = self._review(connection, str(journey["journey_id"]))
        timeline = self._timeline(connection, journey, warnings)
        identity = self._identity(review, warnings)
        identity_state = self._identity_state(journey, review)
        canonical_uid = identity_state["canonical_person_uid"]
        person = (
            self._person(connection, str(canonical_uid))
            if identity_state["identity_confirmed"] and canonical_uid
            else None
        )
        item = {
            "journey_id": journey["journey_id"],
            "person_uid": (
                canonical_uid if identity_state["identity_confirmed"] else None
            ),
            "person_status": self._effective_person_status(journey, review),
            "visit_count": person["visit_count"] if person else None,
            "journey_status": journey["status"],
            "route": timeline["route"],
            "nodes": timeline["nodes"],
            "captures": self._captures(
                connection,
                str(journey["journey_id"]),
                warnings,
            ),
            "entry_at": journey["entry_at"],
            "passage_at": journey["passage_at"],
            "arrival_at": journey["arrival_at"],
            "completed_at": journey["completed_at"],
            "completion_duration_seconds": timeline[
                "completion_duration_seconds"
            ],
            "d_exit_at": timeline["d_exit"],
            "journey_elapsed_seconds": timeline["elapsed_seconds"],
            "initial_decision": identity["initial_decision"],
            "final_review_result": identity["final_result"],
            **identity_state,
        }
        if warnings:
            item["validation_warnings"] = warnings
        return item

    def journey(self, journey_id: str) -> dict[str, Any]:
        if not JOURNEY_ID_PATTERN.fullmatch(journey_id):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_journey_id",
                journey_id=journey_id,
            )
        with self.connect() as connection:
            journey = connection.execute(
                "SELECT * FROM journeys WHERE journey_id = ?",
                (journey_id,),
            ).fetchone()
            if journey is None:
                raise ApiError(
                    HTTPStatus.NOT_FOUND,
                    "journey_not_found",
                    journey_id=journey_id,
                )
            review = self._review(connection, journey_id)
            warnings: list[str] = []
            timeline = self._timeline(connection, journey, warnings)
            identity = self._identity(review, warnings)
            identity_state = self._identity_state(journey, review)
            canonical_uid = identity_state["canonical_person_uid"]
            identity["canonical_person_uid"] = (
                canonical_uid if identity_state["identity_confirmed"] else None
            )
            person = (
                self._person(connection, str(canonical_uid))
                if identity_state["identity_confirmed"] and canonical_uid
                else None
            )
            captures = self._captures(connection, journey_id, warnings)
            capture_groups = self._capture_groups(
                connection,
                journey_id,
                warnings,
            )
            gallery_summary = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT node_id, modality, embedding_dim,
                           COUNT(*) sample_count,
                           MIN(quality) min_quality,
                           MAX(quality) max_quality
                    FROM journey_gallery
                    WHERE journey_id = ?
                    GROUP BY node_id, modality, embedding_dim
                    ORDER BY node_id, modality
                    """,
                    (journey_id,),
                )
            ]
        return {
            "journey_id": journey_id,
            "person": {
                "person_uid": (
                    canonical_uid
                    if identity_state["identity_confirmed"]
                    else None
                ),
                "candidate_person_uid": identity_state[
                    "candidate_person_uid"
                ],
                "tracking_person_uid": identity_state[
                    "tracking_person_uid"
                ],
                "identity_confirmed": identity_state[
                    "identity_confirmed"
                ],
                "status": (
                    person["status"]
                    if person
                    else self._effective_person_status(journey, review)
                ),
                "visit_count": person["visit_count"] if person else None,
            },
            "journey_status": journey["status"],
            "person_status": self._effective_person_status(journey, review),
            **identity_state,
            "route": timeline["route"],
            "entry_at": journey["entry_at"],
            "passage_at": journey["passage_at"],
            "arrival_at": journey["arrival_at"],
            "completed_at": journey["completed_at"],
            "timing": {
                "a_start": timeline["a_start"],
                "arrival_at": journey["arrival_at"],
                "completed_at": journey["completed_at"],
                "completion_duration_seconds": timeline[
                    "completion_duration_seconds"
                ],
                "d_exit": timeline["d_exit"],
                "elapsed_seconds": timeline["elapsed_seconds"],
                "total_route_seconds": timeline["total_route_seconds"],
                "segments": timeline["segments"],
            },
            "identity": identity,
            "nodes": timeline["nodes"],
            "captures": captures,
            "capture_groups": capture_groups,
            "gallery_summary": gallery_summary,
            "validation_warnings": warnings,
        }

    @staticmethod
    def _captures(
        connection: sqlite3.Connection,
        journey_id: str,
        warnings: list[str],
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT capture_id, node_id, captured_at, image_path,
                   similarity, quality, verification_status, metadata_json
            FROM journey_captures
            WHERE journey_id = ?
            ORDER BY capture_id
            """,
            (journey_id,),
        ).fetchall()
        captures: list[dict[str, Any]] = []
        for row in rows:
            metadata, raw = _parse_json_field(
                row["metadata_json"],
                f"capture.{row['capture_id']}.metadata_json",
                dict,
                warnings,
            )
            item = {
                "capture_id": row["capture_id"],
                "node_id": row["node_id"],
                "captured_at": row["captured_at"],
                "capture_path": row["image_path"],
                "similarity": row["similarity"],
                "quality": row["quality"],
                "verification_status": row["verification_status"],
                "metadata": metadata,
            }
            if raw is not None:
                item["metadata_raw"] = raw
            captures.append(item)
        return captures

    def _capture_groups(
        self,
        connection: sqlite3.Connection,
        journey_id: str,
        warnings: list[str],
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        row = connection.execute(
            """
            SELECT payload_json
            FROM journey_events
            WHERE journey_id = ?
              AND node_id = 'A'
              AND event_type = 'ENTRY'
            ORDER BY event_id
            LIMIT 1
            """,
            (journey_id,),
        ).fetchone()
        grouped: dict[str, dict[str, list[dict[str, Any]]]] = {
            "A": {"body": [], "face": []}
        }
        if self._table_exists(connection, "captures"):
            cached_rows = connection.execute(
                """
                SELECT * FROM captures
                WHERE journey_id = ? AND camera_id = 'A'
                ORDER BY capture_id
                """,
                (journey_id,),
            ).fetchall()
            for cached in cached_rows:
                modality = str(cached["capture_type"]).lower()
                if modality not in {"body", "face"}:
                    continue
                item = self._capture_payload(cached)
                item["rank"] = len(grouped["A"][modality]) + 1
                item["quality"] = cached["quality_score"]
                item["url"] = item["image_url"]
                grouped["A"][modality].append(item)
            if grouped["A"]["body"] or grouped["A"]["face"]:
                return grouped
        if row is None:
            return grouped

        payload, _ = _parse_json_field(
            row["payload_json"],
            "journey_events.A_ENTRY.payload_json",
            dict,
            warnings,
        )
        if not isinstance(payload, dict):
            return grouped

        for modality in ("body", "face"):
            raw_paths = payload.get(f"{modality}_capture_paths")
            raw_qualities = payload.get(f"{modality}_qualities")
            if raw_paths is None:
                continue
            if not isinstance(raw_paths, list):
                warnings.append(
                    f"A.{modality}_capture_paths: expected list"
                )
                continue
            qualities = raw_qualities if isinstance(raw_qualities, list) else []
            if raw_qualities is not None and not isinstance(
                raw_qualities,
                list,
            ):
                warnings.append(f"A.{modality}_qualities: expected list")
            for index, raw_path in enumerate(raw_paths[:3]):
                quality: float | None = None
                if index < len(qualities):
                    raw_quality = qualities[index]
                    if isinstance(raw_quality, (int, float)) and not isinstance(
                        raw_quality,
                        bool,
                    ):
                        quality = float(raw_quality)
                    elif raw_quality is not None:
                        warnings.append(
                            f"A.{modality}[{index + 1}].quality: invalid number"
                        )
                grouped["A"][modality].append(
                    {
                        "rank": index + 1,
                        "quality": quality,
                        "url": self._camera_a_capture_url(
                            raw_path,
                            modality,
                            index + 1,
                            warnings,
                        ),
                    }
                )
        return grouped

    def _camera_a_capture_url(
        self,
        raw_path: Any,
        modality: str,
        rank: int,
        warnings: list[str],
    ) -> str | None:
        if not isinstance(raw_path, str) or not raw_path.strip():
            warnings.append(f"A.{modality}[{rank}].capture_path: invalid path")
            return None
        path = PurePosixPath(raw_path.strip())
        root = self.camera_a_capture_roots[modality]
        try:
            relative = path.relative_to(root)
        except ValueError:
            warnings.append(
                f"A.{modality}[{rank}].capture_path: outside allowed root"
            )
            return None
        if not path.is_absolute() or not relative.parts or ".." in relative.parts:
            warnings.append(
                f"A.{modality}[{rank}].capture_path: unsafe relative path"
            )
            return None
        encoded_relative = quote(relative.as_posix(), safe="/")
        return (
            f"{self.camera_a_image_base_url}/captures/{modality}/"
            f"{encoded_relative}"
        )

    def persons(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit, offset = _pagination(query)
        include_merged = _boolean_query(query, "include_merged")
        where = "" if include_merged else "WHERE merged_into_person_uid IS NULL"
        with self.connect() as connection:
            has_representative = self._column_exists(
                connection, "persons", "representative_capture_id"
            )
            representative_columns = (
                "representative_capture_id, representative_source, "
                "representative_updated_at"
                if has_representative
                else "NULL representative_capture_id, NULL representative_source, "
                "NULL representative_updated_at"
            )
            rows = connection.execute(
                f"""
                SELECT person_uid, status, visit_count, created_at,
                       last_seen_at, merged_into_person_uid,
                       {representative_columns}
                FROM persons
                {where}
                ORDER BY last_seen_at DESC, person_uid DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            capture_id = item.get("representative_capture_id")
            item["representative_image_url"] = (
                f"/api/captures/{capture_id}/image" if capture_id else None
            )
            items.append(item)
        return {
            "items": items,
            "limit": limit,
            "offset": offset,
            "include_merged": include_merged,
        }

    def person(
        self,
        person_uid: str,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        if not PERSON_UID_PATTERN.fullmatch(person_uid):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_person_uid",
                person_uid=person_uid,
            )
        raw_limit = _single_query_value(query, "journey_limit")
        try:
            journey_limit = 20 if raw_limit is None else int(raw_limit)
        except ValueError as error:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="journey_limit",
            ) from error
        if journey_limit < 1 or journey_limit > 100:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="journey_limit",
                allowed="1..100",
            )
        with self.connect() as connection:
            person = self._person(connection, person_uid)
            if person is None:
                raise ApiError(
                    HTTPStatus.NOT_FOUND,
                    "person_not_found",
                    person_uid=person_uid,
                )
            rows = connection.execute(
                """
                SELECT DISTINCT j.*
                FROM journeys j
                LEFT JOIN review_cases r ON r.journey_id = j.journey_id
                LEFT JOIN persons jp ON jp.person_uid = j.person_uid
                WHERE (((j.person_uid = ? OR jp.merged_into_person_uid = ?)
                        AND j.person_status IN ('NEW', 'RETURNING', 'MERGED')
                        AND (r.status IS NULL OR r.status <> 'PENDING'))
                       OR r.canonical_person_uid = ?)
                ORDER BY j.entry_at DESC, j.journey_id DESC
                LIMIT ?
                """,
                (
                    person_uid,
                    person_uid,
                    person_uid,
                    journey_limit,
                ),
            ).fetchall()
            journeys: list[dict[str, Any]] = []
            for journey in rows:
                warnings: list[str] = []
                timeline = self._timeline(connection, journey, warnings)
                journey_review = self._review(
                    connection, str(journey["journey_id"])
                )
                item = {
                    "journey_id": journey["journey_id"],
                    "journey_status": journey["status"],
                    **self._identity_state(journey, journey_review),
                    "route": timeline["route"],
                    "entry_at": journey["entry_at"],
                    "d_exit_at": timeline["d_exit"],
                    "elapsed_seconds": timeline["elapsed_seconds"],
                }
                if warnings:
                    item["validation_warnings"] = warnings
                journeys.append(item)
            representative_capture_id = (
                person["representative_capture_id"]
                if "representative_capture_id" in person.keys()
                else None
            )
            representative_source = (
                person["representative_source"]
                if "representative_source" in person.keys()
                else None
            )
            representative_updated_at = (
                person["representative_updated_at"]
                if "representative_updated_at" in person.keys()
                else None
            )
        return {
            "person_uid": person["person_uid"],
            "status": person["status"],
            "visit_count": person["visit_count"],
            "created_at": person["created_at"],
            "last_seen_at": person["last_seen_at"],
            "merged_into_person_uid": person["merged_into_person_uid"],
            "representative_capture_id": representative_capture_id,
            "representative_source": representative_source,
            "representative_updated_at": representative_updated_at,
            "representative_image_url": (
                f"/api/captures/{representative_capture_id}/image"
                if representative_capture_id
                else None
            ),
            "journeys": journeys,
        }

    def reviews(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit, offset = _pagination(query)
        status = _single_query_value(query, "status")
        if status is not None and status not in REVIEW_STATUSES:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                parameter="status",
            )
        where = "WHERE status = ?" if status else ""
        params: tuple[Any, ...] = (status, limit, offset) if status else (
            limit,
            offset,
        )
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM review_cases
                {where}
                ORDER BY created_at DESC, review_id DESC
                LIMIT ? OFFSET ?
                """,
                params,
            ).fetchall()
        items = [self._review_summary(row) for row in rows]
        return {"items": items, "limit": limit, "offset": offset}

    @staticmethod
    def _review_summary(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "review_id": row["review_id"],
            "review_case_id": row["review_id"],
            "journey_id": row["journey_id"],
            "temporary_person_uid": row["provisional_person_uid"],
            "candidate_person_uid": row["candidate_person_uid"],
            "initial_decision": row["initial_decision"],
            "final_review_result": row["final_review_result"],
            "final_candidate_person_uid": row[
                "final_candidate_person_uid"
            ],
            "canonical_person_uid": row["canonical_person_uid"],
            "status": row["status"],
            "action": row["action"],
            "resolution_source": row["resolution_source"],
            "resolved_at": row["resolved_at"],
            "final_reviewed_at": row["final_reviewed_at"],
        }

    def review(self, review_or_journey_id: str) -> dict[str, Any]:
        is_review_id = REVIEW_ID_PATTERN.fullmatch(review_or_journey_id)
        is_journey_id = JOURNEY_ID_PATTERN.fullmatch(review_or_journey_id)
        if not is_review_id and not is_journey_id:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_review_id",
                review_id=review_or_journey_id,
            )
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM review_cases WHERE "
                + ("review_id = ?" if is_review_id else "journey_id = ?"),
                (review_or_journey_id,),
            ).fetchone()
            if row is None:
                raise ApiError(
                    HTTPStatus.NOT_FOUND,
                    "review_not_found",
                    review_id=review_or_journey_id,
                )
            warnings: list[str] = []
            identity = self._identity(row, warnings)
            summary = self._review_summary(row)
            candidates = []
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='identity_review_candidates'"
            ).fetchone()
            if table_exists is not None:
                candidate_rows = connection.execute(
                    """
                    SELECT * FROM identity_review_candidates
                    WHERE review_id = ? ORDER BY rank
                    """,
                    (row["review_id"],),
                ).fetchall()
                for candidate in candidate_rows:
                    route, _ = _parse_json_field(
                        candidate["candidate_recent_route_json"],
                        "candidate_recent_route_json",
                        list,
                        warnings,
                    )
                    query_capture_url = None
                    candidate_capture_url = None
                    if self._table_exists(connection, "captures"):
                        query_capture = connection.execute(
                            """
                            SELECT capture_id FROM captures
                            WHERE journey_id = ? AND cache_status = 'CACHED'
                            ORDER BY CASE capture_type WHEN 'FACE' THEN 0 ELSE 1 END,
                                     COALESCE(quality_score, -1) DESC,
                                     capture_id ASC LIMIT 1
                            """,
                            (row["journey_id"],),
                        ).fetchone()
                        if query_capture is not None:
                            query_capture_url = (
                                f"/api/captures/{query_capture['capture_id']}/image"
                            )
                        if self._column_exists(
                            connection, "persons", "representative_capture_id"
                        ):
                            representative = connection.execute(
                                "SELECT representative_capture_id FROM persons "
                                "WHERE person_uid = ?",
                                (candidate["candidate_person_uid"],),
                            ).fetchone()
                            if representative and representative["representative_capture_id"]:
                                candidate_capture_url = (
                                    "/api/captures/"
                                    f"{representative['representative_capture_id']}/image"
                                )
                    candidates.append(
                        {
                            "candidate_person_uid": candidate[
                                "candidate_person_uid"
                            ],
                            "rank": candidate["rank"],
                            "body_similarity": candidate["body_similarity"],
                            "face_similarity": candidate["face_similarity"],
                            "fused_similarity": candidate["fused_similarity"],
                            "score_margin": candidate["score_margin"],
                            "query_capture_path": candidate[
                                "query_capture_path"
                            ],
                            "query_capture_url": query_capture_url
                            or self._camera_a_capture_url(
                                candidate["query_capture_path"],
                                "body",
                                int(candidate["rank"]),
                                warnings,
                            ),
                            "candidate_capture_path": candidate[
                                "candidate_capture_path"
                            ],
                            "candidate_capture_url": candidate_capture_url
                            or self._camera_a_capture_url(
                                candidate["candidate_capture_path"],
                                "body",
                                int(candidate["rank"]),
                                warnings,
                            ),
                            "last_seen_at": candidate[
                                "candidate_last_seen_at"
                            ],
                            "recent_route": route,
                            "journeys": [
                                dict(journey)
                                for journey in connection.execute(
                                    """
                                    SELECT journey_id, status, route_json, entry_at,
                                           completed_at, identity_result
                                    FROM journeys WHERE person_uid = ?
                                    ORDER BY entry_at DESC LIMIT 10
                                    """,
                                    (candidate["candidate_person_uid"],),
                                ).fetchall()
                            ],
                        }
                    )
        return {
            **summary,
            "identity": identity,
            "identity_result": (
                "UNKNOWN" if row["status"] == "PENDING" else row["final_review_result"]
            ),
            "review_status": row["status"],
            "candidates": candidates,
            "validation_warnings": warnings,
        }

    def resolve_review(
        self,
        review_id: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        if not REVIEW_ID_PATTERN.fullmatch(review_id):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_review_id",
                review_id=review_id,
            )
        action = str(request.get("action", "")).strip().upper()
        aliases = {
            "SELECT_EXISTING": "MERGE_EXISTING",
            "EXISTING": "MERGE_EXISTING",
            "CREATE_NEW": "CONFIRM_NEW",
            "NEW": "CONFIRM_NEW",
            "HOLD": "HOLD",
        }
        action = aliases.get(action, action)
        if action not in {"MERGE_EXISTING", "CONFIRM_NEW", "HOLD"}:
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_action",
                allowed=["SELECT_EXISTING", "CREATE_NEW", "HOLD"],
            )
        selected = request.get("selected_person_uid")
        if action == "MERGE_EXISTING" and (
            not isinstance(selected, str)
            or not PERSON_UID_PATTERN.fullmatch(selected)
        ):
            raise ApiError(
                HTTPStatus.BAD_REQUEST,
                "invalid_selected_person_uid",
            )

        if action == "HOLD":
            with self.connect_write() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT status FROM review_cases WHERE review_id = ?",
                    (review_id,),
                ).fetchone()
                if row is None:
                    raise ApiError(HTTPStatus.NOT_FOUND, "review_not_found")
                if row["status"] != "PENDING":
                    raise ApiError(HTTPStatus.CONFLICT, "review_already_resolved")
                connection.execute(
                    """
                    INSERT INTO identity_review_audit (
                        review_id, action, selected_person_uid,
                        decision_source, request_json, created_at
                    ) VALUES (?, 'HOLD', NULL, 'MANUAL_REVIEW', ?, ?)
                    """,
                    (
                        review_id,
                        json.dumps(request, ensure_ascii=False),
                        datetime.now().astimezone().isoformat(timespec="seconds"),
                    ),
                )
                connection.commit()
            return {"review_id": review_id, "outcome": "PENDING", "action": "HOLD"}

        # Reuse the Main process' transaction-safe resolution implementation.
        from cctv_main import main_server

        with self.resolution_lock:
            original_db_path = main_server.DB_PATH
            main_server.DB_PATH = self.db_path
            try:
                if action == "MERGE_EXISTING":
                    result = main_server.resolve_review_merge_existing(
                        review_id,
                        str(selected),
                    )
                else:
                    result = main_server.resolve_review_confirm_new(review_id)
            finally:
                main_server.DB_PATH = original_db_path
        if result["outcome"] == "CONFLICT":
            raise ApiError(
                HTTPStatus.CONFLICT,
                "review_resolution_conflict",
                reason=result.get("reason"),
            )
        with self.connect_write() as connection:
            connection.execute(
                """
                INSERT INTO identity_review_audit (
                    review_id, action, selected_person_uid,
                    decision_source, request_json, created_at
                ) VALUES (?, ?, ?, 'MANUAL_REVIEW', ?, ?)
                """,
                (
                    review_id,
                    action,
                    result.get("target_person_uid"),
                    json.dumps(request, ensure_ascii=False),
                    datetime.now().astimezone().isoformat(timespec="seconds"),
                ),
            )
            connection.commit()
        return result


IDENTITY_REVIEW_HTML = r"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Identity Reviews</title><style>
body{font:14px system-ui;margin:0;background:#f4f6f8;color:#17202a}header{padding:18px 24px;background:#17202a;color:white}
main{padding:20px;max-width:1200px;margin:auto}.case,.candidate{background:white;border:1px solid #d9e0e7;border-radius:10px;padding:16px;margin:12px 0}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}img{width:100%;height:220px;object-fit:contain;background:#eef1f4}
button{padding:9px 13px;margin:6px 6px 0 0;border:0;border-radius:7px;cursor:pointer}.select{background:#1769aa;color:white}.new{background:#18794e;color:white}.hold{background:#6b7280;color:white}
.muted{color:#667085}.error{color:#b42318}.score{font-variant-numeric:tabular-nums}
</style></head><body><header><h1>Identity Pending 검토</h1></header><main id="app">불러오는 중…</main>
<script>
const app=document.getElementById('app'), esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(url,opt){const r=await fetch(url,opt);const j=await r.json();if(!r.ok)throw Error(j.reason||j.error||r.status);return j}
function image(url,label){return url?`<img src="${esc(url)}" alt="${esc(label)}" onerror="this.replaceWith(Object.assign(document.createElement('p'),{className:'error',textContent:'이미지를 불러올 수 없습니다.'}))">`:'<p class="muted">사진 URL 없음</p>'}
async function resolve(id,action,uid){if(action!=='HOLD'&&!confirm('이 결정을 적용할까요?'))return;await api(`/api/identity-reviews/${id}/resolve`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,selected_person_uid:uid})});await load()}
async function detail(id){const d=await api(`/api/identity-reviews/${id}`);const q=d.candidates?.[0]?.query_capture_url;return `<section class="case"><h2>${esc(d.review_id)} · ${esc(d.journey_id)}</h2><p>Identity: ${esc(d.identity_result)} / Review: ${esc(d.review_status)}</p><div class="grid"><article>${image(q,'현재 인물')}<b>현재 인물</b></article>${(d.candidates||[]).map(c=>`<article class="candidate">${image(c.candidate_capture_url,c.candidate_person_uid)}<h3>#${esc(c.rank)} ${esc(c.candidate_person_uid)}</h3><p class="score">Body ${esc(c.body_similarity)}<br>Face ${esc(c.face_similarity)}<br>Fused ${esc(c.fused_similarity)}<br>Margin ${esc(c.score_margin)}</p><p>최근 등장 ${esc(c.last_seen_at)}<br>최근 경로 ${esc((c.recent_route||[]).join(' → '))}</p><details><summary>기존 Journey ${(c.journeys||[]).length}건</summary>${(c.journeys||[]).map(j=>`<div>${esc(j.journey_id)} · ${esc(j.status)} · ${esc(j.entry_at)}</div>`).join('')}</details><button class="select" onclick="resolve('${esc(d.review_id)}','SELECT_EXISTING','${esc(c.candidate_person_uid)}')">기존 후보 선택</button></article>`).join('')}</div><button class="new" onclick="resolve('${esc(d.review_id)}','CREATE_NEW')">동일인 없음 · 신규</button><button class="hold" onclick="resolve('${esc(d.review_id)}','HOLD')">보류</button></section>`}
async function load(){try{const l=await api('/api/identity-reviews?status=PENDING');app.innerHTML=l.items.length?(await Promise.all(l.items.map(x=>detail(x.review_id)))).join(''):'<p>대기 중인 검토가 없습니다.</p>'}catch(e){app.innerHTML=`<p class="error">${esc(e.message)}</p>`}}load();
</script></body></html>"""


class MainApiServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        repository: ReadOnlyRepository,
        cors_origin: str,
        admin_token: str | None = None,
        admin_client: AdminControlClient | None = None,
    ) -> None:
        self.repository = repository
        self.database_gate = repository.database_gate
        self.cors_origin = cors_origin
        self.admin_token = admin_token
        self.admin_client = admin_client
        super().__init__(server_address, MainApiHandler)


class MainApiHandler(BaseHTTPRequestHandler):
    server: MainApiServer
    protocol_version = "HTTP/1.1"

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", self.server.cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.send_header("Access-Control-Allow-Origin", self.server.cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    @staticmethod
    def _is_admin_path(path: str) -> bool:
        return path == "/api/admin/database/status" or path.startswith(
            "/api/admin/database/"
        )

    def _require_admin(self) -> None:
        expected = self.server.admin_token
        if expected is None:
            raise ApiError(HTTPStatus.SERVICE_UNAVAILABLE, "ADMIN_API_DISABLED")
        header = self.headers.get("Authorization")
        if not header or not header.startswith("Bearer "):
            raise ApiError(HTTPStatus.UNAUTHORIZED, "ADMIN_AUTH_REQUIRED")
        supplied = header[len("Bearer ") :]
        if not supplied or not hmac.compare_digest(supplied, expected):
            raise ApiError(HTTPStatus.FORBIDDEN, "ADMIN_FORBIDDEN")

    def _admin_request(
        self,
        method: str,
        path: str,
        request: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self._require_admin()
        client = self.server.admin_client
        if client is None:
            raise ApiError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "MAIN_ADMIN_CONTROL_UNAVAILABLE",
            )
        try:
            return client.request(method, path, request)
        except AdminControlError as error:
            raise ApiError(error.status, **error.payload) from error

    def _watch_reset_job(self, job_id: str) -> None:
        client = self.server.admin_client
        if client is None:
            self.server.database_gate.resume()
            return
        try:
            while True:
                _, job = client.request(
                    "GET", f"/api/admin/database/jobs/{job_id}"
                )
                if job.get("status") in {"COMPLETED", "FAILED"}:
                    return
                time.sleep(0.1)
        except Exception:
            # Avoid leaving all normal APIs disabled if Main becomes unavailable.
            return
        finally:
            self.server.database_gate.resume()

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            if self._is_admin_path(parsed.path):
                status, payload = self._admin_request("GET", parsed.path)
                return self._json(status, payload)
            if parsed.path in {"/identity-reviews", "/identity-reviews/"}:
                return self._identity_review_page()
            capture_prefix = "/api/captures/"
            capture_suffix = "/image"
            if parsed.path.startswith(capture_prefix) and parsed.path.endswith(
                capture_suffix
            ):
                capture_id = parsed.path[
                    len(capture_prefix) : -len(capture_suffix)
                ].strip("/")
                return self._capture_image(capture_id)
            query = parse_qs(parsed.query, keep_blank_values=True)
            payload = self._route(parsed.path, query)
            self._json(HTTPStatus.OK, payload)
        except ApiError as error:
            self._json(error.status, error.payload)
        except sqlite3.Error:
            self._json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "database_unavailable"},
            )
        except Exception:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_server_error"},
            )

    def _route(
        self,
        path: str,
        query: dict[str, list[str]],
    ) -> dict[str, Any]:
        repository = self.server.repository
        if path in {"/api/health", "/api/status"}:
            return repository.health()
        if path == "/api/dashboard/summary":
            return repository.dashboard_summary()
        if path == "/api/journeys":
            return repository.journeys(query)
        if path == "/api/events":
            return repository.events(query)
        if path.startswith("/api/journeys/"):
            return repository.journey(path[len("/api/journeys/") :])
        if path == "/api/persons":
            return repository.persons(query)
        if path.startswith("/api/persons/") and path.endswith("/captures"):
            person_uid = path[len("/api/persons/") : -len("/captures")].strip("/")
            return repository.person_captures(person_uid)
        if path.startswith("/api/persons/"):
            return repository.person(
                path[len("/api/persons/") :], query
            )
        if path == "/api/reviews":
            return repository.reviews(query)
        if path.startswith("/api/reviews/"):
            return repository.review(path[len("/api/reviews/") :])
        if path == "/api/identity-reviews":
            return repository.reviews(query)
        if path.startswith("/api/identity-reviews/"):
            return repository.review(
                path[len("/api/identity-reviews/") :]
            )
        raise ApiError(HTTPStatus.NOT_FOUND, "endpoint_not_found", path=path)

    def _capture_image(self, capture_id: str) -> None:
        path, mime_type, sha256 = self.server.repository.capture_image(capture_id)
        size = path.stat().st_size
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type)
        self.send_header("Content-Length", str(size))
        self.send_header("Access-Control-Allow-Origin", self.server.cors_origin)
        self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        if sha256:
            self.send_header("ETag", f'"{sha256}"')
        self.end_headers()
        with path.open("rb") as image:
            while True:
                chunk = image.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def _request_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as error:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_content_length") from error
        if length < 2 or length > 65536:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_request_size")
        try:
            request = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_json") from error
        if not isinstance(request, dict):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_json_object")
        return request

    def _optional_request_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as error:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_content_length") from error
        if length == 0:
            return {}
        return self._request_json()

    def do_POST(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            if self._is_admin_path(parsed.path):
                allowed = {
                    "/api/admin/database/backup",
                    "/api/admin/database/reset/preview",
                    "/api/admin/database/reset/execute",
                }
                if parsed.path not in allowed:
                    return self._method_not_allowed()
                request = self._optional_request_json()
                reset_execute = parsed.path.endswith("/reset/execute")
                if reset_execute:
                    self.server.database_gate.pause_and_wait()
                try:
                    status, payload = self._admin_request(
                        "POST",
                        parsed.path,
                        request,
                    )
                except Exception:
                    if reset_execute:
                        self.server.database_gate.resume()
                    raise
                if reset_execute:
                    job_id = payload.get("job_id")
                    if status == HTTPStatus.ACCEPTED and job_id:
                        threading.Thread(
                            target=self._watch_reset_job,
                            args=(str(job_id),),
                            daemon=True,
                            name=f"api-reset-watch-{job_id}",
                        ).start()
                    else:
                        self.server.database_gate.resume()
                return self._json(status, payload)
            prefix = "/api/identity-reviews/"
            suffix = "/resolve"
            if not parsed.path.startswith(prefix) or not parsed.path.endswith(suffix):
                return self._method_not_allowed()
            review_id = parsed.path[len(prefix) : -len(suffix)].strip("/")
            request = self._request_json()
            payload = self.server.repository.resolve_review(review_id, request)
            self._json(HTTPStatus.OK, payload)
        except ApiError as error:
            self._json(error.status, error.payload)
        except sqlite3.Error:
            self._json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "database_unavailable"},
            )
        except Exception:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_server_error"},
            )

    def do_PATCH(self) -> None:  # noqa: N802
        try:
            parsed = urlsplit(self.path)
            prefix = "/api/persons/"
            suffix = "/representative-capture"
            if not parsed.path.startswith(prefix) or not parsed.path.endswith(suffix):
                return self._method_not_allowed()
            person_uid = parsed.path[len(prefix) : -len(suffix)].strip("/")
            payload = self.server.repository.set_representative_capture(
                person_uid, self._request_json()
            )
            self._json(HTTPStatus.OK, payload)
        except ApiError as error:
            self._json(error.status, error.payload)
        except sqlite3.Error:
            self._json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "database_unavailable"},
            )
        except Exception:
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error": "internal_server_error"},
            )

    def _identity_review_page(self) -> None:
        body = IDENTITY_REVIEW_HTML.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _method_not_allowed(self) -> None:
        self._json(
            HTTPStatus.METHOD_NOT_ALLOWED,
            {
                "error": "method_not_allowed",
                "allowed": ["GET", "POST", "PATCH", "OPTIONS"],
            },
        )

    do_PUT = _method_not_allowed
    do_DELETE = _method_not_allowed

    def log_message(self, format: str, *args: Any) -> None:
        print(
            f"[CCTV Main API] {self.client_address[0]} "
            f"{format % args}",
            flush=True,
        )


def create_server(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    db_path: Path | str = DEFAULT_DB_PATH,
    cors_origin: str = "*",
    camera_a_image_base_url: str = DEFAULT_CAMERA_A_IMAGE_BASE_URL,
    camera_a_body_root: str = DEFAULT_CAMERA_A_BODY_ROOT,
    camera_a_face_root: str = DEFAULT_CAMERA_A_FACE_ROOT,
    capture_storage_root: Path | str | None = None,
    admin_token: str | None = None,
    admin_control_url: str | None = None,
    admin_client: AdminControlClient | None = None,
) -> MainApiServer:
    resolved_admin_token = (
        admin_token.strip() or None
        if admin_token is not None
        else os.environ.get("MAIN_ADMIN_TOKEN", "").strip() or None
    )
    if admin_client is None and resolved_admin_token is not None:
        resolved_control_url = admin_control_url or os.environ.get(
            "MAIN_ADMIN_CONTROL_URL",
            f"http://127.0.0.1:{ADMIN_CONTROL_DEFAULT_PORT}",
        )
        admin_client = AdminControlClient(
            resolved_control_url,
            resolved_admin_token,
        )
    database_gate = ApiDatabaseGate()
    return MainApiServer(
        (host, port),
        ReadOnlyRepository(
            db_path,
            camera_a_image_base_url,
            camera_a_body_root,
            camera_a_face_root,
            capture_storage_root,
            database_gate,
        ),
        cors_origin,
        resolved_admin_token,
        admin_client,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="CCTV Main REST API")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--cors-origin", default="*")
    parser.add_argument(
        "--camera-a-image-base-url",
        default=DEFAULT_CAMERA_A_IMAGE_BASE_URL,
    )
    parser.add_argument(
        "--camera-a-body-root",
        default=DEFAULT_CAMERA_A_BODY_ROOT,
    )
    parser.add_argument(
        "--camera-a-face-root",
        default=DEFAULT_CAMERA_A_FACE_ROOT,
    )
    parser.add_argument("--capture-storage-root", type=Path, default=None)
    args = parser.parse_args()

    server = create_server(
        args.host,
        args.port,
        args.db,
        args.cors_origin,
        args.camera_a_image_base_url,
        args.camera_a_body_root,
        args.camera_a_face_root,
        args.capture_storage_root,
    )
    server.repository.health()
    print("CCTV Main REST API", flush=True)
    print(f"Host : {args.host}", flush=True)
    print(f"Port : {server.server_port}", flush=True)
    print(f"DB   : {server.repository.db_path}", flush=True)
    print("Mode : READ + identity review resolution", flush=True)
    print(f"CORS : {args.cors_origin} (GET/POST/PATCH/OPTIONS)", flush=True)
    print(f"Images: {server.repository.capture_storage_root}", flush=True)
    print(f"A Img: {args.camera_a_image_base_url}", flush=True)
    print(
        "Admin: enabled (Main-owned control)"
        if server.admin_token is not None
        else "Admin: disabled (MAIN_ADMIN_TOKEN unset)",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("CCTV Main REST API 종료", flush=True)


if __name__ == "__main__":
    main()
