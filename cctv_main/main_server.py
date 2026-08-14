from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import sqlite3
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import paho.mqtt.client as mqtt
import yaml

from cctv_main.capture_cache import (
    cache_capture,
    choose_automatic_representative,
    insert_capture_rows,
    insert_failed_capture_rows,
    parse_capture_specs,
    settings_from_document,
)
from cctv_main.admin_control import (
    ADMIN_CONTROL_DEFAULT_HOST,
    ADMIN_CONTROL_DEFAULT_PORT,
    DATABASE_SCHEMA_VERSION,
    DatabaseAdminController,
    IngestionCoordinator,
    MainAdminControlServer,
    configured_admin_token,
)


# ============================================================
# 기본 설정
# ============================================================

ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent


def load_runtime_settings() -> tuple[str, int, int, Path]:
    config_path = Path(
        os.environ.get(
            "CCTV_MQTT_CONFIG",
            PROJECT_ROOT / "configs" / "mqtt.yaml",
        )
    ).expanduser()

    document = yaml.safe_load(
        config_path.read_text(encoding="utf-8")
    )
    if not isinstance(document, dict):
        raise ValueError(
            f"MQTT config must be a mapping: {config_path}"
        )

    broker = document.get("broker")
    if not isinstance(broker, dict):
        raise ValueError(
            f"MQTT config is missing broker: {config_path}"
        )

    host = str(broker.get("host", "")).strip()
    port = int(broker.get("port", 1883))
    qos = int(broker.get("qos", 1))

    if host in {"", "localhost", "127.0.0.1", "10.10.20.56"}:
        raise ValueError(
            f"Main Server MQTT host is not allowed: {host!r}"
        )
    if not 1 <= port <= 65535:
        raise ValueError(f"MQTT port is out of range: {port}")
    if qos not in {0, 1, 2}:
        raise ValueError(f"MQTT qos is invalid: {qos}")

    return host, port, qos, config_path


DB_PATH = Path(
    os.environ.get(
        "CCTV_MAIN_DB",
        PROJECT_ROOT / "data" / "main_server.db",
    )
).expanduser()

MQTT_HOST, MQTT_PORT, MQTT_QOS, MQTT_CONFIG_PATH = (
    load_runtime_settings()
)
# Candidate delivery is rebuilt from the central active-Journey state after a
# reconnect.  QoS 1 is deliberately not used here: Paho may retransmit an
# already queued, now-expired candidate as DUP without re-entering application
# validation.  QoS 0 plus recovery re-publication keeps the DB authoritative.
MQTT_CANDIDATE_QOS = 0

IDENTITY_CONFIG_PATH = Path(
    os.environ.get(
        "CCTV_IDENTITY_CONFIG",
        PROJECT_ROOT / "configs" / "identity.yaml",
    )
).expanduser()

JOURNEY_VALIDATION_CONFIG_PATH = Path(
    os.environ.get(
        "CCTV_JOURNEY_VALIDATION_CONFIG",
        PROJECT_ROOT / "configs" / "journey_validation.yaml",
    )
).expanduser()

CAPTURE_CACHE_CONFIG_PATH = Path(
    os.environ.get(
        "CCTV_CAPTURE_CACHE_CONFIG",
        PROJECT_ROOT / "configs" / "capture_cache.yaml",
    )
).expanduser()

D_ARRIVAL_RX_LOG_DIR = Path(
    os.environ.get(
        "CCTV_D_ARRIVAL_RX_LOG_DIR",
        PROJECT_ROOT / "data" / "logs",
    )
).expanduser()
_d_arrival_rx_log_lock = threading.Lock()


def load_capture_cache_settings():
    document: dict[str, Any] = {}
    if CAPTURE_CACHE_CONFIG_PATH.exists():
        loaded = yaml.safe_load(
            CAPTURE_CACHE_CONFIG_PATH.read_text(encoding="utf-8")
        )
        if loaded is not None and not isinstance(loaded, dict):
            raise ValueError(
                "Capture cache config must be a mapping: "
                f"{CAPTURE_CACHE_CONFIG_PATH}"
            )
        document = loaded or {}
    return settings_from_document(document, PROJECT_ROOT)


CAPTURE_CACHE_SETTINGS = load_capture_cache_settings()


def load_identity_settings() -> dict[str, Any]:
    if not IDENTITY_CONFIG_PATH.exists():
        return {}
    document = yaml.safe_load(
        IDENTITY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    if document is None:
        return {}
    if not isinstance(document, dict):
        raise ValueError(
            f"Identity config must be a mapping: {IDENTITY_CONFIG_PATH}"
        )
    return document


IDENTITY_SETTINGS = load_identity_settings()


def load_journey_validation_settings() -> dict[str, Any]:
    document = yaml.safe_load(
        JOURNEY_VALIDATION_CONFIG_PATH.read_text(encoding="utf-8")
    )
    if not isinstance(document, dict):
        raise ValueError(
            "Journey validation config must be a mapping: "
            f"{JOURNEY_VALIDATION_CONFIG_PATH}"
        )
    values = document.get("d_arrival", {})
    if not isinstance(values, dict):
        raise ValueError("journey_validation.d_arrival must be a mapping")
    return values


D_ARRIVAL_SETTINGS = load_journey_validation_settings()


def identity_setting(section: str, key: str, default: Any) -> Any:
    values = IDENTITY_SETTINGS.get(section, {})
    if not isinstance(values, dict):
        return default
    return values.get(key, default)


# B 경로는 항상 유지하고, C 병렬 후보 발행/구독은 topology 설정으로
# 활성화한다. 어느 middle node가 먼저 passage를 확정하든 D로 진행한다.
ENABLE_CAMERA_C = bool(
    identity_setting("topology", "camera_c_enabled", False)
)


# Camera → Main
TOPIC_A_ENTRY = "cctv/events/a/entry"
TOPIC_B_PASSAGE = "cctv/events/b/passage"
TOPIC_C_PASSAGE = "cctv/events/c/passage"
TOPIC_D_ARRIVAL = "cctv/events/d/arrival"
TOPIC_D_DETECTION = "cctv/events/d/detection"
TOPIC_A_TIMING = "cctv/events/a/timing"
TOPIC_B_TIMING = "cctv/events/b/timing"
TOPIC_C_TIMING = "cctv/events/c/timing"
TOPIC_D_TIMING = "cctv/events/d/timing"
TIMING_TOPIC_NODES = {
    TOPIC_A_TIMING: "A",
    TOPIC_B_TIMING: "B",
    TOPIC_C_TIMING: "C",
    TOPIC_D_TIMING: "D",
}


# Main → Camera
TOPIC_A_ENTRY_RESPONSE = "cctv/responses/a/entry"

TOPIC_CANDIDATE_B = "cctv/candidates/b"
TOPIC_CANDIDATE_C = "cctv/candidates/c"
TOPIC_CANDIDATE_D = "cctv/candidates/d"

TOPIC_JOURNEY_COMPLETED = "cctv/main/journey/completed"
TOPIC_D_JOURNEY_CONTROL = "cctv/control/d/journey"
TOPIC_B_JOURNEY_CONTROL = "cctv/control/b/journey"
TOPIC_C_JOURNEY_CONTROL = "cctv/control/c/journey"
JOURNEY_CONTROL_TOPICS = {
    "B": TOPIC_B_JOURNEY_CONTROL,
    "C": TOPIC_C_JOURNEY_CONTROL,
    "D": TOPIC_D_JOURNEY_CONTROL,
}


# Person UID 판정 기준
#
# 단일 최고점 1개만 보고 동일인 처리하지 않는다.
# BEST + TOP-K 평균 + 종합점수 + 1/2등 Person 간 Margin을 함께 사용한다.
PERSON_MATCH_THRESHOLD = float(
    identity_setting("matching", "best_match_threshold", 0.75)
)
PERSON_TOPK_THRESHOLD = float(
    identity_setting("matching", "topk_match_threshold", 0.68)
)
PERSON_COMBINED_THRESHOLD = float(
    identity_setting("matching", "combined_match_threshold", 0.72)
)
PERSON_REVIEW_THRESHOLD = float(
    identity_setting("matching", "review_best_threshold", 0.72)
)
PERSON_REVIEW_COMBINED_THRESHOLD = float(
    identity_setting("matching", "review_combined_threshold", 0.70)
)
PERSON_MATCH_MARGIN = float(
    identity_setting("matching", "margin_threshold", 0.05)
)
AUTO_MATCH_THRESHOLD = PERSON_COMBINED_THRESHOLD
AUTO_NEW_THRESHOLD = PERSON_REVIEW_COMBINED_THRESHOLD
MARGIN_THRESHOLD = PERSON_MATCH_MARGIN
SINGLE_CANDIDATE_MARGIN_PASS = bool(
    identity_setting("matching", "single_candidate_margin_pass", True)
)

PERSON_TOPK = int(identity_setting("matching", "review_top_k", 3))
PERSON_BEST_WEIGHT = 0.45
PERSON_TOPK_WEIGHT = 0.55
BODY_SCORE_WEIGHT = float(
    identity_setting("matching", "body_weight", 0.8)
)
FACE_SCORE_WEIGHT = float(
    identity_setting("matching", "face_weight", 0.2)
)
MIN_CONSISTENT_BODY_FRAMES = int(
    identity_setting("matching", "min_consistent_body_frames", 2)
)
MIN_CONSISTENT_FACE_FRAMES = int(
    identity_setting("matching", "min_consistent_face_frames", 2)
)
AUTO_DECISION_MIN_BODY_QUALITY = float(
    identity_setting("matching", "auto_decision_min_body_quality", 0.80)
)
# Camera C PASSAGE final evidence is calibrated independently from the BODY
# quality used by A entry, identity, gallery promotion, and D processing.
C_PASSAGE_MIN_QUALITY = float(
    os.environ.get("CCTV_C_PASSAGE_MIN_QUALITY", "0.74")
)
AUTO_DECISION_MIN_FACE_QUALITY = float(
    identity_setting("matching", "auto_decision_min_face_quality", 0.70)
)
SIMILARITY_AGGREGATION = str(
    identity_setting("matching", "aggregation", "query_max_mean")
).strip().lower()

# 완료된 영구 Gallery 없이 현재 진행 중 A 특징만으로 매칭할 때는
# 오매칭을 줄이기 위해 더 높은 기준을 사용한다.
PERSON_ACTIVE_ONLY_THRESHOLD = 0.82

# RETURNING으로 추적은 허용하더라도 영구 Gallery에 새 특징을
# 추가하는 기준은 더 엄격하게 적용하여 Person Profile 오염을 막는다.
PERSON_GALLERY_PROMOTE_BEST_THRESHOLD = float(
    identity_setting("gallery", "promote_best_threshold", 0.80)
)
PERSON_GALLERY_PROMOTE_TOPK_THRESHOLD = float(
    identity_setting("gallery", "promote_topk_threshold", 0.72)
)
PERSON_GALLERY_PROMOTE_COMBINED_THRESHOLD = float(
    identity_setting("gallery", "promote_combined_threshold", 0.76)
)
GALLERY_MIN_SAMPLE_QUALITY = float(
    identity_setting("gallery", "min_sample_quality", 0.70)
)

# 같은 사람의 영구 Gallery 관리
GALLERY_DUPLICATE_THRESHOLD = float(
    identity_setting("gallery", "duplicate_similarity", 0.97)
)
MAX_PERSON_GALLERY = int(
    identity_setting("gallery", "max_samples_per_modality", 8)
)

# 미완료 Journey 자동 만료
# A에서 등록된 뒤 B/C로 가지 못했거나,
# B/C 통과 뒤 D에 도착하지 못한 Journey를 5분 후 EXPIRED 처리한다.
WAITING_B_OR_C_TIMEOUT_SECONDS = 300.0
WAITING_D_TIMEOUT_SECONDS = float(
    D_ARRIVAL_SETTINGS.get("waiting_d_ttl_seconds", 300.0)
)
JOURNEY_CLEANUP_INTERVAL_SECONDS = 30.0
MQTT_INGESTION_QUEUE_WARN_SIZE = int(
    os.environ.get("CCTV_MQTT_INGESTION_QUEUE_WARN_SIZE", "1024")
)

D_CLOCK_TOLERANCE_SECONDS = float(
    D_ARRIVAL_SETTINGS.get("clock_tolerance_seconds", 1.0)
)
D_MIN_TRAVEL_SECONDS = float(
    D_ARRIVAL_SETTINGS.get("minimum_travel_seconds", 1.0)
)
D_MAX_TRAVEL_SECONDS = float(
    D_ARRIVAL_SETTINGS.get("maximum_travel_seconds", 300.0)
)
D_MIN_CONFIRMATION_SAMPLES = int(
    D_ARRIVAL_SETTINGS.get("minimum_confirmation_samples", 3)
)
D_MIN_CONFIRMATION_PASSES = int(
    D_ARRIVAL_SETTINGS.get("minimum_confirmation_passes", 2)
)
D_MIN_JOURNEY_MARGIN = float(
    D_ARRIVAL_SETTINGS.get("minimum_journey_margin", 0.05)
)
D_ELIGIBLE_REASONS = {
    str(value).strip().upper()
    for value in D_ARRIVAL_SETTINGS.get(
        "eligible_reasons",
        [
            "ELIGIBLE",
            "ELIGIBLE_NEW_ENTRY",
            "CONFIRMED",
            "MULTIFRAME_CONFIRMED",
        ],
    )
}


def structured_log(event: str, **fields: Any) -> None:
    print(
        "[MAIN_EVENT] "
        + json.dumps(
            {"event": event, "timestamp": now_iso(), **fields},
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ),
        flush=True,
    )


def _first_payload_value(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        if payload.get(name) is not None:
            return payload[name]
    return None


def d_arrival_event_id(payload: dict[str, Any], raw_sha256: str) -> str:
    supplied = _first_payload_value(
        payload, "arrival_event_id", "event_id", "request_id"
    )
    if supplied is not None and str(supplied).strip():
        return str(supplied).strip()
    return f"DARR-{raw_sha256[:24]}"


def append_d_arrival_rx_jsonl(record: dict[str, Any], received_at: str) -> Path:
    """Append one exact MQTT D payload envelope outside the network callback."""
    received = datetime.fromisoformat(received_at)
    path = D_ARRIVAL_RX_LOG_DIR / (
        f"d_arrival_rx_{received.strftime('%Y%m%d')}.jsonl"
    )
    line = json.dumps(
        record, ensure_ascii=False, separators=(",", ":"), default=str
    )
    with _d_arrival_rx_log_lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(line + "\n")
            stream.flush()
    return path


_mqtt_connection_sequence = 0
_mqtt_connection_sequence_lock = threading.Lock()

BODY_EMBEDDING_DIM = int(
    identity_setting("embedding", "body_dim", 512)
)
FACE_EMBEDDING_DIM = int(
    identity_setting("embedding", "face_dim", 128)
)
EMBEDDING_DIM = BODY_EMBEDDING_DIM
MAX_A_SAMPLES_PER_MODALITY = int(
    identity_setting("embedding", "max_query_samples", 3)
)
MIN_EMBEDDING_QUALITY = float(
    identity_setting("embedding", "min_quality", 0.0)
)

# OpenCV SFace 공식 LFW cosine 기준. Body 점수와 가중 평균하지 않고
# Face가 독립적으로 강한 Person을 지지하는지 판정할 때만 사용한다.
FACE_MATCH_THRESHOLD = float(
    os.environ.get("CCTV_FACE_MATCH_THRESHOLD", "0.363")
)

# Some idempotent replay paths revalidate the Journey immediately before an
# MQTT publish while they still own the transaction serialization guard.  The
# lock must therefore be re-entrant; SQLite write transactions are explicitly
# committed before those replay publications below.
db_lock = threading.RLock()
INGESTION_COORDINATOR = IngestionCoordinator()


def clear_runtime_state() -> dict[str, list[str]]:
    # Identity galleries, Journey candidates, captures and idempotency state are
    # currently loaded from SQLite per operation; there is no materialized
    # process cache to retain across a database replacement. This hook keeps the
    # reset boundary explicit for future caches.
    return {"cleared_caches": []}


# ============================================================
# 공통 함수
# ============================================================

def now_iso() -> str:
    return datetime.now().astimezone().isoformat(
        timespec="seconds"
    )


def parse_iso_epoch(
    value: str | None,
) -> float | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value
        ).timestamp()
    except (TypeError, ValueError):
        return None


def normalize_embedding(
    embedding: list[float] | np.ndarray,
    expected_dim: int = BODY_EMBEDDING_DIM,
) -> np.ndarray:
    array = np.asarray(
        embedding,
        dtype=np.float32,
    ).reshape(-1)

    if array.size != expected_dim:
        raise ValueError(
            f"Embedding 크기 오류: "
            f"{array.size}, 예상값={expected_dim}"
        )

    if not np.all(np.isfinite(array)):
        raise ValueError(
            "Embedding에 NaN 또는 Inf가 있습니다."
        )

    norm = float(np.linalg.norm(array))

    if norm <= 1e-12:
        raise ValueError(
            "Embedding norm이 0입니다."
        )

    return array / norm


def embedding_to_blob(
    embedding: np.ndarray,
    expected_dim: int = BODY_EMBEDDING_DIM,
) -> bytes:
    return normalize_embedding(
        embedding,
        expected_dim,
    ).astype(np.float32).tobytes()


def blob_to_embedding(
    blob: bytes,
    expected_dim: int = BODY_EMBEDDING_DIM,
) -> np.ndarray:
    return normalize_embedding(
        np.frombuffer(
            blob,
            dtype=np.float32,
        ).copy(),
        expected_dim,
    )


def cosine_similarity(
    embedding_a: np.ndarray,
    embedding_b: np.ndarray,
) -> float:
    return float(
        np.dot(
            embedding_a,
            embedding_b,
        )
    )


def extract_single_embedding(
    payload: dict[str, Any],
) -> np.ndarray:
    raw_embedding = payload.get("embedding")

    if isinstance(raw_embedding, list):
        return normalize_embedding(
            raw_embedding
        )

    gallery = payload.get("gallery", [])

    if isinstance(gallery, list):
        for item in gallery:
            if not isinstance(item, dict):
                continue

            embedding = item.get("embedding")

            if isinstance(embedding, list):
                return normalize_embedding(
                    embedding
                )

    raise ValueError(
        "메시지에 사용할 수 있는 embedding이 없습니다."
    )


def _sample_value(
    values: Any,
    index: int,
    default: Any = None,
) -> Any:
    if isinstance(values, list) and index < len(values):
        return values[index]
    return default


def _parse_embedding_samples(
    *,
    embeddings: Any,
    expected_dim: int,
    declared_dim: Any,
    declared_count: Any = None,
    qualities: Any,
    confidences: Any,
    capture_paths: Any,
    frame_indices: Any = None,
    frontal_scores: Any = None,
    sharpness_values: Any = None,
    rejection_counts: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    rejected = rejection_counts if rejection_counts is not None else {}

    def reject(reason: str) -> None:
        rejected[reason] = rejected.get(reason, 0) + 1

    if not isinstance(embeddings, list):
        reject("NO_GALLERY")
        return []
    if not embeddings:
        reject("NO_GALLERY")
        return []

    try:
        parsed_dim = int(declared_dim)
    except (TypeError, ValueError):
        parsed_dim = expected_dim

    try:
        sample_limit = min(
            MAX_A_SAMPLES_PER_MODALITY,
            max(0, int(declared_count)),
        )
    except (TypeError, ValueError):
        sample_limit = MAX_A_SAMPLES_PER_MODALITY

    samples: list[dict[str, Any]] = []
    for index, raw_embedding in enumerate(
        embeddings[:sample_limit]
    ):
        if parsed_dim != expected_dim or not isinstance(
            raw_embedding,
            list,
        ):
            reject("INVALID_DIM")
            continue

        try:
            embedding = normalize_embedding(
                raw_embedding,
                expected_dim,
            )
        except (TypeError, ValueError) as error:
            message = str(error)
            if "NaN" in message or "Inf" in message:
                reject("NAN_OR_INF")
            elif "norm" in message:
                reject("ZERO_NORM")
            else:
                reject("INVALID_DIM")
            continue

        try:
            quality = float(
                _sample_value(qualities, index, 1.0)
            )
        except (TypeError, ValueError):
            quality = 1.0

        quality = max(0.0, min(1.0, quality))
        if quality < MIN_EMBEDDING_QUALITY:
            reject("BELOW_MIN_QUALITY")
            continue

        samples.append(
            {
                "embedding": embedding,
                "quality": quality,
                "confidence": _sample_value(
                    confidences,
                    index,
                ),
                "capture_path": _sample_value(
                    capture_paths,
                    index,
                ),
                "frame_index": _sample_value(
                    frame_indices,
                    index,
                ),
                "frontal_score": _sample_value(
                    frontal_scores,
                    index,
                ),
                "sharpness": _sample_value(
                    sharpness_values,
                    index,
                ),
            }
        )

    return samples


def parse_a_entry_samples(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Camera A의 legacy/new payload를 modality별 유효 sample로 정규화한다."""
    rejection_counts: dict[str, int] = {}
    raw_body_embeddings = payload.get("body_embeddings")
    if isinstance(raw_body_embeddings, list) and raw_body_embeddings:
        body_samples = _parse_embedding_samples(
            embeddings=raw_body_embeddings,
            expected_dim=BODY_EMBEDDING_DIM,
            declared_dim=payload.get("body_embedding_dim"),
            declared_count=payload.get("body_count"),
            qualities=payload.get("body_qualities"),
            confidences=payload.get("body_confidences"),
            frame_indices=payload.get("body_frame_indices"),
            capture_paths=payload.get("body_capture_paths"),
            rejection_counts=rejection_counts,
        )
    else:
        legacy_embedding = payload.get("embedding")
        body_samples = _parse_embedding_samples(
            embeddings=(
                [legacy_embedding]
                if isinstance(legacy_embedding, list)
                else []
            ),
            expected_dim=BODY_EMBEDDING_DIM,
            declared_dim=payload.get(
                "embedding_dim",
                BODY_EMBEDDING_DIM,
            ),
            qualities=[payload.get("quality", 1.0)],
            confidences=[payload.get("confidence")],
            frame_indices=[payload.get("frame_index")],
            capture_paths=[payload.get("capture_path")],
            rejection_counts=rejection_counts,
        )

    if not body_samples:
        raise ValueError(
            "A ENTRY에 유효한 512-D Body embedding이 없습니다. "
            f"rejections={rejection_counts}"
        )

    face_samples: list[dict[str, Any]] = []
    if bool(payload.get("face_available")):
        face_samples = _parse_embedding_samples(
            embeddings=payload.get("face_embeddings"),
            expected_dim=FACE_EMBEDDING_DIM,
            declared_dim=payload.get("face_embedding_dim"),
            qualities=payload.get("face_qualities"),
            confidences=payload.get("face_confidences"),
            frontal_scores=payload.get("face_frontal_scores"),
            sharpness_values=payload.get("face_sharpness"),
            capture_paths=payload.get("face_capture_paths"),
            rejection_counts=rejection_counts,
        )

    return {
        "body_samples": body_samples,
        "face_samples": face_samples,
        "rejection_counts": rejection_counts,
    }


def extract_local_track_id(
    payload: dict[str, Any],
) -> Any:
    for key in (
        "local_track_id",
        "track_id",
        "local_id",
        "id",
    ):
        if key in payload:
            return payload.get(key)

    return None


def safe_json_loads(
    value: str | None,
    default: Any,
) -> Any:
    if not value:
        return default

    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


# ============================================================
# SQLite DB
# ============================================================

class ClosingSqliteConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_value, traceback_value):
        try:
            return super().__exit__(exc_type, exc_value, traceback_value)
        finally:
            self.close()

def connect_db() -> sqlite3.Connection:
    connection = sqlite3.connect(
        DB_PATH,
        timeout=30,
        factory=ClosingSqliteConnection,
    )

    connection.row_factory = sqlite3.Row
    connection.execute(
        "PRAGMA foreign_keys = ON"
    )
    connection.execute(
        "PRAGMA journal_mode = WAL"
    )

    return connection


def table_columns(
    connection: sqlite3.Connection,
    table_name: str,
) -> set[str]:
    rows = connection.execute(
        f"PRAGMA table_info({table_name})"
    ).fetchall()

    return {
        str(row["name"])
        for row in rows
    }


def add_column_if_missing(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    if column_name in table_columns(
        connection,
        table_name,
    ):
        return

    connection.execute(
        f"ALTER TABLE {table_name} "
        f"ADD COLUMN {column_name} {column_sql}"
    )


def initialize_database(*, repair_legacy_rows: bool = False) -> None:
    DB_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with connect_db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS persons (
                person_uid TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                status TEXT NOT NULL,
                visit_count INTEGER NOT NULL DEFAULT 0,
                merged_into_person_uid TEXT
            );

            CREATE TABLE IF NOT EXISTS person_embeddings (
                embedding_id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_uid TEXT NOT NULL,
                node_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                quality REAL NOT NULL,
                modality TEXT NOT NULL DEFAULT 'BODY',
                embedding_dim INTEGER NOT NULL DEFAULT 512,
                embedding BLOB NOT NULL,

                FOREIGN KEY (person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE TABLE IF NOT EXISTS journeys (
                journey_id TEXT PRIMARY KEY,
                request_id TEXT,
                person_uid TEXT NOT NULL,
                visit_no INTEGER NOT NULL,
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
                identity_result TEXT NOT NULL DEFAULT 'UNKNOWN',
                review_status TEXT NOT NULL DEFAULT 'NOT_REQUIRED',
                canonical_person_uid TEXT,
                decision_reason TEXT,
                score_margin REAL,
                query_gallery_count INTEGER NOT NULL DEFAULT 0,
                candidate_pool_size INTEGER NOT NULL DEFAULT 0,

                FOREIGN KEY (person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE TABLE IF NOT EXISTS a_entry_requests (
                request_id TEXT PRIMARY KEY,
                journey_id TEXT NOT NULL,
                received_at TEXT NOT NULL,
                candidate_republish_allowed INTEGER NOT NULL DEFAULT 1,

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id)
            );

            CREATE TABLE IF NOT EXISTS journey_gallery (
                gallery_id INTEGER PRIMARY KEY AUTOINCREMENT,
                journey_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                quality REAL NOT NULL,
                modality TEXT NOT NULL DEFAULT 'BODY',
                embedding_dim INTEGER NOT NULL DEFAULT 512,
                embedding BLOB NOT NULL,

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id)
            );

            CREATE TABLE IF NOT EXISTS journey_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                journey_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id)
            );

            CREATE TABLE IF NOT EXISTS detection_events (
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

            CREATE INDEX IF NOT EXISTS idx_detection_events_at
            ON detection_events(event_at, event_id);

            CREATE TABLE IF NOT EXISTS d_arrival_attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key TEXT NOT NULL UNIQUE,
                journey_id TEXT,
                d_local_track_id TEXT,
                d_track_key TEXT,
                d_track_first_seen_at TEXT,
                arrival_at TEXT,
                received_at TEXT NOT NULL,
                accepted INTEGER NOT NULL DEFAULT 0,
                reason_code TEXT NOT NULL,
                reason_json TEXT NOT NULL DEFAULT '[]',
                payload_json TEXT NOT NULL,

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_d_arrival_accepted_track
            ON d_arrival_attempts(d_track_key)
            WHERE accepted = 1 AND d_track_key IS NOT NULL;

            CREATE INDEX IF NOT EXISTS
                idx_d_arrival_attempts_journey
            ON d_arrival_attempts(journey_id, attempt_id);

            CREATE TABLE IF NOT EXISTS journey_captures (
                capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
                journey_id TEXT NOT NULL,
                person_uid TEXT NOT NULL,
                node_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                image_path TEXT NOT NULL,
                similarity REAL,
                quality REAL NOT NULL DEFAULT 1.0,
                verification_status TEXT NOT NULL
                    DEFAULT 'AUTO_MATCHED',
                metadata_json TEXT NOT NULL DEFAULT '{}',

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id),

                FOREIGN KEY (person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE TABLE IF NOT EXISTS review_cases (
                review_id TEXT PRIMARY KEY,
                journey_id TEXT NOT NULL UNIQUE,
                provisional_person_uid TEXT NOT NULL,
                candidate_person_uid TEXT,
                initial_decision TEXT,
                initial_scores_json TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                action TEXT,
                target_person_uid TEXT,
                final_review_result TEXT,
                final_candidate_person_uid TEXT,
                canonical_person_uid TEXT,
                final_scores_json TEXT,
                route_json TEXT,
                resolution_source TEXT,
                final_reviewed_at TEXT,
                pending_person_created INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                resolved_at TEXT,

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id),

                FOREIGN KEY (provisional_person_uid)
                    REFERENCES persons(person_uid),

                FOREIGN KEY (candidate_person_uid)
                    REFERENCES persons(person_uid),

                FOREIGN KEY (target_person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE TABLE IF NOT EXISTS identity_review_candidates (
                candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT NOT NULL,
                journey_id TEXT NOT NULL,
                query_request_id TEXT,
                candidate_person_uid TEXT NOT NULL,
                rank INTEGER NOT NULL,
                body_similarity REAL,
                face_similarity REAL,
                fused_similarity REAL NOT NULL,
                score_margin REAL,
                query_capture_path TEXT,
                candidate_capture_path TEXT,
                candidate_last_seen_at TEXT,
                candidate_recent_route_json TEXT,
                created_at TEXT NOT NULL,

                UNIQUE (review_id, rank),
                UNIQUE (review_id, candidate_person_uid),

                FOREIGN KEY (review_id)
                    REFERENCES review_cases(review_id),
                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id),
                FOREIGN KEY (candidate_person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE TABLE IF NOT EXISTS identity_review_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT NOT NULL,
                action TEXT NOT NULL,
                selected_person_uid TEXT,
                decision_source TEXT NOT NULL,
                request_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,

                FOREIGN KEY (review_id)
                    REFERENCES review_cases(review_id)
            );

            CREATE TABLE IF NOT EXISTS journey_node_visits (
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

                PRIMARY KEY (journey_id, node_id),

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id),

                FOREIGN KEY (person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE TABLE IF NOT EXISTS captures (
                capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
                capture_key TEXT NOT NULL UNIQUE,
                request_id TEXT NOT NULL,
                journey_id TEXT NOT NULL,
                person_uid TEXT,
                camera_id TEXT NOT NULL,
                capture_type TEXT NOT NULL,
                source_url TEXT NOT NULL,
                stored_path TEXT,
                quality_score REAL,
                sha256 TEXT,
                mime_type TEXT,
                captured_at TEXT NOT NULL,
                cache_status TEXT NOT NULL DEFAULT 'PENDING',
                cache_error TEXT,
                created_at TEXT NOT NULL,

                FOREIGN KEY (journey_id)
                    REFERENCES journeys(journey_id),
                FOREIGN KEY (person_uid)
                    REFERENCES persons(person_uid)
            );

            CREATE INDEX IF NOT EXISTS
                idx_journeys_status
            ON journeys(status);

            CREATE INDEX IF NOT EXISTS
                idx_journeys_person_uid
            ON journeys(person_uid);

            CREATE INDEX IF NOT EXISTS
                idx_a_entry_requests_journey
            ON a_entry_requests(journey_id);

            CREATE INDEX IF NOT EXISTS
                idx_person_embeddings_uid
            ON person_embeddings(person_uid);

            CREATE INDEX IF NOT EXISTS
                idx_journey_gallery_id
            ON journey_gallery(journey_id);

            CREATE INDEX IF NOT EXISTS
                idx_journey_captures_journey
            ON journey_captures(journey_id);

            CREATE INDEX IF NOT EXISTS
                idx_captures_journey
            ON captures(journey_id, capture_id);

            CREATE INDEX IF NOT EXISTS
                idx_captures_person
            ON captures(person_uid, capture_id);

            CREATE INDEX IF NOT EXISTS
                idx_captures_request
            ON captures(request_id, capture_id);

            CREATE INDEX IF NOT EXISTS
                idx_review_cases_status
            ON review_cases(status);

            CREATE INDEX IF NOT EXISTS
                idx_review_cases_provisional
            ON review_cases(provisional_person_uid);

            CREATE INDEX IF NOT EXISTS
                idx_identity_review_candidates_review
            ON identity_review_candidates(review_id, rank);

            CREATE INDEX IF NOT EXISTS
                idx_identity_review_candidates_person
            ON identity_review_candidates(candidate_person_uid);

            CREATE INDEX IF NOT EXISTS
                idx_identity_review_audit_review
            ON identity_review_audit(review_id, created_at);

            CREATE INDEX IF NOT EXISTS
                idx_journey_node_visits_journey
            ON journey_node_visits(journey_id);

            CREATE INDEX IF NOT EXISTS
                idx_journey_node_visits_person
            ON journey_node_visits(person_uid);

            CREATE INDEX IF NOT EXISTS
                idx_journey_node_visits_node
            ON journey_node_visits(node_id);
            """
        )

        # 기존 DB로도 문법/실행 확인이 가능하도록 자동 보강
        add_column_if_missing(
            connection,
            "persons",
            "visit_count",
            "INTEGER NOT NULL DEFAULT 0",
        )
        add_column_if_missing(
            connection,
            "persons",
            "merged_into_person_uid",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "persons",
            "representative_capture_id",
            "INTEGER",
        )
        add_column_if_missing(
            connection,
            "persons",
            "representative_source",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "persons",
            "representative_updated_at",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "request_id",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "visit_no",
            "INTEGER",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "second_match_score",
            "REAL",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "person_status",
            "TEXT NOT NULL DEFAULT 'NEW'",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "candidate_person_uid",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "entry_local_track_id",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "person_best_score",
            "REAL",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "person_topk_score",
            "REAL",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "person_combined_score",
            "REAL",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "second_person_score",
            "REAL",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "match_source",
            "TEXT",
        )
        add_column_if_missing(
            connection,
            "journeys",
            "gallery_promotion_allowed",
            "INTEGER NOT NULL DEFAULT 0",
        )
        for column_name, column_sql in (
            ("identity_result", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
            ("review_status", "TEXT NOT NULL DEFAULT 'NOT_REQUIRED'"),
            ("canonical_person_uid", "TEXT"),
            ("decision_reason", "TEXT"),
            ("score_margin", "REAL"),
            ("query_gallery_count", "INTEGER NOT NULL DEFAULT 0"),
            ("candidate_pool_size", "INTEGER NOT NULL DEFAULT 0"),
        ):
            add_column_if_missing(
                connection,
                "journeys",
                column_name,
                column_sql,
            )
        add_column_if_missing(
            connection,
            "a_entry_requests",
            "candidate_republish_allowed",
            "INTEGER NOT NULL DEFAULT 1",
        )
        add_column_if_missing(
            connection,
            "person_embeddings",
            "modality",
            "TEXT NOT NULL DEFAULT 'BODY'",
        )
        add_column_if_missing(
            connection,
            "person_embeddings",
            "embedding_dim",
            "INTEGER NOT NULL DEFAULT 512",
        )
        add_column_if_missing(
            connection,
            "journey_gallery",
            "modality",
            "TEXT NOT NULL DEFAULT 'BODY'",
        )
        add_column_if_missing(
            connection,
            "journey_gallery",
            "embedding_dim",
            "INTEGER NOT NULL DEFAULT 512",
        )
        for column_name, column_sql in (
            ("initial_decision", "TEXT"),
            ("initial_scores_json", "TEXT"),
            ("final_review_result", "TEXT"),
            ("final_candidate_person_uid", "TEXT"),
            ("canonical_person_uid", "TEXT"),
            ("final_scores_json", "TEXT"),
            ("route_json", "TEXT"),
            ("resolution_source", "TEXT"),
            ("final_reviewed_at", "TEXT"),
            ("pending_person_created", "INTEGER NOT NULL DEFAULT 1"),
        ):
            add_column_if_missing(
                connection,
                "review_cases",
                column_name,
                column_sql,
            )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_person_embeddings_uid_modality
            ON person_embeddings(person_uid, modality)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_journey_gallery_id_modality
            ON journey_gallery(journey_id, modality)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_persons_merged_into
            ON persons(merged_into_person_uid)
            """
        )

        # Person은 canonical identity이고 Journey는 이동 세션이다. 같은
        # Person이 겹치는 이동 세션을 가질 수 있으므로 과거 1:1 제약을
        # 안전하게 제거한다. 테이블/행은 건드리지 않는다.
        connection.execute(
            "DROP INDEX IF EXISTS idx_one_active_journey_per_person"
        )

        if repair_legacy_rows:
            connection.execute(
                """
                UPDATE journeys
                SET identity_result = CASE
                        WHEN person_status = 'NEW' THEN 'NEW'
                        WHEN person_status IN ('RETURNING', 'REVISIT', 'MERGED')
                            THEN 'RETURNING'
                        ELSE 'UNKNOWN'
                    END
                WHERE identity_result IS NULL
                   OR identity_result NOT IN ('NEW', 'RETURNING', 'UNKNOWN')
                   OR (
                        identity_result = 'UNKNOWN'
                        AND person_status IN (
                            'NEW', 'RETURNING', 'REVISIT', 'MERGED'
                        )
                   )
                """
            )
            connection.execute(
                """
                UPDATE journeys
                SET canonical_person_uid = person_uid
                WHERE identity_result IN ('NEW', 'RETURNING')
                  AND canonical_person_uid IS NULL
                """
            )
            connection.execute(
                """
                UPDATE journeys
                SET review_status = CASE
                        WHEN EXISTS (
                            SELECT 1 FROM review_cases r
                            WHERE r.journey_id = journeys.journey_id
                              AND r.status = 'PENDING'
                        ) THEN 'PENDING'
                        WHEN EXISTS (
                            SELECT 1 FROM review_cases r
                            WHERE r.journey_id = journeys.journey_id
                              AND r.status = 'RESOLVED'
                        ) THEN 'RESOLVED'
                        ELSE 'NOT_REQUIRED'
                    END
                WHERE review_status IS NULL
                   OR review_status NOT IN ('NOT_REQUIRED', 'PENDING', 'RESOLVED')
                   OR (
                        review_status = 'NOT_REQUIRED'
                        AND EXISTS (
                            SELECT 1 FROM review_cases r
                            WHERE r.journey_id = journeys.journey_id
                        )
                   )
                """
            )

        # MQTT QoS 1 duplicate delivery가 새 Journey를 만들지 않도록
        # A request_id를 DB 수준에서도 유일하게 보장한다.
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_journeys_request_id
            ON journeys(request_id)
            WHERE request_id IS NOT NULL
            """
        )

        # 기존 Journey의 대표 request_id도 새 idempotency 매핑에
        # 보존한다. 이후 같은 사람의 새 request_id 재검출도 이
        # 테이블에 기록하므로 Journey 완료 후 재수신까지 방어한다.
        if repair_legacy_rows:
            connection.execute(
                """
                INSERT OR IGNORE INTO a_entry_requests (
                    request_id,
                    journey_id,
                    received_at
                )
                SELECT
                    request_id,
                    journey_id,
                    entry_at
                FROM journeys
                WHERE request_id IS NOT NULL
                """
            )

        # 1차 Multimodal 배포와 2차 Review migration 사이에 생성된
        # 미해결 REVIEW_REQUIRED Journey도 검토 대상에서 누락되지 않게
        # review_cases를 idempotent하게 보강한다.
        review_journeys = connection.execute(
            """
            SELECT
                journeys.journey_id,
                journeys.person_uid,
                journeys.candidate_person_uid,
                journeys.entry_at,
                CASE
                    WHEN persons.status IN (
                        'IDENTITY_PENDING', 'REVIEW_REQUIRED'
                    ) THEN 1 ELSE 0
                END AS pending_person_created
            FROM journeys
            JOIN persons
              ON persons.person_uid = journeys.person_uid
            LEFT JOIN review_cases
              ON review_cases.journey_id = journeys.journey_id
            WHERE journeys.person_status IN (
                    'IDENTITY_PENDING',
                    'REVIEW_REQUIRED'
                  )
              AND review_cases.review_id IS NULL
            ORDER BY journeys.entry_at, journeys.journey_id
            """
        ).fetchall() if repair_legacy_rows else []
        for journey in review_journeys:
            create_review_case(
                connection,
                str(journey["journey_id"]),
                str(journey["person_uid"]),
                journey["candidate_person_uid"],
                str(journey["entry_at"]),
                pending_person_created=bool(
                    journey["pending_person_created"]
                ),
            )

        connection.execute(
            f"PRAGMA user_version = {DATABASE_SCHEMA_VERSION}"
        )

    print(
        f"SQLite DB 준비 완료: {DB_PATH}"
    )


def generate_next_id(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    prefix: str,
) -> str:
    row = connection.execute(
        f"""
        SELECT MAX(
            CAST(
                SUBSTR({column_name}, 2)
                AS INTEGER
            )
        ) AS max_number
        FROM {table_name}
        """
    ).fetchone()

    max_number = (
        int(row["max_number"])
        if row["max_number"] is not None
        else 0
    )

    return f"{prefix}{max_number + 1:06d}"


def create_review_case(
    connection: sqlite3.Connection,
    journey_id: str,
    provisional_person_uid: str,
    candidate_person_uid: str | None,
    created_at: str,
    *,
    initial_decision: str | None = None,
    initial_scores: dict[str, Any] | None = None,
    route: list[str] | None = None,
    pending_person_created: bool = True,
) -> sqlite3.Row:
    """Journey당 하나의 영속 Review Case를 생성하거나 재사용한다."""
    existing = connection.execute(
        """
        SELECT *
        FROM review_cases
        WHERE journey_id = ?
        """,
        (journey_id,),
    ).fetchone()
    normalized_candidate = (
        str(candidate_person_uid)
        if candidate_person_uid is not None
        else None
    )
    if existing is not None:
        if (
            str(existing["provisional_person_uid"])
            != provisional_person_uid
            or existing["candidate_person_uid"]
            != normalized_candidate
        ):
            raise ValueError(
                "동일 Journey의 Review Case identity가 일치하지 않음: "
                f"{journey_id}"
            )
        return existing

    review_id = generate_next_id(
        connection,
        "review_cases",
        "review_id",
        "R",
    )
    connection.execute(
        """
        INSERT INTO review_cases (
            review_id,
            journey_id,
            provisional_person_uid,
            candidate_person_uid,
            initial_decision,
            initial_scores_json,
            status,
            action,
            target_person_uid,
            route_json,
            pending_person_created,
            created_at,
            resolved_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'PENDING', NULL, NULL, ?, ?, ?, NULL)
        """,
        (
            review_id,
            journey_id,
            provisional_person_uid,
            normalized_candidate,
            initial_decision,
            (
                json.dumps(
                    initial_scores,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                if initial_scores is not None
                else None
            ),
            (
                json.dumps(route, separators=(",", ":"))
                if route is not None
                else None
            ),
            int(pending_person_created),
            created_at,
        ),
    )
    created = connection.execute(
        """
        SELECT *
        FROM review_cases
        WHERE review_id = ?
        """,
        (review_id,),
    ).fetchone()
    if created is None:
        raise RuntimeError(f"Review Case 생성 실패: {journey_id}")
    return created


def save_identity_review_candidates(
    connection: sqlite3.Connection,
    review_id: str,
    journey_id: str,
    request_id: str | None,
    candidates: list[dict[str, Any]],
    query_capture_path: str | None,
    created_at: str,
) -> None:
    """Persist immutable Top-K evidence without promoting query embeddings."""
    if not candidates:
        return
    top1_score = float(candidates[0]["fused_similarity"])
    top2_score = (
        float(candidates[1]["fused_similarity"])
        if len(candidates) > 1
        else None
    )
    score_margin = (
        top1_score - top2_score if top2_score is not None else None
    )
    for rank, candidate in enumerate(candidates, start=1):
        person_uid = str(candidate["person_uid"])
        capture = connection.execute(
            """
            SELECT image_path
            FROM journey_captures
            WHERE person_uid = ?
            ORDER BY captured_at DESC, capture_id DESC
            LIMIT 1
            """,
            (person_uid,),
        ).fetchone()
        recent = connection.execute(
            """
            SELECT route_json
            FROM journeys
            WHERE person_uid = ? AND identity_result IN ('NEW', 'RETURNING')
            ORDER BY entry_at DESC, journey_id DESC
            LIMIT 1
            """,
            (person_uid,),
        ).fetchone()
        person = connection.execute(
            "SELECT last_seen_at FROM persons WHERE person_uid = ?",
            (person_uid,),
        ).fetchone()
        connection.execute(
            """
            INSERT OR REPLACE INTO identity_review_candidates (
                review_id, journey_id, query_request_id,
                candidate_person_uid, rank, body_similarity,
                face_similarity, fused_similarity, score_margin,
                query_capture_path, candidate_capture_path,
                candidate_last_seen_at, candidate_recent_route_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                review_id,
                journey_id,
                request_id,
                person_uid,
                rank,
                candidate.get("body_similarity"),
                candidate.get("face_similarity"),
                float(candidate["fused_similarity"]),
                score_margin,
                query_capture_path,
                capture["image_path"] if capture is not None else None,
                person["last_seen_at"] if person is not None else None,
                recent["route_json"] if recent is not None else None,
                created_at,
            ),
        )


def save_journey_event(
    connection: sqlite3.Connection,
    journey_id: str,
    node_id: str,
    event_type: str,
    event_at: str,
    payload: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO journey_events (
            journey_id,
            node_id,
            event_type,
            event_at,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            journey_id,
            node_id,
            event_type,
            event_at,
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        ),
    )


def save_journey_embedding(
    connection: sqlite3.Connection,
    journey_id: str,
    node_id: str,
    embedding: np.ndarray,
    captured_at: str,
    quality: float = 1.0,
    modality: str = "BODY",
) -> None:
    normalized_modality = str(modality).strip().upper()
    if normalized_modality not in {"BODY", "FACE"}:
        raise ValueError(f"지원하지 않는 modality: {modality}")
    embedding_dim = (
        BODY_EMBEDDING_DIM
        if normalized_modality == "BODY"
        else FACE_EMBEDDING_DIM
    )
    connection.execute(
        """
        INSERT INTO journey_gallery (
            journey_id,
            node_id,
            captured_at,
            quality,
            modality,
            embedding_dim,
            embedding
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            journey_id,
            node_id,
            captured_at,
            float(
                max(
                    0.0,
                    min(1.0, quality),
                )
            ),
            normalized_modality,
            embedding_dim,
            embedding_to_blob(
                embedding,
                embedding_dim,
            ),
        ),
    )


def save_capture_record_if_present(
    connection: sqlite3.Connection,
    journey_id: str,
    person_uid: str,
    node_id: str,
    captured_at: str,
    payload: dict[str, Any],
) -> bool:
    image_path = (
        payload.get("capture_path")
        or payload.get("image_path")
        or payload.get("crop_path")
    )

    if not isinstance(image_path, str):
        return False

    image_path = image_path.strip()
    if not image_path:
        return False

    similarity = payload.get("similarity")
    try:
        similarity = (
            float(similarity)
            if similarity is not None
            else None
        )
    except (TypeError, ValueError):
        similarity = None

    quality = payload.get("quality", 1.0)
    try:
        quality = float(quality)
    except (TypeError, ValueError):
        quality = 1.0
    quality = max(0.0, min(1.0, quality))

    verification_status = str(
        payload.get(
            "verification_status",
            "AUTO_MATCHED",
        )
    ).strip().upper()

    if verification_status not in {
        "AUTO_MATCHED",
        "VERIFIED",
        "REJECTED",
        "REVIEW_REQUIRED",
    }:
        verification_status = "AUTO_MATCHED"

    metadata = {
        "local_track_id": extract_local_track_id(
            payload
        ),
        "request_id": payload.get("request_id"),
    }

    connection.execute(
        """
        INSERT INTO journey_captures (
            journey_id,
            person_uid,
            node_id,
            captured_at,
            image_path,
            similarity,
            quality,
            verification_status,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            journey_id,
            person_uid,
            node_id,
            captured_at,
            image_path,
            similarity,
            quality,
            verification_status,
            json.dumps(
                metadata,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        ),
    )

    return True


def load_journey_gallery(
    connection: sqlite3.Connection,
    journey_id: str,
    modality: str | None = None,
) -> list[dict[str, Any]]:
    parameters: tuple[Any, ...]
    modality_clause = ""
    if modality is None:
        parameters = (journey_id,)
    else:
        modality_clause = " AND modality = ?"
        parameters = (journey_id, str(modality).strip().upper())

    rows = connection.execute(
        f"""
        SELECT
            node_id,
            captured_at,
            quality,
            modality,
            embedding_dim,
            embedding
        FROM journey_gallery
        WHERE journey_id = ?{modality_clause}
        ORDER BY gallery_id ASC
        """,
        parameters,
    ).fetchall()

    gallery = []

    for row in rows:
        embedding = blob_to_embedding(
            row["embedding"],
            int(row["embedding_dim"]),
        )

        gallery.append(
            {
                "node_id": row["node_id"],
                "captured_at": row["captured_at"],
                "quality": float(row["quality"]),
                "modality": row["modality"],
                "embedding_dim": int(row["embedding_dim"]),
                "embedding": embedding.tolist(),
            }
        )

    return gallery


def load_body_journey_gallery(
    connection: sqlite3.Connection,
    journey_id: str,
) -> list[dict[str, Any]]:
    """Edge Candidate에 허용된 BODY Gallery만 반환한다."""
    return load_journey_gallery(
        connection,
        journey_id,
        modality="BODY",
    )


# ============================================================
# 미완료 Journey 자동 만료
# ============================================================

def expire_stale_journeys(
    connection: sqlite3.Connection,
    client: mqtt.Client | None = None,
) -> tuple[int, int]:
    """
    오래된 WAITING Journey를 EXPIRED로 바꾼다.

    - WAITING_B_OR_C: entry_at 기준 5분
    - WAITING_D: passage_at 기준 5분
    - Journey/이벤트 기록은 남긴다.
    - EXPIRED 상태는 Person 매칭 후보와 재시작 복구에서 제외된다.
    """

    rows = connection.execute(
        """
        SELECT
            journey_id,
            person_uid,
            status,
            entry_at,
            passage_at,
            route_json
        FROM journeys
        WHERE status IN (
            'WAITING_B_OR_C',
            'WAITING_D'
        )
        """
    ).fetchall()

    current_epoch = datetime.now().astimezone().timestamp()
    expired_b_or_c = 0
    expired_d = 0

    for row in rows:
        status = str(row["status"])

        if status == "WAITING_B_OR_C":
            reference_at = row["entry_at"]
            timeout_seconds = (
                WAITING_B_OR_C_TIMEOUT_SECONDS
            )
        else:
            reference_at = (
                row["passage_at"]
                or row["entry_at"]
            )
            timeout_seconds = (
                WAITING_D_TIMEOUT_SECONDS
            )

        reference_epoch = parse_iso_epoch(
            reference_at
        )

        if reference_epoch is None:
            continue

        age_seconds = max(
            0.0,
            current_epoch - reference_epoch,
        )

        if age_seconds < timeout_seconds:
            continue

        expired_at = now_iso()
        journey_id = str(row["journey_id"])

        connection.execute(
            """
            UPDATE journeys
            SET status = 'EXPIRED'
            WHERE journey_id = ?
              AND status = ?
            """,
            (journey_id, status),
        )

        save_journey_event(
            connection,
            journey_id,
            "MAIN",
            "EXPIRED",
            expired_at,
            {
                "event": "EXPIRED",
                "journey_id": journey_id,
                "person_uid": row["person_uid"],
                "previous_status": status,
                "route": safe_json_loads(
                    row["route_json"],
                    [],
                ),
                "reference_at": reference_at,
                "expired_at": expired_at,
                "age_seconds": round(
                    age_seconds,
                    3,
                ),
                "timeout_seconds": timeout_seconds,
                "reason": "TIMEOUT",
            },
        )

        if status == "WAITING_B_OR_C":
            expired_b_or_c += 1
        else:
            expired_d += 1

        print()
        print("===== MAIN: Journey 자동 만료 =====")
        print(f"Journey ID : {journey_id}")
        print(f"Person UID  : {row['person_uid']}")
        print(f"이전 상태   : {status}")
        print("새 상태     : EXPIRED")
        print(f"경과 시간   : {age_seconds:.1f} sec")
        print(f"만료 기준   : {timeout_seconds:.0f} sec")
        print("==================================")
        if client is not None:
            publish_journey_invalidation(
                client,
                journey_id,
                "EXPIRED",
                journey_status="EXPIRED",
                reason_codes=[f"{status}_TTL_EXCEEDED"],
            )

    return expired_b_or_c, expired_d


def expire_stale_journeys_once() -> tuple[int, int]:
    with db_lock:
        with connect_db() as connection:
            return expire_stale_journeys(
                connection
            )


def journey_cleanup_loop(
    stop_event: threading.Event,
    client: mqtt.Client | None = None,
) -> None:
    while not stop_event.wait(
        JOURNEY_CLEANUP_INTERVAL_SECONDS
    ):
        try:
            with INGESTION_COORDINATOR.work():
                with db_lock:
                    with connect_db() as connection:
                        expire_stale_journeys(connection, client)
        except Exception as error:
            print(
                f"[MAIN] Journey 자동 만료 오류: "
                f"{error}"
            )


# ============================================================
# Person UID 판정
# ============================================================

def find_existing_person(
    connection: sqlite3.Connection,
    embeddings: np.ndarray | list[np.ndarray],
    modality: str = "BODY",
    *,
    permanent_only: bool = False,
    exclude_person_uids: set[str] | None = None,
) -> dict[str, Any]:
    """Compare every valid query sample with each canonical Person gallery.

    Aggregation is intentionally query-balanced: each query sample contributes
    its best candidate-gallery score, then those per-query scores are averaged
    (or median-aggregated by configuration). A large candidate gallery therefore
    cannot win merely by producing more pairwise scores.
    """
    exclusion_counts = {
        reason: 0
        for reason in (
            "INVALID_DIM",
            "ZERO_NORM",
            "NAN_OR_INF",
            "NO_GALLERY",
            "STATE_FILTERED",
            "TIME_FILTERED",
            "BELOW_MIN_QUALITY",
        )
    }
    if not permanent_only:
        expired_counts = expire_stale_journeys(connection)
        exclusion_counts["TIME_FILTERED"] = sum(expired_counts)

    normalized_modality = str(modality).strip().upper()
    expected_dim = (
        BODY_EMBEDDING_DIM
        if normalized_modality == "BODY"
        else FACE_EMBEDDING_DIM
    )
    raw_queries = (
        [embeddings]
        if isinstance(embeddings, np.ndarray)
        else list(embeddings)
    )
    query_embeddings: list[np.ndarray] = []
    for embedding in raw_queries:
        try:
            query_embeddings.append(
                normalize_embedding(embedding, expected_dim)
            )
        except (TypeError, ValueError) as error:
            message = str(error)
            reason = (
                "NAN_OR_INF"
                if "NaN" in message or "Inf" in message
                else "ZERO_NORM"
                if "norm" in message
                else "INVALID_DIM"
            )
            exclusion_counts[reason] += 1

    empty_result = {
        "ranking": [],
        "best_candidate": None,
        "second_combined_score": -1.0,
        "modality": normalized_modality,
        "query_gallery_count": len(query_embeddings),
        "candidate_pool_size": 0,
        "query_best_person_uids": [],
        "query_best_scores": [],
        "exclusion_counts": exclusion_counts,
        "aggregation": SIMILARITY_AGGREGATION,
    }
    if not query_embeddings:
        exclusion_counts["NO_GALLERY"] += 1
        return empty_result

    excluded_uids = exclude_person_uids or set()
    permanent_rows = connection.execute(
        """
        SELECT pe.person_uid, pe.embedding, pe.embedding_dim,
               pe.quality, p.status AS person_db_status,
               'PERMANENT' AS source
        FROM person_embeddings AS pe
        JOIN persons AS p ON p.person_uid = pe.person_uid
        WHERE p.status = 'ACTIVE' AND p.merged_into_person_uid IS NULL
          AND pe.modality = ?
        """,
        (normalized_modality,),
    ).fetchall()
    active_rows: list[sqlite3.Row] = []
    if not permanent_only:
        active_rows = connection.execute(
            """
            SELECT j.person_uid, g.embedding, g.embedding_dim, g.quality,
                   p.status AS person_db_status, 'ACTIVE_JOURNEY' AS source
            FROM journey_gallery AS g
            JOIN journeys AS j ON j.journey_id = g.journey_id
            JOIN persons AS p ON p.person_uid = j.person_uid
            WHERE j.status IN ('WAITING_B_OR_C', 'WAITING_D')
              AND j.identity_result IN ('NEW', 'RETURNING')
              AND j.review_status = 'NOT_REQUIRED'
              AND g.node_id = 'A' AND g.modality = ?
              AND p.status = 'ACTIVE' AND p.merged_into_person_uid IS NULL
            """,
            (normalized_modality,),
        ).fetchall()

    if not permanent_only:
        state_filtered = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM journey_gallery AS g
            JOIN journeys AS j ON j.journey_id = g.journey_id
            WHERE g.modality = ?
              AND (j.identity_result = 'UNKNOWN' OR j.review_status = 'PENDING')
            """,
            (normalized_modality,),
        ).fetchone()
        exclusion_counts["STATE_FILTERED"] = int(state_filtered["count"])

    grouped: dict[str, dict[str, Any]] = {}
    for row in [*permanent_rows, *active_rows]:
        person_uid = str(row["person_uid"])
        if person_uid in excluded_uids:
            exclusion_counts["STATE_FILTERED"] += 1
            continue
        if float(row["quality"]) < max(
            MIN_EMBEDDING_QUALITY,
            GALLERY_MIN_SAMPLE_QUALITY,
        ):
            exclusion_counts["BELOW_MIN_QUALITY"] += 1
            continue
        try:
            stored = blob_to_embedding(
                row["embedding"], int(row["embedding_dim"])
            )
        except (TypeError, ValueError) as error:
            message = str(error)
            reason = (
                "NAN_OR_INF"
                if "NaN" in message or "Inf" in message
                else "ZERO_NORM"
                if "norm" in message
                else "INVALID_DIM"
            )
            exclusion_counts[reason] += 1
            continue
        item = grouped.setdefault(
            person_uid,
            {
                "gallery": [],
                "sources": set(),
                "permanent_count": 0,
                "active_count": 0,
                "person_db_status": row["person_db_status"],
            },
        )
        item["gallery"].append(stored)
        source = str(row["source"])
        item["sources"].add(source)
        if source == "PERMANENT":
            item["permanent_count"] += 1
        else:
            item["active_count"] += 1

    ranking: list[dict[str, Any]] = []
    for person_uid, item in grouped.items():
        matrix = np.asarray(
            [
                [cosine_similarity(query, stored) for stored in item["gallery"]]
                for query in query_embeddings
            ],
            dtype=np.float32,
        )
        per_query_best = matrix.max(axis=1)
        flattened = matrix.reshape(-1)
        top_count = min(PERSON_TOPK, int(flattened.size))
        topk_mean = float(
            np.mean(np.sort(flattened)[::-1][:top_count])
        )
        if SIMILARITY_AGGREGATION == "query_max_median":
            topk_mean = float(np.median(per_query_best))
            aggregate = (
                PERSON_BEST_WEIGHT * float(matrix.max())
                + PERSON_TOPK_WEIGHT * topk_mean
            )
        elif SIMILARITY_AGGREGATION == "top2_mean":
            top = np.sort(matrix.reshape(-1))[::-1][:2]
            topk_mean = float(np.mean(top))
            aggregate = (
                PERSON_BEST_WEIGHT * float(matrix.max())
                + PERSON_TOPK_WEIGHT * topk_mean
            )
        elif SIMILARITY_AGGREGATION == "query_max_mean":
            topk_mean = float(np.mean(per_query_best))
            aggregate = (
                PERSON_BEST_WEIGHT * float(matrix.max())
                + PERSON_TOPK_WEIGHT * topk_mean
            )
        else:
            aggregate = (
                PERSON_BEST_WEIGHT * float(matrix.max())
                + PERSON_TOPK_WEIGHT * topk_mean
            )
        best_score = float(matrix.max())
        sources = item["sources"]
        match_source = (
            "PERMANENT+ACTIVE_JOURNEY"
            if len(sources) > 1
            else next(iter(sources))
        )
        ranking.append(
            {
                "person_uid": person_uid,
                "best_score": best_score,
                "topk_mean": topk_mean,
                "combined_score": aggregate,
                "query_min_score": float(per_query_best.min()),
                "query_mean_score": float(per_query_best.mean()),
                "per_query_best_scores": [
                    float(value) for value in per_query_best
                ],
                "sample_count": int(matrix.size),
                "query_count": len(query_embeddings),
                "gallery_count": len(item["gallery"]),
                "permanent_count": int(item["permanent_count"]),
                "active_count": int(item["active_count"]),
                "match_source": match_source,
                "person_db_status": item["person_db_status"],
            }
        )

    ranking.sort(
        key=lambda item: (item["combined_score"], item["best_score"]),
        reverse=True,
    )
    if not ranking:
        exclusion_counts["NO_GALLERY"] += 1
        return empty_result
    second_score = (
        float(ranking[1]["combined_score"])
        if len(ranking) > 1
        else -1.0
    )
    query_best_person_uids: list[str] = []
    query_best_scores: list[float] = []
    for query_index in range(len(query_embeddings)):
        query_winner = max(
            ranking,
            key=lambda item: item["per_query_best_scores"][query_index],
        )
        query_best_person_uids.append(str(query_winner["person_uid"]))
        query_best_scores.append(
            float(query_winner["per_query_best_scores"][query_index])
        )
    return {
        "ranking": ranking,
        "best_candidate": ranking[0],
        "second_combined_score": second_score,
        "modality": normalized_modality,
        "query_gallery_count": len(query_embeddings),
        "candidate_pool_size": len(ranking),
        "query_best_person_uids": query_best_person_uids,
        "query_best_scores": query_best_scores,
        "exclusion_counts": exclusion_counts,
        "aggregation": SIMILARITY_AGGREGATION,
    }


def create_new_person(
    connection: sqlite3.Connection,
    timestamp: str,
    status: str,
) -> str:
    person_uid = generate_next_id(
        connection,
        "persons",
        "person_uid",
        "P",
    )

    connection.execute(
        """
        INSERT INTO persons (
            person_uid,
            created_at,
            last_seen_at,
            status,
            visit_count
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            person_uid,
            timestamp,
            timestamp,
            status,
            0,
        ),
    )

    return person_uid


def print_person_match_analysis(
    match_result: dict[str, Any],
    decision: str,
    selected_person_uid: str,
    promotion_allowed: bool,
) -> None:
    ranking = match_result.get(
        "ranking",
        [],
    )

    print()
    print(
        "===== MAIN: PERSON MATCH 분석 ====="
    )

    if not ranking:
        print(
            "등록된 Person Gallery 없음"
        )
    else:
        for index, item in enumerate(
            ranking[:3],
            start=1,
        ):
            print(
                f"#{index} "
                f"{item['person_uid']} | "
                f"BEST={item['best_score']:.4f} | "
                f"TOP{min(PERSON_TOPK, item['sample_count'])}"
                f"={item['topk_mean']:.4f} | "
                f"COMBINED={item['combined_score']:.4f} | "
                f"PERM={item['permanent_count']} | "
                f"ACTIVE={item['active_count']}"
            )

        second_score = match_result.get(
            "second_combined_score",
            -1.0,
        )

        best = ranking[0]

        if second_score >= 0:
            margin = (
                best["combined_score"]
                - second_score
            )
            print(
                f"1/2등 Margin : {margin:.4f}"
            )
        else:
            print(
                "1/2등 Margin : 비교 대상 없음"
            )

    print(
        f"Decision     : {decision}"
    )
    print(
        f"Selected UID : {selected_person_uid}"
    )
    print(
        "Gallery 추가 : "
        + (
            "ALLOW"
            if promotion_allowed
            else "BLOCK"
        )
    )
    print(
        "=================================="
    )


def print_identity_decision(
    request_id: str | None,
    journey_id: str,
    result: dict[str, Any],
) -> None:
    candidates = result.get("review_candidates", [])
    top1 = candidates[0] if candidates else None
    top2 = candidates[1] if len(candidates) > 1 else None
    exclusions = result.get("exclusion_counts", {})
    print()
    print("===== MAIN IDENTITY DECISION =====")
    print(f"Request ID          : {request_id}")
    print(f"Journey ID          : {journey_id}")
    print(f"Query Gallery Count : {result.get('query_gallery_count', 0)}")
    print(f"Candidate Pool Size : {result.get('candidate_pool_size', 0)}")
    print(
        "Top-1 Person UID    : "
        f"{top1.get('person_uid') if top1 else None}"
    )
    print(
        "Top-1 Body Score    : "
        f"{top1.get('body_similarity') if top1 else None}"
    )
    print(
        "Top-1 Face Score    : "
        f"{top1.get('face_similarity') if top1 else None}"
    )
    print(
        "Top-1 Fused Score   : "
        f"{top1.get('fused_similarity') if top1 else None}"
    )
    print(
        "Top-2 Person UID    : "
        f"{top2.get('person_uid') if top2 else None}"
    )
    print(
        "Top-2 Fused Score   : "
        f"{top2.get('fused_similarity') if top2 else None}"
    )
    print(f"Score Margin        : {result.get('match_margin')}")
    print(
        "BODY Thresholds    : "
        f"best>={PERSON_MATCH_THRESHOLD}, "
        f"topk>={PERSON_TOPK_THRESHOLD}, "
        f"combined>={PERSON_COMBINED_THRESHOLD}"
    )
    print(
        "BODY Review        : "
        f"best>={PERSON_REVIEW_THRESHOLD} or "
        f"combined>={PERSON_REVIEW_COMBINED_THRESHOLD}"
    )
    print(
        "FACE Threshold     : "
        f"cosine>={FACE_MATCH_THRESHOLD}"
    )
    print(
        "Evidence Minimums  : "
        f"body={MIN_CONSISTENT_BODY_FRAMES}, "
        f"face={MIN_CONSISTENT_FACE_FRAMES}"
    )
    print(f"New Threshold       : {AUTO_NEW_THRESHOLD}")
    print(f"Margin Threshold    : {MARGIN_THRESHOLD}")
    print(f"Decision            : {result.get('person_status')}")
    print(f"Decision Reason     : {result.get('decision_reason')}")
    print(f"Assigned Person UID : {result.get('assigned_person_uid')}")
    print(
        "Excluded Candidates : "
        + ", ".join(
            f"{reason}={int(exclusions.get(reason, 0))}"
            for reason in (
                "INVALID_DIM",
                "ZERO_NORM",
                "NAN_OR_INF",
                "NO_GALLERY",
                "STATE_FILTERED",
                "TIME_FILTERED",
                "BELOW_MIN_QUALITY",
            )
        )
    )
    print("==============================")


def _strong_face_candidate(
    match_result: dict[str, Any],
) -> dict[str, Any] | None:
    best = match_result.get("best_candidate")
    if best is None:
        return None
    second_score = float(
        match_result.get("second_combined_score", -1.0)
    )
    margin_ok = (
        second_score < 0
        or float(best["combined_score"]) - second_score
        >= PERSON_MATCH_MARGIN
    )
    if (
        float(best["best_score"]) >= FACE_MATCH_THRESHOLD
        and margin_ok
    ):
        return best
    return None


def _qualified_evidence_count(
    embeddings: np.ndarray | list[np.ndarray] | None,
    qualities: list[float] | None,
    minimum_quality: float,
) -> int:
    if embeddings is None:
        return 0
    count = 1 if isinstance(embeddings, np.ndarray) else len(embeddings)
    if qualities is None:
        return count
    return sum(
        1
        for index in range(count)
        if index < len(qualities) and float(qualities[index]) >= minimum_quality
    )


def _candidate_margin_ok(match_result: dict[str, Any]) -> bool:
    best = match_result.get("best_candidate")
    if best is None:
        return False
    second_score = float(match_result.get("second_combined_score", -1.0))
    if second_score < 0:
        return SINGLE_CANDIDATE_MARGIN_PASS
    return (
        float(best["combined_score"]) - second_score
        >= PERSON_MATCH_MARGIN
    )


def _body_promotion_allowed(match_result: dict[str, Any]) -> bool:
    best = match_result.get("best_candidate")
    if best is None:
        return False
    return bool(
        float(best["best_score"]) >= PERSON_GALLERY_PROMOTE_BEST_THRESHOLD
        and float(best["topk_mean"]) >= PERSON_GALLERY_PROMOTE_TOPK_THRESHOLD
        and float(best["combined_score"])
        >= PERSON_GALLERY_PROMOTE_COMBINED_THRESHOLD
        and _candidate_margin_ok(match_result)
    )


def resolve_person_uid(
    connection: sqlite3.Connection,
    body_embeddings: np.ndarray | list[np.ndarray],
    timestamp: str,
    face_embeddings: list[np.ndarray] | None = None,
    *,
    body_qualities: list[float] | None = None,
    face_qualities: list[float] | None = None,
) -> dict[str, Any]:
    body_result = find_existing_person(connection, body_embeddings, "BODY")
    face_result = (
        find_existing_person(connection, face_embeddings, "FACE")
        if face_embeddings
        else {
            "ranking": [],
            "best_candidate": None,
            "second_combined_score": -1.0,
            "modality": "FACE",
            "query_gallery_count": 0,
            "candidate_pool_size": 0,
            "exclusion_counts": {},
        }
    )

    body_by_uid = {
        str(item["person_uid"]): item for item in body_result["ranking"]
    }
    face_by_uid = {
        str(item["person_uid"]): item for item in face_result["ranking"]
    }
    fused_ranking: list[dict[str, Any]] = []
    for person_uid in set(body_by_uid) | set(face_by_uid):
        body = body_by_uid.get(person_uid)
        face = face_by_uid.get(person_uid)
        body_score = (
            float(body["combined_score"]) if body is not None else None
        )
        face_score = (
            float(face["combined_score"]) if face is not None else None
        )
        if body_score is not None and face_score is not None:
            # OSNet BODY와 SFace cosine은 서로 다른 calibrated scale이다.
            # 수치 가중합으로 BODY threshold를 우회하지 않고 FACE는 독립적인
            # corroboration/conflict evidence로만 사용한다.
            fused_score = body_score
            match_source = "BODY+FACE"
        elif body_score is not None:
            fused_score = body_score
            match_source = "BODY"
        else:
            fused_score = float(face_score)
            match_source = "FACE"
        fused_ranking.append(
            {
                "person_uid": person_uid,
                "body_similarity": body_score,
                "face_similarity": face_score,
                "fused_similarity": float(fused_score),
                "body": body,
                "face": face,
                "match_source": match_source,
            }
        )
    fused_ranking.sort(
        key=lambda item: item["fused_similarity"], reverse=True
    )

    best_fused = fused_ranking[0] if fused_ranking else None
    second_fused = (
        float(fused_ranking[1]["fused_similarity"])
        if len(fused_ranking) > 1
        else -1.0
    )
    face_best = face_result.get("best_candidate")
    body_best = body_result.get("best_candidate")
    strong_face = _strong_face_candidate(face_result)
    qualified_body_count = _qualified_evidence_count(
        body_embeddings,
        body_qualities,
        AUTO_DECISION_MIN_BODY_QUALITY,
    )
    qualified_face_count = _qualified_evidence_count(
        face_embeddings,
        face_qualities,
        AUTO_DECISION_MIN_FACE_QUALITY,
    )
    body_frame_candidate_uids = [
        str(value)
        for value in body_result.get("query_best_person_uids", [])
    ]
    body_frame_candidate_conflict = bool(
        len(set(body_frame_candidate_uids)) > 1
    )
    body_top_candidate_frame_scores = (
        [
            float(value)
            for value in body_best.get("per_query_best_scores", [])
        ]
        if body_best is not None
        else []
    )
    body_consistent_match_count = sum(
        1
        for index, score in enumerate(body_top_candidate_frame_scores)
        if score >= PERSON_REVIEW_COMBINED_THRESHOLD
        and (
            body_qualities is None
            or (
                index < len(body_qualities)
                and float(body_qualities[index])
                >= AUTO_DECISION_MIN_BODY_QUALITY
            )
        )
    )
    face_details = {
        "face_candidate_person_uid": (
            face_best["person_uid"] if face_best is not None else None
        ),
        "face_best_score": (
            float(face_best["best_score"]) if face_best is not None else None
        ),
        "face_topk_score": (
            float(face_best["topk_mean"]) if face_best is not None else None
        ),
        "face_combined_score": (
            float(face_best["combined_score"])
            if face_best is not None
            else None
        ),
        "face_match_source": (
            face_best["match_source"] if face_best is not None else None
        ),
        "qualified_body_count": qualified_body_count,
        "qualified_face_count": qualified_face_count,
        "body_frame_candidate_person_uids": body_frame_candidate_uids,
        "body_top_candidate_frame_scores": body_top_candidate_frame_scores,
        "body_consistent_match_count": body_consistent_match_count,
        "body_frame_candidate_conflict": body_frame_candidate_conflict,
    }

    def common_result(
        *,
        person_uid: str,
        person_status: str,
        identity_result: str,
        review_status: str,
        decision_reason: str,
        promotion_allowed: bool,
        row: sqlite3.Row | None = None,
    ) -> dict[str, Any]:
        body = best_fused.get("body") if best_fused is not None else None
        best_score = float(body["best_score"]) if body is not None else -1.0
        topk_score = (
            float(body["topk_mean"]) if body is not None else -1.0
        )
        return {
            "person_uid": person_uid,
            "assigned_person_uid": (
                person_uid if identity_result != "UNKNOWN" else None
            ),
            "person_status": person_status,
            "identity_result": identity_result,
            "review_status": review_status,
            "decision_reason": decision_reason,
            "candidate_person_uid": (
                str(best_fused["person_uid"])
                if best_fused is not None
                else None
            ),
            "best_score": best_score,
            "topk_score": topk_score,
            "combined_score": (
                float(best_fused["fused_similarity"])
                if best_fused is not None
                else -1.0
            ),
            "body_similarity": (
                best_fused.get("body_similarity")
                if best_fused is not None
                else None
            ),
            "second_score": second_fused,
            "match_margin": (
                float(best_fused["fused_similarity"]) - second_fused
                if best_fused is not None and second_fused >= 0
                else None
            ),
            "match_source": (
                best_fused["match_source"]
                if best_fused is not None
                else None
            ),
            "visit_count": int(row["visit_count"]) if row is not None else 0,
            "previous_last_seen_at": (
                row["last_seen_at"] if row is not None else None
            ),
            "gallery_promotion_allowed": promotion_allowed,
            "review_candidates": fused_ranking[:PERSON_TOPK],
            "query_gallery_count": int(
                body_result.get("query_gallery_count", 0)
            ),
            "candidate_pool_size": len(fused_ranking),
            "exclusion_counts": body_result.get("exclusion_counts", {}),
            "pending_person_created": False,
            **face_details,
        }

    if best_fused is None:
        person_uid = create_new_person(connection, timestamp, "ACTIVE")
        return common_result(
            person_uid=person_uid,
            person_status="NEW",
            identity_result="NEW",
            review_status="NOT_REQUIRED",
            decision_reason="NO_EXISTING_GALLERY",
            promotion_allowed=True,
        )

    best_uid = str(best_fused["person_uid"])
    top1 = float(best_fused["fused_similarity"])
    match_margin = top1 - second_fused if second_fused >= 0 else None
    margin_ok = (
        SINGLE_CANDIDATE_MARGIN_PASS
        if second_fused < 0
        else bool(match_margin is not None and match_margin >= MARGIN_THRESHOLD)
    )
    body_best_uid = str(body_best["person_uid"]) if body_best is not None else None
    face_best_uid = (
        str(strong_face["person_uid"])
        if strong_face is not None
        else None
    )
    modalities_conflict = (
        body_best_uid is not None
        and face_best_uid is not None
        and body_best_uid != face_best_uid
    )
    person_row = connection.execute(
        "SELECT visit_count, last_seen_at FROM persons WHERE person_uid = ?",
        (best_uid,),
    ).fetchone()

    body_returning, body_review_candidate = _body_identity_flags(body_result)
    body_multiframe_consistent = bool(
        body_best is not None
        and qualified_body_count >= MIN_CONSISTENT_BODY_FRAMES
        and not body_frame_candidate_conflict
        and float(body_best.get("query_min_score", body_best["combined_score"]))
        >= PERSON_REVIEW_COMBINED_THRESHOLD
    )
    face_details["body_multiframe_consistent"] = body_multiframe_consistent
    face_consistent = bool(
        strong_face is not None
        and qualified_face_count >= MIN_CONSISTENT_FACE_FRAMES
    )
    face_supports_body = bool(
        face_consistent
        and body_best_uid is not None
        and str(strong_face["person_uid"]) == body_best_uid
    )
    body_auto_returning = bool(
        body_returning
        and body_best_uid == best_uid
        and (body_multiframe_consistent or face_supports_body)
    )
    face_auto_returning = bool(
        face_consistent
        and str(strong_face["person_uid"]) == best_uid
        and (body_best_uid is None or body_best_uid == best_uid)
    )

    if (
        (body_auto_returning or face_auto_returning)
        and margin_ok
        and not modalities_conflict
    ):
        promotion_allowed = bool(
            body_auto_returning
            and _body_promotion_allowed(body_result)
        )
        return common_result(
            person_uid=best_uid,
            person_status="RETURNING",
            identity_result="RETURNING",
            review_status="NOT_REQUIRED",
            decision_reason=(
                "AUTO_MATCH_BODY_THRESHOLDS_MARGIN_AND_CONSISTENCY"
                if body_auto_returning
                else "AUTO_MATCH_FACE_THRESHOLD_MARGIN_AND_CONSISTENCY"
            ),
            promotion_allowed=promotion_allowed,
            row=person_row,
        )

    face_has_identity_evidence = bool(
        face_best is not None
        and float(face_best["best_score"]) >= FACE_MATCH_THRESHOLD
    )
    if (
        top1 < AUTO_NEW_THRESHOLD
        and not modalities_conflict
        and not face_has_identity_evidence
        and (qualified_body_count > 0 or qualified_face_count > 0)
    ):
        person_uid = create_new_person(connection, timestamp, "ACTIVE")
        return common_result(
            person_uid=person_uid,
            person_status="NEW",
            identity_result="NEW",
            review_status="NOT_REQUIRED",
            decision_reason="TOP1_BELOW_AUTO_NEW_THRESHOLD",
            promotion_allowed=True,
        )

    reason = (
        "BODY_FACE_CANDIDATE_CONFLICT"
        if modalities_conflict
        else "BODY_FRAME_CANDIDATE_CONFLICT"
        if body_frame_candidate_conflict
        else "INSUFFICIENT_QUALITY"
        if qualified_body_count == 0 and qualified_face_count == 0
        else "INSUFFICIENT_MULTIFRAME_CONSISTENCY"
        if body_returning and not body_multiframe_consistent and not face_supports_body
        else "FACE_EVIDENCE_REQUIRES_REVIEW"
        if face_has_identity_evidence and top1 < AUTO_NEW_THRESHOLD
        else "INSUFFICIENT_MARGIN"
        if not margin_ok
        else "BODY_THRESHOLD_REQUIRES_REVIEW"
        if body_review_candidate
        else "AMBIGUOUS_THRESHOLD_BAND"
    )
    # Pending Journey는 기존 후보 UID를 transport 호환용으로 유지하지만
    # canonical assignment/visit count는 관리자 결정 전까지 하지 않는다.
    # 따라서 새 persons 행은 생성되지 않는다.
    return common_result(
        person_uid=best_uid,
        person_status="IDENTITY_PENDING",
        identity_result="UNKNOWN",
        review_status="PENDING",
        decision_reason=reason,
        promotion_allowed=False,
        row=person_row,
    )


# ============================================================
# MQTT 발행
# ============================================================

def publish_json(
    client: mqtt.Client,
    topic: str,
    payload: dict[str, Any],
    *,
    qos: int | None = None,
) -> None:
    serialized_payload = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    publish_qos = MQTT_QOS if qos is None else int(qos)
    result = client.publish(
        topic,
        serialized_payload,
        qos=publish_qos,
        retain=False,
    )

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(
            f"MQTT 발행 실패: "
            f"topic={topic}, rc={result.rc}"
        )

    print(
        "[MQTT TX ACCEPTED] "
        f"topic={topic}, mid={getattr(result, 'mid', None)}, "
        f"request_id={payload.get('request_id')}, qos={publish_qos}"
    )
    if topic == TOPIC_A_ENTRY_RESPONSE:
        print(f"[MQTT TX ENTRY_RESULT] {serialized_payload}")


def candidate_expiry_fields(
    reference_at: Any,
    ttl_seconds: float,
) -> dict[str, Any]:
    reference_epoch = parse_iso_epoch(reference_at)
    expires_at = None
    if reference_epoch is not None:
        expires_at = datetime.fromtimestamp(
            reference_epoch + ttl_seconds,
            tz=datetime.now().astimezone().tzinfo,
        ).isoformat(timespec="seconds")
    return {
        "candidate_ttl_seconds": float(ttl_seconds),
        "expires_at": expires_at,
    }


def publish_journey_invalidation(
    client: mqtt.Client,
    journey_id: str,
    terminal_status: str,
    *,
    journey_status: str,
    reason_codes: list[str] | None = None,
    target_nodes: tuple[str, ...] = ("B", "C", "D"),
) -> None:
    timestamp = now_iso()
    for node_id in target_nodes:
        if node_id == "C" and not ENABLE_CAMERA_C:
            continue
        publish_json(
            client,
            JOURNEY_CONTROL_TOPICS[node_id],
            {
                "schema_version": "1",
                "event": (
                    "JOURNEY_RELEASE"
                    if node_id == "D"
                    else "JOURNEY_INVALIDATION"
                ),
                "action": "REMOVE",
                "target_node": node_id,
                "journey_id": journey_id,
                "status": terminal_status,
                "journey_status": journey_status,
                "release_candidate": True,
                "reason_codes": reason_codes or [],
                "timestamp": timestamp,
            },
        )


def publish_active_journey_candidate(
    client: mqtt.Client,
    topic: str,
    payload: dict[str, Any],
    expected_status: str,
) -> bool:
    """Revalidate status and TTL immediately before edge candidate publish."""
    journey_id = str(payload.get("journey_id") or "").strip()
    if not journey_id:
        structured_log(
            "candidate_publish_blocked",
            topic=topic,
            reason="MISSING_JOURNEY_ID",
        )
        return False

    expired = False
    actual_status = None
    age_seconds = None
    ttl_seconds = (
        WAITING_B_OR_C_TIMEOUT_SECONDS
        if expected_status == "WAITING_B_OR_C"
        else WAITING_D_TIMEOUT_SECONDS
    )
    with db_lock:
        with connect_db() as connection:
            row = connection.execute(
                "SELECT status, entry_at, passage_at FROM journeys "
                "WHERE journey_id = ?",
                (journey_id,),
            ).fetchone()
            if row is not None:
                actual_status = str(row["status"])
                reference_at = (
                    row["entry_at"]
                    if expected_status == "WAITING_B_OR_C"
                    else row["passage_at"] or row["entry_at"]
                )
                reference_epoch = parse_iso_epoch(reference_at)
                if reference_epoch is not None:
                    age_seconds = max(
                        0.0,
                        datetime.now().astimezone().timestamp() - reference_epoch,
                    )
                if (
                    actual_status == expected_status
                    and age_seconds is not None
                    and age_seconds >= ttl_seconds
                ):
                    expire_stale_journeys(connection, client)
                    actual_status = "EXPIRED"
                    expired = True
            active = row is not None and actual_status == expected_status

    if not active:
        structured_log(
            "candidate_publish_blocked",
            topic=topic,
            journey_id=journey_id,
            expected_status=expected_status,
            actual_status=actual_status,
            age_seconds=(round(age_seconds, 3) if age_seconds is not None else None),
            reason="TTL_EXPIRED" if expired else "JOURNEY_NOT_ACTIVE",
        )
        structured_log(
            "mqtt_queued_publish_discarded",
            topic=topic,
            journey_id=journey_id,
            expected_status=expected_status,
            actual_status=actual_status,
            reason="TTL_EXPIRED" if expired else "JOURNEY_NOT_ACTIVE",
        )
        return False

    structured_log(
        "mqtt_candidate_publish",
        topic=topic,
        journey_id=journey_id,
        qos=MQTT_CANDIDATE_QOS,
        delivery_policy="QOS0_REBUILD_FROM_ACTIVE_DB_ON_RECONNECT",
    )
    publish_json(client, topic, payload, qos=MQTT_CANDIDATE_QOS)
    return True


def invalidate_active_journeys_for_reset(
    client: mqtt.Client | None,
) -> int:
    if client is None:
        return 0
    with db_lock:
        with connect_db() as connection:
            rows = connection.execute(
                "SELECT journey_id FROM journeys "
                "WHERE status IN ('WAITING_B_OR_C','WAITING_D') "
                "ORDER BY journey_id"
            ).fetchall()
    for row in rows:
        publish_journey_invalidation(
            client,
            str(row["journey_id"]),
            "RESET",
            journey_status="RESET",
            reason_codes=["DATABASE_RESET"],
        )
    return len(rows)


# ============================================================
# A ENTRY 처리
# ============================================================

def find_journey_by_request_id(
    connection: sqlite3.Connection,
    request_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            journeys.*,
            COALESCE(
                journeys.visit_no,
                persons.visit_count
            ) AS visit_count,
            persons.last_seen_at,
            a_entry_requests.candidate_republish_allowed
        FROM a_entry_requests
        JOIN journeys
          ON journeys.journey_id = a_entry_requests.journey_id
        JOIN persons
          ON persons.person_uid = journeys.person_uid
        WHERE a_entry_requests.request_id = ?
        """,
        (request_id,),
    ).fetchone()


def find_active_journey(
    connection: sqlite3.Connection,
    person_uid: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            journeys.*,
            COALESCE(
                journeys.visit_no,
                persons.visit_count
            ) AS visit_count,
            persons.last_seen_at
        FROM journeys
        JOIN persons
          ON persons.person_uid = journeys.person_uid
        WHERE journeys.person_uid = ?
          AND journeys.status IN (
              'WAITING_B_OR_C',
              'WAITING_D'
          )
        ORDER BY journeys.entry_at DESC, journeys.journey_id DESC
        LIMIT 1
        """,
        (person_uid,),
    ).fetchone()


def reuse_active_journey_for_entry(
    connection: sqlite3.Connection,
    active_journey: sqlite3.Row,
    request_id: str | None,
    entry_at: str,
    local_track_id: Any,
    capture_specs: list[CaptureSpec],
    capture_parse_errors: list[dict[str, Any]],
    payload: dict[str, Any],
) -> None:
    """Map a redetection to the one active Journey without starting a visit."""
    journey_id = str(active_journey["journey_id"])
    person_uid = str(active_journey["person_uid"])
    record_a_entry_request(
        connection,
        request_id,
        journey_id,
        entry_at,
        candidate_republish_allowed=True,
    )
    insert_capture_rows(
        connection,
        capture_specs,
        journey_id,
        active_journey["canonical_person_uid"] or person_uid,
        now_iso(),
    )
    insert_failed_capture_rows(
        connection,
        capture_parse_errors,
        request_id,
        journey_id,
        str(entry_at),
        now_iso(),
    )
    save_journey_event(
        connection,
        journey_id,
        "A",
        "ENTRY_REUSED_ACTIVE_JOURNEY",
        entry_at,
        {
            "event": "ENTRY_REUSED_ACTIVE_JOURNEY",
            "request_id": request_id,
            "journey_id": journey_id,
            "person_uid": person_uid,
            "local_track_id": local_track_id,
            "active_status": str(active_journey["status"]),
            "policy": "REUSE_LATEST_ACTIVE_JOURNEY",
            "payload": payload,
        },
    )


def reconcile_duplicate_active_journeys(
    connection: sqlite3.Connection,
    client: mqtt.Client | None = None,
) -> int:
    """Keep only the latest valid Journey for each tracking Person."""
    rows = connection.execute(
        """
        SELECT journey_id, person_uid, status, entry_at
        FROM journeys
        WHERE status IN ('WAITING_B_OR_C', 'WAITING_D')
        ORDER BY person_uid, entry_at DESC, journey_id DESC
        """
    ).fetchall()
    latest_by_person: dict[str, str] = {}
    rejected = 0
    for row in rows:
        person_uid = str(row["person_uid"])
        journey_id = str(row["journey_id"])
        kept_journey_id = latest_by_person.setdefault(person_uid, journey_id)
        if kept_journey_id == journey_id:
            continue
        rejected_at = now_iso()
        connection.execute(
            "UPDATE journeys SET status='REJECTED' "
            "WHERE journey_id=? AND status IN ('WAITING_B_OR_C','WAITING_D')",
            (journey_id,),
        )
        save_journey_event(
            connection,
            journey_id,
            "MAIN",
            "REJECTED",
            rejected_at,
            {
                "event": "REJECTED",
                "journey_id": journey_id,
                "person_uid": person_uid,
                "reason": "SUPERSEDED_ACTIVE_JOURNEY",
                "kept_journey_id": kept_journey_id,
                "previous_status": str(row["status"]),
                "policy": "LATEST_ACTIVE_JOURNEY_PER_PERSON",
            },
        )
        if client is not None:
            publish_journey_invalidation(
                client,
                journey_id,
                "REJECTED",
                journey_status="REJECTED",
                reason_codes=["SUPERSEDED_ACTIVE_JOURNEY"],
            )
        structured_log(
            "duplicate_active_journey_rejected",
            person_uid=person_uid,
            rejected_journey_id=journey_id,
            kept_journey_id=kept_journey_id,
            previous_status=str(row["status"]),
        )
        rejected += 1
    return rejected


def record_a_entry_request(
    connection: sqlite3.Connection,
    request_id: str | None,
    journey_id: str,
    received_at: str,
    *,
    candidate_republish_allowed: bool,
) -> None:
    if request_id is None:
        return

    connection.execute(
        """
        INSERT INTO a_entry_requests (
            request_id,
            journey_id,
            received_at,
            candidate_republish_allowed
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            request_id,
            journey_id,
            received_at,
            int(candidate_republish_allowed),
        ),
    )


def cache_a_entry_images(
    request_id: str | None,
    capture_keys: list[str] | None = None,
) -> dict[str, int]:
    """Cache failures are isolated from identity/Journey processing."""
    if not request_id or not CAPTURE_CACHE_SETTINGS.enabled:
        return {"cached": 0, "failed": 0}
    with connect_db() as connection:
        if capture_keys is None:
            rows = connection.execute(
                """
                SELECT capture_key FROM captures
                WHERE request_id = ? AND cache_status IN ('PENDING', 'FAILED')
                ORDER BY capture_id
                """,
                (request_id,),
            ).fetchall()
            keys = [str(row["capture_key"]) for row in rows]
        else:
            keys = list(dict.fromkeys(capture_keys))

    result = {"cached": 0, "failed": 0}
    for capture_key in keys:
        cached = cache_capture(connect_db, capture_key, CAPTURE_CACHE_SETTINGS)
        status = str(cached.get("cache_status", "FAILED"))
        result["cached" if status == "CACHED" else "failed"] += 1
        if status != "CACHED":
            print(
                "[A CAPTURE CACHE 실패] "
                f"capture_key={capture_key}, reason={cached.get('cache_error')}"
            )

    with connect_db() as connection:
        journey = connection.execute(
            """
            SELECT canonical_person_uid, identity_result
            FROM journeys WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()
        canonical_uid = (
            str(journey["canonical_person_uid"])
            if journey is not None
            and journey["identity_result"] in {"NEW", "RETURNING"}
            and journey["canonical_person_uid"]
            else None
        )
        choose_automatic_representative(
            connection,
            canonical_uid,
            request_id,
            now_iso(),
        )
    return result


def begin_new_visit(
    connection: sqlite3.Connection,
    person_result: dict[str, Any],
    timestamp: str,
) -> dict[str, Any]:
    """Identity 판정 후 실제 새 방문일 때만 visit_count를 증가시킨다."""
    person_uid = str(person_result["person_uid"])
    row = connection.execute(
        """
        SELECT visit_count, last_seen_at
        FROM persons
        WHERE person_uid = ?
        """,
        (person_uid,),
    ).fetchone()

    if row is None:
        raise ValueError(f"Person을 찾을 수 없음: {person_uid}")

    previous_visit_count = int(row["visit_count"])
    visit_no = previous_visit_count + 1
    previous_last_seen_at = (
        row["last_seen_at"]
        if previous_visit_count > 0
        else None
    )
    requested_person_status = str(
        person_result.get("person_status", "ACTIVE")
    )
    next_person_status = (
        requested_person_status
        if requested_person_status in {
            "IDENTITY_PENDING",
            "REVIEW_REQUIRED",
        }
        else "ACTIVE"
    )

    connection.execute(
        """
        UPDATE persons
        SET
            last_seen_at = ?,
            visit_count = ?,
            status = ?
        WHERE person_uid = ?
        """,
        (
            timestamp,
            visit_no,
            next_person_status,
            person_uid,
        ),
    )

    result = dict(person_result)
    result["visit_count"] = visit_no
    result["previous_last_seen_at"] = previous_last_seen_at
    return result


def replay_existing_a_entry(
    client: mqtt.Client,
    connection: sqlite3.Connection,
    journey: sqlite3.Row,
    request_id: str | None,
    *,
    publish_candidate: bool,
    local_track_id: Any = None,
) -> None:
    """기존 Journey의 ENTRY_RESULT를 재사용해 응답한다."""
    gallery = load_body_journey_gallery(
        connection,
        journey["journey_id"],
    )
    best_score = journey["person_best_score"]

    common_payload = {
        "journey_id": journey["journey_id"],
        "person_uid": journey["person_uid"],
        "global_person_id": journey["person_uid"],
        "tracking_person_uid": journey["person_uid"],
        "person_status": journey["person_status"],
        "identity_result": journey["identity_result"],
        "review_status": journey["review_status"],
        "canonical_person_uid": journey["canonical_person_uid"],
        "candidate_person_uid": journey["candidate_person_uid"],
        "identity_candidate_key": (
            journey["canonical_person_uid"] or journey["person_uid"]
        ),
        "journey_selection_key": journey["journey_id"],
        "margin_scope": "DISTINCT_IDENTITY_CANDIDATES",
        "active_journey_policy": "LATEST_PER_PERSON",
        "identity_confirmed": bool(
            journey["identity_result"] in {"NEW", "RETURNING"}
            and journey["review_status"] != "PENDING"
            and journey["canonical_person_uid"] is not None
        ),
        "decision_reason": journey["decision_reason"],
        "visit_count": journey["visit_count"],
        "previous_last_seen_at": journey["last_seen_at"],
        "person_match_score": journey["person_match_score"],
        "second_match_score": journey["second_match_score"],
        "person_best_score": best_score,
        "person_topk_score": journey["person_topk_score"],
        "person_combined_score": journey["person_combined_score"],
        "person_match_margin": journey["score_margin"],
        "gallery_promotion_allowed": bool(
            journey["gallery_promotion_allowed"]
        ),
        "match_source": journey["match_source"],
        "route": ["A"],
        "entry_timestamp": journey["entry_at"],
    }

    response_payload = {
        "event": "ENTRY_RESULT",
        "stage": journey["status"],
        "node_id": "A",
        "request_id": request_id,
        "local_track_id": (
            local_track_id
            if local_track_id is not None
            else journey["entry_local_track_id"]
        ),
        "current_node": "A",
        "timestamp": journey["entry_at"],
        **common_payload,
    }
    publish_json(
        client,
        TOPIC_A_ENTRY_RESPONSE,
        response_payload,
    )

    # 이미 다음 단계로 진행된 Journey를 B 후보 상태로 되돌리지 않는다.
    if (
        publish_candidate
        and journey["status"] == "WAITING_B_OR_C"
    ):
        candidate_payload = {
            "event": "CANDIDATE",
            "stage": "WAITING_B_OR_C",
            "gallery_count": len(gallery),
            "gallery": gallery,
            **candidate_expiry_fields(
                journey["entry_at"], WAITING_B_OR_C_TIMEOUT_SECONDS
            ),
            **common_payload,
        }
        publish_active_journey_candidate(
            client,
            TOPIC_CANDIDATE_B,
            candidate_payload,
            "WAITING_B_OR_C",
        )
        if ENABLE_CAMERA_C:
            publish_active_journey_candidate(
                client,
                TOPIC_CANDIDATE_C,
                candidate_payload,
                "WAITING_B_OR_C",
            )

    print(
        "[MAIN 중복 ENTRY 재사용] "
        f"request_id={request_id}, "
        f"journey_id={journey['journey_id']}, "
        f"person_uid={journey['person_uid']}, "
        f"candidate_republished={publish_candidate}"
    )

def handle_a_entry(
    client: mqtt.Client,
    payload: dict[str, Any],
) -> None:
    entry_at = payload.get(
        "timestamp",
        now_iso(),
    )

    parsed_samples = parse_a_entry_samples(
        payload
    )
    body_samples = parsed_samples["body_samples"]
    face_samples = parsed_samples["face_samples"]

    local_track_id = extract_local_track_id(
        payload
    )
    raw_request_id = payload.get("request_id")
    request_id = (
        str(raw_request_id).strip()
        if raw_request_id is not None
        else None
    )
    if request_id == "":
        request_id = None

    capture_specs, capture_parse_errors = parse_capture_specs(
        payload,
        request_id,
        str(entry_at),
        CAPTURE_CACHE_SETTINGS,
    )

    # QoS 1 duplicate delivery also acts as a safe retry trigger for prior
    # PENDING/FAILED downloads. No identity state is changed here.
    cache_a_entry_images(request_id)

    with db_lock:
        with connect_db() as connection:
            # Candidate read, ID allocation, Person/Journey write and
            # request-id mapping are one serialized transaction. This also
            # protects against a second Main/API process racing this ENTRY.
            connection.execute("BEGIN IMMEDIATE")
            # Resolve timeout before either idempotency replay or same-person
            # active-Journey reuse. A stale session must never absorb a new
            # physical visit.
            expire_stale_journeys(connection, client)
            if request_id is not None:
                existing_journey = find_journey_by_request_id(
                    connection,
                    request_id,
                )
                if existing_journey is not None:
                    # Do not keep BEGIN IMMEDIATE open while the replay path
                    # performs the final active/TTL check through a fresh DB
                    # connection.  This also avoids blocking an expiry update
                    # for a stale QoS retry.
                    connection.commit()
                    replay_existing_a_entry(
                        client,
                        connection,
                        existing_journey,
                        request_id,
                        publish_candidate=bool(
                            existing_journey[
                                "candidate_republish_allowed"
                            ]
                        ),
                    )
                    return

            person_result = resolve_person_uid(
                connection,
                [
                    sample["embedding"]
                    for sample in body_samples
                ],
                entry_at,
                [
                    sample["embedding"]
                    for sample in face_samples
                ],
                body_qualities=[
                    float(sample["quality"])
                    for sample in body_samples
                ],
                face_qualities=[
                    float(sample["quality"])
                    for sample in face_samples
                ],
            )

            person_uid = person_result[
                "person_uid"
            ]

            active_journey = find_active_journey(connection, person_uid)
            if active_journey is not None:
                reuse_active_journey_for_entry(
                    connection,
                    active_journey,
                    request_id,
                    str(entry_at),
                    local_track_id,
                    capture_specs,
                    capture_parse_errors,
                    payload,
                )
                connection.commit()
                cache_a_entry_images(
                    request_id,
                    [spec.capture_key for spec in capture_specs],
                )
                structured_log(
                    "active_journey_reused",
                    policy="REUSE_LATEST_ACTIVE_JOURNEY",
                    request_id=request_id,
                    journey_id=str(active_journey["journey_id"]),
                    person_uid=person_uid,
                    journey_status=str(active_journey["status"]),
                    identity_result=str(active_journey["identity_result"]),
                    canonical_person_uid=active_journey[
                        "canonical_person_uid"
                    ],
                )
                replay_existing_a_entry(
                    client,
                    connection,
                    active_journey,
                    request_id,
                    publish_candidate=True,
                    local_track_id=local_track_id,
                )
                return

            if person_result["identity_result"] != "UNKNOWN":
                person_result = begin_new_visit(
                    connection,
                    person_result,
                    entry_at,
                )

            journey_id = generate_next_id(
                connection,
                "journeys",
                "journey_id",
                "J",
            )

            connection.execute(
                """
                INSERT INTO journeys (
                    journey_id,
                    request_id,
                    person_uid,
                    visit_no,
                    status,
                    route_json,
                    entry_at,

                    person_match_score,
                    second_match_score,

                    person_best_score,
                    person_topk_score,
                    person_combined_score,
                    second_person_score,
                    match_source,
                    gallery_promotion_allowed,

                    person_status,
                    candidate_person_uid,
                    entry_local_track_id,
                    identity_result,
                    review_status,
                    canonical_person_uid,
                    decision_reason,
                    score_margin,
                    query_gallery_count,
                    candidate_pool_size
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    journey_id,
                    request_id,
                    person_uid,
                    person_result["visit_count"],
                    "WAITING_B_OR_C",
                    json.dumps(["A"]),
                    entry_at,

                    (
                        person_result[
                            "combined_score"
                        ]
                        if person_result[
                            "combined_score"
                        ] >= 0
                        else None
                    ),
                    (
                        person_result[
                            "second_score"
                        ]
                        if person_result[
                            "second_score"
                        ] >= 0
                        else None
                    ),

                    (
                        person_result[
                            "best_score"
                        ]
                        if person_result[
                            "best_score"
                        ] >= 0
                        else None
                    ),
                    (
                        person_result[
                            "topk_score"
                        ]
                        if person_result[
                            "topk_score"
                        ] >= 0
                        else None
                    ),
                    (
                        person_result[
                            "combined_score"
                        ]
                        if person_result[
                            "combined_score"
                        ] >= 0
                        else None
                    ),
                    (
                        person_result[
                            "second_score"
                        ]
                        if person_result[
                            "second_score"
                        ] >= 0
                        else None
                    ),
                    person_result[
                        "match_source"
                    ],
                    int(
                        bool(
                            person_result[
                                "gallery_promotion_allowed"
                            ]
                        )
                    ),

                    person_result[
                        "person_status"
                    ],
                    person_result[
                        "candidate_person_uid"
                    ],
                    (
                        str(local_track_id)
                        if local_track_id
                        is not None
                        else None
                    ),
                    person_result["identity_result"],
                    person_result["review_status"],
                    person_result["assigned_person_uid"],
                    person_result["decision_reason"],
                    person_result["match_margin"],
                    person_result["query_gallery_count"],
                    person_result["candidate_pool_size"],
                ),
            )

            review_case = None
            if person_result["person_status"] in {
                "IDENTITY_PENDING",
                "REVIEW_REQUIRED",
            }:
                review_case = create_review_case(
                    connection,
                    journey_id,
                    person_uid,
                    person_result["candidate_person_uid"],
                    entry_at,
                    initial_decision=str(
                        person_result["person_status"]
                    ),
                    initial_scores={
                        "body": {
                            "best_score": person_result.get("best_score"),
                            "topk_score": person_result.get("topk_score"),
                            "combined_score": person_result.get(
                                "combined_score"
                            ),
                            "second_score": person_result.get("second_score"),
                            "match_margin": person_result.get("match_margin"),
                            "match_source": person_result.get("match_source"),
                        },
                        "face": {
                            "candidate_person_uid": person_result.get(
                                "face_candidate_person_uid"
                            ),
                            "best_score": person_result.get("face_best_score"),
                            "topk_score": person_result.get("face_topk_score"),
                            "combined_score": person_result.get(
                                "face_combined_score"
                            ),
                            "match_source": person_result.get(
                                "face_match_source"
                            ),
                        },
                    },
                    route=["A"],
                    pending_person_created=bool(
                        person_result.get("pending_person_created", False)
                    ),
                )

            record_a_entry_request(
                connection,
                request_id,
                journey_id,
                entry_at,
                candidate_republish_allowed=True,
            )

            insert_capture_rows(
                connection,
                capture_specs,
                journey_id,
                person_result["assigned_person_uid"],
                now_iso(),
            )
            insert_failed_capture_rows(
                connection,
                capture_parse_errors,
                request_id,
                journey_id,
                str(entry_at),
                now_iso(),
            )

            for sample in body_samples:
                save_journey_embedding(
                    connection,
                    journey_id,
                    "A",
                    sample["embedding"],
                    entry_at,
                    sample["quality"],
                    "BODY",
                )

            for sample in face_samples:
                save_journey_embedding(
                    connection,
                    journey_id,
                    "A",
                    sample["embedding"],
                    entry_at,
                    sample["quality"],
                    "FACE",
                )

            save_journey_event(
                connection,
                journey_id,
                "A",
                "ENTRY",
                entry_at,
                payload,
            )

            capture_saved = (
                save_capture_record_if_present(
                    connection,
                    journey_id,
                    person_uid,
                    "A",
                    entry_at,
                    payload,
                )
            )

            if review_case is not None:
                query_capture_path = next(
                    (
                        str(sample["capture_path"])
                        for sample in body_samples
                        if sample.get("capture_path")
                    ),
                    None,
                )
                save_identity_review_candidates(
                    connection,
                    str(review_case["review_id"]),
                    journey_id,
                    request_id,
                    person_result.get("review_candidates", []),
                    query_capture_path,
                    entry_at,
                )

            gallery = load_body_journey_gallery(
                connection,
                journey_id,
            )

    cache_result = cache_a_entry_images(
        request_id,
        [spec.capture_key for spec in capture_specs],
    )

    common_payload = {
        "journey_id": journey_id,
        "person_uid": person_uid,
        "global_person_id": person_uid,
        "tracking_person_uid": person_uid,
        "person_status": person_result[
            "person_status"
        ],
        "identity_result": person_result["identity_result"],
        "review_status": person_result["review_status"],
        "canonical_person_uid": person_result["assigned_person_uid"],
        "identity_candidate_key": (
            person_result["assigned_person_uid"] or person_uid
        ),
        "journey_selection_key": journey_id,
        "margin_scope": "DISTINCT_IDENTITY_CANDIDATES",
        "active_journey_policy": "LATEST_PER_PERSON",
        "identity_confirmed": bool(
            person_result["identity_result"] in {"NEW", "RETURNING"}
            and person_result["review_status"] != "PENDING"
            and person_result["assigned_person_uid"] is not None
        ),
        "decision_reason": person_result["decision_reason"],
        "match_margin": person_result["match_margin"],
        "qualified_body_count": person_result.get("qualified_body_count", 0),
        "qualified_face_count": person_result.get("qualified_face_count", 0),
        "body_consistent_match_count": person_result.get(
            "body_consistent_match_count", 0
        ),
        "body_multiframe_consistent": bool(
            person_result.get("body_multiframe_consistent", False)
        ),
        "body_frame_candidate_person_uids": person_result.get(
            "body_frame_candidate_person_uids", []
        ),
        "body_top_candidate_frame_scores": person_result.get(
            "body_top_candidate_frame_scores", []
        ),
        "body_frame_candidate_conflict": bool(
            person_result.get("body_frame_candidate_conflict", False)
        ),
        "candidate_person_uid": (
            person_result[
                "candidate_person_uid"
            ]
        ),
        "visit_count": person_result[
            "visit_count"
        ],
        "previous_last_seen_at": (
            person_result[
                "previous_last_seen_at"
            ]
        ),

        # 기존 필드 호환:
        # person_match_score는 이제 Combined Score
        "person_match_score": (
            person_result[
                "combined_score"
            ]
            if person_result[
                "combined_score"
            ] >= 0
            else None
        ),
        "second_match_score": (
            person_result[
                "second_score"
            ]
            if person_result[
                "second_score"
            ] >= 0
            else None
        ),

        # 상세 Person Re-ID 점수
        "person_best_score": (
            person_result[
                "best_score"
            ]
            if person_result[
                "best_score"
            ] >= 0
            else None
        ),
        "person_topk_score": (
            person_result[
                "topk_score"
            ]
            if person_result[
                "topk_score"
            ] >= 0
            else None
        ),
        "person_combined_score": (
            person_result[
                "combined_score"
            ]
            if person_result[
                "combined_score"
            ] >= 0
            else None
        ),
        "person_match_margin": (
            person_result[
                "match_margin"
            ]
        ),
        "gallery_promotion_allowed": (
            bool(
                person_result[
                    "gallery_promotion_allowed"
                ]
            )
        ),
        "match_source": person_result[
            "match_source"
        ],
        "face_candidate_person_uid": person_result.get(
            "face_candidate_person_uid"
        ),
        "face_best_score": person_result.get(
            "face_best_score"
        ),
        "face_topk_score": person_result.get(
            "face_topk_score"
        ),
        "face_combined_score": person_result.get(
            "face_combined_score"
        ),
        "face_match_source": person_result.get(
            "face_match_source"
        ),

        "route": ["A"],
        "entry_timestamp": entry_at,
    }

    response_payload = {
        "event": "ENTRY_RESULT",
        "stage": "WAITING_B_OR_C",
        "node_id": "A",
        "request_id": request_id,
        "local_track_id": local_track_id,
        "current_node": "A",
        "timestamp": entry_at,
        **common_payload,
    }

    candidate_payload = {
        "event": "CANDIDATE",
        "stage": "WAITING_B_OR_C",
        "gallery_count": len(gallery),
        "gallery": gallery,
        **candidate_expiry_fields(
            entry_at, WAITING_B_OR_C_TIMEOUT_SECONDS
        ),
        **common_payload,
    }

    publish_json(
        client,
        TOPIC_A_ENTRY_RESPONSE,
        response_payload,
    )

    publish_active_journey_candidate(
        client,
        TOPIC_CANDIDATE_B,
        candidate_payload,
        "WAITING_B_OR_C",
    )

    if ENABLE_CAMERA_C:
        publish_active_journey_candidate(
            client,
            TOPIC_CANDIDATE_C,
            candidate_payload,
            "WAITING_B_OR_C",
        )

    print_identity_decision(
        request_id,
        journey_id,
        person_result,
    )

    print()
    print(
        "===== MAIN: A ENTRY 처리 ====="
    )
    print(
        f"Person UID : {person_uid}"
    )
    print(
        f"Journey ID : {journey_id}"
    )
    print(
        "Person 상태: "
        f"{person_result['person_status']}"
    )
    print(
        "방문 횟수  : "
        f"{person_result['visit_count']}"
    )
    print(
        "BEST       : "
        f"{common_payload['person_best_score']}"
    )
    print(
        "TOP-K Mean : "
        f"{common_payload['person_topk_score']}"
    )
    print(
        "Combined   : "
        f"{common_payload['person_combined_score']}"
    )
    print(
        "2nd Person : "
        f"{common_payload['second_match_score']}"
    )
    print(
        "Match Source: "
        f"{person_result['match_source']}"
    )
    print(
        "검토 후보  : "
        f"{person_result['candidate_person_uid']}"
    )
    print(
        "영구 Gallery 추가 허용: "
        f"{person_result['gallery_promotion_allowed']}"
    )
    print(
        f"Capture 저장: {capture_saved}"
    )
    print(
        "A 사진 캐시: "
        f"CACHED={cache_result['cached']}, "
        f"FAILED={cache_result['failed']}, "
        f"INVALID={len(capture_parse_errors)}"
    )
    print(
        "Journey     : WAITING_B_OR_C"
    )
    print(
        "A 응답 토픽 : "
        "cctv/responses/a/entry"
    )
    print(
        "전달 대상   : B / C"
    )
    print(
        "=============================="
    )


# ============================================================
# B / C PASSAGE 처리
# ============================================================

def validate_c_passage_evidence(
    connection: sqlite3.Connection,
    journey_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Re-evaluate C's final multi-frame evidence against the A gallery."""
    root_similarity = payload.get("combined_score", payload.get("similarity"))
    root_quality = payload.get("quality")
    reasons: list[str] = []
    try:
        final_similarity = float(root_similarity)
    except (TypeError, ValueError):
        final_similarity = None
        reasons.append("C_MISSING_OR_INVALID_FINAL_SIMILARITY")
    try:
        final_quality = float(root_quality)
    except (TypeError, ValueError):
        final_quality = None
        reasons.append("C_MISSING_OR_INVALID_FINAL_QUALITY")

    a_rows = connection.execute(
        """
        SELECT embedding, embedding_dim
        FROM journey_gallery
        WHERE journey_id=? AND node_id='A' AND modality='BODY'
        ORDER BY gallery_id
        """,
        (journey_id,),
    ).fetchall()
    a_embeddings = [
        blob_to_embedding(row["embedding"], int(row["embedding_dim"]))
        for row in a_rows
    ]
    c_samples: list[dict[str, Any]] = []
    for item in payload.get("gallery", []):
        if not isinstance(item, dict) or item.get("node_id") != "C":
            continue
        raw_embedding = item.get("embedding")
        if not isinstance(raw_embedding, list):
            continue
        try:
            embedding = normalize_embedding(raw_embedding)
            quality = float(
                item.get("quality", final_quality if final_quality is not None else -1)
            )
        except (TypeError, ValueError):
            continue
        if quality < C_PASSAGE_MIN_QUALITY:
            continue
        c_samples.append(
            {"item": item, "embedding": embedding, "quality": quality}
        )

    best_score = topk_score = combined_score = None
    per_frame_best: list[float] = []
    consistent_count = 0
    if not a_embeddings:
        reasons.append("C_MISSING_A_GALLERY")
    if len(c_samples) < MIN_CONSISTENT_BODY_FRAMES:
        reasons.append("C_INSUFFICIENT_QUALITY_SAMPLES")
    if a_embeddings and c_samples:
        matrix = np.asarray(
            [
                [cosine_similarity(sample["embedding"], a) for a in a_embeddings]
                for sample in c_samples
            ],
            dtype=np.float32,
        )
        per_frame_best = [float(value) for value in matrix.max(axis=1)]
        flattened = np.sort(matrix.reshape(-1))[::-1]
        best_score = float(flattened[0])
        top_count = min(PERSON_TOPK, int(flattened.size))
        topk_score = float(np.mean(flattened[:top_count]))
        combined_score = (
            PERSON_BEST_WEIGHT * best_score
            + PERSON_TOPK_WEIGHT * topk_score
        )
        consistent_count = sum(
            score >= PERSON_REVIEW_COMBINED_THRESHOLD
            for score in per_frame_best
        )
        if best_score < PERSON_MATCH_THRESHOLD:
            reasons.append("C_BEST_BELOW_THRESHOLD")
        if topk_score < PERSON_TOPK_THRESHOLD:
            reasons.append("C_TOPK_BELOW_THRESHOLD")
        if combined_score < PERSON_COMBINED_THRESHOLD:
            reasons.append("C_COMBINED_BELOW_THRESHOLD")
        if consistent_count < MIN_CONSISTENT_BODY_FRAMES:
            reasons.append("C_MULTIFRAME_INCONSISTENT")
    if (
        final_similarity is not None
        and final_similarity < PERSON_COMBINED_THRESHOLD
    ):
        reasons.append("C_FINAL_SIMILARITY_BELOW_THRESHOLD")
    if final_quality is not None and final_quality < C_PASSAGE_MIN_QUALITY:
        reasons.append("C_FINAL_QUALITY_BELOW_THRESHOLD")

    reasons = list(dict.fromkeys(reasons))
    predicates = [
        {
            "name": "final_similarity",
            "expected": f">={PERSON_COMBINED_THRESHOLD}",
            "actual": final_similarity,
            "pass": final_similarity is not None
            and final_similarity >= PERSON_COMBINED_THRESHOLD,
        },
        {
            "name": "final_quality",
            "expected": f">={C_PASSAGE_MIN_QUALITY}",
            "actual": final_quality,
            "pass": final_quality is not None
            and final_quality >= C_PASSAGE_MIN_QUALITY,
        },
        {
            "name": "best_score",
            "expected": f">={PERSON_MATCH_THRESHOLD}",
            "actual": best_score,
            "pass": best_score is not None and best_score >= PERSON_MATCH_THRESHOLD,
        },
        {
            "name": "topk_score",
            "expected": f">={PERSON_TOPK_THRESHOLD}",
            "actual": topk_score,
            "pass": topk_score is not None and topk_score >= PERSON_TOPK_THRESHOLD,
        },
        {
            "name": "combined_score",
            "expected": f">={PERSON_COMBINED_THRESHOLD}",
            "actual": combined_score,
            "pass": combined_score is not None
            and combined_score >= PERSON_COMBINED_THRESHOLD,
        },
        {
            "name": "multiframe_consistency",
            "expected": f">={MIN_CONSISTENT_BODY_FRAMES}",
            "actual": consistent_count,
            "pass": consistent_count >= MIN_CONSISTENT_BODY_FRAMES,
        },
    ]
    return {
        "accepted": not reasons,
        "reason_codes": reasons,
        "final_similarity": final_similarity,
        "final_quality": final_quality,
        "best_score": best_score,
        "topk_score": topk_score,
        "combined_score": combined_score,
        "consistent_count": consistent_count,
        "per_frame_best_scores": per_frame_best,
        "accepted_samples": c_samples,
        "predicates": predicates,
    }

def handle_passage(
    client: mqtt.Client,
    payload: dict[str, Any],
    node_id: str,
) -> dict[str, Any] | None:
    journey_id = payload.get(
        "journey_id"
    )

    if not journey_id:
        raise ValueError(
            "PASSAGE 메시지에 journey_id가 없습니다."
        )

    passage_at = payload.get(
        "b_passage_timestamp"
        if node_id == "B"
        else "c_passage_timestamp",
        payload.get(
            "timestamp",
            now_iso(),
        ),
    )

    with db_lock:
        with connect_db() as connection:
            journey = connection.execute(
                """
                SELECT *
                FROM journeys
                WHERE journey_id = ?
                """,
                (journey_id,),
            ).fetchone()

            if journey is None:
                print(
                    f"[MAIN 무시] 존재하지 않는 Journey: "
                    f"{journey_id}"
                )
                return

            if journey["status"] != "WAITING_B_OR_C":
                print(
                    f"[MAIN 무시] {journey_id} 현재 상태: "
                    f"{journey['status']}"
                )
                return

            c_validation = None
            if node_id == "C":
                c_validation = validate_c_passage_evidence(
                    connection,
                    str(journey_id),
                    payload,
                )
                structured_log(
                    "c_passage_final_validation",
                    journey_id=journey_id,
                    person_uid=str(journey["person_uid"]),
                    accepted=bool(c_validation["accepted"]),
                    reason_codes=c_validation["reason_codes"],
                    predicates=c_validation["predicates"],
                    per_frame_best_scores=c_validation[
                        "per_frame_best_scores"
                    ],
                )
                if not c_validation["accepted"]:
                    save_journey_event(
                        connection,
                        str(journey_id),
                        "C",
                        "PASSAGE_REJECTED",
                        str(passage_at),
                        {
                            "event": "PASSAGE_REJECTED",
                            "journey_id": journey_id,
                            "person_uid": journey["person_uid"],
                            "reason_codes": c_validation["reason_codes"],
                            "predicates": c_validation["predicates"],
                            "per_frame_best_scores": c_validation[
                                "per_frame_best_scores"
                            ],
                            "payload": payload,
                        },
                    )
                    save_capture_record_if_present(
                        connection,
                        str(journey_id),
                        str(journey["person_uid"]),
                        "C",
                        str(passage_at),
                        payload,
                    )
                    return {
                        "accepted": False,
                        "journey_status": "WAITING_B_OR_C",
                        "reason_codes": c_validation["reason_codes"],
                        "validation": c_validation,
                    }

            gallery_items = payload.get(
                "gallery",
                [],
            )

            added_count = 0

            if isinstance(gallery_items, list):
                for item in gallery_items:
                    if not isinstance(item, dict):
                        continue

                    if item.get("node_id") != node_id:
                        continue

                    if node_id == "C" and c_validation is not None:
                        if not any(
                            sample["item"] is item
                            for sample in c_validation["accepted_samples"]
                        ):
                            continue

                    raw_embedding = item.get(
                        "embedding"
                    )

                    if not isinstance(
                        raw_embedding,
                        list,
                    ):
                        continue

                    save_journey_embedding(
                        connection,
                        journey_id,
                        node_id,
                        normalize_embedding(
                            raw_embedding
                        ),
                        item.get(
                            "captured_at",
                            passage_at,
                        ),
                        float(
                            item.get(
                                "quality",
                                (
                                    c_validation["final_quality"]
                                    if node_id == "C" and c_validation is not None
                                    else 1.0
                                ),
                            )
                        ),
                    )

                    added_count += 1

            route = ["A", node_id]

            connection.execute(
                """
                UPDATE journeys
                SET
                    status = ?,
                    route_json = ?,
                    passage_at = ?
                WHERE journey_id = ?
                """,
                (
                    "WAITING_D",
                    json.dumps(route),
                    passage_at,
                    journey_id,
                ),
            )

            save_journey_event(
                connection,
                journey_id,
                node_id,
                "PASSAGE",
                passage_at,
                payload,
            )

            gallery = load_body_journey_gallery(
                connection,
                journey_id,
            )

            person_uid = journey[
                "person_uid"
            ]

            capture_saved = (
                save_capture_record_if_present(
                    connection,
                    journey_id,
                    person_uid,
                    node_id,
                    passage_at,
                    payload,
                )
            )

            entry_at = journey[
                "entry_at"
            ]

    d_candidate_payload = {
        "event": "CANDIDATE",
        "stage": "WAITING_D",

        "journey_id": journey_id,
        "person_uid": person_uid,
        "global_person_id": person_uid,
        "identity_result": journey["identity_result"],
        "review_status": journey["review_status"],
        "canonical_person_uid": journey["canonical_person_uid"],
        "tracking_person_uid": person_uid,
        "identity_candidate_key": (
            journey["canonical_person_uid"] or person_uid
        ),
        "journey_selection_key": journey_id,
        "margin_scope": "DISTINCT_IDENTITY_CANDIDATES",
        "active_journey_policy": "LATEST_PER_PERSON",
        "middle_node": node_id,

        "route": route,
        "entry_timestamp": entry_at,
        "passage_timestamp": passage_at,
        **candidate_expiry_fields(
            passage_at, WAITING_D_TIMEOUT_SECONDS
        ),

        "gallery_count": len(gallery),
        "gallery": gallery,
    }

    publish_active_journey_candidate(
        client,
        TOPIC_CANDIDATE_D,
        d_candidate_payload,
        "WAITING_D",
    )

    print()
    print(
        f"===== MAIN: {node_id} PASSAGE 처리 ====="
    )
    print(f"Person UID : {person_uid}")
    print(f"Journey ID: {journey_id}")
    print(f"Identity Result: {journey['identity_result']}")
    print(f"Review Status: {journey['review_status']}")
    print(f"Gallery Count: {len(gallery)}")
    print(f"추가 특징값: {added_count}")
    print(f"Capture 저장: {capture_saved}")
    print("Journey    : WAITING_D")
    print("전달 대상  : D")
    print("================================")
    return {
        "accepted": True,
        "journey_status": "WAITING_D",
        "reason_codes": [],
    }


# ============================================================
# Person 영구 Gallery 저장
# ============================================================

def promote_journey_gallery(
    connection: sqlite3.Connection,
    journey_id: str,
    person_uid: str,
) -> int:
    existing_rows = connection.execute(
        """
        SELECT modality, embedding_dim, embedding
        FROM person_embeddings
        WHERE person_uid = ?
        """,
        (person_uid,),
    ).fetchall()

    permanent_embeddings: dict[str, list[np.ndarray]] = {
        "BODY": [],
        "FACE": [],
    }
    for row in existing_rows:
        modality = str(row["modality"])
        permanent_embeddings.setdefault(modality, []).append(
            blob_to_embedding(
                row["embedding"],
                int(row["embedding_dim"]),
            )
        )

    gallery_rows = connection.execute(
        """
        SELECT
            node_id,
            captured_at,
            quality,
            modality,
            embedding_dim,
            embedding
        FROM journey_gallery
        WHERE journey_id = ?
        ORDER BY quality DESC
        """,
        (journey_id,),
    ).fetchall()

    eligible_rows = [
        row
        for row in gallery_rows
        if float(row["quality"]) >= GALLERY_MIN_SAMPLE_QUALITY
    ]
    candidates: dict[str, list[tuple[int, np.ndarray]]] = {
        "BODY": [],
        "FACE": [],
    }
    for row_index, row in enumerate(eligible_rows):
        candidates.setdefault(str(row["modality"]), []).append(
            (
                row_index,
                blob_to_embedding(
                    row["embedding"], int(row["embedding_dim"])
                ),
            )
        )

    def largest_consistent_cluster(
        entries: list[tuple[int, np.ndarray]],
        threshold: float,
        minimum_count: int,
    ) -> list[tuple[int, np.ndarray]]:
        """Return the densest seed-centred cluster, excluding outliers."""
        best_cluster: list[tuple[int, np.ndarray]] = []
        for _, seed in entries:
            cluster = [
                entry
                for entry in entries
                if cosine_similarity(seed, entry[1]) >= threshold
            ]
            if len(cluster) > len(best_cluster):
                best_cluster = cluster
        if len(best_cluster) < minimum_count:
            return []
        return best_cluster

    body_entries = candidates.get("BODY", [])
    face_entries = candidates.get("FACE", [])
    target_has_body = bool(permanent_embeddings.get("BODY"))
    target_has_face = bool(permanent_embeddings.get("FACE"))
    allowed_row_indices: set[int] = set()

    if target_has_body:
        target_body = permanent_embeddings["BODY"]
        matched_body = [
            entry
            for entry in body_entries
            if max(
                cosine_similarity(entry[1], existing)
                for existing in target_body
            ) >= PERSON_GALLERY_PROMOTE_BEST_THRESHOLD
        ]
        body_result = find_existing_person(
            connection,
            [embedding for _, embedding in matched_body],
            "BODY",
            permanent_only=True,
        )
        body_best = body_result.get("best_candidate")
        allow_body = bool(
            len(matched_body) >= MIN_CONSISTENT_BODY_FRAMES
            and body_best is not None
            and str(body_best["person_uid"]) == person_uid
            and _body_promotion_allowed(body_result)
        )
        if allow_body:
            allowed_row_indices.update(index for index, _ in matched_body)
    else:
        matched_body = largest_consistent_cluster(
            body_entries,
            PERSON_REVIEW_COMBINED_THRESHOLD,
            MIN_CONSISTENT_BODY_FRAMES,
        )
        allow_body = bool(matched_body)
        allowed_row_indices.update(index for index, _ in matched_body)

    if target_has_face:
        target_face = permanent_embeddings["FACE"]
        matched_face = [
            entry
            for entry in face_entries
            if max(
                cosine_similarity(entry[1], existing)
                for existing in target_face
            ) >= FACE_MATCH_THRESHOLD
        ]
        face_result = find_existing_person(
            connection,
            [embedding for _, embedding in matched_face],
            "FACE",
            permanent_only=True,
        )
        face_best = _strong_face_candidate(face_result)
        allow_face = bool(
            len(matched_face) >= MIN_CONSISTENT_FACE_FRAMES
            and face_best is not None
            and str(face_best["person_uid"]) == person_uid
        )
        if allow_face:
            allowed_row_indices.update(index for index, _ in matched_face)
    else:
        matched_face = largest_consistent_cluster(
            face_entries,
            FACE_MATCH_THRESHOLD,
            MIN_CONSISTENT_FACE_FRAMES,
        )
        allow_face = bool(allow_body and matched_face)
        if allow_face:
            allowed_row_indices.update(index for index, _ in matched_face)

    added_count = 0

    for row_index, row in enumerate(eligible_rows):
        modality = str(row["modality"])
        if row_index not in allowed_row_indices:
            continue
        modality_embeddings = permanent_embeddings.setdefault(
            modality,
            [],
        )
        if (
            len(modality_embeddings)
            >= MAX_PERSON_GALLERY
        ):
            continue

        embedding = blob_to_embedding(
            row["embedding"],
            int(row["embedding_dim"]),
        )

        duplicate = any(
            cosine_similarity(
                embedding,
                existing,
            )
            >= GALLERY_DUPLICATE_THRESHOLD
            for existing
            in modality_embeddings
        )

        if duplicate:
            continue

        connection.execute(
            """
            INSERT INTO person_embeddings (
                person_uid,
                node_id,
                captured_at,
                quality,
                modality,
                embedding_dim,
                embedding
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                person_uid,
                row["node_id"],
                row["captured_at"],
                float(row["quality"]),
                modality,
                int(row["embedding_dim"]),
                embedding_to_blob(
                    embedding,
                    int(row["embedding_dim"]),
                ),
            ),
        )

        modality_embeddings.append(
            embedding
        )

        added_count += 1

    # 이동 중 임시 Gallery 삭제
    connection.execute(
        """
        DELETE FROM journey_gallery
        WHERE journey_id = ?
        """,
        (journey_id,),
    )

    return added_count


def discard_journey_gallery(
    connection: sqlite3.Connection,
    journey_id: str,
) -> int:
    """
    신뢰도가 충분하지 않은 RETURNING Journey는 추적에는 사용하되
    Person 영구 Gallery에는 섞지 않는다. REVIEW_REQUIRED Gallery는
    후속 검토를 위해 별도로 보존한다.
    """

    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM journey_gallery
        WHERE journey_id = ?
        """,
        (journey_id,),
    ).fetchone()

    removed_count = (
        int(row["count"])
        if row is not None
        else 0
    )

    connection.execute(
        """
        DELETE FROM journey_gallery
        WHERE journey_id = ?
        """,
        (journey_id,),
    )

    return removed_count


class ReviewResolutionConflict(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _find_review_case_for_resolution(
    connection: sqlite3.Connection,
    review_id: str | None,
    journey_id: str | None,
) -> sqlite3.Row:
    if review_id is None and journey_id is None:
        raise ReviewResolutionConflict("REVIEW_IDENTIFIER_REQUIRED")

    clauses: list[str] = []
    parameters: list[str] = []
    if review_id is not None:
        if journey_id is None:
            clauses.append(
                "(review_cases.review_id = ? "
                "OR review_cases.journey_id = ?)"
            )
            parameters.extend((str(review_id), str(review_id)))
        else:
            clauses.append("review_cases.review_id = ?")
            parameters.append(str(review_id))
    if journey_id is not None:
        clauses.append("review_cases.journey_id = ?")
        parameters.append(str(journey_id))

    row = connection.execute(
        f"""
        SELECT
            review_cases.*,
            journeys.status AS journey_status,
            journeys.person_uid AS journey_person_uid,
            journeys.person_status AS journey_person_status,
            journeys.visit_no AS journey_visit_no,
            journeys.request_id AS journey_request_id,
            journeys.entry_at,
            journeys.arrival_at,
            journeys.completed_at,
            persons.status AS provisional_status,
            persons.visit_count AS provisional_visit_count
        FROM review_cases
        JOIN journeys
          ON journeys.journey_id = review_cases.journey_id
        JOIN persons
          ON persons.person_uid = review_cases.provisional_person_uid
        WHERE {' AND '.join(clauses)}
        """,
        tuple(parameters),
    ).fetchone()
    if row is None:
        raise ReviewResolutionConflict("REVIEW_CASE_NOT_FOUND")
    return row


def _validate_pending_review(row: sqlite3.Row) -> None:
    if str(row["status"]) != "PENDING":
        raise ReviewResolutionConflict("REVIEW_ALREADY_RESOLVED")
    if (
        bool(row["pending_person_created"])
        and str(row["provisional_status"]) not in {
            "IDENTITY_PENDING",
            "REVIEW_REQUIRED",
        }
    ):
        raise ReviewResolutionConflict(
            "PROVISIONAL_PERSON_NOT_REVIEW_REQUIRED"
        )
    if str(row["journey_status"]) not in {
        "WAITING_B_OR_C",
        "WAITING_D",
        "COMPLETED",
    }:
        raise ReviewResolutionConflict("REVIEW_JOURNEY_NOT_RESOLVABLE")
    if (
        str(row["journey_person_uid"])
        != str(row["provisional_person_uid"])
    ):
        raise ReviewResolutionConflict("REVIEW_JOURNEY_OWNER_MISMATCH")


def _review_result(
    row: sqlite3.Row | None,
    action: str,
    outcome: str,
    *,
    reason: str | None = None,
    target_person_uid: str | None = None,
    promoted_count: int = 0,
    target_visit_count: int | None = None,
    requested_review_id: str | None = None,
    requested_journey_id: str | None = None,
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "reason": reason,
        "action": action,
        "review_id": (
            str(row["review_id"])
            if row is not None
            else requested_review_id
        ),
        "journey_id": (
            str(row["journey_id"])
            if row is not None
            else requested_journey_id
        ),
        "provisional_person_uid": (
            str(row["provisional_person_uid"])
            if row is not None
            else None
        ),
        "target_person_uid": target_person_uid,
        "promoted_count": promoted_count,
        "target_visit_count": target_visit_count,
    }


def resolve_review_confirm_new(
    review_id: str | None = None,
    *,
    journey_id: str | None = None,
) -> dict[str, Any]:
    """완료된 Review Journey의 provisional Person을 정식 확정한다."""
    action = "CONFIRM_NEW"
    row: sqlite3.Row | None = None
    with db_lock:
        connection = connect_db()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = _find_review_case_for_resolution(
                connection,
                review_id,
                journey_id,
            )
            provisional_uid = str(row["provisional_person_uid"])
            pending_person_created = bool(row["pending_person_created"])

            if str(row["status"]) == "RESOLVED":
                if str(row["action"]) == action and row["target_person_uid"]:
                    resolved_uid = str(row["target_person_uid"])
                    target = connection.execute(
                        "SELECT visit_count FROM persons WHERE person_uid = ?",
                        (resolved_uid,),
                    ).fetchone()
                    connection.commit()
                    return _review_result(
                        row,
                        action,
                        "ALREADY_RESOLVED",
                        target_person_uid=resolved_uid,
                        target_visit_count=(
                            int(target["visit_count"])
                            if target is not None
                            else None
                        ),
                    )
                raise ReviewResolutionConflict(
                    "REVIEW_ALREADY_RESOLVED"
                )

            _validate_pending_review(row)
            if pending_person_created:
                target_uid = provisional_uid
                target_visit_count = int(row["provisional_visit_count"])
            else:
                target_uid = create_new_person(
                    connection,
                    str(row["entry_at"]),
                    "ACTIVE",
                )
                target_visit_count = 1
                connection.execute(
                    """
                    UPDATE persons
                    SET visit_count = 1, last_seen_at = ?
                    WHERE person_uid = ?
                    """,
                    (row["completed_at"] or row["entry_at"], target_uid),
                )
                connection.execute(
                    """
                    UPDATE journeys
                    SET person_uid = ?, visit_no = 1
                    WHERE journey_id = ?
                    """,
                    (target_uid, row["journey_id"]),
                )
                connection.execute(
                    "UPDATE journey_captures SET person_uid = ? WHERE journey_id = ?",
                    (target_uid, row["journey_id"]),
                )
                connection.execute(
                    "UPDATE journey_node_visits SET person_uid = ? WHERE journey_id = ?",
                    (target_uid, row["journey_id"]),
                )
            connection.execute(
                "UPDATE captures SET person_uid = ? WHERE journey_id = ?",
                (target_uid, row["journey_id"]),
            )
            promoted_count = promote_journey_gallery(
                connection,
                str(row["journey_id"]),
                target_uid,
            )
            resolved_at = now_iso()
            connection.execute(
                """
                UPDATE persons
                SET
                    status = 'ACTIVE',
                    merged_into_person_uid = NULL
                WHERE person_uid = ?
                """,
                (target_uid,),
            )
            connection.execute(
                """
                UPDATE journeys
                SET
                    person_status = 'NEW',
                    gallery_promotion_allowed = 1,
                    identity_result = 'NEW',
                    review_status = 'RESOLVED',
                    canonical_person_uid = ?
                WHERE journey_id = ?
                """,
                (target_uid, row["journey_id"]),
            )
            connection.execute(
                """
                UPDATE review_cases
                SET
                    status = 'RESOLVED',
                    action = 'CONFIRM_NEW',
                    target_person_uid = ?,
                    canonical_person_uid = ?,
                    resolution_source = COALESCE(
                        resolution_source,
                        'MANUAL_REVIEW'
                    ),
                    resolved_at = ?
                WHERE review_id = ?
                  AND status = 'PENDING'
                """,
                (
                    target_uid,
                    target_uid,
                    resolved_at,
                    row["review_id"],
                ),
            )
            if row["journey_request_id"]:
                choose_automatic_representative(
                    connection,
                    target_uid,
                    str(row["journey_request_id"]),
                    resolved_at,
                )
            connection.commit()
            return _review_result(
                row,
                action,
                "RESOLVED",
                target_person_uid=target_uid,
                promoted_count=promoted_count,
                target_visit_count=target_visit_count,
            )
        except ReviewResolutionConflict as conflict:
            connection.rollback()
            return _review_result(
                row,
                action,
                "CONFLICT",
                reason=conflict.reason,
                requested_review_id=review_id,
                requested_journey_id=journey_id,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def resolve_review_merge_existing(
    review_id: str | None = None,
    target_person_uid: str = "",
    *,
    journey_id: str | None = None,
) -> dict[str, Any]:
    """Review Journey를 기존 canonical Person에 병합한다."""
    action = "MERGE_EXISTING"
    row: sqlite3.Row | None = None
    normalized_target_uid = str(target_person_uid).strip()
    with db_lock:
        connection = connect_db()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = _find_review_case_for_resolution(
                connection,
                review_id,
                journey_id,
            )
            provisional_uid = str(row["provisional_person_uid"])

            if str(row["status"]) == "RESOLVED":
                if (
                    str(row["action"]) == action
                    and str(row["target_person_uid"])
                    == normalized_target_uid
                ):
                    target = connection.execute(
                        """
                        SELECT visit_count
                        FROM persons
                        WHERE person_uid = ?
                        """,
                        (normalized_target_uid,),
                    ).fetchone()
                    connection.commit()
                    return _review_result(
                        row,
                        action,
                        "ALREADY_RESOLVED",
                        target_person_uid=normalized_target_uid,
                        target_visit_count=(
                            int(target["visit_count"])
                            if target is not None
                            else None
                        ),
                    )
                raise ReviewResolutionConflict(
                    "REVIEW_ALREADY_RESOLVED"
                )

            _validate_pending_review(row)
            if not normalized_target_uid:
                raise ReviewResolutionConflict(
                    "TARGET_PERSON_REQUIRED"
                )
            pending_person_created = bool(row["pending_person_created"])
            if normalized_target_uid == provisional_uid and pending_person_created:
                raise ReviewResolutionConflict(
                    "TARGET_IS_PROVISIONAL_PERSON"
                )

            target = connection.execute(
                """
                SELECT
                    person_uid,
                    status,
                    visit_count,
                    last_seen_at,
                    merged_into_person_uid
                FROM persons
                WHERE person_uid = ?
                """,
                (normalized_target_uid,),
            ).fetchone()
            if target is None:
                raise ReviewResolutionConflict("TARGET_PERSON_NOT_FOUND")
            if str(target["status"]) != "ACTIVE":
                raise ReviewResolutionConflict(
                    "TARGET_PERSON_NOT_CANONICAL_ACTIVE"
                )

            promoted_count = promote_journey_gallery(
                connection,
                str(row["journey_id"]),
                normalized_target_uid,
            )
            target_visit_count = int(target["visit_count"]) + 1
            review_last_seen = (
                row["completed_at"]
                or row["arrival_at"]
                or row["entry_at"]
            )
            target_last_seen = str(target["last_seen_at"])
            review_epoch = parse_iso_epoch(str(review_last_seen))
            target_epoch = parse_iso_epoch(target_last_seen)
            merged_last_seen = (
                str(review_last_seen)
                if target_epoch is None
                or (
                    review_epoch is not None
                    and review_epoch > target_epoch
                )
                else target_last_seen
            )

            connection.execute(
                """
                UPDATE persons
                SET
                    visit_count = ?,
                    last_seen_at = ?
                WHERE person_uid = ?
                """,
                (
                    target_visit_count,
                    merged_last_seen,
                    normalized_target_uid,
                ),
            )
            if pending_person_created:
                connection.execute(
                    """
                    UPDATE persons
                    SET status = 'MERGED', merged_into_person_uid = ?
                    WHERE person_uid = ?
                    """,
                    (normalized_target_uid, provisional_uid),
                )
            connection.execute(
                """
                UPDATE journeys
                SET
                    person_uid = ?,
                    visit_no = ?,
                    person_status = 'RETURNING',
                    identity_result = 'RETURNING',
                    review_status = 'RESOLVED',
                    canonical_person_uid = ?,
                    gallery_promotion_allowed = 1
                WHERE journey_id = ?
                """,
                (
                    normalized_target_uid,
                    target_visit_count,
                    normalized_target_uid,
                    row["journey_id"],
                ),
            )
            connection.execute(
                """
                UPDATE journey_captures
                SET person_uid = ?
                WHERE journey_id = ?
                """,
                (
                    normalized_target_uid,
                    row["journey_id"],
                ),
            )
            connection.execute(
                """
                UPDATE journey_node_visits
                SET
                    person_uid = ?,
                    updated_at = ?
                WHERE journey_id = ?
                """,
                (
                    normalized_target_uid,
                    now_iso(),
                    row["journey_id"],
                ),
            )
            connection.execute(
                "UPDATE captures SET person_uid = ? WHERE journey_id = ?",
                (normalized_target_uid, row["journey_id"]),
            )
            resolved_at = now_iso()
            connection.execute(
                """
                UPDATE review_cases
                SET
                    status = 'RESOLVED',
                    action = 'MERGE_EXISTING',
                    target_person_uid = ?,
                    canonical_person_uid = ?,
                    resolution_source = COALESCE(
                        resolution_source,
                        'MANUAL_REVIEW'
                    ),
                    resolved_at = ?
                WHERE review_id = ?
                  AND status = 'PENDING'
                """,
                (
                    normalized_target_uid,
                    normalized_target_uid,
                    resolved_at,
                    row["review_id"],
                ),
            )
            if row["journey_request_id"]:
                choose_automatic_representative(
                    connection,
                    normalized_target_uid,
                    str(row["journey_request_id"]),
                    resolved_at,
                )
            connection.commit()
            return _review_result(
                row,
                action,
                "RESOLVED",
                target_person_uid=normalized_target_uid,
                promoted_count=promoted_count,
                target_visit_count=target_visit_count,
            )
        except ReviewResolutionConflict as conflict:
            connection.rollback()
            return _review_result(
                row,
                action,
                "CONFLICT",
                reason=conflict.reason,
                target_person_uid=normalized_target_uid,
                requested_review_id=review_id,
                requested_journey_id=journey_id,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def _identity_score_summary(
    match_result: dict[str, Any],
) -> dict[str, Any]:
    best = match_result.get("best_candidate")
    second_score = float(
        match_result.get("second_combined_score", -1.0)
    )
    if best is None:
        return {
            "person_uid": None,
            "best_score": None,
            "topk_score": None,
            "combined_score": None,
            "second_score": None,
            "match_margin": None,
            "sample_count": 0,
        }
    combined_score = float(best["combined_score"])
    return {
        "person_uid": str(best["person_uid"]),
        "best_score": float(best["best_score"]),
        "topk_score": float(best["topk_mean"]),
        "combined_score": combined_score,
        "second_score": second_score if second_score >= 0 else None,
        "match_margin": (
            combined_score - second_score
            if second_score >= 0
            else None
        ),
        "sample_count": int(best["sample_count"]),
    }


def _body_identity_flags(
    match_result: dict[str, Any],
) -> tuple[bool, bool]:
    best = match_result.get("best_candidate")
    if best is None:
        return False, False
    best_score = float(best["best_score"])
    topk_score = float(best["topk_mean"])
    combined_score = float(best["combined_score"])
    second_score = float(
        match_result.get("second_combined_score", -1.0)
    )
    margin_ok = (
        second_score < 0
        or combined_score - second_score >= PERSON_MATCH_MARGIN
    )
    returning = (
        best_score >= PERSON_MATCH_THRESHOLD
        and topk_score >= PERSON_TOPK_THRESHOLD
        and combined_score >= PERSON_COMBINED_THRESHOLD
        and margin_ok
        and (
            int(best.get("permanent_count", 0)) > 0
            or combined_score >= PERSON_ACTIVE_ONLY_THRESHOLD
        )
    )
    review_candidate = (
        best_score >= PERSON_REVIEW_THRESHOLD
        or combined_score >= PERSON_REVIEW_COMBINED_THRESHOLD
    )
    return returning, review_candidate


def evaluate_final_route_identity(
    connection: sqlite3.Connection,
    journey_id: str,
) -> dict[str, Any]:
    review = connection.execute(
        """
        SELECT
            review_cases.*,
            journeys.person_uid,
            journeys.person_status,
            journeys.status AS journey_status,
            journeys.route_json AS journey_route_json,
            journeys.entry_local_track_id
        FROM review_cases
        JOIN journeys
          ON journeys.journey_id = review_cases.journey_id
        WHERE review_cases.journey_id = ?
        """,
        (journey_id,),
    ).fetchone()
    if review is None:
        raise ValueError(f"Final Review Case를 찾을 수 없음: {journey_id}")
    if str(review["journey_status"]) != "COMPLETED":
        raise ValueError(f"Final Review Journey가 완료되지 않음: {journey_id}")

    temporary_uid = str(review["provisional_person_uid"])
    gallery_rows = connection.execute(
        """
        SELECT node_id, modality, embedding_dim, embedding
        FROM journey_gallery
        WHERE journey_id = ?
        ORDER BY gallery_id
        """,
        (journey_id,),
    ).fetchall()
    body_all: list[np.ndarray] = []
    body_route: list[np.ndarray] = []
    face_all: list[np.ndarray] = []
    for row in gallery_rows:
        embedding = blob_to_embedding(
            row["embedding"],
            int(row["embedding_dim"]),
        )
        modality = str(row["modality"])
        if modality == "BODY":
            body_all.append(embedding)
            if str(row["node_id"]) != "A":
                body_route.append(embedding)
        elif modality == "FACE":
            face_all.append(embedding)

    excluded = (
        {temporary_uid}
        if bool(review["pending_person_created"])
        else set()
    )
    body_result = find_existing_person(
        connection,
        body_all,
        "BODY",
        permanent_only=True,
        exclude_person_uids=excluded,
    )
    route_body_result = find_existing_person(
        connection,
        body_route,
        "BODY",
        permanent_only=True,
        exclude_person_uids=excluded,
    )
    face_result = find_existing_person(
        connection,
        face_all,
        "FACE",
        permanent_only=True,
        exclude_person_uids=excluded,
    )
    body_returning, _ = _body_identity_flags(body_result)
    route_returning, route_review = _body_identity_flags(
        route_body_result
    )
    body_best = body_result.get("best_candidate")
    route_best = route_body_result.get("best_candidate")
    strong_face = _strong_face_candidate(face_result)
    body_uid = str(body_best["person_uid"]) if body_best else None
    route_uid = str(route_best["person_uid"]) if route_best else None
    face_uid = (
        str(strong_face["person_uid"])
        if strong_face is not None
        else None
    )
    face_conflict = face_uid is not None and face_uid != body_uid

    if (
        body_returning
        and route_returning
        and body_uid == route_uid
        and not face_conflict
    ):
        final_result = "REVISIT"
        canonical_uid = body_uid
    elif body_route and not route_review and strong_face is None:
        final_result = "NEW"
        canonical_uid = temporary_uid
    else:
        final_result = "MANUAL_REVIEW_REQUIRED"
        canonical_uid = None

    route = safe_json_loads(review["journey_route_json"], [])
    final_scores = {
        "body_all": _identity_score_summary(body_result),
        "body_route": _identity_score_summary(route_body_result),
        "face": _identity_score_summary(face_result),
        "body_evidence_count": len(body_all),
        "route_body_evidence_count": len(body_route),
        "face_evidence_count": len(face_all),
        "thresholds": {
            "match_best": PERSON_MATCH_THRESHOLD,
            "match_topk": PERSON_TOPK_THRESHOLD,
            "match_combined": PERSON_COMBINED_THRESHOLD,
            "review_best": PERSON_REVIEW_THRESHOLD,
            "review_combined": PERSON_REVIEW_COMBINED_THRESHOLD,
            "margin": PERSON_MATCH_MARGIN,
            "face_match": FACE_MATCH_THRESHOLD,
        },
    }
    final_candidate_uid = body_uid or face_uid
    return {
        "review_id": str(review["review_id"]),
        "journey_id": journey_id,
        "route": route,
        "temporary_person_uid": temporary_uid,
        "initial_decision": (
            str(review["initial_decision"])
            if review["initial_decision"] is not None
            else "IDENTITY_PENDING"
        ),
        "initial_candidate_person_uid": review["candidate_person_uid"],
        "final_review_result": final_result,
        "final_candidate_person_uid": final_candidate_uid,
        "canonical_person_uid": canonical_uid,
        "final_scores": final_scores,
        "entry_local_track_id": review["entry_local_track_id"],
    }


def _record_final_route_identity(
    result: dict[str, Any],
    *,
    canonical_person_uid: str | None,
    resolution_outcome: str,
) -> None:
    reviewed_at = now_iso()
    with db_lock:
        with connect_db() as connection:
            if result["final_review_result"] == "MANUAL_REVIEW_REQUIRED":
                connection.execute(
                    """
                    UPDATE persons
                    SET status = 'REVIEW_REQUIRED'
                    WHERE person_uid = ?
                      AND status = 'IDENTITY_PENDING'
                    """,
                    (result["temporary_person_uid"],),
                )
                connection.execute(
                    """
                    UPDATE journeys
                    SET
                        person_status = 'REVIEW_REQUIRED',
                        gallery_promotion_allowed = 0,
                        identity_result = 'UNKNOWN',
                        review_status = 'PENDING'
                    WHERE journey_id = ?
                    """,
                    (result["journey_id"],),
                )
            elif result["final_review_result"] == "NEW":
                connection.execute(
                    """
                    UPDATE journeys
                    SET
                        person_status = 'NEW',
                        gallery_promotion_allowed = 1
                    WHERE journey_id = ?
                    """,
                    (result["journey_id"],),
                )

            scores = dict(result["final_scores"])
            scores["resolution_outcome"] = resolution_outcome
            connection.execute(
                """
                UPDATE review_cases
                SET
                    final_review_result = ?,
                    final_candidate_person_uid = ?,
                    canonical_person_uid = ?,
                    final_scores_json = ?,
                    route_json = ?,
                    resolution_source = 'FINAL_ROUTE_IDENTITY',
                    final_reviewed_at = ?
                WHERE review_id = ?
                """,
                (
                    result["final_review_result"],
                    result["final_candidate_person_uid"],
                    canonical_person_uid,
                    json.dumps(scores, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(result["route"], separators=(",", ":")),
                    reviewed_at,
                    result["review_id"],
                ),
            )


def resolve_final_route_identity(
    journey_id: str,
) -> dict[str, Any]:
    with db_lock:
        with connect_db() as connection:
            existing = connection.execute(
                """
                SELECT final_review_result, canonical_person_uid
                FROM review_cases
                WHERE journey_id = ?
                """,
                (journey_id,),
            ).fetchone()
            if existing is None:
                raise ValueError(f"Final Review Case를 찾을 수 없음: {journey_id}")
            if existing["final_review_result"] is not None:
                return {
                    "journey_id": journey_id,
                    "final_review_result": str(
                        existing["final_review_result"]
                    ),
                    "canonical_person_uid": existing[
                        "canonical_person_uid"
                    ],
                    "already_resolved": True,
                }
            result = evaluate_final_route_identity(connection, journey_id)

    decision = str(result["final_review_result"])
    resolution_outcome = "PENDING_MANUAL_REVIEW"
    canonical_uid = result["canonical_person_uid"]
    promoted_count = 0
    target_visit_count: int | None = None
    if decision == "REVISIT":
        resolution = resolve_review_merge_existing(
            result["review_id"],
            str(canonical_uid),
        )
        if resolution["outcome"] not in {"RESOLVED", "ALREADY_RESOLVED"}:
            result["final_review_result"] = "MANUAL_REVIEW_REQUIRED"
            result["canonical_person_uid"] = None
            canonical_uid = None
            resolution_outcome = str(resolution.get("reason") or "MERGE_CONFLICT")
        else:
            resolution_outcome = str(resolution["outcome"])
            promoted_count = int(resolution.get("promoted_count") or 0)
            target_visit_count = resolution.get("target_visit_count")
            canonical_uid = resolution.get("target_person_uid")
            result["canonical_person_uid"] = canonical_uid
    elif decision == "NEW":
        resolution = resolve_review_confirm_new(result["review_id"])
        if resolution["outcome"] not in {"RESOLVED", "ALREADY_RESOLVED"}:
            result["final_review_result"] = "MANUAL_REVIEW_REQUIRED"
            result["canonical_person_uid"] = None
            canonical_uid = None
            resolution_outcome = str(resolution.get("reason") or "CONFIRM_CONFLICT")
        else:
            resolution_outcome = str(resolution["outcome"])
            promoted_count = int(resolution.get("promoted_count") or 0)
            target_visit_count = resolution.get("target_visit_count")
            canonical_uid = resolution.get("target_person_uid")
            result["canonical_person_uid"] = canonical_uid

    _record_final_route_identity(
        result,
        canonical_person_uid=(str(canonical_uid) if canonical_uid else None),
        resolution_outcome=resolution_outcome,
    )
    result.update(
        {
            "canonical_person_uid": canonical_uid,
            "promoted_count": promoted_count,
            "target_visit_count": target_visit_count,
            "resolution_outcome": resolution_outcome,
            "already_resolved": False,
        }
    )
    return result


def print_final_identity_review(
    result: dict[str, Any],
    d_local_track_id: Any = None,
) -> None:
    score = result["final_scores"]["body_all"]
    route_text = " -> ".join(str(node) for node in result["route"])
    final_score_text = (
        f"{float(score['combined_score']):.3f}"
        if score["combined_score"] is not None
        else "None"
    )
    final_margin_text = (
        f"{float(score['match_margin']):.3f}"
        if score["match_margin"] is not None
        else "None"
    )
    print("\n===== FINAL IDENTITY REVIEW =====")
    print(f"Journey ID       : {result['journey_id']}")
    print(f"Route            : {route_text}")
    print(f"Temporary UID    : {result['temporary_person_uid']}")
    print(f"A Local ID       : {result.get('entry_local_track_id')}")
    print(f"D LOCAL TRACK    : {d_local_track_id}")
    print(f"A Decision       : {result['initial_decision']}")
    print(f"DB Candidate     : {result['initial_candidate_person_uid']}")
    print(f"Final Best UID   : {score['person_uid']}")
    print(f"Final Score      : {final_score_text}")
    print(f"Final Margin     : {final_margin_text}")
    print(f"REVIEW RESULT    : {result['final_review_result']}")
    if result["final_review_result"] == "REVISIT":
        print(f"PERSON ID        : {result['canonical_person_uid']}")
        print(f"MERGED FROM      : {result['temporary_person_uid']}")
        print("VISIT TYPE       : RETURNING")
        print(f"VISIT COUNT      : {result['target_visit_count']}")
    elif result["final_review_result"] == "NEW":
        print(f"PERSON ID        : {result['canonical_person_uid']}")
        print("VISIT TYPE       : FIRST VISIT")
    else:
        print(f"CANDIDATE ID     : {result['final_candidate_person_uid']}")
    print("=================================")


# ============================================================
# D ARRIVAL 처리
# ============================================================

def _aware_timestamp(value: Any, field_name: str) -> datetime:
    if value is None or str(value).strip() == "":
        raise ValueError(f"MISSING_{field_name.upper()}")
    normalized = str(value).strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(f"INVALID_{field_name.upper()}") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"TIMEZONE_MISSING_{field_name.upper()}")
    return parsed


def _arrival_event_key(payload: dict[str, Any]) -> str:
    supplied = (
        payload.get("arrival_event_id")
        or payload.get("event_id")
        or payload.get("request_id")
    )
    if supplied is not None and str(supplied).strip():
        return f"D:{str(supplied).strip()}"
    identity = {
        "journey_id": payload.get("journey_id"),
        "local_track_id": extract_local_track_id(payload),
        "d_track_first_seen_at": payload.get("d_track_first_seen_at"),
        "d_arrival_timestamp": payload.get(
            "d_arrival_timestamp", payload.get("timestamp")
        ),
    }
    encoded = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "D:sha256:" + hashlib.sha256(encoded).hexdigest()


def _d_track_key(payload: dict[str, Any]) -> str | None:
    local_track_id = extract_local_track_id(payload)
    first_seen = payload.get("d_track_first_seen_at")
    if local_track_id is None or first_seen is None:
        return None
    return f"D:{local_track_id}:{str(first_seen).strip()}"


def _float_field(
    payload: dict[str, Any], field_name: str, errors: list[str]
) -> float | None:
    value = payload.get(field_name)
    if value is None:
        errors.append(f"MISSING_{field_name.upper()}")
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        errors.append(f"INVALID_{field_name.upper()}")
        return None
    if not np.isfinite(result):
        errors.append(f"INVALID_{field_name.upper()}")
        return None
    return result


def _int_field(
    payload: dict[str, Any], field_name: str, errors: list[str]
) -> int | None:
    value = payload.get(field_name)
    if value is None:
        errors.append(f"MISSING_{field_name.upper()}")
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        errors.append(f"INVALID_{field_name.upper()}")
        return None
    return result


def validate_d_arrival(
    connection: sqlite3.Connection,
    journey: sqlite3.Row | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    if payload.get("schema_version") not in (1, "1"):
        errors.append("INVALID_SCHEMA_VERSION")
    if str(payload.get("event") or "").upper() != "ARRIVAL":
        errors.append("INVALID_D_EVENT")
    if str(payload.get("node_id") or "").upper() != "D":
        errors.append("INVALID_D_NODE")
    arrival_value = payload.get(
        "d_arrival_timestamp", payload.get("timestamp")
    )
    parsed_times: dict[str, datetime] = {}
    for field_name, value in (
        ("d_arrival_timestamp", arrival_value),
        ("d_track_first_seen_at", payload.get("d_track_first_seen_at")),
        ("passage_timestamp", payload.get("passage_timestamp")),
        ("candidate_received_at", payload.get("candidate_received_at")),
    ):
        try:
            parsed_times[field_name] = _aware_timestamp(value, field_name)
        except ValueError as error:
            errors.append(str(error))

    route: list[str] = []
    middle_node: str | None = None
    database_passage: datetime | None = None
    passage_event_at: str | None = None
    if journey is None:
        errors.append("JOURNEY_NOT_FOUND")
    else:
        journey_status = str(journey["status"])
        if journey_status != "WAITING_D":
            errors.append(
                "JOURNEY_ALREADY_TERMINAL"
                if journey_status in {
                    "COMPLETED", "EXPIRED", "CANCELLED", "REJECTED"
                }
                else "JOURNEY_NOT_WAITING_D"
            )
        route = safe_json_loads(journey["route_json"], [])
        if route not in (["A", "B"], ["A", "C"]):
            errors.append("INVALID_CENTRAL_ROUTE")
        else:
            middle_node = route[1]
            passage_event = connection.execute(
                """
                SELECT event_id, event_at FROM journey_events
                WHERE journey_id = ? AND node_id = ? AND event_type = 'PASSAGE'
                ORDER BY event_id DESC LIMIT 1
                """,
                (journey["journey_id"], middle_node),
            ).fetchone()
            if passage_event is None:
                errors.append("MISSING_CENTRAL_PASSAGE_EVENT")
            else:
                passage_event_at = str(passage_event["event_at"])
        if journey["passage_at"] is None:
            errors.append("MISSING_CENTRAL_PASSAGE_TIMESTAMP")
        else:
            try:
                database_passage = _aware_timestamp(
                    journey["passage_at"], "central_passage_timestamp"
                )
            except ValueError as error:
                errors.append(str(error))
        payload_person_uids = [
            str(value)
            for value in (
                payload.get("person_uid"),
                payload.get("global_person_id"),
                payload.get("tracking_person_uid"),
            )
            if value is not None and str(value).strip()
        ]
        if not payload_person_uids or any(
            value != str(journey["person_uid"])
            for value in payload_person_uids
        ):
            errors.append("ACTIVE_CANDIDATE_MISMATCH")

    arrival = parsed_times.get("d_arrival_timestamp")
    first_seen = parsed_times.get("d_track_first_seen_at")
    reported_passage = parsed_times.get("passage_timestamp")
    candidate_received = parsed_times.get("candidate_received_at")
    if database_passage is not None:
        central_age_seconds = (
            datetime.now().astimezone() - database_passage
        ).total_seconds()
        if central_age_seconds < -D_CLOCK_TOLERANCE_SECONDS:
            errors.append("CENTRAL_PASSAGE_TIMESTAMP_IN_FUTURE")
        elif central_age_seconds > WAITING_D_TIMEOUT_SECONDS:
            errors.append("WAITING_D_TTL_EXCEEDED")
        expected_offset = database_passage.utcoffset()
        for field_name, parsed in parsed_times.items():
            if parsed.utcoffset() != expected_offset:
                errors.append(f"TIMEZONE_MISMATCH_{field_name.upper()}")
        if reported_passage is not None and abs(
            (reported_passage - database_passage).total_seconds()
        ) > D_CLOCK_TOLERANCE_SECONDS:
            errors.append("PASSAGE_TIMESTAMP_MISMATCH")
        if first_seen is not None and (
            first_seen - database_passage
        ).total_seconds() < -D_CLOCK_TOLERANCE_SECONDS:
            errors.append("D_TRACK_SEEN_BEFORE_PASSAGE")
        if candidate_received is not None and (
            candidate_received - database_passage
        ).total_seconds() < -D_CLOCK_TOLERANCE_SECONDS:
            errors.append("CANDIDATE_RECEIVED_BEFORE_PASSAGE")
    actual_duration: float | None = None
    if arrival is not None and database_passage is not None:
        actual_duration = (arrival - database_passage).total_seconds()
        if actual_duration <= 0:
            errors.append("NON_POSITIVE_TRAVEL_TIME")
        elif actual_duration < D_MIN_TRAVEL_SECONDS:
            errors.append("TRAVEL_TIME_BELOW_MINIMUM")
        if actual_duration > D_MAX_TRAVEL_SECONDS:
            errors.append("TRAVEL_TIME_ABOVE_MAXIMUM")
        if actual_duration > WAITING_D_TIMEOUT_SECONDS:
            errors.append("WAITING_D_TTL_EXCEEDED")
    if arrival is not None:
        if first_seen is not None and first_seen > arrival:
            errors.append("D_TRACK_FIRST_SEEN_AFTER_ARRIVAL")
        if candidate_received is not None and candidate_received > arrival:
            errors.append("CANDIDATE_RECEIVED_AFTER_ARRIVAL")

    reported_duration = _float_field(
        payload, "passage_to_d_duration_seconds", errors
    )
    if reported_duration is not None:
        if reported_duration <= 0:
            errors.append("NON_POSITIVE_REPORTED_TRAVEL_TIME")
        if (
            actual_duration is not None
            and abs(reported_duration - actual_duration)
            > D_CLOCK_TOLERANCE_SECONDS
        ):
            errors.append("TRAVEL_DURATION_MISMATCH")

    sample_count = _int_field(
        payload, "confirmation_sample_count", errors
    )
    pass_count = _int_field(payload, "confirmation_pass_count", errors)
    if sample_count is not None and sample_count < D_MIN_CONFIRMATION_SAMPLES:
        errors.append("INSUFFICIENT_CONFIRMATION_SAMPLES")
    if pass_count is not None and pass_count < D_MIN_CONFIRMATION_PASSES:
        errors.append("INSUFFICIENT_CONFIRMATION_PASSES")
    if (
        sample_count is not None
        and pass_count is not None
        and pass_count > sample_count
    ):
        errors.append("CONFIRMATION_PASS_COUNT_EXCEEDS_SAMPLES")

    best_score = _float_field(payload, "best_journey_score", errors)
    second_score = _float_field(payload, "second_journey_score", errors)
    journey_margin = _float_field(payload, "journey_margin", errors)
    if journey_margin is not None and journey_margin < D_MIN_JOURNEY_MARGIN:
        errors.append("INSUFFICIENT_JOURNEY_MARGIN")
    if best_score is not None and second_score is not None:
        calculated_margin = best_score - second_score
        if calculated_margin < D_MIN_JOURNEY_MARGIN:
            errors.append("INSUFFICIENT_CALCULATED_JOURNEY_MARGIN")
        if (
            journey_margin is not None
            and abs(calculated_margin - journey_margin)
            > D_CLOCK_TOLERANCE_SECONDS / 100.0
        ):
            errors.append("JOURNEY_MARGIN_MISMATCH")

    eligibility_reason = str(
        payload.get("eligibility_reason") or ""
    ).strip().upper()
    if not eligibility_reason:
        errors.append("MISSING_ELIGIBILITY_REASON")
    elif eligibility_reason not in D_ELIGIBLE_REASONS:
        errors.append("ELIGIBILITY_REASON_NOT_ALLOWED")

    track_key = _d_track_key(payload)
    if track_key is None:
        errors.append("MISSING_D_TRACK_IDENTITY")
    elif journey is not None:
        owner = connection.execute(
            """
            SELECT journey_id FROM d_arrival_attempts
            WHERE d_track_key = ? AND accepted = 1 AND journey_id <> ?
            LIMIT 1
            """,
            (track_key, journey["journey_id"]),
        ).fetchone()
        if owner is not None:
            errors.append("D_TRACK_ALREADY_LINKED_TO_OTHER_JOURNEY")

    # Preserve deterministic order while avoiding duplicate reason codes.
    errors = list(dict.fromkeys(errors))
    error_set = set(errors)

    def field_status(
        field_name: str,
        expected_type: str,
        value: Any,
        valid: bool,
    ) -> dict[str, Any]:
        return {
            "field": field_name,
            "present": value is not None,
            "expected_type": expected_type,
            "actual_type": type(value).__name__ if value is not None else None,
            "valid_type": bool(valid),
        }

    def predicate(
        name: str,
        expected: Any,
        actual: Any,
        blocking_codes: tuple[str, ...],
    ) -> dict[str, Any]:
        passed = not any(code in error_set for code in blocking_codes)
        return {
            "name": name,
            "predicate": name.upper(),
            "expected": expected,
            "actual": actual,
            "pass": passed,
            "passed": passed,
            "failure_codes": [
                code for code in blocking_codes if code in error_set
            ],
        }

    number_fields = (
        "passage_to_d_duration_seconds",
        "confirmation_sample_count",
        "confirmation_pass_count",
        "best_journey_score",
        "second_journey_score",
        "journey_margin",
    )
    timestamp_fields = (
        "passage_timestamp",
        "d_track_first_seen_at",
        "candidate_received_at",
    )
    payload_fields = [
        field_status(
            "journey_id",
            "non-empty string",
            payload.get("journey_id"),
            bool(str(payload.get("journey_id") or "").strip()),
        ),
        field_status(
            "person_uid",
            "non-empty string",
            payload.get("person_uid"),
            bool(str(payload.get("person_uid") or "").strip()),
        ),
        *[
            field_status(
                name,
                "timezone-aware ISO-8601 string",
                payload.get(name),
                name in parsed_times,
            )
            for name in timestamp_fields
        ],
        field_status(
            "d_arrival_timestamp",
            "timezone-aware ISO-8601 string",
            arrival_value,
            "d_arrival_timestamp" in parsed_times,
        ),
        *[
            field_status(
                name,
                "number",
                payload.get(name),
                isinstance(payload.get(name), (int, float))
                and not isinstance(payload.get(name), bool),
            )
            for name in number_fields
        ],
        field_status(
            "eligibility_reason",
            "non-empty string",
            payload.get("eligibility_reason"),
            bool(eligibility_reason),
        ),
        field_status(
            "d_local_track_id",
            "string or integer",
            payload.get("d_local_track_id", payload.get("local_track_id")),
            track_key is not None,
        ),
    ]

    journey_status = str(journey["status"]) if journey is not None else None
    central_person_uid = str(journey["person_uid"]) if journey is not None else None
    canonical_person_uid = (
        journey["canonical_person_uid"] if journey is not None else None
    )
    identity_result = str(journey["identity_result"]) if journey is not None else None
    review_status = str(journey["review_status"]) if journey is not None else None
    identity_confirmed = bool(
        identity_result in {"NEW", "RETURNING"}
        and review_status != "PENDING"
        and canonical_person_uid is not None
    )
    person_status = str(journey["person_status"]) if journey is not None else None
    if person_status == "NEW":
        identity_policy = "CONFIRMED_NEW_COMPLETE_AND_PROMOTE_IF_ALLOWED"
    elif person_status == "RETURNING":
        identity_policy = "CONFIRMED_RETURNING_COMPLETE_AND_PROMOTE_HIGH_CONFIDENCE_ONLY"
    else:
        identity_policy = "PENDING_COMPLETE_ROUTE_THEN_FINAL_REVIEW_NO_AUTO_PROMOTION"

    predicate_results = [
        predicate(
            "schema_version",
            "1",
            payload.get("schema_version"),
            ("INVALID_SCHEMA_VERSION",),
        ),
        predicate(
            "event_type",
            "ARRIVAL",
            payload.get("event"),
            ("INVALID_D_EVENT",),
        ),
        predicate(
            "node_id",
            "D",
            payload.get("node_id"),
            ("INVALID_D_NODE",),
        ),
        predicate("journey_exists", True, journey is not None, ("JOURNEY_NOT_FOUND",)),
        predicate(
            "journey_status",
            "WAITING_D",
            journey_status,
            ("JOURNEY_ALREADY_TERMINAL", "JOURNEY_NOT_WAITING_D"),
        ),
        predicate(
            "central_route",
            "['A','B'] or ['A','C']",
            route,
            ("INVALID_CENTRAL_ROUTE", "MISSING_CENTRAL_PASSAGE_EVENT"),
        ),
        predicate(
            "person_binding",
            central_person_uid,
            {
                "person_uid": payload.get("person_uid"),
                "global_person_id": payload.get("global_person_id"),
                "tracking_person_uid": payload.get("tracking_person_uid"),
            },
            ("ACTIVE_CANDIDATE_MISMATCH",),
        ),
        predicate(
            "passage_timestamp",
            journey["passage_at"] if journey is not None else None,
            payload.get("passage_timestamp"),
            (
                "MISSING_CENTRAL_PASSAGE_TIMESTAMP",
                "PASSAGE_TIMESTAMP_MISMATCH",
                "CENTRAL_PASSAGE_TIMESTAMP_IN_FUTURE",
                "MISSING_PASSAGE_TIMESTAMP",
                "INVALID_PASSAGE_TIMESTAMP",
                "TIMEZONE_MISSING_PASSAGE_TIMESTAMP",
                "TIMEZONE_MISMATCH_PASSAGE_TIMESTAMP",
            ),
        ),
        predicate(
            "arrival_timestamp",
            "timezone-aware and after passage",
            arrival_value,
            (
                "MISSING_D_ARRIVAL_TIMESTAMP",
                "INVALID_D_ARRIVAL_TIMESTAMP",
                "TIMEZONE_MISSING_D_ARRIVAL_TIMESTAMP",
                "TIMEZONE_MISMATCH_D_ARRIVAL_TIMESTAMP",
            ),
        ),
        predicate(
            "d_track_after_passage",
            f">= passage-{D_CLOCK_TOLERANCE_SECONDS}s",
            payload.get("d_track_first_seen_at"),
            (
                "MISSING_D_TRACK_FIRST_SEEN_AT",
                "INVALID_D_TRACK_FIRST_SEEN_AT",
                "TIMEZONE_MISSING_D_TRACK_FIRST_SEEN_AT",
                "TIMEZONE_MISMATCH_D_TRACK_FIRST_SEEN_AT",
                "D_TRACK_SEEN_BEFORE_PASSAGE",
                "D_TRACK_FIRST_SEEN_AFTER_ARRIVAL",
            ),
        ),
        predicate(
            "candidate_received_sequence",
            "passage <= candidate_received_at <= arrival",
            payload.get("candidate_received_at"),
            (
                "MISSING_CANDIDATE_RECEIVED_AT",
                "INVALID_CANDIDATE_RECEIVED_AT",
                "TIMEZONE_MISSING_CANDIDATE_RECEIVED_AT",
                "TIMEZONE_MISMATCH_CANDIDATE_RECEIVED_AT",
                "CANDIDATE_RECEIVED_BEFORE_PASSAGE",
                "CANDIDATE_RECEIVED_AFTER_ARRIVAL",
            ),
        ),
        predicate(
            "travel_time",
            f"{D_MIN_TRAVEL_SECONDS}..{D_MAX_TRAVEL_SECONDS}s",
            actual_duration,
            (
                "NON_POSITIVE_TRAVEL_TIME",
                "TRAVEL_TIME_BELOW_MINIMUM",
                "TRAVEL_TIME_ABOVE_MAXIMUM",
                "WAITING_D_TTL_EXCEEDED",
            ),
        ),
        predicate(
            "reported_travel_time",
            f"central elapsed ±{D_CLOCK_TOLERANCE_SECONDS}s",
            reported_duration,
            ("NON_POSITIVE_REPORTED_TRAVEL_TIME", "TRAVEL_DURATION_MISMATCH"),
        ),
        predicate(
            "confirmation_samples",
            f">={D_MIN_CONFIRMATION_SAMPLES}",
            sample_count,
            ("INSUFFICIENT_CONFIRMATION_SAMPLES",),
        ),
        predicate(
            "confirmation_passes",
            f">={D_MIN_CONFIRMATION_PASSES} and <= samples",
            pass_count,
            (
                "INSUFFICIENT_CONFIRMATION_PASSES",
                "CONFIRMATION_PASS_COUNT_EXCEEDS_SAMPLES",
            ),
        ),
        predicate(
            "journey_margin",
            f">={D_MIN_JOURNEY_MARGIN}",
            {
                "best": best_score,
                "second": second_score,
                "reported_margin": journey_margin,
            },
            (
                "INSUFFICIENT_JOURNEY_MARGIN",
                "INSUFFICIENT_CALCULATED_JOURNEY_MARGIN",
                "JOURNEY_MARGIN_MISMATCH",
            ),
        ),
        predicate(
            "eligibility_reason",
            sorted(D_ELIGIBLE_REASONS),
            eligibility_reason,
            (
                "MISSING_ELIGIBILITY_REASON",
                "ELIGIBILITY_REASON_NOT_ALLOWED",
            ),
        ),
        predicate(
            "d_track_ownership",
            "not linked to another Journey",
            track_key,
            ("MISSING_D_TRACK_IDENTITY", "D_TRACK_ALREADY_LINKED_TO_OTHER_JOURNEY"),
        ),
    ]
    mapped_codes = {
        code
        for result in predicate_results
        for code in result["failure_codes"]
    }
    for code in errors:
        if code not in mapped_codes:
            predicate_results.append(
                {
                    "name": f"validation_{code.lower()}",
                    "predicate": code,
                    "expected": "condition satisfied",
                    "actual": code,
                    "pass": False,
                    "passed": False,
                    "failure_codes": [code],
                }
            )
    context = {
        "journey_status": journey_status,
        "route": route,
        "db_route": route,
        "person_uid": central_person_uid,
        "db_person_uid": central_person_uid,
        "tracking_person_uid": payload.get(
            "tracking_person_uid", payload.get("person_uid")
        ),
        "canonical_person_uid": canonical_person_uid,
        "db_canonical_person_uid": canonical_person_uid,
        "candidate_person_uid": (
            journey["candidate_person_uid"] if journey is not None else None
        ),
        "db_candidate_person_uid": (
            journey["candidate_person_uid"] if journey is not None else None
        ),
        "person_status": person_status,
        "identity_result": identity_result,
        "review_status": review_status,
        "identity_confirmed": identity_confirmed,
        "identity_completion_policy": identity_policy,
        "passage_timestamp": journey["passage_at"] if journey is not None else None,
        "c_passage_stored": bool(middle_node == "C" and passage_event_at),
        "c_passage_timestamp": passage_event_at if middle_node == "C" else None,
        "c_passage_saved_at": passage_event_at if middle_node == "C" else None,
        "active": journey_status in {"WAITING_B_OR_C", "WAITING_D"},
        "expired": journey_status == "EXPIRED",
        "arrival_timestamp": arrival_value,
        "elapsed_seconds": actual_duration,
        "best_journey_score": best_score,
        "second_journey_score": second_score,
        "combined_score": payload.get("combined_score"),
        "top2_mean": payload.get("top2_mean"),
        "confirmation_sample_count": sample_count,
        "confirmation_pass_count": pass_count,
    }
    return {
        "accepted": not errors,
        "reason_codes": errors,
        "arrival_at": (
            str(arrival_value) if arrival is not None else now_iso()
        ),
        "actual_duration_seconds": actual_duration,
        "route": route,
        "middle_node": middle_node,
        "track_key": track_key,
        "context": context,
        "payload_fields": payload_fields,
        "predicates": predicate_results,
    }


def publish_d_journey_release(
    client: mqtt.Client,
    journey_id: str,
    terminal_status: str,
    *,
    journey_status: str,
    reason_codes: list[str] | None = None,
) -> None:
    publish_journey_invalidation(
        client,
        journey_id,
        terminal_status,
        journey_status=journey_status,
        reason_codes=reason_codes,
        target_nodes=("D",),
    )

def handle_d_arrival(
    client: mqtt.Client,
    payload: dict[str, Any],
    *,
    mqtt_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mqtt_metadata = dict(mqtt_metadata or {})
    final_identity_result: dict[str, Any] | None = None
    journey_id = payload.get(
        "journey_id"
    )

    if not journey_id:
        raise ValueError(
            "ARRIVAL 메시지에 journey_id가 없습니다."
        )

    received_at = str(mqtt_metadata.get("received_at") or now_iso())
    raw_sha256 = str(mqtt_metadata.get("raw_sha256") or "")
    arrival_event_id = str(
        mqtt_metadata.get("arrival_event_id")
        or d_arrival_event_id(payload, raw_sha256 or hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest())
    )
    event_key = _arrival_event_key(payload)
    local_track_id = extract_local_track_id(payload)
    track_key = _d_track_key(payload)

    final_score = payload.get(
        "combined_score",
        payload.get(
            "similarity",
        ),
    )

    structured_log(
        "d_arrival_db_transaction",
        arrival_event_id=arrival_event_id,
        journey_id=journey_id,
        phase="BEGIN",
    )

    with db_lock:
        with connect_db() as connection:
            existing_attempt = connection.execute(
                "SELECT * FROM d_arrival_attempts WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            if existing_attempt is None and track_key is not None:
                existing_attempt = connection.execute(
                    """
                    SELECT * FROM d_arrival_attempts
                    WHERE journey_id = ? AND d_track_key = ? AND accepted = 1
                    ORDER BY attempt_id LIMIT 1
                    """,
                    (str(journey_id), track_key),
                ).fetchone()
            if existing_attempt is not None:
                duplicate_journey_status = None
                if existing_attempt["journey_id"] is not None:
                    duplicate_journey = connection.execute(
                        "SELECT status FROM journeys WHERE journey_id = ?",
                        (existing_attempt["journey_id"],),
                    ).fetchone()
                    if duplicate_journey is not None:
                        duplicate_journey_status = str(
                            duplicate_journey["status"]
                        )
                if duplicate_journey_status in {
                    "COMPLETED", "EXPIRED", "CANCELLED", "REJECTED"
                }:
                    publish_d_journey_release(
                        client,
                        str(existing_attempt["journey_id"]),
                        duplicate_journey_status,
                        journey_status=duplicate_journey_status,
                        reason_codes=safe_json_loads(
                            existing_attempt["reason_json"], []
                        ),
                    )
                print(
                    "[MAIN D ARRIVAL DUPLICATE] "
                    f"journey_id={journey_id}, event_key={event_key}, "
                    f"accepted={bool(existing_attempt['accepted'])}, "
                    f"reason={existing_attempt['reason_code']}"
                )
                structured_log(
                    "d_arrival_decision",
                    arrival_event_id=arrival_event_id,
                    journey_id=journey_id,
                    decision="DUPLICATE",
                    duplicate=True,
                    original_accepted=bool(existing_attempt["accepted"]),
                    final_journey_status=duplicate_journey_status,
                    failed_reasons=safe_json_loads(
                        existing_attempt["reason_json"], []
                    ),
                )
                return {
                    "accepted": bool(existing_attempt["accepted"]),
                    "duplicate": True,
                    "journey_status": duplicate_journey_status,
                    "reason_codes": safe_json_loads(
                        existing_attempt["reason_json"], []
                    ),
                }

            journey = connection.execute(
                """
                SELECT *
                FROM journeys
                WHERE journey_id = ?
                """,
                (journey_id,),
            ).fetchone()

            validation = validate_d_arrival(connection, journey, payload)
            structured_log(
                "d_arrival_validation",
                arrival_event_id=arrival_event_id,
                journey_id=journey_id,
                decision="ACCEPTED" if validation["accepted"] else "REJECTED",
                failed_reasons=validation["reason_codes"],
                db_state=validation["context"],
                payload_fields=validation["payload_fields"],
                checks=validation["predicates"],
            )
            arrival_at = str(validation["arrival_at"])
            reason_codes = list(validation["reason_codes"])
            stored_journey_id = str(journey_id) if journey is not None else None
            connection.execute(
                """
                INSERT INTO d_arrival_attempts (
                    event_key, journey_id, d_local_track_id, d_track_key,
                    d_track_first_seen_at, arrival_at, received_at,
                    accepted, reason_code, reason_json, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    stored_journey_id,
                    str(local_track_id) if local_track_id is not None else None,
                    validation["track_key"],
                    payload.get("d_track_first_seen_at"),
                    arrival_at,
                    received_at,
                    int(bool(validation["accepted"])),
                    reason_codes[0] if reason_codes else "ACCEPTED",
                    json.dumps(reason_codes, separators=(",", ":")),
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                ),
            )
            if not validation["accepted"]:
                if journey is not None:
                    save_journey_event(
                        connection,
                        str(journey_id),
                        "D",
                        "ARRIVAL_REJECTED",
                        received_at,
                        {
                            "event": "ARRIVAL_REJECTED",
                            "journey_id": journey_id,
                            "journey_status": str(journey["status"]),
                            "arrival_event_id": arrival_event_id,
                            "event_key": event_key,
                            "local_track_id": local_track_id,
                            "reason_codes": reason_codes,
                            "validation": {
                                "actual_duration_seconds": validation[
                                    "actual_duration_seconds"
                                ],
                                "clock_tolerance_seconds": (
                                    D_CLOCK_TOLERANCE_SECONDS
                                ),
                                "minimum_travel_seconds": D_MIN_TRAVEL_SECONDS,
                                "maximum_travel_seconds": D_MAX_TRAVEL_SECONDS,
                                "minimum_confirmation_samples": (
                                    D_MIN_CONFIRMATION_SAMPLES
                                ),
                                "minimum_confirmation_passes": (
                                    D_MIN_CONFIRMATION_PASSES
                                ),
                                "minimum_journey_margin": D_MIN_JOURNEY_MARGIN,
                                "context": validation["context"],
                                "payload_fields": validation["payload_fields"],
                                "predicates": validation["predicates"],
                            },
                            "payload": payload,
                        },
                    )
                connection.commit()
                structured_log(
                    "d_arrival_db_transaction",
                    arrival_event_id=arrival_event_id,
                    journey_id=journey_id,
                    phase="COMMITTED_REJECTION_AUDIT",
                    final_journey_status=(
                        str(journey["status"]) if journey is not None else "NOT_FOUND"
                    ),
                )
                structured_log(
                    "d_arrival_decision",
                    arrival_event_id=arrival_event_id,
                    journey_id=journey_id,
                    decision="REJECTED",
                    duplicate=False,
                    failed_reasons=reason_codes,
                    checks=validation["predicates"],
                    final_journey_status=(
                        str(journey["status"]) if journey is not None else "NOT_FOUND"
                    ),
                )
                journey_status = (
                    str(journey["status"])
                    if journey is not None
                    else "NOT_FOUND"
                )
                if journey_status in {
                    "COMPLETED", "EXPIRED", "CANCELLED", "REJECTED"
                }:
                    publish_d_journey_release(
                        client,
                        str(journey_id),
                        journey_status,
                        journey_status=journey_status,
                        reason_codes=reason_codes,
                    )
                print(
                    "[MAIN D ARRIVAL REJECTED] "
                    f"journey_id={journey_id}, local_track_id={local_track_id}, "
                    f"journey_status={journey_status}, "
                    f"reasons={','.join(reason_codes)}"
                )
                return {
                    "accepted": False,
                    "duplicate": False,
                    "arrival_event_id": arrival_event_id,
                    "reason_codes": reason_codes,
                    "journey_status": journey_status,
                    "validation": {
                        "context": validation["context"],
                        "payload_fields": validation["payload_fields"],
                        "predicates": validation["predicates"],
                    },
                }

            assert journey is not None
            route = list(validation["route"])
            middle_node = str(validation["middle_node"])

            if "D" not in route:
                route.append("D")

            connection.execute(
                """
                UPDATE journeys
                SET
                    status = ?,
                    route_json = ?,
                    arrival_at = ?,
                    completed_at = ?
                WHERE journey_id = ?
                """,
                (
                    "COMPLETED",
                    json.dumps(route),
                    arrival_at,
                    arrival_at,
                    journey_id,
                ),
            )

            save_journey_event(
                connection,
                journey_id,
                "D",
                "ARRIVAL",
                arrival_at,
                payload,
            )

            person_uid = journey[
                "person_uid"
            ]

            # D에서 최종 매칭에 사용한
            # 512차원 특징값을 임시 Journey Gallery에 저장
            d_embedding_added = 0

            raw_d_embedding = payload.get(
                "embedding"
            )

            if isinstance(
                raw_d_embedding,
                list,
            ):
                try:
                    d_embedding = normalize_embedding(
                        raw_d_embedding
                    )

                    save_journey_embedding(
                        connection,
                        journey_id,
                        "D",
                        d_embedding,
                        arrival_at,
                        float(
                            payload.get(
                                "quality",
                                1.0,
                            )
                        ),
                    )

                    d_embedding_added = 1

                except (
                    TypeError,
                    ValueError,
                ) as error:
                    print(
                        "[MAIN] D embedding "
                        "저장 실패: "
                        f"{error}"
                    )

            capture_saved = (
                save_capture_record_if_present(
                    connection,
                    journey_id,
                    person_uid,
                    "D",
                    arrival_at,
                    payload,
                )
            )

            person_status = str(
                journey[
                    "person_status"
                ]
            )

            promotion_allowed = bool(
                journey[
                    "gallery_promotion_allowed"
                ]
            )

            if (
                person_status == "NEW"
                and promotion_allowed
            ):
                promotion_reason = (
                    "NEW_PROFILE_INITIAL_GALLERY"
                )

            elif (
                person_status == "RETURNING"
                and promotion_allowed
            ):
                promotion_reason = (
                    "HIGH_CONFIDENCE_RETURNING"
                )

            else:
                promotion_allowed = False

                if person_status in {
                    "IDENTITY_PENDING",
                    "REVIEW_REQUIRED",
                }:
                    promotion_reason = (
                        "BLOCK_IDENTITY_PENDING"
                        if person_status == "IDENTITY_PENDING"
                        else "BLOCK_REVIEW_REQUIRED"
                    )
                elif (
                    person_status
                    == "RETURNING"
                ):
                    promotion_reason = (
                        "BLOCK_BORDERLINE_RETURNING"
                    )
                else:
                    promotion_reason = (
                        "BLOCK_BY_POLICY"
                    )

            if promotion_allowed:
                promoted_count = (
                    promote_journey_gallery(
                        connection,
                        journey_id,
                        person_uid,
                    )
                )
                discarded_count = 0
            elif person_status in {
                "IDENTITY_PENDING",
                "REVIEW_REQUIRED",
            }:
                # Review 해결 전에는 Permanent Gallery로 승격하지 않고,
                # 이후 CONFIRM_NEW/MERGE_EXISTING에 사용할 임시 Gallery도
                # 삭제하지 않는다.
                promoted_count = 0
                discarded_count = 0
            else:
                promoted_count = 0
                discarded_count = (
                    discard_journey_gallery(
                        connection,
                        journey_id,
                    )
                )

            if str(journey["identity_result"]) != "UNKNOWN":
                connection.execute(
                    """
                    UPDATE persons
                    SET last_seen_at = ?
                    WHERE person_uid = ?
                    """,
                    (arrival_at, person_uid),
                )

            completed_row = connection.execute(
                "SELECT status, route_json, completed_at FROM journeys "
                "WHERE journey_id = ?",
                (journey_id,),
            ).fetchone()
            completed_db_state = {
                "final_journey_status": str(completed_row["status"]),
                "route": safe_json_loads(completed_row["route_json"], []),
                "route_includes_d": "D" in safe_json_loads(
                    completed_row["route_json"], []
                ),
                "completed_at": completed_row["completed_at"],
                "completed_at_saved": completed_row["completed_at"] is not None,
            }

    structured_log(
        "d_arrival_db_transaction",
        arrival_event_id=arrival_event_id,
        journey_id=journey_id,
        phase="COMMITTED",
        **completed_db_state,
    )
    structured_log(
        "d_arrival_decision",
        arrival_event_id=arrival_event_id,
        journey_id=journey_id,
        decision="ACCEPTED",
        duplicate=False,
        failed_reasons=[],
        checks=validation["predicates"],
        **completed_db_state,
    )

    if person_status == "IDENTITY_PENDING":
        final_identity_result = resolve_final_route_identity(
            str(journey_id)
        )
        decision = str(
            final_identity_result["final_review_result"]
        )
        canonical_uid = final_identity_result.get(
            "canonical_person_uid"
        )
        if decision == "REVISIT":
            person_uid = str(canonical_uid)
            person_status = "RETURNING"
            promoted_count = int(
                final_identity_result.get("promoted_count") or 0
            )
            promotion_allowed = True
            promotion_reason = "FINAL_REVIEW_REVISIT"
        elif decision == "NEW":
            person_uid = str(canonical_uid)
            person_status = "NEW"
            promoted_count = int(
                final_identity_result.get("promoted_count") or 0
            )
            promotion_allowed = True
            promotion_reason = "FINAL_REVIEW_NEW"
        else:
            person_status = "REVIEW_REQUIRED"
            promotion_allowed = False
            promoted_count = 0
            promotion_reason = "BLOCK_REVIEW_REQUIRED"
        discarded_count = 0
        print_final_identity_review(
            final_identity_result,
            payload.get(
                "d_local_track_id",
                payload.get("local_track_id"),
            ),
        )

    final_review_result = (
        final_identity_result.get("final_review_result")
        if final_identity_result is not None
        else None
    )
    canonical_person_uid = (
        final_identity_result.get("canonical_person_uid")
        if final_identity_result is not None
        else journey["canonical_person_uid"]
    )
    identity_result = str(journey["identity_result"])
    review_status = str(journey["review_status"])
    if final_review_result in {"REVISIT", "NEW"} and canonical_person_uid:
        identity_result = (
            "RETURNING" if final_review_result == "REVISIT" else "NEW"
        )
        review_status = "RESOLVED"
    elif final_review_result == "MANUAL_REVIEW_REQUIRED":
        identity_result = "UNKNOWN"
        review_status = "PENDING"
        canonical_person_uid = None
    identity_confirmed = bool(
        identity_result in {"NEW", "RETURNING"}
        and review_status != "PENDING"
        and canonical_person_uid is not None
        and final_review_result != "MANUAL_REVIEW_REQUIRED"
    )

    completed_payload = {
        "event": "JOURNEY_COMPLETED",

        "journey_id": journey_id,
        "person_uid": person_uid,
        "global_person_id": person_uid,
        "middle_node": middle_node,

        "route": route,
        "completed_at": arrival_at,
        "final_score": final_score,

        "person_status": person_status,
        "journey_status": "COMPLETED",
        "identity_result": identity_result,
        "review_status": review_status,
        "identity_confirmed": identity_confirmed,
        "final_review_result": final_review_result,
        "canonical_person_uid": canonical_person_uid,
        "gallery_promoted": (
            promotion_allowed
        ),
        "gallery_promotion_reason": (
            promotion_reason
        ),

        "status": "COMPLETED",
    }

    publish_json(
        client,
        TOPIC_JOURNEY_COMPLETED,
        completed_payload,
    )
    publish_journey_invalidation(
        client,
        str(journey_id),
        "COMPLETED",
        journey_status="COMPLETED",
    )

    print()
    print(
        "===== MAIN: D ARRIVAL 처리 ====="
    )
    print(
        f"Person UID : {person_uid}"
    )
    print(
        f"Journey ID : {journey_id}"
    )
    print(
        f"Route      : {route}"
    )
    print(
        f"Final Score: {final_score}"
    )
    print(
        f"Person 상태: {person_status}"
    )
    print(
        f"D 특징 수신: {d_embedding_added}"
    )
    print(
        f"영구 특징 추가: {promoted_count}"
    )
    print(
        f"임시 특징 폐기: {discarded_count}"
    )
    print(
        "Gallery 정책: "
        f"{promotion_reason}"
    )
    print(
        f"Capture 저장 : {capture_saved}"
    )
    print(
        "Journey      : COMPLETED"
    )
    print(
        "==============================="
    )
    return {
        "accepted": True,
        "duplicate": False,
        "arrival_event_id": arrival_event_id,
        "journey_status": "COMPLETED",
        "identity_confirmed": identity_confirmed,
        "final_review_result": final_review_result,
        "validation": {
            "context": validation["context"],
            "payload_fields": validation["payload_fields"],
            "predicates": validation["predicates"],
        },
    }


# ============================================================
# 서버 재시작 시 미완료 Journey 복구
# ============================================================

def recover_active_journeys(
    client: mqtt.Client,
) -> None:
    with db_lock:
        with connect_db() as connection:
            expired_b_or_c, expired_d = (
                expire_stale_journeys(
                    connection,
                    client,
                )
            )
            duplicate_active_rejected = reconcile_duplicate_active_journeys(
                connection,
                client,
            )

            rows = connection.execute(
                """
                SELECT *
                FROM journeys
                WHERE status IN (
                    'WAITING_B_OR_C',
                    'WAITING_D'
                )
                ORDER BY entry_at ASC
                """
            ).fetchall()

            recovery_items = []

            for row in rows:
                route = safe_json_loads(
                    row["route_json"],
                    ["A"],
                )
                gallery = load_body_journey_gallery(
                    connection,
                    row["journey_id"],
                )

                payload = {
                    "event": "CANDIDATE",
                    "stage": row["status"],
                    "journey_id": row["journey_id"],
                    "person_uid": row["person_uid"],
                    "global_person_id": row["person_uid"],
                    "person_status": row["person_status"],
                    "candidate_person_uid": (
                        row["candidate_person_uid"]
                    ),
                    "route": route,
                    "middle_node": (
                        route[1]
                        if len(route) > 1
                        and route[1] in ("B", "C")
                        else None
                    ),
                    "entry_timestamp": row["entry_at"],
                    "passage_timestamp": row["passage_at"],
                    **candidate_expiry_fields(
                        (
                            row["entry_at"]
                            if row["status"] == "WAITING_B_OR_C"
                            else row["passage_at"] or row["entry_at"]
                        ),
                        (
                            WAITING_B_OR_C_TIMEOUT_SECONDS
                            if row["status"] == "WAITING_B_OR_C"
                            else WAITING_D_TIMEOUT_SECONDS
                        ),
                    ),
                    "person_match_score": (
                        row["person_match_score"]
                    ),
                    "second_match_score": (
                        row["second_match_score"]
                    ),
                    "person_best_score": (
                        row["person_best_score"]
                    ),
                    "person_topk_score": (
                        row["person_topk_score"]
                    ),
                    "person_combined_score": (
                        row["person_combined_score"]
                    ),
                    "gallery_promotion_allowed": (
                        bool(
                            row[
                                "gallery_promotion_allowed"
                            ]
                        )
                    ),
                    "match_source": (
                        row["match_source"]
                    ),
                    "gallery_count": len(gallery),
                    "gallery": gallery,
                }

                recovery_items.append(
                    (row["status"], payload)
                )

    waiting_b_or_c = 0
    waiting_d = 0

    for status, payload in recovery_items:
        if status == "WAITING_B_OR_C":
            published = publish_active_journey_candidate(
                client,
                TOPIC_CANDIDATE_B,
                payload,
                "WAITING_B_OR_C",
            )
            if ENABLE_CAMERA_C:
                publish_active_journey_candidate(
                    client,
                    TOPIC_CANDIDATE_C,
                    payload,
                    "WAITING_B_OR_C",
                )
            waiting_b_or_c += int(published)

        elif status == "WAITING_D":
            published = publish_active_journey_candidate(
                client,
                TOPIC_CANDIDATE_D,
                payload,
                "WAITING_D",
            )
            waiting_d += int(published)

    print()
    print("===== MAIN: 미완료 Journey 복구 =====")
    print(
        f"WAITING_B_OR_C 재전송: "
        f"{waiting_b_or_c}"
    )
    print(
        f"WAITING_D 재전송     : "
        f"{waiting_d}"
    )
    print(
        f"시작 시 EXPIRED(B/C): "
        f"{expired_b_or_c}"
    )
    print(
        f"시작 시 EXPIRED(D)  : "
        f"{expired_d}"
    )
    print(
        "중복 활성 REJECTED  : "
        f"{duplicate_active_rejected}"
    )
    print("====================================")


# ============================================================
# NODE_TIMING 저장 / Timeline 조회
# ============================================================

def _timing_timestamp(
    value: Any,
    field_name: str,
    *,
    required: bool,
) -> tuple[str | None, float | None]:
    if value is None or str(value).strip() == "":
        if required:
            raise ValueError(f"NODE_TIMING {field_name}가 없습니다.")
        return None, None

    normalized = str(value).strip()
    epoch = parse_iso_epoch(normalized)
    if epoch is None:
        raise ValueError(
            f"NODE_TIMING {field_name} 형식 오류: {normalized}"
        )
    return normalized, epoch


def _earlier_timestamp(
    first: str | None,
    second: str | None,
) -> str | None:
    if first is None:
        return second
    if second is None:
        return first
    first_epoch = parse_iso_epoch(first)
    second_epoch = parse_iso_epoch(second)
    if first_epoch is None or second_epoch is None:
        raise ValueError("저장된 NODE_TIMING timestamp 형식 오류")
    return first if first_epoch <= second_epoch else second


def _later_timestamp(
    first: str | None,
    second: str | None,
) -> str | None:
    if first is None:
        return second
    if second is None:
        return first
    first_epoch = parse_iso_epoch(first)
    second_epoch = parse_iso_epoch(second)
    if first_epoch is None or second_epoch is None:
        raise ValueError("저장된 NODE_TIMING timestamp 형식 오류")
    return first if first_epoch >= second_epoch else second


def _nonnegative_timing_seconds(
    start: str | None,
    end: str | None,
    metric_name: str,
    warnings: list[str],
) -> float | None:
    if start is None or end is None:
        return None
    start_epoch = parse_iso_epoch(start)
    end_epoch = parse_iso_epoch(end)
    if start_epoch is None or end_epoch is None:
        warning = f"{metric_name}: timestamp 형식 오류"
        warnings.append(warning)
        return None
    seconds = end_epoch - start_epoch
    if seconds < 0:
        warning = (
            f"{metric_name}: 음수 시간 {seconds:.3f}s "
            f"(start={start}, end={end})"
        )
        warnings.append(warning)
        return None
    return round(seconds, 3)


def _journey_elapsed_seconds(
    a_departure_at: str | None,
    d_exit_at: str | None,
    journey_id: str,
    warnings: list[str],
) -> float | None:
    metric_name = f"{journey_id}.journey_elapsed_seconds"
    if a_departure_at is None:
        warnings.append(f"{metric_name}: A ENTRY timestamp 없음")
        return None
    if d_exit_at is None:
        warnings.append(f"{metric_name}: D exited_at 없음")
        return None
    return _nonnegative_timing_seconds(
        a_departure_at,
        d_exit_at,
        metric_name,
        warnings,
    )


def _print_journey_time_completed(timeline: dict[str, Any]) -> None:
    route_text = " -> ".join(str(node) for node in timeline["route"])
    print(
        "\n===== JOURNEY TIME COMPLETED =====\n"
        f"Person UID : {timeline['person_uid']}\n"
        f"Journey ID : {timeline['journey_id']}\n"
        f"Route      : {route_text}\n"
        f"A Start    : {timeline['a_departure_at']}\n"
        f"D Exit     : {timeline['d_exit_at']}\n"
        f"Elapsed    : {timeline['journey_elapsed_seconds']:.3f} sec\n"
        "=================================="
    )


def handle_node_timing(
    payload: dict[str, Any],
    expected_node_id: str | None = None,
) -> dict[str, Any]:
    if payload.get("schema_version") != 1:
        raise ValueError("NODE_TIMING schema_version은 1이어야 합니다.")
    if payload.get("event") != "NODE_TIMING":
        raise ValueError("NODE_TIMING event 값이 올바르지 않습니다.")

    node_id = str(payload.get("node_id", "")).strip().upper()
    if node_id not in {"A", "B", "C", "D"}:
        raise ValueError(f"NODE_TIMING node_id 오류: {node_id}")
    if (
        expected_node_id is not None
        and node_id != expected_node_id.strip().upper()
    ):
        raise ValueError(
            "NODE_TIMING topic/node_id 불일치: "
            f"topic={expected_node_id}, payload={node_id}"
        )

    journey_id = str(payload.get("journey_id", "")).strip()
    person_uid = str(payload.get("person_uid", "")).strip()
    if not journey_id:
        raise ValueError("NODE_TIMING journey_id가 없습니다.")
    if not person_uid:
        raise ValueError("NODE_TIMING person_uid가 없습니다.")
    global_person_id = payload.get("global_person_id")
    if (
        global_person_id is not None
        and str(global_person_id).strip() != person_uid
    ):
        raise ValueError(
            "NODE_TIMING global_person_id/person_uid 불일치"
        )

    entered_at, _ = _timing_timestamp(
        payload.get("entered_at"),
        "entered_at",
        required=True,
    )
    matched_at, _ = _timing_timestamp(
        payload.get("matched_at"),
        "matched_at",
        required=False,
    )
    exited_at, new_exited_epoch = _timing_timestamp(
        payload.get("exited_at"),
        "exited_at",
        required=False,
    )

    raw_track_id = payload.get("local_track_id")
    try:
        local_track_id = (
            int(raw_track_id)
            if raw_track_id is not None
            else None
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"NODE_TIMING local_track_id 오류: {raw_track_id}"
        ) from error
    raw_exit_reason = payload.get("exit_reason")
    exit_reason = (
        str(raw_exit_reason).strip()
        if raw_exit_reason is not None
        and str(raw_exit_reason).strip()
        else None
    )

    warnings: list[str] = []
    should_print_journey_completion = False
    with db_lock:
        with connect_db() as connection:
            journey = connection.execute(
                """
                SELECT person_uid, status, route_json
                FROM journeys
                WHERE journey_id = ?
                """,
                (journey_id,),
            ).fetchone()
            if journey is None:
                raise ValueError(
                    f"NODE_TIMING Journey를 찾을 수 없음: {journey_id}"
                )
            canonical_person_uid = str(journey["person_uid"])
            if person_uid != canonical_person_uid:
                alias = connection.execute(
                    """
                    SELECT merged_into_person_uid
                    FROM persons
                    WHERE person_uid = ?
                    """,
                    (person_uid,),
                ).fetchone()
                if (
                    alias is None
                    or alias["merged_into_person_uid"]
                    != canonical_person_uid
                ):
                    raise ValueError(
                        "NODE_TIMING canonical person_uid 불일치: "
                        f"payload={person_uid}, journey={canonical_person_uid}"
                    )

            route = safe_json_loads(journey["route_json"], [])
            if node_id == "A":
                if "A" not in route:
                    raise ValueError("NODE_TIMING A가 Journey route에 없음")
            elif node_id in {"B", "C"}:
                if node_id not in route:
                    raise ValueError(
                        "NODE_TIMING losing middle node 거부: "
                        f"node={node_id}, route={route}"
                    )
            elif str(journey["status"]) not in {
                "WAITING_D",
                "COMPLETED",
            }:
                raise ValueError(
                    "NODE_TIMING D는 WAITING_D/COMPLETED에서만 허용: "
                    f"status={journey['status']}"
                )

            existing = connection.execute(
                """
                SELECT *
                FROM journey_node_visits
                WHERE journey_id = ? AND node_id = ?
                """,
                (journey_id, node_id),
            ).fetchone()
            merged_entered = _earlier_timestamp(
                existing["entered_at"] if existing else None,
                entered_at,
            )
            merged_matched = _earlier_timestamp(
                existing["matched_at"] if existing else None,
                matched_at,
            )
            merged_exited = _later_timestamp(
                existing["exited_at"] if existing else None,
                exited_at,
            )
            should_print_journey_completion = (
                node_id == "D"
                and merged_exited is not None
                and (
                    existing is None
                    or existing["exited_at"] is None
                )
            )

            merged_track_id = (
                existing["local_track_id"]
                if existing is not None
                and existing["local_track_id"] is not None
                else local_track_id
            )
            if (
                existing is not None
                and existing["local_track_id"] is not None
                and local_track_id is not None
                and int(existing["local_track_id"]) != local_track_id
            ):
                warnings.append(
                    "local_track_id 충돌: 기존 값을 유지함 "
                    f"({existing['local_track_id']} != {local_track_id})"
                )

            existing_exited_epoch = (
                parse_iso_epoch(existing["exited_at"])
                if existing is not None
                else None
            )
            use_new_exit_metadata = (
                exited_at is not None
                and (
                    existing_exited_epoch is None
                    or (
                        new_exited_epoch is not None
                        and new_exited_epoch >= existing_exited_epoch
                    )
                )
            )
            merged_exit_reason = (
                exit_reason
                if use_new_exit_metadata and exit_reason is not None
                else (
                    existing["exit_reason"]
                    if existing is not None
                    else exit_reason
                )
            )
            dwell_seconds = _nonnegative_timing_seconds(
                merged_entered,
                merged_exited,
                f"{journey_id}.{node_id}_dwell_seconds",
                warnings,
            )
            timestamp = now_iso()
            created_at = (
                str(existing["created_at"])
                if existing is not None
                else timestamp
            )
            connection.execute(
                """
                INSERT INTO journey_node_visits (
                    journey_id,
                    person_uid,
                    node_id,
                    local_track_id,
                    entered_at,
                    matched_at,
                    exited_at,
                    dwell_seconds,
                    exit_reason,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(journey_id, node_id) DO UPDATE SET
                    person_uid = excluded.person_uid,
                    local_track_id = excluded.local_track_id,
                    entered_at = excluded.entered_at,
                    matched_at = excluded.matched_at,
                    exited_at = excluded.exited_at,
                    dwell_seconds = excluded.dwell_seconds,
                    exit_reason = excluded.exit_reason,
                    updated_at = excluded.updated_at
                """,
                (
                    journey_id,
                    canonical_person_uid,
                    node_id,
                    merged_track_id,
                    merged_entered,
                    merged_matched,
                    merged_exited,
                    dwell_seconds,
                    merged_exit_reason,
                    created_at,
                    timestamp,
                ),
            )

    for warning in warnings:
        print(f"[NODE_TIMING 경고] {warning}")
    print(
        "[NODE_TIMING 저장] "
        f"journey_id={journey_id}, person_uid={canonical_person_uid}, "
        f"node_id={node_id}, dwell_seconds={dwell_seconds}"
    )
    if should_print_journey_completion:
        timeline = get_journey_timeline(journey_id)
        if timeline["journey_elapsed_seconds"] is not None:
            _print_journey_time_completed(timeline)
    return {
        "journey_id": journey_id,
        "person_uid": canonical_person_uid,
        "node_id": node_id,
        "entered_at": merged_entered,
        "matched_at": merged_matched,
        "exited_at": merged_exited,
        "dwell_seconds": dwell_seconds,
        "warnings": warnings,
        "created": existing is None,
    }


def get_journey_timeline(
    journey_id: str,
) -> dict[str, Any]:
    db_uri = f"file:{DB_PATH.as_posix()}?mode=ro"
    connection = sqlite3.connect(db_uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        journey = connection.execute(
            """
            SELECT journey_id, person_uid, status, route_json, entry_at
            FROM journeys
            WHERE journey_id = ?
            """,
            (journey_id,),
        ).fetchone()
        if journey is None:
            raise ValueError(f"Journey를 찾을 수 없음: {journey_id}")
        rows = connection.execute(
            """
            SELECT *
            FROM journey_node_visits
            WHERE journey_id = ?
            """,
            (journey_id,),
        ).fetchall()
    finally:
        connection.close()

    route = safe_json_loads(journey["route_json"], [])
    by_node = {str(row["node_id"]): row for row in rows}
    warnings: list[str] = []
    middle_node = next(
        (node for node in route if node in {"B", "C"}),
        None,
    )
    node_order = ["A"]
    if middle_node is not None:
        node_order.append(middle_node)
    node_order.append("D")

    nodes: list[dict[str, Any]] = []
    for node_id in node_order:
        row = by_node.get(node_id)
        if row is None:
            continue
        dwell_seconds = _nonnegative_timing_seconds(
            row["entered_at"],
            row["exited_at"],
            f"{journey_id}.{node_id}_dwell_seconds",
            warnings,
        )
        nodes.append(
            {
                "node_id": node_id,
                "local_track_id": row["local_track_id"],
                "entered_at": row["entered_at"],
                "matched_at": row["matched_at"],
                "exited_at": row["exited_at"],
                "dwell_seconds": dwell_seconds,
                "exit_reason": row["exit_reason"],
            }
        )

    segments: dict[str, float | None] = {}
    if middle_node is not None:
        a_row = by_node.get("A")
        middle_row = by_node.get(middle_node)
        d_row = by_node.get("D")
        first_metric = f"A_to_{middle_node}_seconds"
        second_metric = f"{middle_node}_to_D_seconds"
        segments[first_metric] = _nonnegative_timing_seconds(
            a_row["exited_at"] if a_row else None,
            middle_row["entered_at"] if middle_row else None,
            f"{journey_id}.{first_metric}",
            warnings,
        )
        segments[second_metric] = _nonnegative_timing_seconds(
            middle_row["exited_at"] if middle_row else None,
            d_row["entered_at"] if d_row else None,
            f"{journey_id}.{second_metric}",
            warnings,
        )

    a_row = by_node.get("A")
    d_row = by_node.get("D")
    total_route_seconds = _nonnegative_timing_seconds(
        a_row["entered_at"] if a_row else None,
        d_row["exited_at"] if d_row else None,
        f"{journey_id}.total_route_seconds",
        warnings,
    )
    raw_a_departure_at = journey["entry_at"]
    a_departure_at = (
        str(raw_a_departure_at).strip()
        if raw_a_departure_at is not None
        and str(raw_a_departure_at).strip()
        else None
    )
    raw_d_exit_at = d_row["exited_at"] if d_row else None
    d_exit_at = (
        str(raw_d_exit_at).strip()
        if raw_d_exit_at is not None
        and str(raw_d_exit_at).strip()
        else None
    )
    journey_elapsed_seconds = _journey_elapsed_seconds(
        a_departure_at,
        d_exit_at,
        journey_id,
        warnings,
    )
    for warning in warnings:
        print(f"[NODE_TIMING 경고] {warning}")
    return {
        "journey_id": str(journey["journey_id"]),
        "person_uid": str(journey["person_uid"]),
        "journey_status": str(journey["status"]),
        "route": route,
        "nodes": nodes,
        "segments": segments,
        "total_route_seconds": total_route_seconds,
        "a_departure_at": a_departure_at,
        "d_exit_at": d_exit_at,
        "journey_elapsed_seconds": journey_elapsed_seconds,
        "validation_warnings": warnings,
    }


# ============================================================
# MQTT Callback
# ============================================================

def handle_stranger_detection(
    payload: dict[str, Any],
    *,
    received_at: str | None = None,
) -> dict[str, Any]:
    event_id = payload.get("event_id")
    if not isinstance(event_id, str) or not re.fullmatch(
        r"D-[A-Za-z0-9_.:+-]+-L\d+",
        event_id,
    ):
        raise ValueError("STRANGER_DETECTED event_id 형식이 올바르지 않습니다.")

    event_at = payload.get("at")
    if not isinstance(event_at, str):
        raise ValueError("STRANGER_DETECTED at이 없습니다.")
    try:
        parsed_at = datetime.fromisoformat(event_at)
    except ValueError as error:
        raise ValueError("STRANGER_DETECTED at이 ISO-8601이 아닙니다.") from error
    if parsed_at.tzinfo is None or parsed_at.utcoffset() is None:
        raise ValueError("STRANGER_DETECTED at에는 timezone이 필요합니다.")

    if payload.get("node") != "D":
        raise ValueError("STRANGER_DETECTED node는 D여야 합니다.")
    if payload.get("kind") != "STRANGER_DETECTED":
        raise ValueError("detection kind는 STRANGER_DETECTED여야 합니다.")
    if payload.get("identity_status") != "UNREGISTERED":
        raise ValueError("STRANGER_DETECTED identity_status는 UNREGISTERED여야 합니다.")

    local_track_id = payload.get("local_track_id")
    if (
        isinstance(local_track_id, bool)
        or not isinstance(local_track_id, int)
        or local_track_id < 0
    ):
        raise ValueError("STRANGER_DETECTED local_track_id가 올바르지 않습니다.")

    for field_name in (
        "journey_id",
        "person_uid",
        "canonical_person_uid",
    ):
        if payload.get(field_name) is not None:
            raise ValueError(
                f"STRANGER_DETECTED {field_name}는 null이어야 합니다."
            )

    normalized_received_at = received_at or now_iso()
    normalized_payload = {
        "event_id": event_id,
        "at": event_at,
        "node": "D",
        "kind": "STRANGER_DETECTED",
        "identity_status": "UNREGISTERED",
        "local_track_id": local_track_id,
        "journey_id": None,
        "person_uid": None,
        "canonical_person_uid": None,
    }
    with db_lock, connect_db() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO detection_events (
                event_id, event_at, node_id, event_type,
                identity_status, local_track_id, journey_id,
                person_uid, canonical_person_uid, payload_json,
                received_at
            ) VALUES (?, ?, 'D', 'STRANGER_DETECTED', 'UNREGISTERED', ?,
                      NULL, NULL, NULL, ?, ?)
            """,
            (
                event_id,
                event_at,
                local_track_id,
                json.dumps(
                    normalized_payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                normalized_received_at,
            ),
        )
        inserted = cursor.rowcount == 1

    structured_log(
        "stranger_detection_ingested" if inserted else "stranger_detection_duplicate",
        event_id=event_id,
        node="D",
        local_track_id=local_track_id,
        event_at=event_at,
        received_at=normalized_received_at,
        inserted=inserted,
    )
    return {**normalized_payload, "inserted": inserted}

def process_mqtt_message(
    client: mqtt.Client,
    topic: str,
    raw_payload: bytes,
    *,
    retain: bool = False,
    request_id_hint: str | None = None,
    qos: int | None = None,
    duplicate: bool = False,
    received_at: str | None = None,
) -> None:
    received_at = received_at or now_iso()
    raw_sha256 = hashlib.sha256(raw_payload).hexdigest()
    raw_text = raw_payload.decode("utf-8")
    payload = json.loads(raw_text)

    d_metadata: dict[str, Any] | None = None
    if topic == TOPIC_D_ARRIVAL:
        arrival_event_id = d_arrival_event_id(payload, raw_sha256)
        d_metadata = {
            "arrival_event_id": arrival_event_id,
            "topic": topic,
            "qos": qos,
            "duplicate": bool(duplicate),
            "retain": bool(retain),
            "payload_bytes": len(raw_payload),
            "raw_sha256": raw_sha256,
            "received_at": received_at,
        }
        rx_record = {
            **d_metadata,
            "person_uid": payload.get("person_uid"),
            "tracking_person_uid": payload.get("tracking_person_uid"),
            "canonical_person_uid": payload.get("canonical_person_uid"),
            "journey_id": payload.get("journey_id"),
            "local_track_id": extract_local_track_id(payload),
            "route": payload.get("route"),
            "stage": payload.get("stage", payload.get("status")),
            "scores": {
                "best_journey_score": payload.get("best_journey_score"),
                "second_journey_score": payload.get("second_journey_score"),
                "journey_margin": payload.get("journey_margin"),
                "best_similarity": payload.get("best_similarity"),
                "top2_mean": payload.get("top2_mean"),
                "combined_score": payload.get("combined_score"),
            },
            "confirmation_count": {
                "samples": payload.get("confirmation_sample_count"),
                "passes": payload.get("confirmation_pass_count"),
            },
            "timestamps": {
                "passage": payload.get("passage_timestamp"),
                "matched": _first_payload_value(
                    payload,
                    "matched_at",
                    "match_timestamp",
                    "matched_timestamp",
                    "match_completed_at",
                ),
                "published": _first_payload_value(
                    payload,
                    "published_at",
                    "publish_timestamp",
                    "published_timestamp",
                    "event_published_at",
                ),
                "arrival": _first_payload_value(
                    payload, "d_arrival_timestamp", "arrival_timestamp", "timestamp"
                ),
                "received": received_at,
            },
            # Exact decoded MQTT bytes. SHA-256 is over raw bytes, not this JSON
            # serialization, allowing a byte-for-byte comparison with D TX.
            "raw_payload": raw_text,
        }
        log_path = append_d_arrival_rx_jsonl(rx_record, received_at)
        structured_log(
            "d_arrival_received",
            **{key: value for key, value in rx_record.items() if key != "raw_payload"},
            jsonl_path=str(log_path),
        )

    if topic == TOPIC_A_ENTRY:
        handle_a_entry(client, payload)
    elif topic == TOPIC_B_PASSAGE:
        handle_passage(client, payload, "B")
    elif ENABLE_CAMERA_C and topic == TOPIC_C_PASSAGE:
        handle_passage(client, payload, "C")
    elif topic == TOPIC_D_ARRIVAL:
        handle_d_arrival(client, payload, mqtt_metadata=d_metadata)
    elif topic == TOPIC_D_DETECTION:
        if retain:
            raise ValueError("retained STRANGER_DETECTED 메시지는 허용하지 않습니다.")
        handle_stranger_detection(payload, received_at=received_at)
    elif topic in TIMING_TOPIC_NODES:
        if retain:
            raise ValueError("retained NODE_TIMING 메시지는 허용하지 않습니다.")
        handle_node_timing(payload, TIMING_TOPIC_NODES[topic])


class MqttIngestionWorker:
    """Single ordered worker; Paho callbacks only copy and enqueue messages."""

    def __init__(
        self,
        client: mqtt.Client,
        *,
        maxsize: int = MQTT_INGESTION_QUEUE_WARN_SIZE,
    ) -> None:
        self.client = client
        # Never block or drop inside Paho's callback. ``maxsize`` remains the
        # backwards-compatible warning threshold; the FIFO itself is
        # intentionally unbounded and reports sustained backlog.
        self.queue_warn_size = max(1, maxsize)
        self.queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="mqtt-ingestion-worker",
        )
        self._started = False

    def start(self) -> None:
        if not self._started:
            self._started = True
            self.thread.start()

    def enqueue_message(
        self,
        topic: str,
        raw_payload: bytes,
        *,
        retain: bool,
        request_id_hint: str | None,
        qos: int | None = None,
        duplicate: bool = False,
        received_at: str | None = None,
    ) -> bool:
        item = {
            "kind": "message",
            "topic": topic,
            "raw_payload": bytes(raw_payload),
            "retain": retain,
            "request_id": request_id_hint,
            "qos": qos,
            "duplicate": bool(duplicate),
            "received_at": received_at or now_iso(),
            "enqueued_monotonic": time.monotonic(),
        }
        self.queue.put_nowait(item)
        if self.queue.qsize() >= self.queue_warn_size:
            structured_log(
                "mqtt_ingestion_backlog_high",
                topic=topic,
                request_id=request_id_hint,
                queue_size=self.queue.qsize(),
                warning_size=self.queue_warn_size,
            )
        return True

    def enqueue_recovery(self) -> None:
        self.queue.put_nowait(
            {
                "kind": "recovery",
                "enqueued_monotonic": time.monotonic(),
            }
        )
        if self.queue.qsize() >= self.queue_warn_size:
            structured_log(
                "mqtt_ingestion_backlog_high",
                topic="__recovery__",
                queue_size=self.queue.qsize(),
                warning_size=self.queue_warn_size,
            )

    def _run(self) -> None:
        structured_log("mqtt_ingestion_worker_started")
        while True:
            item = self.queue.get()
            try:
                if item is None:
                    return
                started = time.monotonic()
                queue_wait_ms = round(
                    (started - float(item["enqueued_monotonic"])) * 1000.0,
                    3,
                )
                kind = str(item["kind"])
                topic = str(item.get("topic") or "__recovery__")
                request_id_hint = item.get("request_id")
                structured_log(
                    "mqtt_handler_started",
                    kind=kind,
                    topic=topic,
                    request_id=request_id_hint,
                    queue_wait_ms=queue_wait_ms,
                    queue_size=self.queue.qsize(),
                )
                outcome = "success"
                error_type = None
                try:
                    with INGESTION_COORDINATOR.work():
                        if kind == "recovery":
                            recover_active_journeys(self.client)
                        else:
                            process_mqtt_message(
                                self.client,
                                topic,
                                bytes(item["raw_payload"]),
                                retain=bool(item.get("retain", False)),
                                request_id_hint=(
                                    str(request_id_hint)
                                    if request_id_hint is not None
                                    else None
                                ),
                                qos=item.get("qos"),
                                duplicate=bool(item.get("duplicate", False)),
                                received_at=str(item.get("received_at") or now_iso()),
                            )
                except Exception as error:
                    outcome = "failed"
                    error_type = type(error).__name__
                    print()
                    print("===== MAIN 처리 오류 =====")
                    print(f"Topic : {topic}")
                    print(f"Request ID : {request_id_hint}")
                    print(f"Error : {error}")
                    print("Traceback:")
                    traceback.print_exc()
                    print("==========================")
                finally:
                    structured_log(
                        "mqtt_handler_finished",
                        kind=kind,
                        topic=topic,
                        request_id=request_id_hint,
                        outcome=outcome,
                        error_type=error_type,
                        duration_ms=round(
                            (time.monotonic() - started) * 1000.0, 3
                        ),
                    )
            finally:
                self.queue.task_done()

    def stop(self, timeout: float = 30.0) -> None:
        if not self._started:
            return
        self.queue.put(None)
        self.thread.join(timeout=timeout)
        structured_log(
            "mqtt_ingestion_worker_stopped",
            alive=self.thread.is_alive(),
            pending=self.queue.qsize(),
        )

def on_connect(
    client: mqtt.Client,
    userdata,
    flags,
    reason_code,
    properties,
) -> None:
    global _mqtt_connection_sequence
    if reason_code != 0:
        structured_log(
            "mqtt_connect_failed",
            broker=f"{MQTT_HOST}:{MQTT_PORT}",
            reason_code=str(reason_code),
        )
        return

    with _mqtt_connection_sequence_lock:
        _mqtt_connection_sequence += 1
        connection_sequence = _mqtt_connection_sequence
    structured_log(
        "mqtt_connected",
        broker=f"{MQTT_HOST}:{MQTT_PORT}",
        reason_code=str(reason_code),
        flags=str(flags),
        connection_sequence=connection_sequence,
        reconnect=connection_sequence > 1,
    )
    print(
        f"Main MQTT 연결 완료: "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )

    topics = [
        TOPIC_A_ENTRY,
        TOPIC_B_PASSAGE,
        TOPIC_D_ARRIVAL,
        TOPIC_D_DETECTION,
        TOPIC_A_TIMING,
        TOPIC_B_TIMING,
        TOPIC_C_TIMING,
        TOPIC_D_TIMING,
    ]

    if ENABLE_CAMERA_C:
        topics.append(TOPIC_C_PASSAGE)

    for topic in topics:
        subscription = client.subscribe(
            topic,
            qos=MQTT_QOS,
        )
        if isinstance(subscription, tuple) and len(subscription) == 2:
            subscribe_result, message_id = subscription
        else:
            # Minimal test/fake clients may not expose Paho's (rc, mid).
            subscribe_result, message_id = None, None

        print(
            "Main MQTT 구독 요청: "
            f"topic={topic}, qos={MQTT_QOS}, "
            f"rc={subscribe_result}, mid={message_id}"
        )

    if isinstance(userdata, MqttIngestionWorker):
        userdata.enqueue_recovery()
    else:
        # Unit-test and embedded callers without runtime userdata remain
        # deterministic; production always supplies MqttIngestionWorker.
        recover_active_journeys(client)


def on_disconnect(
    client: mqtt.Client,
    userdata,
    disconnect_flags,
    reason_code,
    properties,
) -> None:
    del client, userdata
    structured_log(
        "mqtt_disconnected",
        broker=f"{MQTT_HOST}:{MQTT_PORT}",
        reason_code=str(reason_code),
        disconnect_flags=str(disconnect_flags),
        properties=str(properties),
        unexpected=(str(reason_code) not in {"0", "Success", "Normal disconnection"}),
    )


def on_subscribe(
    client: mqtt.Client,
    userdata,
    message_id: int,
    reason_code_list,
    properties,
) -> None:
    del client, userdata, properties
    granted = [str(code) for code in reason_code_list]
    print(
        "[MQTT SUBACK] "
        f"mid={message_id}, granted={granted}"
    )


def on_message(
    client: mqtt.Client,
    userdata,
    message: mqtt.MQTTMessage,
) -> None:
    raw_payload = bytes(message.payload)
    received_at = now_iso()
    request_match = re.search(
        rb'"request_id"\s*:\s*"([^"\\]*)"',
        raw_payload,
    )
    request_id_hint = (
        request_match.group(1).decode("utf-8", errors="replace")
        if request_match is not None
        else None
    )
    print(
        "[MQTT RX] "
        f"topic={message.topic}, payload_bytes={len(raw_payload)}, "
        f"request_id={request_id_hint}, qos={getattr(message, 'qos', None)}, "
        f"duplicate={bool(getattr(message, 'dup', False))}"
    )
    if isinstance(userdata, MqttIngestionWorker):
        userdata.enqueue_message(
            str(message.topic),
            raw_payload,
            retain=bool(getattr(message, "retain", False)),
            request_id_hint=request_id_hint,
            qos=getattr(message, "qos", None),
            duplicate=bool(getattr(message, "dup", False)),
            received_at=received_at,
        )
        return

    # Compatibility path for direct unit tests; the live Main always passes a
    # worker through Paho userdata, so its network loop never executes handlers.
    try:
        with INGESTION_COORDINATOR.work():
            process_mqtt_message(
                client,
                str(message.topic),
                raw_payload,
                retain=bool(getattr(message, "retain", False)),
                request_id_hint=request_id_hint,
                qos=getattr(message, "qos", None),
                duplicate=bool(getattr(message, "dup", False)),
                received_at=received_at,
            )
    except Exception as error:
        print()
        print("===== MAIN 처리 오류 =====")
        print(f"Topic : {message.topic}")
        print(f"Payload Bytes : {len(raw_payload)}")
        print(f"Request ID : {request_id_hint}")
        print(f"Error : {error}")
        print("Traceback:")
        traceback.print_exc()
        print("==========================")


# ============================================================
# 실행
# ============================================================

def main() -> None:
    initialize_database()

    cleanup_stop_event = threading.Event()
    admin_server: MainAdminControlServer | None = None
    admin_thread: threading.Thread | None = None
    admin_token = configured_admin_token()
    mqtt_client_holder: dict[str, mqtt.Client] = {}

    if admin_token is not None:
        admin_port = int(
            os.environ.get(
                "MAIN_ADMIN_CONTROL_PORT",
                str(ADMIN_CONTROL_DEFAULT_PORT),
            )
        )
        backup_root = Path(
            os.environ.get(
                "MAIN_ADMIN_BACKUP_ROOT",
                DB_PATH.parent / "backups" / "admin",
            )
        ).expanduser()
        controller = DatabaseAdminController(
            DB_PATH,
            CAPTURE_CACHE_SETTINGS.storage_root,
            backup_root,
            initialize_database,
            INGESTION_COORDINATOR,
            clear_runtime_state,
            before_reset=lambda: invalidate_active_journeys_for_reset(
                mqtt_client_holder.get("client")
            ),
            confirmation_ttl_seconds=int(
                os.environ.get("MAIN_ADMIN_CONFIRMATION_TTL_SECONDS", "300")
            ),
        )
        admin_server = MainAdminControlServer(
            (ADMIN_CONTROL_DEFAULT_HOST, admin_port),
            controller,
            admin_token,
        )
        admin_thread = threading.Thread(
            target=admin_server.serve_forever,
            daemon=True,
            name="main-admin-control",
        )
        admin_thread.start()
        print(
            "Main 관리자 제어 API 활성화: "
            f"{ADMIN_CONTROL_DEFAULT_HOST}:{admin_server.server_port}"
        )
    else:
        print("Main 관리자 제어 API 비활성화: MAIN_ADMIN_TOKEN 미설정")

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="cctv_main_server_pc",
    )
    mqtt_client_holder["client"] = client
    ingestion_worker = MqttIngestionWorker(client)
    client.user_data_set(ingestion_worker)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    ingestion_worker.start()

    print("CCTV Main Server 시작")
    print(f"DB     : {DB_PATH}")
    print(
        f"Broker : "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )
    print(f"C_PASSAGE_MIN_QUALITY={C_PASSAGE_MIN_QUALITY:.2f}")
    print(
        f"A 응답 : "
        f"{TOPIC_A_ENTRY_RESPONSE}"
    )
    print("종료   : Ctrl + C")

    client.connect(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=60,
    )

    cleanup_thread = threading.Thread(
        target=journey_cleanup_loop,
        args=(cleanup_stop_event, client),
        daemon=True,
        name="journey-expiry-cleanup",
    )
    cleanup_thread.start()

    print(
        "Journey 자동 만료: "
        f"B/C {WAITING_B_OR_C_TIMEOUT_SECONDS:.0f}s, "
        f"D {WAITING_D_TIMEOUT_SECONDS:.0f}s, "
        f"검사 주기 {JOURNEY_CLEANUP_INTERVAL_SECONDS:.0f}s"
    )

    try:
        client.loop_forever()

    except KeyboardInterrupt:
        print()
        print("CCTV Main Server 종료")

    finally:
        cleanup_stop_event.set()
        ingestion_worker.stop()
        client.disconnect()
        if admin_server is not None:
            admin_server.shutdown()
            admin_server.server_close()
        if admin_thread is not None:
            admin_thread.join(timeout=5)


if __name__ == "__main__":
    main()
