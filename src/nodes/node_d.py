from __future__ import annotations

import csv
import hashlib
import json
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import paho.mqtt.client as mqtt
import torch
from ultralytics import YOLO

from src.common.config import load_mqtt_config
from src.common.model_requirements import require_model_files
from src.common.node_d_matching import (
    MatchConfirmation,
    NodeDMatchingConfig,
    TrackEligibility,
    add_confirmation_sample,
    load_node_d_matching_config,
    parse_aware_datetime,
    temporal_rejection_reason,
    update_track_entry,
)
from src.common.stranger_detection import (
    STRANGER_DETECTION_TOPICS,
    StrangerDetectionGate,
    publish_stranger_detection,
)
from src.reid.reid_engine import ReIDTensorRTEngine


# ============================================================
# 기본 설정
# ============================================================

ROOT = Path(__file__).resolve().parents[2]
YOLO_MODEL = ROOT / "yolo26n.pt"
REID_ENGINE = ROOT / "models/reid/person_reid_osnet_x0_25_fp16.engine"
MATCHING_CONFIG_PATH = ROOT / "configs/node_d_matching.yaml"
MATCHING_CONFIG: NodeDMatchingConfig = load_node_d_matching_config(
    MATCHING_CONFIG_PATH
)

# TODO: Move per-board camera sources into a local configuration file.
CAMERA_SOURCE = "/dev/video0"

WIDTH, HEIGHT, FPS = 640, 480, 15
WEB_PORT = 8003

# Camera A /stream과 동일한 실제 JPEG 크기. 입력/inference 해상도와
# 분리하여 표시용 프레임에만 적용한다.
STREAM_WIDTH = 1280
STREAM_HEIGHT = 720
STREAM_JPEG_QUALITY = 80

# PC 송출 코드에서 이미 좌우 반전하므로 D에서는 반전하지 않음
FLIP_HORIZONTAL = True

MQTT_CONFIG = load_mqtt_config()
MQTT_HOST = MQTT_CONFIG.host
MQTT_PORT = MQTT_CONFIG.port
MQTT_QOS = MQTT_CONFIG.qos
CANDIDATE_TOPIC = "cctv/candidates/d"
JOURNEY_CONTROL_TOPIC = "cctv/control/d/journey"
ARRIVAL_TOPIC = "cctv/events/d/arrival"
TIMING_TOPIC = "cctv/events/d/timing"
DETECTION_TOPIC = STRANGER_DETECTION_TOPICS["D"]
MQTT_CLIENT_ID = "camera-d"

CAPTURE_ROOT = ROOT / "outputs" / "captures" / "D"

# A/B와 동일한 아주 약한 밝기/대비 보정
IMAGE_CONTRAST_ALPHA = 1.02
IMAGE_BRIGHTNESS_BETA = 8

MATCH_BEST_THRESHOLD = 0.70
MATCH_TOP2_THRESHOLD = 0.62
MATCH_MARGIN = MATCHING_CONFIG.min_journey_margin
MATCH_CONFIRMATIONS = MATCHING_CONFIG.confirmation_required_passes

VERIFY_THRESHOLD = 0.55
VERIFY_FAILURE_LIMIT = 2

REID_INTERVAL_FRAMES = 3
REID_HISTORY_SIZE = 5
CANDIDATE_TIMEOUT_SECONDS = MATCHING_CONFIG.max_passage_to_d_seconds
ANOMALY_DELAY_SECONDS = 2.0
TRACK_LOST_GRACE_FRAMES = 20

LOG_DIR = ROOT / "logs"
CANDIDATE_CSV = LOG_DIR / "node_d_candidates.csv"
ARRIVAL_CSV = LOG_DIR / "node_d_arrivals.csv"
ARRIVAL_TX_LOG_PREFIX = "d_arrival_tx"
ARRIVAL_PUBACK_TIMEOUT_SECONDS = 5.0
ARRIVAL_EVENT_NAMESPACE = uuid.UUID("9f3c20f0-3594-4f42-8a1a-338d3fab67c4")
CANDIDATE_RX_DIAGNOSTICS_NAME = "node_d_candidate_rx_diagnostics.jsonl"

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()
candidate_lock = threading.Lock()
arrival_tx_log_lock = threading.Lock()
candidate_diagnostics_log_lock = threading.Lock()
subscription_state_lock = threading.Lock()
track_reid_diagnostics_lock = threading.Lock()


# ============================================================
# 데이터 구조
# ============================================================

@dataclass
class Candidate:
    journey_id: str
    person_uid: str
    received_at: str
    entry_timestamp: str
    entry_epoch: float
    b_passage_timestamp: str
    b_passage_epoch: float
    route: list[str]
    gallery: list[np.ndarray]
    gallery_nodes: list[str]
    tracking_person_uid: str | None = None
    canonical_person_uid: str | None = None
    source_stage: str = "WAITING_D"
    arrival_event_id: str | None = None
    status: str = "PENDING"
    matched_d_local_id: int | None = None
    best_similarity: float | None = None
    top2_mean: float | None = None
    combined_score: float | None = None


@dataclass(frozen=True)
class JourneyScore:
    journey_id: str
    best: float
    top2: float
    combined: float


@dataclass(frozen=True)
class CandidateLoadResult:
    result: str
    reason: str | None
    gallery_count: int
    gallery_nodes: list[str]
    registered: bool
    active_candidate_count: int
    active_gallery_count: int


@dataclass
class TrackReIdDiagnostic:
    journey_id: str
    local_track_id: int
    first_evaluated_at: str
    last_evaluated_at: str
    best_similarity: float
    top2_mean: float
    combined_score: float
    matched: bool


@dataclass(frozen=True)
class ArrivalDiagnostics:
    track_first_seen_at: datetime
    candidate_received_at: datetime
    passage_at: datetime
    arrival_at: datetime
    confirmation_sample_count: int
    confirmation_pass_count: int
    best_journey_score: float
    second_journey_score: float
    journey_margin: float
    eligibility_reason: str


@dataclass(frozen=True)
class CompletedTrack:
    journey_id: str
    person_uid: str
    route: list[str]
    gallery: list[np.ndarray]


@dataclass(frozen=True)
class DisplayTransform:
    scale_x: float
    scale_y: float
    crop_x: int
    crop_y: int


@dataclass(frozen=True)
class DisplayAnnotation:
    box: tuple[int, int, int, int]
    label: str
    detail: str
    color: tuple[int, int, int]


@dataclass
class DTimingSession:
    journey_id: str
    person_uid: str
    entered_at: str
    entered_epoch: float
    matched_at: str
    matched_epoch: float
    active_track_ids: set[int] = field(default_factory=set)
    exited_at: str | None = None
    exited_epoch: float | None = None


candidates: dict[str, Candidate] = {}
completed_journey_ids: set[str] = set()
terminal_journey_ids: set[str] = set()
consumed_track_ids: set[int] = set()
completed_tracks: dict[int, CompletedTrack] = {}
arrival_inflight: set[str] = set()
expired_journey_count = 0
timing_sessions: dict[str, DTimingSession] = {}
timing_sent: set[str] = set()
timing_lock = threading.Lock()
pending_subscriptions: dict[int, tuple[str, int]] = {}
track_reid_diagnostics: dict[tuple[str, int], TrackReIdDiagnostic] = {}


# ============================================================
# 공통 함수
# ============================================================

def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def now_aware() -> datetime:
    return datetime.now().astimezone()


def parse_time(value: str) -> float:
    try:
        return parse_aware_datetime(value).timestamp()
    except (TypeError, ValueError):
        return time.time()


def epoch_iso(value: float) -> str:
    return (
        datetime.fromtimestamp(value)
        .astimezone()
        .isoformat(timespec="milliseconds")
    )


def make_arrival_event_id(
    journey_id: str,
    passage_timestamp: str,
) -> str:
    """Return a retry-stable ID for one D ARRIVAL lifecycle."""

    event_key = f"D:ARRIVAL:{journey_id}:{passage_timestamp}"
    return str(uuid.uuid5(ARRIVAL_EVENT_NAMESPACE, event_key))


def append_arrival_tx_jsonl(
    record: dict[str, Any],
    recorded_at: datetime,
) -> Path:
    """Append one structured ARRIVAL transmission record to a daily JSONL."""

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / (
        f"{ARRIVAL_TX_LOG_PREFIX}_{recorded_at.astimezone():%Y%m%d}.jsonl"
    )
    line = json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    with arrival_tx_log_lock:
        with path.open("a", encoding="utf-8") as file:
            file.write(line)
            file.write("\n")
    return path


def append_candidate_diagnostic(record: dict[str, Any]) -> Path | None:
    """Append a candidate/MQTT/Re-ID diagnostic without breaking runtime."""

    enriched = dict(record)
    enriched.setdefault("received_at", now_iso())
    path = LOG_DIR / CANDIDATE_RX_DIAGNOSTICS_NAME
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            enriched,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with candidate_diagnostics_log_lock:
            with path.open("a", encoding="utf-8") as file:
                file.write(line)
                file.write("\n")
    except OSError as error:
        print(f"[D Candidate 진단 로그 저장 실패] {error}")
        return None
    return path


def candidate_inventory() -> tuple[int, int]:
    with candidate_lock:
        active = [
            item
            for item in candidates.values()
            if item.status in {"PENDING", "PENDING_MATCH"}
        ]
        return len(active), sum(len(item.gallery) for item in active)


def update_track_reid_diagnostic(
    *,
    journey_id: str,
    local_track_id: int,
    evaluated_at: datetime,
    best_similarity: float,
    top2_mean: float,
    combined_score: float,
    matched: bool,
) -> None:
    """Keep the best observation in memory; JSONL is written once per track."""

    key = (journey_id, local_track_id)
    timestamp = evaluated_at.isoformat(timespec="milliseconds")
    with track_reid_diagnostics_lock:
        current = track_reid_diagnostics.get(key)
        if current is None:
            track_reid_diagnostics[key] = TrackReIdDiagnostic(
                journey_id=journey_id,
                local_track_id=local_track_id,
                first_evaluated_at=timestamp,
                last_evaluated_at=timestamp,
                best_similarity=best_similarity,
                top2_mean=top2_mean,
                combined_score=combined_score,
                matched=matched,
            )
            return

        current.last_evaluated_at = timestamp
        current.matched = current.matched or matched
        if combined_score > current.combined_score:
            current.best_similarity = best_similarity
            current.top2_mean = top2_mean
            current.combined_score = combined_score


def flush_track_reid_diagnostics(
    local_track_id: int,
    outcome: str,
) -> int:
    """Write accumulated diagnostics once when a track reaches an outcome."""

    with track_reid_diagnostics_lock:
        keys = [
            key
            for key in track_reid_diagnostics
            if key[1] == local_track_id
        ]
        records = [track_reid_diagnostics.pop(key) for key in keys]

    for item in records:
        append_candidate_diagnostic(
            {
                "record_type": "reid_track",
                "received_at": item.last_evaluated_at,
                "first_evaluated_at": item.first_evaluated_at,
                "journey_id": item.journey_id,
                "local_track_id": item.local_track_id,
                "best_similarity": round(item.best_similarity, 6),
                "top2_mean": round(item.top2_mean, 6),
                "combined_score": round(item.combined_score, 6),
                "threshold": {
                    "best_similarity": MATCH_BEST_THRESHOLD,
                    "top2_mean": MATCH_TOP2_THRESHOLD,
                    "journey_margin": MATCHING_CONFIG.min_journey_margin,
                },
                "matched": item.matched,
                "outcome": outcome,
            }
        )
    return len(records)


def register_timing_match(
    journey_id: str,
    person_uid: str,
    local_id: int,
    entered_epoch: float,
    matched_epoch: float,
) -> None:
    """ARRIVAL 성공 Track을 journey 기반 D timing session에 연결한다."""

    with timing_lock:
        if journey_id in timing_sent:
            return

        session = timing_sessions.get(journey_id)
        if session is None:
            session = DTimingSession(
                journey_id=journey_id,
                person_uid=person_uid,
                entered_at=epoch_iso(entered_epoch),
                entered_epoch=entered_epoch,
                matched_at=epoch_iso(matched_epoch),
                matched_epoch=matched_epoch,
            )
            timing_sessions[journey_id] = session
        else:
            if entered_epoch < session.entered_epoch:
                session.entered_epoch = entered_epoch
                session.entered_at = epoch_iso(entered_epoch)
            if matched_epoch < session.matched_epoch:
                session.matched_epoch = matched_epoch
                session.matched_at = epoch_iso(matched_epoch)

        session.active_track_ids.add(local_id)


def build_node_timing_payload(
    session: DTimingSession,
    local_id: int,
    exited_epoch: float,
) -> dict[str, Any]:
    exited_at = epoch_iso(exited_epoch)
    dwell_seconds = max(0.0, exited_epoch - session.entered_epoch)
    return {
        "schema_version": 1,
        "event": "NODE_TIMING",
        "node_id": "D",
        "person_uid": session.person_uid,
        "global_person_id": session.person_uid,
        "journey_id": session.journey_id,
        "local_track_id": local_id,
        "entered_at": session.entered_at,
        "matched_at": session.matched_at,
        "exited_at": exited_at,
        "dwell_seconds": round(dwell_seconds, 3),
        "exit_reason": "TRACK_LOST",
    }


def publish_timing_on_track_lost(
    client: mqtt.Client,
    journey_id: str,
    local_id: int,
    exited_epoch: float,
) -> bool:
    """Journey의 마지막 활성 D Track 종료 시 timing을 한 번 발행한다."""

    with timing_lock:
        if journey_id in timing_sent:
            return False

        session = timing_sessions.get(journey_id)
        if session is None:
            return False

        session.active_track_ids.discard(local_id)
        if session.active_track_ids:
            return False

        payload = build_node_timing_payload(
            session,
            local_id,
            exited_epoch,
        )
        info = client.publish(
            TIMING_TOPIC,
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            qos=MQTT_QOS,
            retain=False,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[D NODE_TIMING MQTT 실패] rc={info.rc}")
            return False

        session.exited_at = payload["exited_at"]
        session.exited_epoch = exited_epoch
        timing_sent.add(journey_id)

    print()
    print("===== D NODE TIMING =====")
    print(f"Person UID : {payload['person_uid']}")
    print(f"Journey ID : {payload['journey_id']}")
    print(f"Entered    : {payload['entered_at']}")
    print(f"Matched    : {payload['matched_at']}")
    print(f"Exited     : {payload['exited_at']}")
    print(f"Dwell      : {payload['dwell_seconds']:.3f} sec")
    print("=========================")
    return True


def normalize(value: np.ndarray | list[float]) -> np.ndarray:
    array = np.asarray(value, dtype=np.float32).reshape(-1)
    if array.size != 512:
        raise ValueError(f"Embedding 크기 오류: {array.size}")
    if not np.all(np.isfinite(array)):
        raise ValueError("Embedding에 NaN/Inf가 있습니다.")
    norm = float(np.linalg.norm(array))
    if norm <= 1e-12:
        raise ValueError("Embedding norm이 0입니다.")
    return array / norm


def average(history: deque[np.ndarray]) -> np.ndarray:
    return normalize(np.mean(np.stack(history), axis=0))


def extract_crop(frame: np.ndarray, box: list[int]) -> np.ndarray:
    height, width = frame.shape[:2]
    x1, y1, x2, y2 = box
    pad_x = int(max(1, x2 - x1) * 0.04)
    pad_y = int(max(1, y2 - y1) * 0.04)
    x1, y1 = max(0, x1 - pad_x), max(0, y1 - pad_y)
    x2, y2 = min(width, x2 + pad_x), min(height, y2 + pad_y)
    crop = frame[y1:y2, x1:x2]
    if crop.size == 0:
        raise RuntimeError("사람 crop이 비었습니다.")
    return crop.copy()


def crop_quality(
    box: list[int],
    confidence: float,
    frame_width: int,
    frame_height: int,
) -> tuple[bool, float]:
    x1, y1, x2, y2 = box
    area_ratio = (
        max(1, x2 - x1)
        * max(1, y2 - y1)
        / float(max(1, frame_width * frame_height))
    )
    side_ok = x1 >= 3 and x2 <= frame_width - 3
    ok = confidence >= 0.50 and area_ratio >= 0.03 and side_ok
    quality = (
        0.65 * confidence
        + 0.35 * min(1.0, area_ratio / 0.20)
    )
    return ok, float(np.clip(quality, 0.0, 1.0))


def apply_small_brightness_adjustment(
    frame: np.ndarray,
) -> np.ndarray:
    return cv2.convertScaleAbs(
        frame,
        alpha=IMAGE_CONTRAST_ALPHA,
        beta=IMAGE_BRIGHTNESS_BETA,
    )


def route_text(route: list[str]) -> str:
    route_nodes = [str(node) for node in route if node]
    if not route_nodes:
        route_nodes = ["A", "B"]
    if route_nodes[-1] == "D":
        route_nodes = route_nodes[:-1]
    return " > ".join(route_nodes + ["[D]"])


def save_arrival_capture(
    crop: np.ndarray,
    person_uid: str,
    journey_id: str,
    local_id: int,
    score: float,
) -> str:
    captured_at = datetime.now().astimezone()
    day_folder = captured_at.strftime("%Y%m%d")
    target_dir = (
        CAPTURE_ROOT
        / day_folder
        / person_uid
        / journey_id
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = (
        f"D_{captured_at.strftime('%H%M%S_%f')}_"
        f"L{local_id}_S{score:.3f}.jpg"
    )
    capture_path = target_dir / filename

    success = cv2.imwrite(
        str(capture_path),
        crop,
        [cv2.IMWRITE_JPEG_QUALITY, 95],
    )
    if not success:
        raise RuntimeError(
            f"D Capture 저장 실패: {capture_path}"
        )

    return str(capture_path)


def gallery_score(
    embedding: np.ndarray,
    gallery: list[np.ndarray],
) -> tuple[float, float, float]:
    scores = sorted(
        (float(np.dot(embedding, reference)) for reference in gallery),
        reverse=True,
    )
    best = scores[0]
    top2 = float(np.mean(scores[:2])) if len(scores) >= 2 else best
    combined = 0.60 * best + 0.40 * top2
    return best, top2, combined


def ensure_csv() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    definitions = [
        (
            CANDIDATE_CSV,
            [
                "received_at", "journey_id", "entry_timestamp",
                "b_passage_timestamp", "route", "gallery_count", "status",
            ],
        ),
        (
            ARRIVAL_CSV,
            [
                "recorded_at", "journey_id", "d_local_track_id",
                "entry_timestamp", "b_passage_timestamp", "d_arrival_timestamp",
                "route", "total_duration_seconds", "b_to_d_duration_seconds",
                "best_similarity", "top2_mean", "combined_score",
                "gallery_count", "status",
            ],
        ),
    ]
    for path, header in definitions:
        if not path.exists():
            with path.open("w", newline="", encoding="utf-8") as file:
                csv.writer(file).writerow(header)


def append_csv(path: Path, row: list[Any]) -> None:
    with path.open("a", newline="", encoding="utf-8") as file:
        csv.writer(file).writerow(row)


# ============================================================
# 후보 관리
# ============================================================

def save_candidate(
    payload: dict[str, Any],
    received_at: str | None = None,
) -> CandidateLoadResult:
    """
    Main Server가 WAITING_D 상태에서 보낸
    A+B 또는 A+C Gallery 후보를 저장한다.
    """

    journey_id = payload.get("journey_id")
    person_uid = payload.get("person_uid")
    stage = payload.get("stage")
    raw_gallery = payload.get("gallery")

    def rejected(reason: str) -> CandidateLoadResult:
        active_count, active_gallery_count = candidate_inventory()
        gallery_nodes = []
        if isinstance(raw_gallery, list):
            gallery_nodes = [
                str(item.get("node_id", "UNKNOWN"))
                for item in raw_gallery
                if isinstance(item, dict)
            ]
        print(f"[D MQTT] 후보 거부: journey={journey_id}, reason={reason}")
        return CandidateLoadResult(
            result="REJECTED",
            reason=reason,
            gallery_count=len(raw_gallery) if isinstance(raw_gallery, list) else 0,
            gallery_nodes=gallery_nodes,
            registered=False,
            active_candidate_count=active_count,
            active_gallery_count=active_gallery_count,
        )

    if not journey_id:
        return rejected("MISSING_JOURNEY_ID")
    if not person_uid:
        return rejected("MISSING_PERSON_UID")
    if payload.get("event") != "CANDIDATE":
        return rejected(f"INVALID_EVENT:{payload.get('event')}")
    if stage != "WAITING_D":
        return rejected(f"INVALID_STAGE:{stage}")
    if not isinstance(raw_gallery, list):
        return rejected("INVALID_GALLERY_TYPE")
    if not raw_gallery:
        return rejected("EMPTY_GALLERY")

    journey_id = str(journey_id)
    person_uid = str(person_uid)

    gallery: list[np.ndarray] = []
    gallery_nodes: list[str] = []

    try:
        for index, item in enumerate(
            raw_gallery
        ):
            if (
                not isinstance(item, dict)
                or not isinstance(
                    item.get("embedding"),
                    list,
                )
            ):
                raise ValueError(
                    f"Gallery {index} 형식 오류"
                )

            gallery.append(
                normalize(
                    item["embedding"]
                )
            )

            gallery_nodes.append(
                str(
                    item.get(
                        "node_id",
                        "UNKNOWN",
                    )
                )
            )

    except ValueError as error:
        return rejected(f"INVALID_GALLERY:{error}")

    entry_timestamp = payload.get("entry_timestamp")
    passage_timestamp = payload.get(
        "passage_timestamp",
        payload.get("b_passage_timestamp"),
    )
    try:
        entry_at = parse_aware_datetime(str(entry_timestamp))
        passage_at = parse_aware_datetime(str(passage_timestamp))
    except (TypeError, ValueError) as error:
        return rejected(f"INVALID_TIMESTAMP:{error}")

    route = list(
        payload.get(
            "route",
            ["A", "B"],
        )
    )

    entry_timestamp = str(entry_timestamp)
    passage_timestamp = str(passage_timestamp)

    candidate = Candidate(
        journey_id=journey_id,
        person_uid=person_uid,

        received_at=received_at or now_iso(),

        entry_timestamp=entry_timestamp,
        entry_epoch=entry_at.timestamp(),

        # 기존 변수명은 호환성을 위해 유지
        b_passage_timestamp=passage_timestamp,
        b_passage_epoch=passage_at.timestamp(),

        route=route,
        gallery=gallery,
        gallery_nodes=gallery_nodes,
        tracking_person_uid=(
            str(payload["tracking_person_uid"])
            if payload.get("tracking_person_uid") is not None
            else None
        ),
        canonical_person_uid=str(
            payload.get("canonical_person_uid") or person_uid
        ),
        source_stage=str(stage),
    )

    rejection_reason: str | None = None
    with candidate_lock:
        old = candidates.get(journey_id)
        if journey_id in terminal_journey_ids:
            rejection_reason = "TERMINAL_JOURNEY"
        elif old is not None:
            rejection_reason = "DUPLICATE_CANDIDATE"
        else:
            candidates[journey_id] = candidate
    if rejection_reason is not None:
        return rejected(rejection_reason)

    try:
        append_csv(
            CANDIDATE_CSV,
            [
                candidate.received_at,
                journey_id,
                entry_timestamp,
                passage_timestamp,
                "-".join(route),
                len(gallery),
                "PENDING",
            ],
        )
    except OSError as error:
        print(f"[D Candidate CSV 저장 실패] {error}")

    print()
    print(
        "===== D Main Gallery 후보 수신 ====="
    )
    print(f"Person UID    : {person_uid}")
    print(f"Journey ID    : {journey_id}")
    print(f"Stage         : {stage}")
    print(f"Route         : {route}")
    print(f"Gallery Count : {len(gallery)}")
    print(f"Gallery Nodes : {gallery_nodes}")
    print("Status        : PENDING")
    print(
        "===================================="
    )
    active_count, active_gallery_count = candidate_inventory()
    return CandidateLoadResult(
        result="LOADED",
        reason=None,
        gallery_count=len(gallery),
        gallery_nodes=gallery_nodes,
        registered=True,
        active_candidate_count=active_count,
        active_gallery_count=active_gallery_count,
    )


def cleanup_candidates(
    current_at: datetime | None = None,
) -> list[Candidate]:
    global expired_journey_count

    current = (current_at or now_aware()).timestamp()
    expired: list[Candidate] = []
    with candidate_lock:
        for journey_id, item in list(candidates.items()):
            if (
                item.status in {"PENDING", "PENDING_MATCH"}
                and current - item.b_passage_epoch > CANDIDATE_TIMEOUT_SECONDS
            ):
                expired.append(candidates.pop(journey_id))
                terminal_journey_ids.add(journey_id)
        expired_journey_count += len(expired)

    for item in expired:
        log_match_reject(
            local_id=None,
            journey_id=item.journey_id,
            track_first_seen_at=None,
            passage_at=parse_aware_datetime(item.b_passage_timestamp),
            passage_to_d_duration=current - item.b_passage_epoch,
            best=-1.0,
            combined=-1.0,
            best_journey=-1.0,
            second_journey=-1.0,
            journey_margin=-1.0,
            confirmation_sample_count=0,
            confirmation_pass_count=0,
            reason="EXPIRED_JOURNEY",
        )
    return expired


def get_candidate(journey_id: str) -> Candidate | None:
    with candidate_lock:
        return candidates.get(journey_id)


def rank_eligible_journeys(
    embedding: np.ndarray,
    track: TrackEligibility,
    evaluated_at: datetime,
) -> tuple[list[JourneyScore], list[tuple[Candidate, str, float]]]:
    cleanup_candidates(evaluated_at)
    eligible: list[Candidate] = []
    rejected: list[tuple[Candidate, str, float]] = []

    with candidate_lock:
        active = [
            item
            for item in candidates.values()
            if item.status in {"PENDING", "PENDING_MATCH"}
        ]

    for item in active:
        passage_at = parse_aware_datetime(item.b_passage_timestamp)
        reason, duration = temporal_rejection_reason(
            track,
            passage_at,
            evaluated_at,
            MATCHING_CONFIG,
        )
        if reason is not None:
            rejected.append((item, reason, duration))
        else:
            eligible.append(item)

    results: list[JourneyScore] = []
    for item in eligible:
        best, top2, combined = gallery_score(embedding, item.gallery)
        results.append(JourneyScore(item.journey_id, best, top2, combined))

    results.sort(key=lambda item: item.combined, reverse=True)
    return results, rejected


def find_best_candidate(
    embedding: np.ndarray,
) -> tuple[str | None, float, float, float, float]:
    """Compatibility helper; runtime matching uses rank_eligible_journeys()."""

    cleanup_candidates()
    results: list[JourneyScore] = []
    with candidate_lock:
        active = [
            item
            for item in candidates.values()
            if item.status in {"PENDING", "PENDING_MATCH"}
        ]
    for item in active:
        best, top2, combined = gallery_score(embedding, item.gallery)
        results.append(JourneyScore(item.journey_id, best, top2, combined))

    if not results:
        return None, -1.0, -1.0, -1.0, -1.0

    results.sort(key=lambda item: item.combined, reverse=True)
    winner = results[0]
    second_combined = results[1].combined if len(results) >= 2 else -1.0
    return (
        winner.journey_id,
        winner.best,
        winner.top2,
        winner.combined,
        second_combined,
    )


def log_match_reject(
    *,
    local_id: int | None,
    journey_id: str,
    track_first_seen_at: datetime | None,
    passage_at: datetime,
    passage_to_d_duration: float,
    best: float,
    combined: float,
    best_journey: float,
    second_journey: float,
    journey_margin: float,
    confirmation_sample_count: int,
    confirmation_pass_count: int,
    reason: str,
) -> None:
    print("\n[D MATCH REJECT]")
    print(f"local_track_id={local_id if local_id is not None else '-'}")
    print(f"journey_id={journey_id}")
    print(
        "track_first_seen_at="
        f"{track_first_seen_at.isoformat() if track_first_seen_at else '-'}"
    )
    print(f"passage_timestamp={passage_at.isoformat()}")
    print(f"passage_to_d_duration={passage_to_d_duration:.3f}")
    print(f"best/combined={best:.6f}/{combined:.6f}")
    print(
        "best_journey/second_journey/margin="
        f"{best_journey:.6f}/{second_journey:.6f}/{journey_margin:.6f}"
    )
    print(
        "confirmation_count="
        f"{confirmation_pass_count}/{confirmation_sample_count}"
    )
    print(f"reason={reason}")


def complete_arrival(
    client: mqtt.Client,
    journey_id: str,
    local_id: int,
    best: float,
    top2: float,
    combined: float,
    d_embedding: np.ndarray,
    capture_crop: np.ndarray,
    capture_quality: float,
    diagnostics: ArrivalDiagnostics,
) -> bool:
    d_embedding = normalize(
        d_embedding
    )

    passage_to_d_duration = (
        diagnostics.arrival_at - diagnostics.passage_at
    ).total_seconds()
    rejection_reason: str | None = None
    if (
        diagnostics.track_first_seen_at.timestamp()
        < diagnostics.passage_at.timestamp()
        - MATCHING_CONFIG.clock_tolerance_seconds
        or diagnostics.eligibility_reason != "ELIGIBLE_NEW_ENTRY"
    ):
        rejection_reason = "PREEXISTING_TRACK"
    elif passage_to_d_duration < MATCHING_CONFIG.min_passage_to_d_seconds:
        rejection_reason = "TOO_EARLY"
    elif passage_to_d_duration > MATCHING_CONFIG.max_passage_to_d_seconds:
        rejection_reason = "EXPIRED_JOURNEY"
    elif (
        diagnostics.confirmation_sample_count
        < MATCHING_CONFIG.confirmation_window_size
        or diagnostics.confirmation_pass_count
        < MATCHING_CONFIG.confirmation_required_passes
    ):
        rejection_reason = "INSUFFICIENT_CONSENSUS"
    elif (
        diagnostics.second_journey_score > 0.0
        and diagnostics.journey_margin
        < MATCHING_CONFIG.min_journey_margin
    ):
        rejection_reason = "INSUFFICIENT_JOURNEY_MARGIN"

    if rejection_reason is not None:
        log_match_reject(
            local_id=local_id,
            journey_id=journey_id,
            track_first_seen_at=diagnostics.track_first_seen_at,
            passage_at=diagnostics.passage_at,
            passage_to_d_duration=passage_to_d_duration,
            best=best,
            combined=combined,
            best_journey=diagnostics.best_journey_score,
            second_journey=diagnostics.second_journey_score,
            journey_margin=diagnostics.journey_margin,
            confirmation_sample_count=diagnostics.confirmation_sample_count,
            confirmation_pass_count=diagnostics.confirmation_pass_count,
            reason=rejection_reason,
        )
        return False

    with candidate_lock:
        item = candidates.get(journey_id)

        if (
            item is None
            or item.status not in {"PENDING", "PENDING_MATCH"}
            or journey_id in completed_journey_ids
            or journey_id in arrival_inflight
            or local_id in consumed_track_ids
        ):
            return False

        arrival_inflight.add(journey_id)

        person_uid = item.person_uid
        tracking_person_uid = item.tracking_person_uid
        canonical_person_uid = item.canonical_person_uid or person_uid
        source_stage = item.source_stage
        if item.arrival_event_id is None:
            item.arrival_event_id = make_arrival_event_id(
                journey_id,
                item.b_passage_timestamp,
            )
        arrival_event_id = item.arrival_event_id

        entry_timestamp = (
            item.entry_timestamp
        )
        entry_epoch = item.entry_epoch

        passage_timestamp = (
            item.b_passage_timestamp
        )
        passage_epoch = (
            item.b_passage_epoch
        )

        gallery_count = len(
            item.gallery
        )

        route = list(
            item.route
        )

        completed_track = CompletedTrack(
            journey_id=journey_id,
            person_uid=person_uid,
            route=list(route),
            gallery=[reference.copy() for reference in item.gallery],
        )

    arrival_timestamp = diagnostics.arrival_at.isoformat(timespec="milliseconds")
    arrival_epoch = diagnostics.arrival_at.timestamp()

    total_duration = max(
        0.0,
        arrival_epoch - entry_epoch,
    )

    passage_to_d_duration = arrival_epoch - passage_epoch

    if not route or route[-1] != "D":
        route.append("D")

    embedding_quality = float(
        np.clip(
            combined,
            0.0,
            1.0,
        )
    )

    try:
        capture_path = save_arrival_capture(
            crop=capture_crop,
            person_uid=person_uid,
            journey_id=journey_id,
            local_id=local_id,
            score=combined,
        )
    except Exception as error:
        print(f"[D Capture 저장 실패] {error}")
        capture_path = ""

    payload = {
        "schema_version": 1,
        "event": "ARRIVAL",
        "arrival_event_id": arrival_event_id,

        "journey_id": journey_id,
        "person_uid": person_uid,

        # 이전 코드 호환용
        "global_person_id": person_uid,

        "node_id": "D",
        "current_node": "D",

        "route": route,

        "entry_timestamp": (
            entry_timestamp
        ),
        "passage_timestamp": (
            passage_timestamp
        ),
        "d_arrival_timestamp": (
            arrival_timestamp
        ),

        "total_duration_seconds": round(
            total_duration,
            3,
        ),

        "passage_to_d_duration_seconds": (
            round(
                passage_to_d_duration,
                3,
            )
        ),

        # D matching diagnostics (additive; existing fields remain unchanged)
        "d_track_first_seen_at": diagnostics.track_first_seen_at.isoformat(
            timespec="milliseconds"
        ),
        "candidate_received_at": diagnostics.candidate_received_at.isoformat(
            timespec="seconds"
        ),
        "confirmation_sample_count": diagnostics.confirmation_sample_count,
        "confirmation_pass_count": diagnostics.confirmation_pass_count,
        "best_journey_score": round(diagnostics.best_journey_score, 6),
        "second_journey_score": round(diagnostics.second_journey_score, 6),
        "journey_margin": round(diagnostics.journey_margin, 6),
        "eligibility_reason": diagnostics.eligibility_reason,

        "d_local_track_id": local_id,
        "gallery_count": gallery_count,

        # Main Server가 바로 읽는 최종 점수
        "best_similarity": round(
            best,
            6,
        ),
        "top2_mean": round(
            top2,
            6,
        ),
        "combined_score": round(
            combined,
            6,
        ),

        # D에서 최종 매칭한 특징값
        "embedding_dim": int(d_embedding.size),
        "embedding": (
            d_embedding
            .astype(np.float32)
            .tolist()
        ),

        # 현재는 최종 매칭 점수를
        # D 특징 신뢰도의 초기값으로 사용
        "quality": round(
            embedding_quality,
            6,
        ),
        "quality_source": (
            "combined_match_score"
        ),

        "match": {
            "best_similarity": round(
                best,
                6,
            ),
            "top2_mean": round(
                top2,
                6,
            ),
            "combined_score": round(
                combined,
                6,
            ),
        },

        # Main Server의 Capture/관리자 DB 기록용
        "local_track_id": local_id,
        "capture_path": capture_path,
        "similarity": round(combined, 6),
        "capture_quality": round(
            float(np.clip(capture_quality, 0.0, 1.0)),
            6,
        ),
        "verification_status": "AUTO_MATCHED",

        "status": "COMPLETED",
    }

    payload_raw = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    payload_bytes = payload_raw.encode("utf-8")
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    published_at_dt = now_aware()
    published_at = published_at_dt.isoformat(timespec="milliseconds")
    tx_metadata = {
        "log_version": 1,
        "record_type": "d_arrival_tx",
        "arrival_event_id": arrival_event_id,
        "topic": ARRIVAL_TOPIC,
        "qos": MQTT_QOS,
        "person_uid": person_uid,
        "tracking_person_uid": tracking_person_uid,
        "canonical_person_uid": canonical_person_uid,
        "journey_id": journey_id,
        "local_track_id": local_id,
        "route": route,
        "stage": source_stage,
        "status": payload["status"],
        "passage_timestamp": passage_timestamp,
        "track_first_seen_at": payload["d_track_first_seen_at"],
        "matched_at": arrival_timestamp,
        "published_at": published_at,
        "best_similarity": payload["best_similarity"],
        "top2_mean": payload["top2_mean"],
        "combined_score": payload["combined_score"],
        "journey_margin": payload["journey_margin"],
        "confirmation_count": diagnostics.confirmation_pass_count,
        "confirmation_sample_count": diagnostics.confirmation_sample_count,
        "embedding_dim": int(d_embedding.size),
        "embedding_norm": round(float(np.linalg.norm(d_embedding)), 9),
        "capture_path": capture_path,
        "payload_size_bytes": len(payload_bytes),
        "payload_sha256": payload_sha256,
    }
    pre_publish_record = {
        **tx_metadata,
        "phase": "PRE_PUBLISH",
        "payload_raw": payload_raw,
    }
    try:
        tx_log_path = append_arrival_tx_jsonl(
            pre_publish_record,
            published_at_dt,
        )
    except OSError as error:
        tx_log_path = None
        print(f"[D ARRIVAL TX 로그 저장 실패] {error}")

    print(
        "[D ARRIVAL TX] "
        f"event_id={arrival_event_id} journey={journey_id} "
        f"local={local_id} topic={ARRIVAL_TOPIC} qos={MQTT_QOS} "
        f"bytes={len(payload_bytes)} sha256={payload_sha256}"
    )

    info = None
    publish_rc: int | None = None
    mid: int | None = None
    puback_received = False
    timed_out = False
    failed = False
    publish_error: str | None = None
    try:
        info = client.publish(
            ARRIVAL_TOPIC,
            payload_bytes,
            qos=MQTT_QOS,
            retain=False,
        )
        publish_rc = int(info.rc)
        mid = int(info.mid)
        if publish_rc == int(mqtt.MQTT_ERR_SUCCESS):
            info.wait_for_publish(timeout=ARRIVAL_PUBACK_TIMEOUT_SECONDS)
            published = bool(info.is_published())
            puback_received = published if MQTT_QOS > 0 else False
            timed_out = not published
            failed = timed_out
        else:
            failed = True
            publish_error = mqtt.error_string(publish_rc)
    except (
        AttributeError,
        RuntimeError,
        TypeError,
        ValueError,
        OSError,
    ) as error:
        failed = True
        publish_error = f"{type(error).__name__}: {error}"
        if info is not None:
            try:
                publish_rc = int(info.rc)
            except (AttributeError, TypeError, ValueError):
                pass
        published = False
        if info is not None and hasattr(info, "is_published"):
            try:
                published = bool(info.is_published())
            except (RuntimeError, ValueError):
                published = False
        timed_out = (
            publish_rc == int(mqtt.MQTT_ERR_SUCCESS)
            and not published
        )

    result_record = {
        **tx_metadata,
        "phase": "PUBLISH_RESULT",
        "publish_rc": publish_rc,
        "mid": mid,
        "puback_received": puback_received,
        "failed": failed,
        "timeout": timed_out,
        "error": publish_error,
    }
    try:
        append_arrival_tx_jsonl(result_record, published_at_dt)
    except OSError as error:
        print(f"[D ARRIVAL TX 결과 로그 저장 실패] {error}")

    print(
        "[D ARRIVAL MQTT] "
        f"event_id={arrival_event_id} rc={publish_rc} mid={mid} "
        f"puback={puback_received} failed={failed} timeout={timed_out}"
        + (f" error={publish_error}" if publish_error else "")
        + (f" log={tx_log_path}" if tx_log_path else "")
    )

    if failed:
        with candidate_lock:
            arrival_inflight.discard(journey_id)
        return False

    with candidate_lock:
        current = candidates.get(journey_id)
        if current is item:
            candidates.pop(journey_id, None)
        arrival_inflight.discard(journey_id)
        completed_journey_ids.add(journey_id)
        terminal_journey_ids.add(journey_id)
        consumed_track_ids.add(local_id)
        completed_tracks[local_id] = completed_track

    append_csv(
        ARRIVAL_CSV,
        [
            now_iso(),
            journey_id,
            local_id,
            entry_timestamp,
            passage_timestamp,
            arrival_timestamp,
            "-".join(route),
            f"{total_duration:.3f}",
            (
                f"{passage_to_d_duration:.3f}"
            ),
            f"{best:.6f}",
            f"{top2:.6f}",
            f"{combined:.6f}",
            gallery_count,
            "COMPLETED",
        ],
    )

    print()
    print("===== D → MAIN 도착 완료 =====")
    print(f"Arrival Event ID: {arrival_event_id}")
    print(f"Person UID      : {person_uid}")
    print(f"Journey ID      : {journey_id}")
    print(f"D Local ID      : {local_id}")
    print(f"Route           : {route}")
    print(
        f"Best Similarity : "
        f"{best:.6f}"
    )
    print(
        f"Top2 Mean       : "
        f"{top2:.6f}"
    )
    print(
        f"Combined Score  : "
        f"{combined:.6f}"
    )
    print(
        f"D Embedding Dim : "
        f"{d_embedding.size}"
    )
    print(
        f"Total Duration  : "
        f"{total_duration:.3f} sec"
    )
    print(f"Capture         : {capture_path or '저장 실패'}")
    print("Status          : COMPLETED")
    print("==============================")
    return True


# ============================================================
# MQTT
# ============================================================

def reason_code_value(reason_code: Any) -> int:
    return int(getattr(reason_code, "value", reason_code))


def subscribe_with_diagnostics(
    client: mqtt.Client,
    topic: str,
) -> tuple[int, int | None]:
    rc, mid = client.subscribe(topic, qos=MQTT_QOS)
    rc_value = int(rc)
    mid_value = int(mid) if mid is not None else None
    if mid_value is not None:
        with subscription_state_lock:
            pending_subscriptions[mid_value] = (topic, MQTT_QOS)
    append_candidate_diagnostic(
        {
            "record_type": "mqtt_subscribe_request",
            "client_id": MQTT_CLIENT_ID,
            "topic": topic,
            "requested_qos": MQTT_QOS,
            "subscribe_rc": rc_value,
            "mid": mid_value,
        }
    )
    print(
        f"Camera D MQTT 구독 요청: topic={topic}, qos={MQTT_QOS}, "
        f"rc={rc_value}, mid={mid_value}"
    )
    return rc_value, mid_value


def on_connect(client, userdata, flags, reason_code, properties) -> None:
    reason_value = reason_code_value(reason_code)
    session_present = getattr(flags, "session_present", None)
    if isinstance(flags, dict):
        session_present = flags.get("session present", flags.get("session_present"))
    append_candidate_diagnostic(
        {
            "record_type": "mqtt_connect",
            "client_id": MQTT_CLIENT_ID,
            "host": MQTT_HOST,
            "port": MQTT_PORT,
            "reason_code": reason_value,
            "connected": reason_value == 0,
            "session_present": session_present,
        }
    )
    if reason_value != 0:
        print(f"Camera D MQTT 연결 실패: {reason_code}")
        return
    print(f"Camera D MQTT 연결 완료: {MQTT_HOST}:{MQTT_PORT}")
    with subscription_state_lock:
        pending_subscriptions.clear()
    subscribe_with_diagnostics(client, CANDIDATE_TOPIC)
    subscribe_with_diagnostics(client, JOURNEY_CONTROL_TOPIC)
    print(f"Camera D MQTT 발행: {ARRIVAL_TOPIC}")
    print(f"Camera D MQTT 발행: {TIMING_TOPIC}")
    print(f"Camera D MQTT 발행: {DETECTION_TOPIC}")


def on_subscribe(
    client,
    userdata,
    mid,
    reason_code_list,
    properties,
) -> None:
    mid_value = int(mid)
    with subscription_state_lock:
        request = pending_subscriptions.pop(mid_value, None)
    topic, requested_qos = request or (None, None)
    granted_qos = [reason_code_value(code) for code in reason_code_list]
    accepted = bool(granted_qos) and all(code < 128 for code in granted_qos)
    append_candidate_diagnostic(
        {
            "record_type": "mqtt_suback",
            "client_id": MQTT_CLIENT_ID,
            "topic": topic,
            "requested_qos": requested_qos,
            "mid": mid_value,
            "granted_qos": granted_qos,
            "accepted": accepted,
        }
    )
    print(
        f"Camera D MQTT SUBACK: topic={topic}, mid={mid_value}, "
        f"granted_qos={granted_qos}, accepted={accepted}"
    )


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties,
) -> None:
    reason_value = reason_code_value(reason_code)
    append_candidate_diagnostic(
        {
            "record_type": "mqtt_disconnect",
            "client_id": MQTT_CLIENT_ID,
            "reason_code": reason_value,
            "unexpected": reason_value != 0,
        }
    )
    print(
        f"Camera D MQTT 연결 종료 감지: reason_code={reason_value}, "
        f"unexpected={reason_value != 0}"
    )


def handle_journey_control(
    payload: dict[str, Any],
    *,
    received_at: str,
    topic: str,
) -> bool:
    raw_journey_id = payload.get("journey_id")
    journey_id = str(raw_journey_id) if raw_journey_id else None
    raw_action = payload.get("action") or payload.get("status") or payload.get("stage")
    action = str(raw_action).upper() if raw_action is not None else None
    remove_actions = {
        "REMOVE",
        "DELETE",
        "EXPIRE",
        "EXPIRED",
        "CANCEL",
        "CANCELLED",
        "CANCELED",
        "COMPLETE",
        "COMPLETED",
    }
    removed = False
    reason: str | None = None
    if journey_id is None:
        reason = "MISSING_JOURNEY_ID"
    elif action not in remove_actions:
        reason = f"UNSUPPORTED_ACTION:{action}"
    else:
        with candidate_lock:
            removed = candidates.pop(journey_id, None) is not None
            terminal_journey_ids.add(journey_id)

    active_count, active_gallery_count = candidate_inventory()
    append_candidate_diagnostic(
        {
            "record_type": "journey_control_rx",
            "received_at": received_at,
            "topic": topic,
            "journey_id": journey_id,
            "action": action,
            "removed": removed,
            "reason": reason,
            "active_candidate_count": active_count,
            "active_gallery_count": active_gallery_count,
        }
    )
    print(
        f"[D Journey Control] journey={journey_id}, action={action}, "
        f"removed={removed}, reason={reason or '-'}"
    )
    return removed


def on_message(
    client,
    userdata,
    message,
) -> None:
    topic = str(message.topic)
    if topic not in {CANDIDATE_TOPIC, JOURNEY_CONTROL_TOPIC}:
        return

    received_at = now_aware().isoformat(timespec="milliseconds")
    raw_payload = bytes(message.payload)
    payload_sha256 = hashlib.sha256(raw_payload).hexdigest()
    try:
        payload = json.loads(
            raw_payload.decode(
                "utf-8"
            )
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        print(
            f"[D MQTT] 잘못된 메시지: "
            f"{error}"
        )
        append_candidate_diagnostic(
            {
                "record_type": (
                    "candidate_rx"
                    if topic == CANDIDATE_TOPIC
                    else "journey_control_rx"
                ),
                "received_at": received_at,
                "topic": topic,
                "journey_id": None,
                "person_uid": None,
                "gallery_count": 0,
                "gallery_node_ids": [],
                "payload_sha256": payload_sha256,
                "result": "REJECTED",
                "reason": f"INVALID_JSON:{error}",
                "message_qos": getattr(message, "qos", None),
                "retain": getattr(message, "retain", None),
                "dup": getattr(message, "dup", None),
            }
        )
        return

    if not isinstance(payload, dict):
        append_candidate_diagnostic(
            {
                "record_type": "candidate_rx" if topic == CANDIDATE_TOPIC else "journey_control_rx",
                "received_at": received_at,
                "topic": topic,
                "journey_id": None,
                "person_uid": None,
                "gallery_count": 0,
                "gallery_node_ids": [],
                "payload_sha256": payload_sha256,
                "result": "REJECTED",
                "reason": "PAYLOAD_NOT_OBJECT",
            }
        )
        return

    if topic == JOURNEY_CONTROL_TOPIC:
        handle_journey_control(
            payload,
            received_at=received_at,
            topic=topic,
        )
        return

    journey_id = payload.get("journey_id")
    terminal_status = payload.get("status") in {
        "COMPLETED",
        "EXPIRED",
        "CANCELLED",
        "CANCELED",
    }
    terminal_stage = payload.get("stage") in {
        "COMPLETED",
        "EXPIRED",
        "CANCELLED",
        "CANCELED",
    }
    if journey_id and (terminal_status or terminal_stage):
        handle_journey_control(
            payload,
            received_at=received_at,
            topic=topic,
        )
        return

    result = save_candidate(payload, received_at=received_at)
    append_candidate_diagnostic(
        {
            "record_type": "candidate_rx",
            "received_at": received_at,
            "topic": topic,
            "journey_id": payload.get("journey_id"),
            "person_uid": payload.get("person_uid"),
            "gallery_count": result.gallery_count,
            "gallery_node_ids": result.gallery_nodes,
            "payload_sha256": payload_sha256,
            "result": result.result,
            "reason": result.reason,
            "registered": result.registered,
            "active_candidate_count": result.active_candidate_count,
            "active_gallery_count": result.active_gallery_count,
            "message_qos": getattr(message, "qos", None),
            "retain": getattr(message, "retain", None),
            "dup": getattr(message, "dup", None),
            "rebuild": bool(
                payload.get("rebuild")
                or payload.get("rebuild_source")
            ),
        }
    )


def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
    )
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


# ============================================================
# 웹 화면
# ============================================================

class ReusableServer(ThreadingHTTPServer):
    allow_reuse_address = True


class StreamHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/":
            html = b"""<!doctype html><html><head><meta charset="utf-8">
<title>Camera D</title><style>
body{margin:0;background:#111;color:#fff;text-align:center;font-family:Arial}
img{width:95%;max-width:1280px;border:2px solid #fff}</style></head>
<body><h2>Camera D - Administrator View</h2><img src="/stream"></body></html>"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return

        if self.path == "/stream":
            self.send_response(200)
            self.send_header(
                "Content-Type", "multipart/x-mixed-replace; boundary=frame"
            )
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            try:
                while True:
                    with frame_lock:
                        data = latest_jpeg
                    if data is None:
                        time.sleep(0.05)
                        continue
                    self.wfile.write(b"--frame\r\nContent-Type: image/jpeg\r\n")
                    self.wfile.write(f"Content-Length: {len(data)}\r\n\r\n".encode())
                    self.wfile.write(data + b"\r\n")
                    self.wfile.flush()
                    time.sleep(0.01)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass
            return

        self.send_error(404)

    def log_message(self, format, *args) -> None:
        return


def start_web() -> None:
    print(f"Camera D 웹 서버: http://<jetson-d-ip>:{WEB_PORT}")
    ReusableServer(("0.0.0.0", WEB_PORT), StreamHandler).serve_forever()


def build_candidate_dashboard(
    frame: np.ndarray,
    annotations: list[DisplayAnnotation] | None = None,
) -> np.ndarray:
    """표시용 frame을 16:9 cover-crop하고 관리자 정보를 overlay한다."""

    cleanup_candidates()

    with candidate_lock:
        snapshot = list(candidates.values())

    dashboard, transform = resize_and_center_crop(frame)
    display_annotations = annotations or []

    pending_count = sum(item.status == "PENDING" for item in snapshot)
    completed_count = len(completed_journey_ids)
    expired_count = expired_journey_count

    status_text, status_color = display_status(display_annotations)
    draw_translucent_panel(dashboard, (16, 16), (290, 82))
    draw_translucent_panel(
        dashboard,
        (STREAM_WIDTH - 536, 16),
        (STREAM_WIDTH - 16, 82),
    )

    # Hershey 폰트는 Unicode bullet을 지원하지 않으므로 빨간 원을 직접 그려
    # Camera A와 같은 "● LIVE | CAM D" HUD를 구성한다.
    cv2.circle(dashboard, (34, 40), 6, (55, 75, 255), -1)
    cv2.putText(
        dashboard,
        "LIVE | CAM D",
        (50, 46),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.62,
        (245, 245, 245),
        2,
    )
    cv2.putText(
        dashboard,
        f"BEST {MATCH_BEST_THRESHOLD:.2f}  TOP2 {MATCH_TOP2_THRESHOLD:.2f}",
        (30, 69),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.52,
        (0, 220, 255),
        1,
    )
    cv2.putText(
        dashboard,
        status_text,
        (STREAM_WIDTH - 520, 44),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.57,
        status_color,
        2,
    )
    cv2.putText(
        dashboard,
        (
            f"PENDING {pending_count}  COMPLETED {completed_count}  "
            f"EXPIRED {expired_count}"
        ),
        (STREAM_WIDTH - 520, 69),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (235, 235, 235),
        1,
    )

    for annotation in display_annotations:
        draw_display_annotation(dashboard, annotation, transform)

    return dashboard


def resize_and_center_crop(
    frame: np.ndarray,
) -> tuple[np.ndarray, DisplayTransform]:
    """왜곡/letterbox 없이 STREAM 크기를 채우는 표시 전용 cover 변환."""

    source_height, source_width = frame.shape[:2]
    if source_width <= 0 or source_height <= 0:
        raise ValueError("표시할 frame 크기가 올바르지 않습니다.")

    scale = max(
        STREAM_WIDTH / source_width,
        STREAM_HEIGHT / source_height,
    )
    resized_width = max(STREAM_WIDTH, round(source_width * scale))
    resized_height = max(STREAM_HEIGHT, round(source_height * scale))
    resized = cv2.resize(
        frame,
        (resized_width, resized_height),
        interpolation=(
            cv2.INTER_AREA
            if scale < 1.0
            else cv2.INTER_LINEAR
        ),
    )
    crop_x = (resized_width - STREAM_WIDTH) // 2
    crop_y = (resized_height - STREAM_HEIGHT) // 2
    display = resized[
        crop_y:crop_y + STREAM_HEIGHT,
        crop_x:crop_x + STREAM_WIDTH,
    ].copy()
    return display, DisplayTransform(
        scale_x=resized_width / source_width,
        scale_y=resized_height / source_height,
        crop_x=crop_x,
        crop_y=crop_y,
    )


def draw_translucent_panel(
    frame: np.ndarray,
    top_left: tuple[int, int],
    bottom_right: tuple[int, int],
    alpha: float = 0.62,
) -> None:
    frame_height, frame_width = frame.shape[:2]
    x1 = max(0, min(frame_width - 1, top_left[0]))
    y1 = max(0, min(frame_height - 1, top_left[1]))
    x2 = max(x1, min(frame_width - 1, bottom_right[0]))
    y2 = max(y1, min(frame_height - 1, bottom_right[1]))
    region = frame[y1:y2 + 1, x1:x2 + 1]
    panel = np.full_like(region, (12, 16, 22))
    cv2.addWeighted(panel, alpha, region, 1.0 - alpha, 0, dst=region)


def display_status(
    annotations: list[DisplayAnnotation],
) -> tuple[str, tuple[int, int, int]]:
    labels = [annotation.label for annotation in annotations]
    if any(label.startswith("ANOMALY") for label in labels):
        return "ANOMALY DETECTED", (40, 60, 255)
    if any("VERIFYING" in label or "CHECKING" in label for label in labels):
        return "STATUS: VERIFYING", (0, 220, 255)
    if any("ARRIVED" in label for label in labels):
        return "STATUS: ARRIVAL", (40, 230, 80)
    return "STATUS: MONITORING", (235, 235, 235)


def draw_display_annotation(
    frame: np.ndarray,
    annotation: DisplayAnnotation,
    transform: DisplayTransform,
) -> None:
    """Inference 좌표를 표시 좌표로 옮겨 box와 문구를 안전 영역에 그린다."""

    source_x1, source_y1, source_x2, source_y2 = annotation.box
    x1 = round(source_x1 * transform.scale_x) - transform.crop_x
    y1 = round(source_y1 * transform.scale_y) - transform.crop_y
    x2 = round(source_x2 * transform.scale_x) - transform.crop_x
    y2 = round(source_y2 * transform.scale_y) - transform.crop_y

    frame_height, frame_width = frame.shape[:2]
    if x2 < 0 or y2 < 0 or x1 >= frame_width or y1 >= frame_height:
        return

    x1 = max(0, min(frame_width - 1, x1))
    y1 = max(0, min(frame_height - 1, y1))
    x2 = max(0, min(frame_width - 1, x2))
    y2 = max(0, min(frame_height - 1, y2))
    cv2.rectangle(frame, (x1, y1), (x2, y2), annotation.color, 3)

    font = cv2.FONT_HERSHEY_SIMPLEX
    label_scale = 0.65
    detail_scale = 0.49
    label_size = cv2.getTextSize(annotation.label, font, label_scale, 2)[0]
    detail_size = cv2.getTextSize(annotation.detail, font, detail_scale, 2)[0]
    text_width = max(label_size[0], detail_size[0])
    text_x = max(8, min(x1, frame_width - text_width - 8))

    if y1 >= 112:
        label_y = y1 - 32
        detail_y = y1 - 8
    elif y2 <= frame_height - 60:
        label_y = y2 + 27
        detail_y = y2 + 51
    else:
        label_y = max(112, min(y1 + 30, frame_height - 32))
        detail_y = min(label_y + 24, frame_height - 8)

    panel_top = max(0, label_y - 22)
    panel_bottom = min(frame_height - 1, detail_y + 7)
    draw_translucent_panel(
        frame,
        (max(0, text_x - 5), panel_top),
        (min(frame_width - 1, text_x + text_width + 5), panel_bottom),
        alpha=0.68,
    )
    cv2.putText(
        frame, annotation.label, (text_x, label_y),
        font, label_scale, annotation.color, 2,
    )
    cv2.putText(
        frame, annotation.detail, (text_x, detail_y),
        font, detail_scale, annotation.color, 2,
    )

# ============================================================
# 메인
# ============================================================

def main() -> None:
    global latest_jpeg

    require_model_files(
        "Camera D",
        {
            "YOLO": YOLO_MODEL,
            "Re-ID TensorRT engine": REID_ENGINE,
        },
    )

    ensure_csv()

    if not torch.cuda.is_available():
        raise RuntimeError("Jetson GPU를 사용할 수 없습니다.")

    yolo = YOLO(str(YOLO_MODEL))
    reid = ReIDTensorRTEngine(REID_ENGINE)

    cap = cv2.VideoCapture(CAMERA_SOURCE)

    if not cap.isOpened():
        raise RuntimeError(
            f"PC Camera D 스트림을 열 수 없습니다: {CAMERA_SOURCE}"
        )

    # cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
    # cap.set(cv2.CAP_PROP_FRAME_WIDTH, WIDTH)
    # cap.set(cv2.CAP_PROP_FRAME_HEIGHT, HEIGHT)
    # cap.set(cv2.CAP_PROP_FPS, FPS)
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))

    first_seen: dict[int, float] = {}
    last_seen: dict[int, int] = {}
    histories: dict[int, deque[np.ndarray]] = {}
    tentative_id: dict[int, str] = {}
    tentative_count: dict[int, int] = {}
    best_scores: dict[int, float] = {}
    top2_scores: dict[int, float] = {}
    combined_scores: dict[int, float] = {}
    arrived_ids: dict[int, str] = {}
    timing_journey_by_local_id: dict[int, str] = {}
    verify_scores: dict[int, float] = {}
    verify_failures: dict[int, int] = {}
    best_capture_by_local_id: dict[int, np.ndarray] = {}
    best_capture_quality_by_local_id: dict[int, float] = {}
    track_eligibility: dict[int, TrackEligibility] = {}
    confirmations: dict[int, MatchConfirmation] = {}
    journey_claims: dict[str, int] = {}
    confirmation_samples: dict[int, int] = {}
    confirmation_passes: dict[int, int] = {}
    rejection_logged_at: dict[tuple[int, str, str], float] = {}
    stranger_gate = StrangerDetectionGate("D")

    CAPTURE_ROOT.mkdir(parents=True, exist_ok=True)

    frame_index = 0
    mqtt_client: mqtt.Client | None = None

    try:
        mqtt_client = create_mqtt_client()
        threading.Thread(target=start_web, daemon=True).start()

        print("GPU:", torch.cuda.get_device_name(0))
        print("Camera D Re-ID Arrival 시작")
        print(f"카메라 입력    : {CAMERA_SOURCE}")
        print(f"웹 포트        : {WEB_PORT}")
        print(f"구독 토픽      : {CANDIDATE_TOPIC}")
        print(f"제어 구독 토픽 : {JOURNEY_CONTROL_TOPIC}")
        print(f"도착 발행 토픽 : {ARRIVAL_TOPIC}")
        print(f"미등록 감지 토픽: {DETECTION_TOPIC}")
        print(f"Capture 저장   : {CAPTURE_ROOT}")
        print(
            f"밝기 보정       : "
            f"alpha={IMAGE_CONTRAST_ALPHA}, "
            f"beta={IMAGE_BRIGHTNESS_BETA}"
        )
        print(f"Best 기준      : {MATCH_BEST_THRESHOLD:.2f}")
        print(f"Top2 기준      : {MATCH_TOP2_THRESHOLD:.2f}")
        print("종료            : Ctrl + C")

        while True:
            ok, frame = cap.read()
            if not ok:
                print("Camera D 프레임 읽기 실패")
                time.sleep(0.05)
                continue

            frame_index += 1
            if FLIP_HORIZONTAL:
                frame = cv2.flip(frame, 1)

            frame = apply_small_brightness_adjustment(
                frame
            )

            result = yolo.track(
                source=frame,
                persist=True,
                tracker="bytetrack.yaml",
                classes=[0],
                conf=0.50,
                iou=0.50,
                end2end=False,
                device=0,
                verbose=False,
            )[0]

            display_annotations: list[DisplayAnnotation] = []

            if result.boxes is not None and result.boxes.id is not None:
                observed_at = now_aware()
                local_ids = result.boxes.id.int().cpu().tolist()
                boxes = result.boxes.xyxy.int().cpu().tolist()
                confidences = result.boxes.conf.cpu().tolist()

                for local_id, box, confidence in zip(
                    local_ids,
                    boxes,
                    confidences,
                ):
                    first_seen.setdefault(local_id, observed_at.timestamp())
                    last_seen[local_id] = frame_index
                    reid_observation_valid = False
                    x1, y1, x2, y2 = box
                    eligibility = track_eligibility.setdefault(
                        local_id,
                        TrackEligibility(local_id, observed_at),
                    )
                    update_track_entry(
                        eligibility,
                        box,
                        frame.shape[1],
                        frame.shape[0],
                        observed_at,
                        MATCHING_CONFIG,
                    )

                    if (frame_index + local_id) % REID_INTERVAL_FRAMES == 0:
                        try:
                            current_crop = extract_crop(frame, box)
                            embedding = normalize(
                                reid.extract(current_crop)
                            )

                            quality_ok, quality = crop_quality(
                                box,
                                float(confidence),
                                frame.shape[1],
                                frame.shape[0],
                            )
                            previous_quality = (
                                best_capture_quality_by_local_id.get(
                                    local_id,
                                    -1.0,
                                )
                            )
                            if quality_ok and quality > previous_quality:
                                best_capture_by_local_id[local_id] = (
                                    current_crop.copy()
                                )
                                best_capture_quality_by_local_id[local_id] = (
                                    quality
                                )

                            history = histories.setdefault(
                                local_id, deque(maxlen=REID_HISTORY_SIZE)
                            )
                            history.append(embedding)
                            averaged = average(history)
                            reid_observation_valid = True

                            arrived_journey = arrived_ids.get(local_id)

                            if arrived_journey is not None:
                                completed = completed_tracks.get(local_id)
                                if (
                                    completed is None
                                    or completed.journey_id != arrived_journey
                                ):
                                    arrived_ids.pop(local_id, None)
                                else:
                                    _, _, verify = gallery_score(
                                        averaged,
                                        completed.gallery,
                                    )
                                    verify_scores[local_id] = verify

                                    if verify >= VERIFY_THRESHOLD:
                                        verify_failures[local_id] = 0
                                    else:
                                        verify_failures[local_id] = (
                                            verify_failures.get(local_id, 0) + 1
                                        )
                                        print(
                                            f"[D 재검증 실패] Local={local_id}, "
                                            f"Journey={arrived_journey}, "
                                            f"Score={verify:.3f}, "
                                            f"Count={verify_failures[local_id]}/"
                                            f"{VERIFY_FAILURE_LIMIT}"
                                        )
                                        if verify_failures[local_id] >= VERIFY_FAILURE_LIMIT:
                                            verify_failures[local_id] = (
                                                VERIFY_FAILURE_LIMIT
                                            )
                                            print(
                                                "[D 완료 Track 재검증 저하] "
                                                f"Local={local_id}, "
                                                f"Journey={arrived_journey}, "
                                                "consumed 상태 유지"
                                            )

                            else:
                                confirmation = confirmations.setdefault(
                                    local_id,
                                    MatchConfirmation(),
                                )

                                if local_id in consumed_track_ids:
                                    candidate_id = confirmation.journey_id or "-"
                                    rejection_key = (
                                        local_id,
                                        candidate_id,
                                        "TRACK_ALREADY_CONSUMED",
                                    )
                                    if (
                                        time.time()
                                        - rejection_logged_at.get(rejection_key, 0.0)
                                        >= 5.0
                                    ):
                                        log_match_reject(
                                            local_id=local_id,
                                            journey_id=candidate_id,
                                            track_first_seen_at=(
                                                eligibility.first_seen_at
                                            ),
                                            passage_at=observed_at,
                                            passage_to_d_duration=0.0,
                                            best=-1.0,
                                            combined=-1.0,
                                            best_journey=-1.0,
                                            second_journey=-1.0,
                                            journey_margin=-1.0,
                                            confirmation_sample_count=0,
                                            confirmation_pass_count=0,
                                            reason="TRACK_ALREADY_CONSUMED",
                                        )
                                        rejection_logged_at[rejection_key] = time.time()
                                else:
                                    ranked, rejected = rank_eligible_journeys(
                                        averaged,
                                        eligibility,
                                        observed_at,
                                    )

                                    for rejected_item, reason, duration in rejected:
                                        rejection_key = (
                                            local_id,
                                            rejected_item.journey_id,
                                            reason,
                                        )
                                        last_logged = rejection_logged_at.get(
                                            rejection_key,
                                            0.0,
                                        )
                                        if time.time() - last_logged >= 5.0:
                                            log_match_reject(
                                                local_id=local_id,
                                                journey_id=rejected_item.journey_id,
                                                track_first_seen_at=(
                                                    eligibility.first_seen_at
                                                ),
                                                passage_at=parse_aware_datetime(
                                                    rejected_item.b_passage_timestamp
                                                ),
                                                passage_to_d_duration=duration,
                                                best=-1.0,
                                                combined=-1.0,
                                                best_journey=-1.0,
                                                second_journey=-1.0,
                                                journey_margin=-1.0,
                                                confirmation_sample_count=0,
                                                confirmation_pass_count=0,
                                                reason=reason,
                                            )
                                            rejection_logged_at[rejection_key] = time.time()

                                    if not ranked:
                                        old_journey = confirmation.journey_id
                                        if old_journey:
                                            journey_claims.pop(old_journey, None)
                                            with candidate_lock:
                                                old_item = candidates.get(old_journey)
                                                if old_item is not None:
                                                    old_item.status = "PENDING"
                                        confirmation.reset()
                                        tentative_id.pop(local_id, None)
                                        tentative_count.pop(local_id, None)
                                    else:
                                        winner = ranked[0]
                                        candidate_id = winner.journey_id
                                        best = winner.best
                                        top2 = winner.top2
                                        combined = winner.combined
                                        second_journey = (
                                            ranked[1].combined
                                            if len(ranked) >= 2
                                            else 0.0
                                        )
                                        journey_margin = combined - second_journey
                                        best_scores[local_id] = best
                                        top2_scores[local_id] = top2
                                        combined_scores[local_id] = combined

                                        old_journey = confirmation.journey_id
                                        if old_journey and old_journey != candidate_id:
                                            journey_claims.pop(old_journey, None)
                                            with candidate_lock:
                                                old_item = candidates.get(old_journey)
                                                if old_item is not None:
                                                    old_item.status = "PENDING"

                                        candidate_snapshot = get_candidate(candidate_id)
                                        if candidate_snapshot is None:
                                            confirmation.reset()
                                            continue

                                        score_passed = (
                                            best >= MATCH_BEST_THRESHOLD
                                            and top2 >= MATCH_TOP2_THRESHOLD
                                        )
                                        margin_passed = (
                                            len(ranked) == 1
                                            or journey_margin
                                            >= MATCHING_CONFIG.min_journey_margin
                                        )
                                        update_track_reid_diagnostic(
                                            journey_id=candidate_id,
                                            local_track_id=local_id,
                                            evaluated_at=observed_at,
                                            best_similarity=best,
                                            top2_mean=top2,
                                            combined_score=combined,
                                            matched=(
                                                score_passed and margin_passed
                                            ),
                                        )
                                        owner = journey_claims.get(candidate_id)
                                        if owner not in {None, local_id}:
                                            log_match_reject(
                                                local_id=local_id,
                                                journey_id=candidate_id,
                                                track_first_seen_at=(
                                                    eligibility.first_seen_at
                                                ),
                                                passage_at=parse_aware_datetime(
                                                    candidate_snapshot.b_passage_timestamp
                                                ),
                                                passage_to_d_duration=(
                                                    observed_at.timestamp()
                                                    - candidate_snapshot.b_passage_epoch
                                                ),
                                                best=best,
                                                combined=combined,
                                                best_journey=combined,
                                                second_journey=second_journey,
                                                journey_margin=journey_margin,
                                                confirmation_sample_count=0,
                                                confirmation_pass_count=0,
                                                reason="TRACK_ALREADY_CONSUMED",
                                            )
                                        else:
                                            if score_passed and margin_passed:
                                                journey_claims[candidate_id] = local_id
                                                with candidate_lock:
                                                    candidate = candidates.get(candidate_id)
                                                    if candidate is not None:
                                                        candidate.status = "PENDING_MATCH"
                                            result_confirmation = add_confirmation_sample(
                                                confirmation,
                                                candidate_id,
                                                observed_at,
                                                score_passed and margin_passed,
                                                combined,
                                                MATCHING_CONFIG,
                                            )
                                            confirmation_samples[local_id] = (
                                                result_confirmation.sample_count
                                            )
                                            confirmation_passes[local_id] = (
                                                result_confirmation.pass_count
                                            )
                                            tentative_id[local_id] = candidate_id
                                            tentative_count[local_id] = (
                                                result_confirmation.pass_count
                                            )

                                            if (
                                                result_confirmation.reset_reason
                                                is not None
                                            ):
                                                journey_claims.pop(candidate_id, None)
                                                with candidate_lock:
                                                    reset_item = candidates.get(
                                                        candidate_id
                                                    )
                                                    if reset_item is not None:
                                                        reset_item.status = "PENDING"
                                                tentative_id.pop(local_id, None)
                                                tentative_count.pop(local_id, None)

                                            reject_reason = None
                                            if not margin_passed:
                                                reject_reason = (
                                                    "INSUFFICIENT_JOURNEY_MARGIN"
                                                )
                                            elif (
                                                not score_passed
                                                or not result_confirmation.confirmed
                                            ):
                                                reject_reason = "INSUFFICIENT_CONSENSUS"

                                            if (
                                                reject_reason is not None
                                                and (
                                                    result_confirmation.accepted_sample
                                                    or result_confirmation.reset_reason
                                                    is not None
                                                )
                                            ):
                                                candidate = get_candidate(candidate_id)
                                                if candidate is not None:
                                                    log_match_reject(
                                                        local_id=local_id,
                                                        journey_id=candidate_id,
                                                        track_first_seen_at=(
                                                            eligibility.first_seen_at
                                                        ),
                                                        passage_at=parse_aware_datetime(
                                                            candidate.b_passage_timestamp
                                                        ),
                                                        passage_to_d_duration=(
                                                            observed_at.timestamp()
                                                            - candidate.b_passage_epoch
                                                        ),
                                                        best=best,
                                                        combined=combined,
                                                        best_journey=combined,
                                                        second_journey=second_journey,
                                                        journey_margin=journey_margin,
                                                        confirmation_sample_count=(
                                                            result_confirmation.sample_count
                                                        ),
                                                        confirmation_pass_count=(
                                                            result_confirmation.pass_count
                                                        ),
                                                        reason=reject_reason,
                                                    )

                                            if result_confirmation.confirmed:
                                                candidate = get_candidate(candidate_id)
                                                if candidate is not None:
                                                    diagnostics = ArrivalDiagnostics(
                                                        track_first_seen_at=(
                                                            eligibility.first_seen_at
                                                        ),
                                                        candidate_received_at=(
                                                            parse_aware_datetime(
                                                                candidate.received_at
                                                            )
                                                        ),
                                                        passage_at=(
                                                            parse_aware_datetime(
                                                                candidate.b_passage_timestamp
                                                            )
                                                        ),
                                                        arrival_at=observed_at,
                                                        confirmation_sample_count=(
                                                            result_confirmation.sample_count
                                                        ),
                                                        confirmation_pass_count=(
                                                            result_confirmation.pass_count
                                                        ),
                                                        best_journey_score=combined,
                                                        second_journey_score=(
                                                            second_journey
                                                        ),
                                                        journey_margin=journey_margin,
                                                        eligibility_reason=(
                                                            "ELIGIBLE_NEW_ENTRY"
                                                        ),
                                                    )
                                                    capture_crop = (
                                                        best_capture_by_local_id.get(
                                                            local_id,
                                                            current_crop,
                                                        )
                                                    )
                                                    capture_quality = (
                                                        best_capture_quality_by_local_id.get(
                                                            local_id,
                                                            quality,
                                                        )
                                                    )
                                                    if complete_arrival(
                                                        mqtt_client,
                                                        candidate_id,
                                                        local_id,
                                                        best,
                                                        top2,
                                                        combined,
                                                        averaged,
                                                        capture_crop,
                                                        capture_quality,
                                                        diagnostics,
                                                    ):
                                                        flush_track_reid_diagnostics(
                                                            local_id,
                                                            "ARRIVAL_CONFIRMED",
                                                        )
                                                        arrived_ids[local_id] = candidate_id
                                                        verify_scores[local_id] = combined
                                                        verify_failures[local_id] = 0
                                                        register_timing_match(
                                                            journey_id=candidate_id,
                                                            person_uid=candidate.person_uid,
                                                            local_id=local_id,
                                                            entered_epoch=first_seen[local_id],
                                                            matched_epoch=(
                                                                observed_at.timestamp()
                                                            ),
                                                        )
                                                        timing_journey_by_local_id[
                                                            local_id
                                                        ] = candidate_id
                                                        journey_claims.pop(
                                                            candidate_id,
                                                            None,
                                                        )
                                                        tentative_id.pop(local_id, None)
                                                        tentative_count.pop(local_id, None)

                        except Exception as error:
                            print(f"[D Re-ID 오류] Local={local_id}: {error}")

                    if reid_observation_valid and mqtt_client is not None:
                        detection_payload = stranger_gate.observe(
                            local_track_id=local_id,
                            observed_at=observed_at,
                            is_unregistered=(
                                local_id not in arrived_ids
                                and local_id not in consumed_track_ids
                            ),
                            matching_in_progress=(
                                tentative_id.get(local_id) is not None
                            ),
                        )
                        if detection_payload is not None:
                            publish_stranger_detection(
                                mqtt_client,
                                DETECTION_TOPIC,
                                detection_payload,
                                qos=MQTT_QOS,
                            )

                    journey_id = arrived_ids.get(local_id)

                    if journey_id is not None:
                        completed = completed_tracks.get(local_id)
                        person_uid = (
                            completed.person_uid
                            if completed is not None
                            else "UNKNOWN"
                        )
                        display_route = (
                            route_text(completed.route)
                            if completed is not None
                            else "A > B/C > [D]"
                        )
                        failures = verify_failures.get(local_id, 0)
                        score = verify_scores.get(local_id, 0.0)
                        if failures:
                            label = f"{person_uid} | VERIFYING"
                            sub = display_route
                            color = (0, 255, 255)
                        else:
                            label = f"{person_uid} | ARRIVED"
                            sub = display_route
                            color = (0, 255, 0)
                    else:
                        elapsed = time.time() - first_seen[local_id]
                        temporary = tentative_id.get(local_id)
                        best = best_scores.get(local_id, -1.0)
                        top2 = top2_scores.get(local_id, -1.0)

                        if temporary:
                            temporary_candidate = get_candidate(
                                temporary
                            )
                            temporary_person_uid = (
                                temporary_candidate.person_uid
                                if temporary_candidate is not None
                                else "UNKNOWN"
                            )
                            label = f"CHECKING: {temporary_person_uid}"
                            sub = (
                                f"BEST {best:.2f} TOP2 {top2:.2f} "
                                f"{tentative_count.get(local_id, 0)}/"
                                f"{MATCHING_CONFIG.confirmation_required_passes}"
                            )
                            color = (0, 255, 255)
                        elif elapsed >= ANOMALY_DELAY_SECONDS:
                            label = "ANOMALY: STRANGER"
                            sub = (
                                f"BEST {best:.2f} TOP2 {top2:.2f}"
                                if best >= 0 else "NO B PASSAGE CANDIDATE"
                            )
                            color = (0, 0, 255)
                        else:
                            label = "STRANGER"
                            sub = "CHECKING A+B GALLERY"
                            color = (0, 165, 255)

                    display_annotations.append(
                        DisplayAnnotation(
                            box=(x1, y1, x2, y2),
                            label=label,
                            detail=sub,
                            color=color,
                        )
                    )

            stale_ids = [
                local_id for local_id, seen in last_seen.items()
                if frame_index - seen > TRACK_LOST_GRACE_FRAMES
            ]
            for local_id in stale_ids:
                stranger_gate.remove_track(local_id)
                flush_track_reid_diagnostics(
                    local_id,
                    "TRACK_ENDED",
                )
                confirmation = confirmations.get(local_id)
                claimed_journey = (
                    confirmation.journey_id
                    if confirmation is not None
                    else None
                )
                if claimed_journey:
                    journey_claims.pop(claimed_journey, None)
                    with candidate_lock:
                        claimed_item = candidates.get(claimed_journey)
                        if claimed_item is not None:
                            claimed_item.status = "PENDING"
                timing_journey_id = timing_journey_by_local_id.get(local_id)
                journey_id = arrived_ids.get(local_id) or timing_journey_id
                if journey_id:
                    print(
                        f"[D Track 종료] Local={local_id}, "
                        f"Journey={journey_id}, COMPLETED 유지"
                    )
                if timing_journey_id and mqtt_client is not None:
                    publish_timing_on_track_lost(
                        client=mqtt_client,
                        journey_id=timing_journey_id,
                        local_id=local_id,
                        exited_epoch=time.time(),
                    )
                for mapping in (
                    first_seen, last_seen, histories, tentative_id,
                    tentative_count, best_scores, top2_scores,
                    combined_scores, arrived_ids, verify_scores,
                    verify_failures, best_capture_by_local_id,
                    best_capture_quality_by_local_id,
                    timing_journey_by_local_id,
                    track_eligibility, confirmations,
                    confirmation_samples, confirmation_passes,
                    completed_tracks,
                ):
                    mapping.pop(local_id, None)

            output_frame = build_candidate_dashboard(
                frame,
                display_annotations,
            )

            encoded, buffer = cv2.imencode(
                ".jpg",
                output_frame,
                [
                    cv2.IMWRITE_JPEG_QUALITY,
                    STREAM_JPEG_QUALITY,
                ],
            )
            if encoded:
                with frame_lock:
                    latest_jpeg = buffer.tobytes()

    except KeyboardInterrupt:
        print("\nCamera D 종료")

    finally:
        cap.release()
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("Camera D MQTT 연결 종료")


if __name__ == "__main__":
    main()
