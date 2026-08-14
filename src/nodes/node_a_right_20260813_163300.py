from __future__ import annotations

import csv
import json
import queue
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import cv2
import numpy as np
import paho.mqtt.client as mqtt
import torch
from ultralytics import YOLO

from src.network.mqtt_client import MqttPublisher
from src.reid.reid_engine import ReIDTensorRTEngine


# ============================================================
# 프로젝트 경로
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

YOLO_MODEL_PATH = PROJECT_ROOT / "yolo26n.pt"

REID_ENGINE_PATH = (
    PROJECT_ROOT
    / "models"
    / "reid"
    / "person_reid_osnet_x0_25_fp16.engine"
)

FACE_DETECTOR_MODEL_PATH = (
    PROJECT_ROOT
    / "models"
    / "face"
    / "face_detection_yunet_2023mar.onnx"
)

FACE_RECOGNIZER_MODEL_PATH = (
    PROJECT_ROOT
    / "models"
    / "face"
    / "face_recognition_sface_2021dec.onnx"
)

EVENT_LOG_PATH = PROJECT_ROOT / "logs" / "node_a_entry_central.csv"

CAPTURE_ROOT = PROJECT_ROOT / "outputs" / "captures" / "A"
FACE_CAPTURE_ROOT = PROJECT_ROOT / "outputs" / "captures" / "A_face"


# ============================================================
# MQTT 설정
# ============================================================

MQTT_HOST = "10.10.20.33"
MQTT_PORT = 1883
TOPIC_A_ENTRY_RESPONSE = "cctv/responses/a/entry"

# NODE_TIMING 전용 topic.
# 기존 ENTRY topic/payload는 건드리지 않고 별도 event로 발행한다.
TOPIC_A_TIMING = "cctv/events/a/timing"
MQTT_QOS = 1


# ============================================================
# 카메라 / 웹 서버
# ============================================================

CAMERA_DEVICE = 0
CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720
CAMERA_FPS = 30
SERVER_PORT = 8000
FLIP_HORIZONTAL = True

IMAGE_CONTRAST_ALPHA = 1.02
IMAGE_BRIGHTNESS_BETA = 8
DASHBOARD_HEIGHT = 175


# ============================================================
# ENTRY LINE
# ============================================================

ENTRY_LINE_X_RATIO = 0.50
ENTRY_DIRECTION = "right"  # right 또는 left


# ============================================================
# Body / Face 자동 선별
# ============================================================

# Body는 같은 Track에서 프레임 간격을 두고 품질 좋은 최대 3장을 유지한다.
# 기존 legacy embedding 1개도 그대로 보내고,
# body_embeddings에 최대 3개의 OSNet 512-D를 추가한다.
BODY_TOP_K = 3
BODY_MIN_FRAME_GAP = 8

# ENTRY 직후에도 Body 후보를 조금 더 모아 TOP3 확보율을 높인다.
# Main 영상/YOLO/ByteTrack loop는 멈추지 않고 Entry Worker만 기다린다.
# BODY_MIN_FRAME_GAP=8을 유지한 채 30FPS 기준 약 18 frame의 추가 기회를 준다.
BODY_ENTRY_GRACE_SEC = 0.60

FACE_SCORE_THRESHOLD = 0.60
FACE_CHECK_INTERVAL_FRAMES = 2
FACE_TOP_K = 3
# ENTRY 직전에 이미 요청된 Face 작업이 늦게 끝나는 경우를 살리기 위한 유예 시간.
# Main 영상 루프는 멈추지 않고, Entry Worker만 최대 이 시간까지 기다린다.
FACE_ENTRY_GRACE_SEC = 0.35
FACE_MIN_FRAME_GAP = 6
FACE_MIN_SIZE_PX = 24
FACE_MIN_SHARPNESS = 10.0
FACE_MIN_FRONTAL_SCORE = 0.20
FACE_UPSCALE_FACTOR = 2.0
FACE_SAVE_PADDING_RATIO = 0.20
FACE_DETECTOR_MODEL_NAME = "yunet_2023mar"
FACE_REID_MODEL_NAME = "sface_2021dec"


# ============================================================
# 추적 / Worker
# ============================================================

TRACK_STATE_TIMEOUT_SEC = 8.0
RECENT_RESULT_LIMIT = 4
ENTRY_QUEUE_MAXSIZE = 16

latest_jpeg: bytes | None = None

frame_lock = threading.Lock()
identity_lock = threading.Lock()
log_lock = threading.Lock()
timing_lock = threading.Lock()


# ============================================================
# 데이터 구조
# ============================================================

@dataclass
class EntryIdentity:
    local_track_id: int
    request_id: str
    person_uid: str | None = None
    journey_id: str | None = None
    person_status: str = "REGISTERING"
    visit_count: int = 0
    match_score: float | None = None
    previous_last_seen_at: str | None = None
    candidate_person_uid: str | None = None

    # A ENTRY LINE을 실제 통과한 시각
    entry_at: str | None = None

    updated_at: str = ""


@dataclass
class BodyCandidate:
    image: np.ndarray
    confidence: float
    quality: float
    selection_score: float
    frame_index: int


@dataclass
class FaceCandidate:
    image: np.ndarray
    confidence: float
    quality: float
    sharpness: float
    area_ratio: float
    frontal_score: float
    frame_index: int
    embedding: np.ndarray


@dataclass
class FaceTask:
    local_track_id: int
    frame_index: int
    person_crop: np.ndarray


@dataclass
class FaceResult:
    local_track_id: int
    candidate: FaceCandidate


class FrameProgress:
    """
    Main Camera/YOLO loop가 실제로 처리한 frame_index를
    Entry Worker에서 읽기 위한 thread-safe 진단 상태.

    Camera FPS 자체가 아니라 YOLO + ByteTrack까지 실제 처리된
    main-loop frame 진행량을 측정한다.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frame_index = 0
        self._monotonic_at = time.monotonic()

    def update(
        self,
        frame_index: int,
        monotonic_at: float,
    ) -> None:
        with self._lock:
            self._frame_index = int(frame_index)
            self._monotonic_at = float(monotonic_at)

    def snapshot(self) -> tuple[int, float]:
        with self._lock:
            return (
                int(self._frame_index),
                float(self._monotonic_at),
            )


class BodyGraceDiagnostics:
    """
    BODY Grace 동작을 바꾸지 않고 측정만 하는 진단 상태.

    rejected_count 정의:
    Grace 시작 이후 들어온 Body crop 중
    '새로운 독립 Gallery slot을 늘리지 못한 crop'의 수.
    - 가까운 frame이라 기존 후보를 교체한 경우 포함
    - 가까운 frame이라 완전히 버린 경우 포함
    - TOP-K 정렬 후 새 후보가 탈락한 경우 포함

    replacement는 품질 개선일 수 있으므로 별도 replaced_count도 기록한다.
    """

    def __init__(
        self,
        request_id: str,
        local_track_id: int,
        initial_count: int,
        target_count: int,
        grace_sec: float,
        start_frame: int,
        start_monotonic: float,
    ) -> None:
        self.request_id = request_id
        self.local_track_id = int(local_track_id)
        self.initial_count = int(initial_count)
        self.target_count = int(target_count)
        self.grace_sec = float(grace_sec)
        self.start_frame = int(start_frame)
        self.start_monotonic = float(start_monotonic)

        self._lock = threading.Lock()

        self.candidate_attempt_count = 0
        self.rejected_count = 0
        self.replaced_count = 0

        self.end_reason: str | None = None
        self.end_frame: int | None = None
        self.end_monotonic: float | None = None

    def record_candidate_result(
        self,
        result: str,
    ) -> None:
        with self._lock:
            self.candidate_attempt_count += 1

            if result == "REPLACED_NEAR":
                self.rejected_count += 1
                self.replaced_count += 1
            elif result.startswith("REJECTED_"):
                self.rejected_count += 1

    def finish_once(
        self,
        reason: str,
        end_frame: int,
        end_monotonic: float,
    ) -> None:
        with self._lock:
            if self.end_reason is not None:
                return

            self.end_reason = str(reason)
            self.end_frame = int(end_frame)
            self.end_monotonic = float(end_monotonic)

    def get_end_reason(self) -> str | None:
        with self._lock:
            return self.end_reason

    def snapshot(
        self,
        fallback_frame: int,
        fallback_monotonic: float,
    ) -> dict[str, int | float | str | None]:
        with self._lock:
            end_frame = (
                int(self.end_frame)
                if self.end_frame is not None
                else int(fallback_frame)
            )
            end_monotonic = (
                float(self.end_monotonic)
                if self.end_monotonic is not None
                else float(fallback_monotonic)
            )
            end_reason = self.end_reason

            duration = max(
                0.0,
                end_monotonic - self.start_monotonic,
            )
            processed_frames = max(
                0,
                end_frame - self.start_frame,
            )

            average_processing_fps = (
                float(processed_frames) / duration
                if duration > 1e-9
                else 0.0
            )

            return {
                "request_id": self.request_id,
                "local_track_id": self.local_track_id,
                "initial_count": self.initial_count,
                "target_count": self.target_count,
                "grace_sec": self.grace_sec,
                "start_frame": self.start_frame,
                "end_frame": end_frame,
                "candidate_attempt_count": self.candidate_attempt_count,
                "rejected_count": self.rejected_count,
                "replaced_count": self.replaced_count,
                "collection_duration": duration,
                "processed_frames": processed_frames,
                "average_processing_fps": average_processing_fps,
                "collection_end_reason": end_reason,
            }


@dataclass
class EntryJob:
    local_track_id: int
    request_id: str
    timestamp: str

    # BODY도 ENTRY 시점 snapshot이 아니라 공유 list를 넘긴다.
    # BODY_ENTRY_GRACE_SEC 동안 Main 영상 loop가 계속 후보를 갱신하고,
    # Entry Worker는 최대 TOP3 또는 grace 종료까지 기다린 뒤 snapshot한다.
    body_candidates: list[BodyCandidate]
    body_grace_deadline: float
    body_grace_diag: BodyGraceDiagnostics
    frame_progress: FrameProgress
    enqueued_monotonic: float

    # FACE_ENTRY_GRACE_SEC 동안 늦게 완료된 Face Worker 결과가 여기에 추가될 수 있다.
    face_candidates: list[FaceCandidate]
    face_grace_deadline: float


@dataclass
class TrackFirstSeen:
    """ByteTrack local id가 실제 화면에 최초 등장한 시각."""
    entered_at: str
    monotonic_at: float


@dataclass
class JourneyTimingSession:
    """Node A canonical Journey timing session."""
    journey_id: str
    person_uid: str
    node_id: str
    entered_at: str
    entered_monotonic: float
    matched_at: str
    matched_monotonic: float
    active_local_track_id: int

    # 공통 session 이름을 유지한다.
    # A에서는 Main ENTRY_RESULT 수신으로 canonical ENTRY가 확정되면 True.
    passage_or_arrival_sent: bool

    last_seen_at: str
    last_seen_monotonic: float
    timing_sent: bool = False


identity_by_local_id: dict[int, EntryIdentity] = {}
local_id_by_request_id: dict[str, int] = {}
logged_response_request_ids: set[str] = set()
recent_results: deque[EntryIdentity] = deque(maxlen=RECENT_RESULT_LIMIT)

# NODE_TIMING 상태
first_seen_at_by_local_id: dict[int, TrackFirstSeen] = {}
journey_timing: dict[str, JourneyTimingSession] = {}
journey_id_by_local_track_id: dict[int, str] = {}
timing_sent: set[str] = set()


# ============================================================
# NODE_TIMING
# ============================================================

def timezone_iso_now_ms() -> str:
    """timezone-aware ISO timestamp, millisecond precision."""
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def register_track_first_seen(
    local_track_id: int,
    monotonic_now: float,
) -> None:
    """
    Re-ID/ENTRY가 아니라 ByteTrack local id가 실제 화면에
    최초 등장한 시각을 entered_at으로 저장한다.
    """
    with timing_lock:
        if local_track_id in first_seen_at_by_local_id:
            return

        first_seen_at_by_local_id[local_track_id] = TrackFirstSeen(
            entered_at=timezone_iso_now_ms(),
            monotonic_at=monotonic_now,
        )


def update_journey_timing_last_seen(
    local_track_id: int,
    monotonic_now: float,
) -> None:
    with timing_lock:
        journey_id = journey_id_by_local_track_id.get(local_track_id)

        if journey_id is None:
            return

        session = journey_timing.get(journey_id)

        if session is None or session.timing_sent:
            return

        if session.active_local_track_id != local_track_id:
            return

        session.last_seen_at = timezone_iso_now_ms()
        session.last_seen_monotonic = monotonic_now


def bind_canonical_journey_timing(
    local_track_id: int,
    person_uid: str | None,
    journey_id: str | None,
) -> None:
    """
    Main ENTRY_RESULT 수신 순간 canonical Journey와 연결한다.

    동일 Journey가 새 local_track_id로 다시 연결되면:
    - entered_at earliest 유지
    - matched_at earliest 유지
    - active_local_track_id만 새 id로 이어간다.
    """
    if not person_uid or not journey_id:
        return

    matched_monotonic = time.monotonic()
    matched_at = timezone_iso_now_ms()

    with timing_lock:
        first_seen = first_seen_at_by_local_id.get(local_track_id)

        if first_seen is None:
            first_seen = TrackFirstSeen(
                entered_at=matched_at,
                monotonic_at=matched_monotonic,
            )

        previous_journey_id = journey_id_by_local_track_id.get(local_track_id)

        if (
            previous_journey_id is not None
            and previous_journey_id != journey_id
        ):
            print(
                "[Camera A][NODE_TIMING] local track Journey 충돌: "
                f"L{local_track_id} "
                f"{previous_journey_id} -> {journey_id}"
            )
            return

        session = journey_timing.get(journey_id)

        if session is None:
            session = JourneyTimingSession(
                journey_id=journey_id,
                person_uid=person_uid,
                node_id="A",
                entered_at=first_seen.entered_at,
                entered_monotonic=first_seen.monotonic_at,
                matched_at=matched_at,
                matched_monotonic=matched_monotonic,
                active_local_track_id=local_track_id,
                passage_or_arrival_sent=True,
                last_seen_at=matched_at,
                last_seen_monotonic=matched_monotonic,
                timing_sent=False,
            )
            journey_timing[journey_id] = session

        else:
            if session.person_uid != person_uid:
                print(
                    "[Camera A][NODE_TIMING] Journey Person UID 충돌: "
                    f"{journey_id} "
                    f"{session.person_uid} != {person_uid}"
                )
                return

            if first_seen.monotonic_at < session.entered_monotonic:
                session.entered_at = first_seen.entered_at
                session.entered_monotonic = first_seen.monotonic_at

            session.active_local_track_id = local_track_id
            session.passage_or_arrival_sent = True

            # matched_at은 최초 canonical match 유지
            if matched_monotonic < session.matched_monotonic:
                session.matched_at = matched_at
                session.matched_monotonic = matched_monotonic

            session.last_seen_at = matched_at
            session.last_seen_monotonic = matched_monotonic

        journey_id_by_local_track_id[local_track_id] = journey_id


def build_node_timing_payload(
    session: JourneyTimingSession,
    local_track_id: int,
    exited_at: str,
    exited_monotonic: float,
) -> dict[str, Any]:
    dwell_seconds = max(
        0.0,
        exited_monotonic - session.entered_monotonic,
    )

    return {
        "schema_version": 1,
        "event": "NODE_TIMING",
        "node_id": "A",
        "person_uid": session.person_uid,
        "global_person_id": session.person_uid,
        "journey_id": session.journey_id,
        "local_track_id": int(local_track_id),
        "entered_at": session.entered_at,
        "matched_at": session.matched_at,
        "exited_at": exited_at,
        "dwell_seconds": round(float(dwell_seconds), 3),
        "exit_reason": "TRACK_LOST",
    }


def publish_node_timing_once(
    mqtt_client: mqtt.Client,
    local_track_id: int,
    exited_monotonic: float,
) -> bool:
    """
    Track Lost 확정 시 한 Node / 한 Journey당 1회만 발행한다.
    Main response가 없던 track은 canonical Journey 연결이 없으므로 발행하지 않는다.
    """
    with timing_lock:
        journey_id = journey_id_by_local_track_id.get(local_track_id)

        if journey_id is None:
            return False

        session = journey_timing.get(journey_id)

        if session is None:
            return False

        # 같은 Journey가 이미 다른 local id로 이어졌다면
        # 이전 local id cleanup에서는 timing을 끝내지 않는다.
        if session.active_local_track_id != local_track_id:
            return False

        if not session.passage_or_arrival_sent:
            return False

        if session.timing_sent or journey_id in timing_sent:
            return False

        exited_at = timezone_iso_now_ms()

        payload = build_node_timing_payload(
            session=session,
            local_track_id=local_track_id,
            exited_at=exited_at,
            exited_monotonic=exited_monotonic,
        )

    info = mqtt_client.publish(
        TOPIC_A_TIMING,
        json.dumps(payload, ensure_ascii=False),
        qos=MQTT_QOS,
        retain=False,
    )

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        print(
            "[Camera A][NODE_TIMING] MQTT 발행 실패: "
            f"journey={payload['journey_id']} rc={info.rc}"
        )
        return False

    with timing_lock:
        session = journey_timing.get(payload["journey_id"])

        if session is None:
            return False

        session.timing_sent = True
        timing_sent.add(payload["journey_id"])

    print()
    print("===== A NODE_TIMING 발행 =====")
    print(f"Person UID     : {payload['person_uid']}")
    print(f"Journey ID     : {payload['journey_id']}")
    print(f"Local ID       : {payload['local_track_id']}")
    print(f"Entered At     : {payload['entered_at']}")
    print(f"Matched At     : {payload['matched_at']}")
    print(f"Exited At      : {payload['exited_at']}")
    print(f"Dwell Seconds  : {payload['dwell_seconds']:.3f}")
    print(f"Exit Reason    : {payload['exit_reason']}")
    print(f"Topic          : {TOPIC_A_TIMING}")
    print("================================")

    return True


# ============================================================
# 최신 Face Task만 보관하는 버퍼
# ============================================================

class LatestFaceTaskBuffer:
    """
    Track별 가장 최신 Crop 하나만 유지한다.

    Face Worker가 느려져도 오래된 Crop이 Queue에 계속 쌓이지 않는다.
    즉 실시간 영상 지연을 만들지 않고, Worker는 가능한 최신 얼굴만 검사한다.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._latest: dict[int, FaceTask] = {}
        self._stopped = False

    def submit(self, task: FaceTask) -> None:
        with self._condition:
            if self._stopped:
                return
            self._latest[task.local_track_id] = task
            self._condition.notify()

    def remove(self, local_track_id: int) -> None:
        with self._condition:
            self._latest.pop(local_track_id, None)

    def get_batch(self) -> list[FaceTask] | None:
        with self._condition:
            while not self._latest and not self._stopped:
                self._condition.wait(timeout=0.5)

            if self._stopped:
                return None

            tasks = list(self._latest.values())
            self._latest.clear()
            return tasks

    def stop(self) -> None:
        with self._condition:
            self._stopped = True
            self._latest.clear()
            self._condition.notify_all()


# ============================================================
# 웹 스트리밍 + READ-ONLY Capture 이미지 제공
# ============================================================

ALLOWED_CAPTURE_EXTENSIONS = {".jpg", ".jpeg", ".png"}
CAPTURE_BODY_URL_PREFIX = "/captures/body/"
CAPTURE_FACE_URL_PREFIX = "/captures/face/"


def decode_url_path_safely(raw_path: str) -> str:
    """
    URL encoded traversal(%2e%2e, %252e%252e 등)을 놓치지 않도록
    최대 3회까지만 반복 decode한다.
    """
    decoded = raw_path

    for _ in range(3):
        next_decoded = unquote(decoded)

        if next_decoded == decoded:
            break

        decoded = next_decoded

    return decoded


def resolve_capture_file(
    root: Path,
    relative_url_path: str,
) -> Path:
    """
    BODY_ROOT/FACE_ROOT 아래의 이미지 파일만 안전하게 resolve한다.

    차단:
    - ../
    - ..\
    - URL encoded traversal
    - 절대경로
    - symlink를 통해 root 밖으로 나가는 경로
    - jpg/jpeg/png 외 확장자
    - 디렉터리 접근
    """
    decoded_relative = decode_url_path_safely(relative_url_path)

    if not decoded_relative:
        raise ValueError("빈 이미지 경로")

    if "\\" in decoded_relative:
        raise PermissionError("backslash 경로 금지")

    relative_path = Path(decoded_relative)

    if relative_path.is_absolute():
        raise PermissionError("절대경로 금지")

    if ".." in relative_path.parts:
        raise PermissionError("상위 디렉터리 접근 금지")

    if relative_path.suffix.lower() not in ALLOWED_CAPTURE_EXTENSIONS:
        raise PermissionError("허용되지 않은 파일 확장자")

    root_resolved = root.resolve(strict=True)
    candidate = (root_resolved / relative_path).resolve(strict=False)

    try:
        candidate.relative_to(root_resolved)
    except ValueError as error:
        raise PermissionError("Capture root 밖 접근 금지") from error

    if not candidate.exists() or not candidate.is_file():
        raise FileNotFoundError(str(candidate))

    final_path = candidate.resolve(strict=True)

    try:
        final_path.relative_to(root_resolved)
    except ValueError as error:
        raise PermissionError("symlink root 밖 접근 금지") from error

    if final_path.suffix.lower() not in ALLOWED_CAPTURE_EXTENSIONS:
        raise PermissionError("허용되지 않은 최종 파일 확장자")

    return final_path


def capture_content_type(path: Path) -> str:
    suffix = path.suffix.lower()

    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"

    if suffix == ".png":
        return "image/png"

    raise ValueError("지원하지 않는 이미지 형식")


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class StreamHandler(BaseHTTPRequestHandler):

    def send_capture_file(
        self,
        *,
        root: Path,
        relative_url_path: str,
        send_body: bool,
    ) -> None:
        """
        Capture 이미지를 READ-ONLY로 제공한다.
        전체 파일을 메모리에 올리지 않고 64 KiB 단위로 전송한다.
        """
        try:
            image_path = resolve_capture_file(
                root=root,
                relative_url_path=relative_url_path,
            )
        except ValueError:
            self.send_error(400, "Bad capture path")
            return
        except PermissionError:
            self.send_error(403, "Forbidden capture path")
            return
        except FileNotFoundError:
            self.send_error(404, "Capture not found")
            return
        except Exception:
            self.send_error(500, "Image service error")
            return

        try:
            file_size = image_path.stat().st_size
            content_type = capture_content_type(image_path)

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(file_size))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()

            if not send_body:
                return

            with image_path.open("rb") as image_file:
                while True:
                    chunk = image_file.read(64 * 1024)

                    if not chunk:
                        break

                    self.wfile.write(chunk)

        except (
            BrokenPipeError,
            ConnectionResetError,
            ConnectionAbortedError,
        ):
            pass
        except Exception:
            # traceback이나 내부 경로를 HTTP response에 쓰지 않는다.
            return

    def route_capture_request(
        self,
        *,
        request_path: str,
        send_body: bool,
    ) -> bool:
        if request_path.startswith(CAPTURE_BODY_URL_PREFIX):
            relative_path = request_path[
                len(CAPTURE_BODY_URL_PREFIX):
            ]
            self.send_capture_file(
                root=CAPTURE_ROOT,
                relative_url_path=relative_path,
                send_body=send_body,
            )
            return True

        if request_path.startswith(CAPTURE_FACE_URL_PREFIX):
            relative_path = request_path[
                len(CAPTURE_FACE_URL_PREFIX):
            ]
            self.send_capture_file(
                root=FACE_CAPTURE_ROOT,
                relative_url_path=relative_path,
                send_body=send_body,
            )
            return True

        return False

    def do_GET(self) -> None:
        request_path = urlsplit(self.path).path

        # 기존 Camera A Web layout
        if request_path == "/":
            html = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Camera A Entrance Tracking</title>
    <style>
        body {
            margin: 0;
            background: #0d1117;
            color: white;
            text-align: center;
            font-family: Arial, sans-serif;
        }
        h2 { margin: 14px 0 8px; }
        p { margin: 0 0 10px; color: #aeb6c2; }
        img {
            width: 96%;
            max-width: 1280px;
            border: 1px solid #3b4655;
            border-radius: 8px;
            background: black;
        }
    </style>
</head>
<body>
    <h2>Camera A - Entrance Management</h2>
    <p>ENTRY Priority / OSNet Body / YuNet + SFace Face Embedding</p>
    <img src="/stream">
</body>
</html>
"""
            data = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        # 기존 MJPEG stream 동작 유지
        if request_path == "/stream":
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "multipart/x-mixed-replace; boundary=frame",
            )
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Pragma", "no-cache")
            self.end_headers()

            try:
                while True:
                    with frame_lock:
                        frame_data = latest_jpeg

                    if frame_data is None:
                        time.sleep(0.05)
                        continue

                    self.wfile.write(b"--frame\r\n")
                    self.wfile.write(b"Content-Type: image/jpeg\r\n")
                    self.wfile.write(
                        f"Content-Length: {len(frame_data)}\r\n\r\n".encode()
                    )
                    self.wfile.write(frame_data)
                    self.wfile.write(b"\r\n")
                    self.wfile.flush()
                    time.sleep(0.01)

            except (
                BrokenPipeError,
                ConnectionResetError,
                ConnectionAbortedError,
            ):
                pass
            return

        # 신규 READ-ONLY capture route
        if self.route_capture_request(
            request_path=request_path,
            send_body=True,
        ):
            return

        self.send_error(404)

    def do_HEAD(self) -> None:
        request_path = urlsplit(self.path).path

        if request_path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            return

        if request_path == "/stream":
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "multipart/x-mixed-replace; boundary=frame",
            )
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Pragma", "no-cache")
            self.end_headers()
            return

        if self.route_capture_request(
            request_path=request_path,
            send_body=False,
        ):
            return

        self.send_error(404)

    def method_not_allowed(self) -> None:
        self.send_response(405)
        self.send_header("Allow", "GET, HEAD")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        self.method_not_allowed()

    def do_PUT(self) -> None:
        self.method_not_allowed()

    def do_PATCH(self) -> None:
        self.method_not_allowed()

    def do_DELETE(self) -> None:
        self.method_not_allowed()

    def log_message(self, format, *args) -> None:
        return


def start_web_server() -> None:
    server = ReusableThreadingHTTPServer(
        ("0.0.0.0", SERVER_PORT),
        StreamHandler,
    )
    print(f"Camera A 웹 서버: http://10.10.20.56:{SERVER_PORT}")
    print(
        "Camera A BODY READ-ONLY: "
        f"http://10.10.20.56:{SERVER_PORT}"
        f"{CAPTURE_BODY_URL_PREFIX}<relative_path>"
    )
    print(
        "Camera A FACE READ-ONLY: "
        f"http://10.10.20.56:{SERVER_PORT}"
        f"{CAPTURE_FACE_URL_PREFIX}<relative_path>"
    )
    server.serve_forever()


# ============================================================
# CSV
# ============================================================

def ensure_log_file() -> None:
    EVENT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    if EVENT_LOG_PATH.exists():
        return

    with EVENT_LOG_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "timestamp",
                "node_id",
                "event",
                "local_track_id",
                "request_id",
                "person_uid",
                "journey_id",
                "person_status",
                "visit_count",
                "match_score",
                "previous_last_seen_at",
                "candidate_person_uid",
            ]
        )

    print(f"CSV 파일 생성: {EVENT_LOG_PATH}")


def save_central_entry_result(identity: EntryIdentity) -> None:
    with log_lock:
        with EVENT_LOG_PATH.open("a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    identity.updated_at,
                    "A",
                    "ENTRY_RESULT",
                    identity.local_track_id,
                    identity.request_id,
                    identity.person_uid,
                    identity.journey_id,
                    identity.person_status,
                    identity.visit_count,
                    identity.match_score,
                    identity.previous_last_seen_at,
                    identity.candidate_person_uid,
                ]
            )


# ============================================================
# Capture
# ============================================================

def make_request_id(local_track_id: int) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    short_uuid = uuid.uuid4().hex[:8]
    return f"A_{timestamp}_L{local_track_id}_{short_uuid}"


def make_capture_error(
    capture_type: str,
    capture_index: int | None,
    error_code: str,
    message: str,
    local_path: str | None = None,
) -> dict:
    """
    사진 저장 실패는 Re-ID/ENTRY 전체 실패와 분리한다.
    MQTT의 capture_errors에 넣을 진단 정보만 만든다.
    """
    error = {
        "capture_type": str(capture_type).upper(),
        "capture_index": capture_index,
        "error_code": str(error_code),
        "message": str(message),
    }

    if local_path:
        error["local_path"] = str(local_path)

    return error


def local_capture_to_source_path(
    local_path: str,
    capture_type: str,
) -> str:
    """
    Jetson 절대경로를 Main이 HTTP GET할 수 있는 source_path로 변환한다.

    BODY:
      /home/.../outputs/captures/A/<relative>
      -> /captures/body/<relative>

    FACE:
      /home/.../outputs/captures/A_face/<relative>
      -> /captures/face/<relative>
    """
    capture_type_upper = str(capture_type).upper()

    if capture_type_upper == "BODY":
        root = CAPTURE_ROOT.resolve()
        prefix = CAPTURE_BODY_URL_PREFIX
    elif capture_type_upper == "FACE":
        root = FACE_CAPTURE_ROOT.resolve()
        prefix = CAPTURE_FACE_URL_PREFIX
    else:
        raise ValueError(f"지원하지 않는 capture_type: {capture_type}")

    path = Path(local_path).resolve()
    relative = path.relative_to(root)

    return prefix + relative.as_posix()


def extract_capture_rank(
    local_path: str,
    capture_type: str,
) -> int | None:
    name = Path(local_path).name
    pattern = (
        r"^body_(\d+)"
        if str(capture_type).upper() == "BODY"
        else r"^face_(\d+)"
    )
    match = re.match(pattern, name)

    if match is None:
        return None

    return int(match.group(1))


def build_capture_items(
    request_id: str,
    timestamp: str,
    capture_type: str,
    saved_paths: list[str],
    qualities: list[float],
    frame_indices: list[int],
) -> tuple[list[dict], list[dict]]:
    """
    실제 저장 성공 + HTTP 공개경로 변환 성공한 사진만 captures에 포함한다.
    """
    capture_type_upper = str(capture_type).upper()
    captures: list[dict] = []
    errors: list[dict] = []

    for local_path in saved_paths:
        rank = extract_capture_rank(
            local_path=local_path,
            capture_type=capture_type_upper,
        )

        # legacy fallback body_best.jpg는 BODY-01로 취급한다.
        if rank is None and capture_type_upper == "BODY":
            if Path(local_path).name == "body_best.jpg":
                rank = 1

        if rank is None:
            errors.append(
                make_capture_error(
                    capture_type=capture_type_upper,
                    capture_index=None,
                    error_code="CAPTURE_RANK_PARSE_FAILED",
                    message=(
                        "저장 파일명에서 capture index를 찾지 못했습니다."
                    ),
                    local_path=local_path,
                )
            )
            continue

        try:
            source_path = local_capture_to_source_path(
                local_path=local_path,
                capture_type=capture_type_upper,
            )
        except Exception as exc:
            errors.append(
                make_capture_error(
                    capture_type=capture_type_upper,
                    capture_index=rank,
                    error_code="SOURCE_PATH_BUILD_FAILED",
                    message=f"{type(exc).__name__}: {exc}",
                    local_path=local_path,
                )
            )
            continue

        quality_score = (
            float(qualities[rank - 1])
            if 0 < rank <= len(qualities)
            else None
        )
        frame_index = (
            int(frame_indices[rank - 1])
            if 0 < rank <= len(frame_indices)
            else None
        )

        captures.append(
            {
                "capture_key": (
                    f"{request_id}-{capture_type_upper}-{rank:02d}"
                ),
                "capture_type": capture_type_upper,
                "capture_index": rank,
                "source_path": source_path,
                "quality_score": quality_score,
                "frame_index": frame_index,
                "captured_at": timestamp,
            }
        )

    captures.sort(
        key=lambda item: (
            item["capture_type"],
            item["capture_index"],
        )
    )

    return captures, errors


def save_entry_capture(
    crop: np.ndarray,
    request_id: str,
    timestamp: str,
) -> tuple[str, list[dict]]:
    """
    Legacy Body 대표사진 fallback.

    저장 실패는 예외를 전파하지 않고 capture_errors만 반환한다.
    """
    errors: list[dict] = []
    day_folder = timestamp[:10].replace("-", "")
    target_dir = CAPTURE_ROOT / day_folder / request_id
    capture_path = target_dir / "body_best.jpg"

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        success = cv2.imwrite(
            str(capture_path),
            crop,
            [cv2.IMWRITE_JPEG_QUALITY, 95],
        )

        if (
            success
            and capture_path.is_file()
            and capture_path.stat().st_size > 0
        ):
            return str(capture_path), errors

        errors.append(
            make_capture_error(
                capture_type="BODY",
                capture_index=1,
                error_code="IMAGE_WRITE_FAILED",
                message="Legacy BODY fallback JPG 저장 실패",
                local_path=str(capture_path),
            )
        )
    except Exception as exc:
        errors.append(
            make_capture_error(
                capture_type="BODY",
                capture_index=1,
                error_code="IMAGE_SAVE_EXCEPTION",
                message=f"{type(exc).__name__}: {exc}",
                local_path=str(capture_path),
            )
        )

    for error in errors:
        print(f"[Camera A] Capture 저장 실패: {error}")

    return "", errors


def save_body_candidates(
    candidates: list[BodyCandidate],
    request_id: str,
    timestamp: str,
) -> tuple[list[str], list[dict]]:
    if not candidates:
        return [], []

    day_folder = timestamp[:10].replace("-", "")
    target_dir = CAPTURE_ROOT / day_folder / request_id

    sorted_candidates = sorted(
        candidates,
        key=lambda item: item.selection_score,
        reverse=True,
    )[:BODY_TOP_K]

    saved_paths: list[str] = []
    errors: list[dict] = []

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        error = make_capture_error(
            capture_type="BODY",
            capture_index=None,
            error_code="DIRECTORY_CREATE_FAILED",
            message=f"{type(exc).__name__}: {exc}",
            local_path=str(target_dir),
        )
        errors.append(error)
        print(f"[Camera A] Body Capture 저장 실패: {error}")
        return saved_paths, errors

    for rank, candidate in enumerate(sorted_candidates, start=1):
        filename = (
            f"body_{rank}"
            f"_Q{candidate.quality:.3f}"
            f"_C{candidate.confidence:.3f}"
            f"_F{candidate.frame_index}.jpg"
        )
        path = target_dir / filename

        try:
            success = cv2.imwrite(
                str(path),
                candidate.image,
                [cv2.IMWRITE_JPEG_QUALITY, 95],
            )

            if (
                success
                and path.is_file()
                and path.stat().st_size > 0
            ):
                saved_paths.append(str(path))
            else:
                error = make_capture_error(
                    capture_type="BODY",
                    capture_index=rank,
                    error_code="IMAGE_WRITE_FAILED",
                    message="BODY JPG 저장 실패",
                    local_path=str(path),
                )
                errors.append(error)
                print(f"[Camera A] Body Capture 저장 실패: {error}")
        except Exception as exc:
            error = make_capture_error(
                capture_type="BODY",
                capture_index=rank,
                error_code="IMAGE_SAVE_EXCEPTION",
                message=f"{type(exc).__name__}: {exc}",
                local_path=str(path),
            )
            errors.append(error)
            print(f"[Camera A] Body Capture 저장 실패: {error}")

    return saved_paths, errors


def save_face_candidates(
    candidates: list[FaceCandidate],
    request_id: str,
    timestamp: str,
) -> tuple[list[str], list[dict]]:
    if not candidates:
        return [], []

    day_folder = timestamp[:10].replace("-", "")
    target_dir = FACE_CAPTURE_ROOT / day_folder / request_id

    sorted_candidates = sorted(
        candidates,
        key=lambda item: item.quality,
        reverse=True,
    )[:FACE_TOP_K]

    saved_paths: list[str] = []
    errors: list[dict] = []

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        error = make_capture_error(
            capture_type="FACE",
            capture_index=None,
            error_code="DIRECTORY_CREATE_FAILED",
            message=f"{type(exc).__name__}: {exc}",
            local_path=str(target_dir),
        )
        errors.append(error)
        print(f"[Camera A] Face Capture 저장 실패: {error}")
        return saved_paths, errors

    for rank, candidate in enumerate(sorted_candidates, start=1):
        filename = (
            f"face_{rank}"
            f"_Q{candidate.quality:.3f}"
            f"_C{candidate.confidence:.3f}"
            f"_F{candidate.frame_index}.jpg"
        )

        path = target_dir / filename

        try:
            success = cv2.imwrite(
                str(path),
                candidate.image,
                [cv2.IMWRITE_JPEG_QUALITY, 95],
            )

            if (
                success
                and path.is_file()
                and path.stat().st_size > 0
            ):
                saved_paths.append(str(path))
            else:
                error = make_capture_error(
                    capture_type="FACE",
                    capture_index=rank,
                    error_code="IMAGE_WRITE_FAILED",
                    message="FACE JPG 저장 실패",
                    local_path=str(path),
                )
                errors.append(error)
                print(f"[Camera A] Face Capture 저장 실패: {error}")
        except Exception as exc:
            error = make_capture_error(
                capture_type="FACE",
                capture_index=rank,
                error_code="IMAGE_SAVE_EXCEPTION",
                message=f"{type(exc).__name__}: {exc}",
                local_path=str(path),
            )
            errors.append(error)
            print(f"[Camera A] Face Capture 저장 실패: {error}")

    return saved_paths, errors


# ============================================================
# Body 후보 선별
# ============================================================

def update_body_candidates(
    candidates: list[BodyCandidate],
    new_candidate: BodyCandidate,
) -> str:
    """
    기존 Body 후보 선정 동작은 그대로 유지하고
    진단용 결과 문자열만 반환한다.

    반환값:
    - ADDED_DISTINCT
    - REPLACED_NEAR
    - REJECTED_NEAR
    - REJECTED_TOP_K
    """
    for index, old_candidate in enumerate(candidates):
        frame_gap = abs(
            new_candidate.frame_index
            - old_candidate.frame_index
        )

        if frame_gap < BODY_MIN_FRAME_GAP:
            if new_candidate.selection_score > old_candidate.selection_score:
                candidates[index] = new_candidate
                result = "REPLACED_NEAR"
            else:
                result = "REJECTED_NEAR"

            candidates.sort(
                key=lambda item: item.selection_score,
                reverse=True,
            )
            del candidates[BODY_TOP_K:]
            return result

    candidates.append(new_candidate)
    candidates.sort(
        key=lambda item: item.selection_score,
        reverse=True,
    )
    del candidates[BODY_TOP_K:]

    if any(
        candidate is new_candidate
        for candidate in candidates
    ):
        return "ADDED_DISTINCT"

    return "REJECTED_TOP_K"


# ============================================================
# 얼굴 품질
# ============================================================

def clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def calculate_face_sharpness(face_image: np.ndarray) -> float:
    if face_image.size == 0:
        return 0.0

    gray = cv2.cvtColor(face_image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def calculate_frontal_score(face: np.ndarray) -> float:
    try:
        eye1_x = float(face[4])
        eye1_y = float(face[5])
        eye2_x = float(face[6])
        eye2_y = float(face[7])
        nose_x = float(face[8])

        eye_mid_x = (eye1_x + eye2_x) / 2.0
        eye_distance = float(
            np.hypot(
                eye2_x - eye1_x,
                eye2_y - eye1_y,
            )
        )

        if eye_distance <= 1.0:
            return 0.0

        nose_offset = abs(nose_x - eye_mid_x) / eye_distance
        center_score = 1.0 - min(nose_offset / 0.65, 1.0)

        eye_slope = abs(eye2_y - eye1_y) / eye_distance
        level_score = 1.0 - min(eye_slope / 0.50, 1.0)

        return clamp01(
            0.70 * center_score
            + 0.30 * level_score
        )

    except Exception:
        return 0.0


def crop_face_with_padding(
    person_crop: np.ndarray,
    x: float,
    y: float,
    width: float,
    height: float,
) -> np.ndarray | None:
    image_height, image_width = person_crop.shape[:2]

    padding_x = width * FACE_SAVE_PADDING_RATIO
    padding_y = height * FACE_SAVE_PADDING_RATIO

    x1 = max(0, int(x - padding_x))
    y1 = max(0, int(y - padding_y))
    x2 = min(image_width, int(x + width + padding_x))
    y2 = min(image_height, int(y + height + padding_y))

    if x2 <= x1 or y2 <= y1:
        return None

    face_crop = person_crop[y1:y2, x1:x2]

    if face_crop.size == 0:
        return None

    return face_crop.copy()


def normalize_face_embedding(feature: np.ndarray) -> np.ndarray:
    embedding = np.asarray(feature, dtype=np.float32).reshape(-1)

    if embedding.size == 0:
        raise RuntimeError("SFace embedding이 비어 있습니다.")

    if not np.all(np.isfinite(embedding)):
        raise RuntimeError("SFace embedding에 NaN/Inf가 있습니다.")

    norm = float(np.linalg.norm(embedding))

    if norm <= 1e-12:
        raise RuntimeError("SFace embedding norm이 0입니다.")

    return embedding / norm


def rescale_yunet_face(
    face: np.ndarray,
    scale: float,
) -> np.ndarray:
    """
    YuNet의 [bbox 4 + landmark 10 + score 1] 결과를
    원본 person crop 좌표계로 되돌린다.
    """
    scaled_face = np.asarray(face, dtype=np.float32).reshape(-1).copy()

    if scaled_face.size < 15:
        raise RuntimeError(
            f"YuNet face 결과 크기가 예상과 다릅니다: {scaled_face.shape}"
        )

    if scale != 1.0:
        scaled_face[:14] /= float(scale)

    return scaled_face


def detect_face_candidate(
    detector,
    recognizer,
    person_crop: np.ndarray,
    frame_index: int,
) -> FaceCandidate | None:
    if person_crop.size == 0:
        return None

    original_height, original_width = person_crop.shape[:2]

    if original_width < 20 or original_height < 20:
        return None

    enlarged = cv2.resize(
        person_crop,
        None,
        fx=FACE_UPSCALE_FACTOR,
        fy=FACE_UPSCALE_FACTOR,
        interpolation=cv2.INTER_CUBIC,
    )

    attempts: list[tuple[np.ndarray, float]] = [
        (person_crop, 1.0),
        (enlarged, FACE_UPSCALE_FACTOR),
    ]

    selected_face: np.ndarray | None = None
    selected_scale = 1.0

    for detect_image, scale in attempts:
        height, width = detect_image.shape[:2]

        try:
            detector.setScoreThreshold(FACE_SCORE_THRESHOLD)
            detector.setInputSize((width, height))
            _, faces = detector.detect(detect_image)
        except cv2.error:
            faces = None

        if faces is None or len(faces) == 0:
            continue

        selected_face = max(
            faces,
            key=lambda item: float(item[-1]),
        )
        selected_scale = scale
        break

    if selected_face is None:
        return None

    face_on_original = rescale_yunet_face(
        selected_face,
        selected_scale,
    )

    confidence = float(face_on_original[-1])

    if confidence < FACE_SCORE_THRESHOLD:
        return None

    face_x = float(face_on_original[0])
    face_y = float(face_on_original[1])
    face_width = float(face_on_original[2])
    face_height = float(face_on_original[3])

    if face_width < FACE_MIN_SIZE_PX or face_height < FACE_MIN_SIZE_PX:
        return None

    raw_x1 = max(0, int(face_x))
    raw_y1 = max(0, int(face_y))
    raw_x2 = min(original_width, int(face_x + face_width))
    raw_y2 = min(original_height, int(face_y + face_height))

    if raw_x2 <= raw_x1 or raw_y2 <= raw_y1:
        return None

    raw_face = person_crop[raw_y1:raw_y2, raw_x1:raw_x2]

    if raw_face.size == 0:
        return None

    sharpness = calculate_face_sharpness(raw_face)

    if sharpness < FACE_MIN_SHARPNESS:
        return None

    frontal_score = calculate_frontal_score(face_on_original)

    if frontal_score < FACE_MIN_FRONTAL_SCORE:
        return None

    face_area = face_width * face_height
    person_area = max(1.0, float(original_width * original_height))
    area_ratio = face_area / person_area

    size_score = clamp01(area_ratio / 0.035)
    sharpness_score = clamp01(sharpness / 120.0)

    quality = (
        0.45 * confidence
        + 0.20 * frontal_score
        + 0.20 * size_score
        + 0.15 * sharpness_score
    )

    # --------------------------------------------------------
    # SFace: YuNet landmark를 이용해 정렬한 뒤 특징 추출
    # --------------------------------------------------------
    try:
        aligned_face = recognizer.alignCrop(
            person_crop,
            face_on_original,
        )
        raw_feature = recognizer.feature(aligned_face)
        face_embedding = normalize_face_embedding(raw_feature)
    except Exception as error:
        print(
            f"[Camera A] SFace 특징 추출 실패 "
            f"F{frame_index}: {error}"
        )
        return None

    saved_face = crop_face_with_padding(
        person_crop=person_crop,
        x=face_x,
        y=face_y,
        width=face_width,
        height=face_height,
    )

    if saved_face is None:
        return None

    saved_height, saved_width = saved_face.shape[:2]
    largest_side = max(saved_width, saved_height)

    if largest_side < 160:
        resize_scale = 160.0 / max(1, largest_side)
        saved_face = cv2.resize(
            saved_face,
            None,
            fx=resize_scale,
            fy=resize_scale,
            interpolation=cv2.INTER_CUBIC,
        )

    return FaceCandidate(
        image=saved_face,
        confidence=confidence,
        quality=float(quality),
        sharpness=float(sharpness),
        area_ratio=float(area_ratio),
        frontal_score=float(frontal_score),
        frame_index=frame_index,
        embedding=face_embedding,
    )

def update_face_candidates(
    candidates: list[FaceCandidate],
    new_candidate: FaceCandidate,
) -> None:
    for index, old_candidate in enumerate(candidates):
        frame_gap = abs(
            new_candidate.frame_index
            - old_candidate.frame_index
        )

        if frame_gap < FACE_MIN_FRAME_GAP:
            if new_candidate.quality > old_candidate.quality:
                candidates[index] = new_candidate

            candidates.sort(
                key=lambda item: item.quality,
                reverse=True,
            )
            del candidates[FACE_TOP_K:]
            return

    candidates.append(new_candidate)
    candidates.sort(
        key=lambda item: item.quality,
        reverse=True,
    )
    del candidates[FACE_TOP_K:]


# ============================================================
# Face Worker
# ============================================================

def face_worker_loop(
    task_buffer: LatestFaceTaskBuffer,
    result_queue: queue.Queue[FaceResult],
    ready_event: threading.Event,
    state: dict[str, str | None],
) -> None:
    try:
        detector = cv2.FaceDetectorYN.create(
            str(FACE_DETECTOR_MODEL_PATH),
            "",
            (320, 320),
            FACE_SCORE_THRESHOLD,
            0.3,
            5000,
        )
        recognizer = cv2.FaceRecognizerSF.create(
            str(FACE_RECOGNIZER_MODEL_PATH),
            "",
        )
        state["error"] = None
    except Exception as error:
        state["error"] = str(error)
        ready_event.set()
        return

    ready_event.set()

    while True:
        tasks = task_buffer.get_batch()

        if tasks is None:
            return

        for task in tasks:
            try:
                candidate = detect_face_candidate(
                    detector=detector,
                    recognizer=recognizer,
                    person_crop=task.person_crop,
                    frame_index=task.frame_index,
                )

                if candidate is None:
                    continue

                result_queue.put(
                    FaceResult(
                        local_track_id=task.local_track_id,
                        candidate=candidate,
                    )
                )

            except Exception as error:
                print(
                    f"[Camera A] Face Worker 오류 "
                    f"L{task.local_track_id}: {error}"
                )


# ============================================================
# ENTRY Worker
# ============================================================

def entry_worker_loop(
    job_queue: queue.Queue[EntryJob | None],
    mqtt_publisher: MqttPublisher,
    ready_event: threading.Event,
    state: dict[str, str | None],
) -> None:
    try:
        reid_engine = ReIDTensorRTEngine(REID_ENGINE_PATH)
        state["error"] = None
    except Exception as error:
        state["error"] = str(error)
        ready_event.set()
        return

    ready_event.set()

    while True:
        job = job_queue.get()

        if job is None:
            job_queue.task_done()
            return

        try:
            # ------------------------------------------------
            # Body Grace
            #
            # Main 영상/YOLO/ByteTrack loop는 계속 동작한다.
            # 단일 Entry Worker만 해당 Job의 Grace 종료를 기다린다.
            # ------------------------------------------------
            worker_started_monotonic = time.monotonic()
            worker_queue_wait_sec = max(
                0.0,
                worker_started_monotonic - job.enqueued_monotonic,
            )

            while True:
                current_reason = job.body_grace_diag.get_end_reason()

                if current_reason is not None:
                    break

                if len(job.body_candidates) >= BODY_TOP_K:
                    end_frame, end_time = job.frame_progress.snapshot()
                    job.body_grace_diag.finish_once(
                        reason="TARGET_REACHED",
                        end_frame=end_frame,
                        end_monotonic=end_time,
                    )
                    break

                now = time.monotonic()
                remaining_body_grace = (
                    job.body_grace_deadline - now
                )

                if remaining_body_grace <= 0:
                    end_frame, end_time = job.frame_progress.snapshot()
                    job.body_grace_diag.finish_once(
                        reason="GRACE_TIMEOUT",
                        end_frame=end_frame,
                        end_monotonic=job.body_grace_deadline,
                    )
                    break

                time.sleep(min(0.02, remaining_body_grace))

            # grace 종료 또는 TOP3 확보 시점의 Body 후보를 고정한다.
            body_candidates_snapshot = list(job.body_candidates)

            sorted_bodies = sorted(
                body_candidates_snapshot,
                key=lambda item: item.selection_score,
                reverse=True,
            )[:BODY_TOP_K]

            if not sorted_bodies:
                end_frame, end_time = job.frame_progress.snapshot()
                job.body_grace_diag.finish_once(
                    reason="NO_VALID_BODY",
                    end_frame=end_frame,
                    end_monotonic=end_time,
                )
                diag = job.body_grace_diag.snapshot(
                    fallback_frame=end_frame,
                    fallback_monotonic=end_time,
                )

                print()
                print("[A BODY GRACE END]")
                print(f"request_id             : {job.request_id}")
                print(f"local_track_id         : {job.local_track_id}")
                print(f"initial_count          : {diag['initial_count']}")
                print("final_count            : 0")
                print(f"rejected_count         : {diag['rejected_count']}")
                print(f"replaced_count         : {diag['replaced_count']}")
                print(
                    "candidate_attempt_count : "
                    f"{diag['candidate_attempt_count']}"
                )
                print(
                    "collection_duration     : "
                    f"{float(diag['collection_duration']):.3f} sec"
                )
                print(
                    "processed_frames        : "
                    f"{diag['processed_frames']}"
                )
                print(
                    "average_processing_fps  : "
                    f"{float(diag['average_processing_fps']):.2f}"
                )
                print(
                    "collection_end_reason   : "
                    f"{diag['collection_end_reason']}"
                )
                print("selected_frame_indices : []")
                print("body_capture_paths     : []")
                print(
                    "worker_queue_wait_sec   : "
                    f"{worker_queue_wait_sec:.3f}"
                )

                raise RuntimeError("ENTRY Body 후보가 없습니다.")

            body_embeddings_np: list[np.ndarray] = []

            for body_item in sorted_bodies:
                body_embedding = reid_engine.extract(body_item.image)
                body_embedding = (
                    body_embedding
                    .astype(np.float32)
                    .reshape(-1)
                )

                if body_embedding.size != 512:
                    raise RuntimeError(
                        "Re-ID embedding 크기 오류: "
                        f"{body_embedding.shape}"
                    )

                if not np.all(np.isfinite(body_embedding)):
                    raise RuntimeError(
                        "Body Re-ID embedding에 NaN/Inf가 있습니다."
                    )

                body_embeddings_np.append(body_embedding)

            # ------------------------------------------------
            # Legacy Body 1개는 기존 Main/B 호환을 위해 그대로 유지
            # ------------------------------------------------
            best_body = sorted_bodies[0]
            best_embedding = body_embeddings_np[0]
            best_embedding_norm = float(
                np.linalg.norm(best_embedding)
            )

            # Body TOP3를 Face와 동일한 구조로
            # 날짜/request_id 폴더 아래에 저장한다.
            body_capture_paths, body_capture_errors = (
                save_body_candidates(
                    candidates=sorted_bodies,
                    request_id=job.request_id,
                    timestamp=job.timestamp,
                )
            )

            # 기존 Main/B 호환용 capture_path 필드는 유지한다.
            # 사진 저장이 모두 실패해도 embedding/ENTRY MQTT는 계속 보낸다.
            if body_capture_paths:
                capture_path = body_capture_paths[0]
                legacy_capture_errors: list[dict] = []
            else:
                (
                    capture_path,
                    legacy_capture_errors,
                ) = save_entry_capture(
                    crop=best_body.image,
                    request_id=job.request_id,
                    timestamp=job.timestamp,
                )

            body_embeddings = [
                embedding.tolist()
                for embedding in body_embeddings_np
            ]
            body_qualities = [
                float(item.quality)
                for item in sorted_bodies
            ]
            body_confidences = [
                float(item.confidence)
                for item in sorted_bodies
            ]
            body_frame_indices = [
                int(item.frame_index)
                for item in sorted_bodies
            ]

            end_frame, end_time = job.frame_progress.snapshot()
            diag = job.body_grace_diag.snapshot(
                fallback_frame=end_frame,
                fallback_monotonic=end_time,
            )

            print()
            print("[A BODY GRACE END]")
            print(f"request_id             : {job.request_id}")
            print(f"local_track_id         : {job.local_track_id}")
            print(f"initial_count          : {diag['initial_count']}")
            print(f"final_count            : {len(sorted_bodies)}")
            print(f"rejected_count         : {diag['rejected_count']}")
            print(f"replaced_count         : {diag['replaced_count']}")
            print(
                "candidate_attempt_count : "
                f"{diag['candidate_attempt_count']}"
            )
            print(
                "collection_duration     : "
                f"{float(diag['collection_duration']):.3f} sec"
            )
            print(
                "processed_frames        : "
                f"{diag['processed_frames']}"
            )
            print(
                "average_processing_fps  : "
                f"{float(diag['average_processing_fps']):.2f}"
            )
            print(
                "collection_end_reason   : "
                f"{diag['collection_end_reason']}"
            )
            print(
                "selected_frame_indices : "
                f"{body_frame_indices}"
            )
            print(
                "body_capture_paths     : "
                f"{body_capture_paths}"
            )
            print(
                "worker_queue_wait_sec   : "
                f"{worker_queue_wait_sec:.3f}"
            )

            # ------------------------------------------------
            # Face
            # ------------------------------------------------
            # Body TOP3 OSNet 처리가 진행되는 동안 Face Worker도 병렬로 동작한다.
            # 그래도 유예시간이 남아 있으면 Entry Worker만 잠깐 기다린다.
            # 메인 영상/ByteTrack 루프는 절대 sleep 하지 않는다.
            remaining_face_grace = (
                job.face_grace_deadline - time.monotonic()
            )
            if remaining_face_grace > 0:
                time.sleep(remaining_face_grace)

            # 유예 시간이 끝난 시점의 Face 후보를 고정해서 저장/전송한다.
            face_candidates_snapshot = list(job.face_candidates)

            face_capture_paths, face_capture_errors = (
                save_face_candidates(
                    candidates=face_candidates_snapshot,
                    request_id=job.request_id,
                    timestamp=job.timestamp,
                )
            )

            sorted_faces = sorted(
                face_candidates_snapshot,
                key=lambda item: item.quality,
                reverse=True,
            )[:FACE_TOP_K]

            face_embeddings = [
                face_item.embedding.astype(np.float32).reshape(-1).tolist()
                for face_item in sorted_faces
            ]
            face_embedding_dim = (
                len(face_embeddings[0])
                if face_embeddings
                else 0
            )
            face_qualities = [
                float(face_item.quality)
                for face_item in sorted_faces
            ]
            face_confidences = [
                float(face_item.confidence)
                for face_item in sorted_faces
            ]
            face_frontal_scores = [
                float(face_item.frontal_score)
                for face_item in sorted_faces
            ]
            face_sharpness = [
                float(face_item.sharpness)
                for face_item in sorted_faces
            ]
            face_frame_indices = [
                int(face_item.frame_index)
                for face_item in sorted_faces
            ]

            # ------------------------------------------------
            # Main HTTP 다운로드용 optional capture interface
            #
            # 기존 absolute *_capture_paths는 호환을 위해 그대로 유지하고,
            # captures[].source_path만 실제 HTTP 공개경로를 사용한다.
            # 실제 저장 성공한 파일만 captures에 들어간다.
            # ------------------------------------------------
            body_capture_items, body_path_errors = (
                build_capture_items(
                    request_id=job.request_id,
                    timestamp=job.timestamp,
                    capture_type="BODY",
                    saved_paths=body_capture_paths,
                    qualities=body_qualities,
                    frame_indices=body_frame_indices,
                )
            )

            face_capture_items, face_path_errors = (
                build_capture_items(
                    request_id=job.request_id,
                    timestamp=job.timestamp,
                    capture_type="FACE",
                    saved_paths=face_capture_paths,
                    qualities=face_qualities,
                    frame_indices=face_frame_indices,
                )
            )

            # TOP-K BODY가 하나도 저장되지 않아 legacy fallback만 성공한 경우
            # fallback JPG도 Main에서 받을 수 있도록 BODY-01 capture로 제공한다.
            if (
                not body_capture_items
                and capture_path
                and capture_path not in body_capture_paths
            ):
                fallback_items, fallback_path_errors = (
                    build_capture_items(
                        request_id=job.request_id,
                        timestamp=job.timestamp,
                        capture_type="BODY",
                        saved_paths=[capture_path],
                        qualities=body_qualities[:1],
                        frame_indices=body_frame_indices[:1],
                    )
                )
                body_capture_items.extend(fallback_items)
                body_path_errors.extend(fallback_path_errors)

            captures = body_capture_items + face_capture_items
            capture_errors = (
                body_capture_errors
                + legacy_capture_errors
                + face_capture_errors
                + body_path_errors
                + face_path_errors
            )

            # ------------------------------------------------
            # MQTT
            #
            # 중요:
            # 1) embedding / quality / capture_path는 기존 계약 유지
            # 2) body_embeddings에 최대 3개를 추가
            # 3) Main이 새 필드를 사용하기 전까지도 기존 흐름은 유지됨
            # ------------------------------------------------
            mqtt_publisher.publish_entry(
                {
                    "request_id": job.request_id,
                    "timestamp": job.timestamp,
                    "node_id": "A",
                    "event": "ENTRY",
                    "local_track_id": job.local_track_id,
                    "next_nodes": ["B", "C"],

                    # Legacy / Body Re-ID contract
                    "reid_model": "osnet_x0_25",
                    "embedding_dim": int(best_embedding.size),
                    "embedding": best_embedding.tolist(),
                    "quality": float(best_body.quality),
                    "capture_path": capture_path,
                    "verification_status": "AUTO_MATCHED",

                    # Body Multi-Gallery extension
                    "body_available": bool(body_embeddings),
                    "body_count": len(body_embeddings),
                    "body_embedding_dim": 512,
                    "body_embeddings": body_embeddings,
                    "body_qualities": body_qualities,
                    "body_confidences": body_confidences,
                    "body_frame_indices": body_frame_indices,
                    "body_capture_paths": body_capture_paths,

                    # Face extension contract
                    "face_available": bool(face_embeddings),
                    "face_detector_model": FACE_DETECTOR_MODEL_NAME,
                    "face_reid_model": FACE_REID_MODEL_NAME,
                    "face_embedding_dim": face_embedding_dim,
                    "face_embeddings": face_embeddings,
                    "face_qualities": face_qualities,
                    "face_confidences": face_confidences,
                    "face_frontal_scores": face_frontal_scores,
                    "face_sharpness": face_sharpness,
                    "face_capture_paths": face_capture_paths,

                    # Optional Main capture-download extension
                    "captures": captures,
                    "capture_errors": capture_errors,
                }
            )

            print()
            print("===== A ENTRY 처리 완료 =====")
            print(f"Local ID       : {job.local_track_id}")
            print(f"Request ID     : {job.request_id}")
            print(f"Entry Time     : {job.timestamp}")
            print(
                f"Body Count     : "
                f"{len(body_embeddings)}/{BODY_TOP_K}"
            )
            print("Body Emb Dim   : 512")
            print(
                f"Best Body Norm : "
                f"{best_embedding_norm:.6f}"
            )
            print(f"Legacy Capture : {capture_path}")

            for rank, (
                body_item,
                body_embedding,
            ) in enumerate(
                zip(sorted_bodies, body_embeddings_np),
                start=1,
            ):
                print(
                    f"  BODY #{rank} "
                    f"Q={body_item.quality:.3f} "
                    f"CONF={body_item.confidence:.3f} "
                    f"F={body_item.frame_index} "
                    f"NORM={np.linalg.norm(body_embedding):.6f}"
                )

            if body_capture_paths:
                print(
                    "Body Folder    : "
                    f"{Path(body_capture_paths[0]).parent}"
                )

            print(
                f"Face Count     : "
                f"{len(face_capture_paths)}/{FACE_TOP_K}"
            )
            print(f"HTTP Captures  : {len(captures)}")
            print(f"Capture Errors : {len(capture_errors)}")

            if sorted_faces:
                print(
                    f"Face Emb Dim   : "
                    f"{face_embedding_dim}"
                )

                for rank, face_item in enumerate(
                    sorted_faces[:FACE_TOP_K],
                    start=1,
                ):
                    print(
                        f"  FACE #{rank} "
                        f"Q={face_item.quality:.3f} "
                        f"CONF={face_item.confidence:.3f} "
                        f"FRONT={face_item.frontal_score:.3f} "
                        f"SHARP={face_item.sharpness:.1f} "
                        f"EMB={face_item.embedding.size}D"
                    )

                if face_capture_paths:
                    print(
                        "Face Folder    : "
                        f"{Path(face_capture_paths[0]).parent}"
                    )
                else:
                    print("Face Folder    : 저장 실패")
            else:
                print("Best Faces     : 없음 (Body Re-ID만 전송)")

            print("===============================")

        except Exception as error:
            with identity_lock:
                identity = identity_by_local_id.get(job.local_track_id)
                if identity is not None:
                    identity.person_status = "SEND_ERROR"

            print()
            print(f"[Camera A] ENTRY Worker 오류: {error}")

        finally:
            job_queue.task_done()


# ============================================================
# MQTT A 응답 수신
# ============================================================

def safe_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def on_response_connect(
    client: mqtt.Client,
    userdata,
    flags,
    reason_code,
    properties,
) -> None:
    if reason_code.is_failure:
        print(f"Camera A 응답 MQTT 연결 실패: {reason_code}")
        return

    client.subscribe(TOPIC_A_ENTRY_RESPONSE, qos=1)
    print(f"Camera A MQTT 응답 구독: {TOPIC_A_ENTRY_RESPONSE}")


def on_response_message(
    client: mqtt.Client,
    userdata,
    message: mqtt.MQTTMessage,
) -> None:
    try:
        payload = json.loads(message.payload.decode("utf-8"))

        if payload.get("event") != "ENTRY_RESULT":
            return

        request_id = payload.get("request_id")
        local_track_id = payload.get("local_track_id")

        with identity_lock:
            if local_track_id is None and request_id:
                local_track_id = local_id_by_request_id.get(str(request_id))

            if local_track_id is None:
                print("[Camera A] ENTRY_RESULT에 Local ID가 없습니다.")
                return

            local_track_id = int(local_track_id)
            request_id = str(
                request_id
                or f"A_RESPONSE_L{local_track_id}"
            )

            # Main 응답으로 EntryIdentity를 새로 만들더라도
            # A가 ENTRY LINE을 실제 통과한 시각은 그대로 보존한다.
            previous_identity = identity_by_local_id.get(local_track_id)

            entry_at = (
                previous_identity.entry_at
                if previous_identity is not None
                else None
            )

            # Main이 명시적으로 entry_at을 돌려주는 경우에도 대응한다.
            if not entry_at:
                entry_at = payload.get("entry_at")

            # 마지막 fallback. 구형 Main 응답에서는 timestamp만 있을 수 있다.
            if not entry_at:
                entry_at = payload.get("timestamp")

            identity = EntryIdentity(
                local_track_id=local_track_id,
                request_id=request_id,
                person_uid=payload.get("person_uid"),
                journey_id=payload.get("journey_id"),
                person_status=str(
                    payload.get("person_status", "UNKNOWN")
                ).upper(),
                visit_count=int(payload.get("visit_count", 0) or 0),
                match_score=safe_float(
                    payload.get("person_match_score")
                ),
                previous_last_seen_at=payload.get(
                    "previous_last_seen_at"
                ),
                candidate_person_uid=payload.get(
                    "candidate_person_uid"
                ),
                entry_at=(
                    str(entry_at)
                    if entry_at
                    else None
                ),
                updated_at=str(
                    payload.get(
                        "timestamp",
                        datetime.now().isoformat(timespec="seconds"),
                    )
                ),
            )

            identity_by_local_id[local_track_id] = identity
            local_id_by_request_id[request_id] = local_track_id
            recent_results.appendleft(identity)

            should_log = request_id not in logged_response_request_ids

            if should_log:
                logged_response_request_ids.add(request_id)

        if should_log:
            save_central_entry_result(identity)

        # Main에서 canonical person_uid / journey_id가 확정된 순간 matched_at 기록
        bind_canonical_journey_timing(
            local_track_id=identity.local_track_id,
            person_uid=identity.person_uid,
            journey_id=identity.journey_id,
        )

        print()
        print("===== A 중앙 ID 수신 =====")
        print(f"Local ID     : {identity.local_track_id}")
        print(f"Entry Time   : {identity.entry_at or '-'}")
        print(f"Person UID   : {identity.person_uid}")
        print(f"Journey ID   : {identity.journey_id}")
        print(f"Person 상태  : {identity.person_status}")
        print(f"방문 횟수    : {identity.visit_count}")
        print(f"Match Score  : {identity.match_score}")

        if identity.person_status == "REVIEW_REQUIRED":
            print(
                f"검토 후보 ID : "
                f"{identity.candidate_person_uid or '-'}"
            )
            print(
                "검토 처리     : Main Server에서 "
                "MERGE_EXISTING 또는 CONFIRM_NEW"
            )

        print("==========================")

    except Exception as error:
        print(f"[Camera A] ENTRY_RESULT 처리 오류: {error}")


def start_response_client() -> mqtt.Client:
    client_id = f"camera_a_response_{uuid.uuid4().hex[:8]}"

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
    )
    client.on_connect = on_response_connect
    client.on_message = on_response_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    return client


# ============================================================
# 영상 처리 보조 함수
# ============================================================

def apply_small_brightness_adjustment(frame: np.ndarray) -> np.ndarray:
    return cv2.convertScaleAbs(
        frame,
        alpha=IMAGE_CONTRAST_ALPHA,
        beta=IMAGE_BRIGHTNESS_BETA,
    )


def extract_person_crop(
    frame: np.ndarray,
    box: list[int],
    padding_ratio: float = 0.04,
) -> np.ndarray:
    frame_height, frame_width = frame.shape[:2]
    x1, y1, x2, y2 = box

    box_width = max(1, x2 - x1)
    box_height = max(1, y2 - y1)

    padding_x = int(box_width * padding_ratio)
    padding_y = int(box_height * padding_ratio)

    crop_x1 = max(0, x1 - padding_x)
    crop_y1 = max(0, y1 - padding_y)
    crop_x2 = min(frame_width, x2 + padding_x)
    crop_y2 = min(frame_height, y2 + padding_y)

    crop = frame[crop_y1:crop_y2, crop_x1:crop_x2]

    if crop.size == 0:
        raise RuntimeError(f"사람 Crop이 비어 있습니다: {box}")

    return crop.copy()


def crossed_entry_line(
    previous_x: int,
    current_x: int,
    line_x: int,
) -> bool:
    if ENTRY_DIRECTION == "right":
        return previous_x < line_x <= current_x

    if ENTRY_DIRECTION == "left":
        return previous_x > line_x >= current_x

    raise ValueError(
        "ENTRY_DIRECTION은 'right' 또는 'left'여야 합니다."
    )


# ============================================================
# 화면 표시
# ============================================================

def draw_entry_guide(
    frame: np.ndarray,
    line_x: int,
    frame_width: int,
    frame_height: int,
) -> None:
    """
    Camera A ENTRY 가이드 UI.

    목표:
    - 하단 Dashboard 없이 카메라 화면만 표시
    - ENTRY 선은 기존보다 굵고 눈에 잘 띄게 표시
    - 과한 문구/영역 색칠 없이 관제 화면 느낌의 최소 UI 유지
    - 좌측 상단 LIVE / CAM A 표시
    """

    # OpenCV BGR
    line_color = (255, 210, 70)
    text_color = (245, 248, 250)
    muted_color = (185, 195, 205)
    panel_color = (16, 20, 26)
    live_color = (90, 90, 255)

    # ---------------------------------------------------------
    # 1. ENTRY LINE GLOW
    # ---------------------------------------------------------
    glow = frame.copy()

    cv2.line(
        glow,
        (line_x, 0),
        (line_x, frame_height),
        line_color,
        12,
        cv2.LINE_AA,
    )

    cv2.addWeighted(
        glow,
        0.12,
        frame,
        0.88,
        0,
        frame,
    )

    # 실제 ENTRY 선: 기존 2px -> 4px
    cv2.line(
        frame,
        (line_x, 0),
        (line_x, frame_height),
        line_color,
        4,
        cv2.LINE_AA,
    )

    font = cv2.FONT_HERSHEY_SIMPLEX

    # ---------------------------------------------------------
    # 2. 좌측 상단 LIVE / CAM A
    # ---------------------------------------------------------
    hud_x1 = 18
    hud_y1 = 18
    hud_x2 = 172
    hud_y2 = 54

    hud_overlay = frame.copy()
    cv2.rectangle(
        hud_overlay,
        (hud_x1, hud_y1),
        (hud_x2, hud_y2),
        panel_color,
        -1,
    )

    cv2.addWeighted(
        hud_overlay,
        0.72,
        frame,
        0.28,
        0,
        frame,
    )

    # LIVE indicator
    cv2.circle(
        frame,
        (hud_x1 + 16, hud_y1 + 18),
        5,
        live_color,
        -1,
        cv2.LINE_AA,
    )

    cv2.putText(
        frame,
        "LIVE",
        (hud_x1 + 29, hud_y1 + 24),
        font,
        0.48,
        text_color,
        1,
        cv2.LINE_AA,
    )

    cv2.putText(
        frame,
        "CAM A",
        (hud_x1 + 88, hud_y1 + 24),
        font,
        0.48,
        muted_color,
        1,
        cv2.LINE_AA,
    )

    # ---------------------------------------------------------
    # 3. ENTRY 라벨
    # ---------------------------------------------------------
    label_text = "ENTRY"
    font_scale = 0.62
    thickness = 1

    (label_w, label_h), _ = cv2.getTextSize(
        label_text,
        font,
        font_scale,
        thickness,
    )

    label_x1 = max(
        12,
        min(
            frame_width - label_w - 34,
            line_x - label_w // 2 - 16,
        ),
    )
    label_y1 = 18
    label_x2 = label_x1 + label_w + 32
    label_y2 = label_y1 + label_h + 22

    label_overlay = frame.copy()

    cv2.rectangle(
        label_overlay,
        (label_x1, label_y1),
        (label_x2, label_y2),
        panel_color,
        -1,
    )

    cv2.addWeighted(
        label_overlay,
        0.72,
        frame,
        0.28,
        0,
        frame,
    )

    # ENTRY 라벨 아래에 작은 강조선
    cv2.line(
        frame,
        (label_x1 + 10, label_y2 - 4),
        (label_x2 - 10, label_y2 - 4),
        line_color,
        2,
        cv2.LINE_AA,
    )

    cv2.putText(
        frame,
        label_text,
        (label_x1 + 16, label_y2 - 11),
        font,
        font_scale,
        text_color,
        thickness,
        cv2.LINE_AA,
    )

    # ---------------------------------------------------------
    # 4. ENTRY 방향 표시
    # ---------------------------------------------------------
    arrow_y = 86
    arrow_length = 92

    if ENTRY_DIRECTION == "right":
        arrow_start = (
            max(26, line_x - arrow_length - 28),
            arrow_y,
        )
        arrow_end = (
            max(36, line_x - 28),
            arrow_y,
        )

        direction_text = "ENTRY"
        text_x = max(
            18,
            arrow_start[0] - 68,
        )

    elif ENTRY_DIRECTION == "left":
        arrow_start = (
            min(frame_width - 26, line_x + arrow_length + 28),
            arrow_y,
        )
        arrow_end = (
            min(frame_width - 36, line_x + 28),
            arrow_y,
        )

        direction_text = "ENTRY"
        text_x = min(
            frame_width - 78,
            arrow_start[0] + 12,
        )

    else:
        raise ValueError(
            "ENTRY_DIRECTION은 'right' 또는 'left'여야 합니다."
        )

    # 화살표 shadow/glow
    arrow_glow = frame.copy()
    cv2.arrowedLine(
        arrow_glow,
        arrow_start,
        arrow_end,
        line_color,
        7,
        cv2.LINE_AA,
        tipLength=0.20,
    )

    cv2.addWeighted(
        arrow_glow,
        0.12,
        frame,
        0.88,
        0,
        frame,
    )

    cv2.arrowedLine(
        frame,
        arrow_start,
        arrow_end,
        line_color,
        2,
        cv2.LINE_AA,
        tipLength=0.20,
    )

    cv2.putText(
        frame,
        direction_text,
        (text_x, arrow_y + 6),
        font,
        0.46,
        muted_color,
        1,
        cv2.LINE_AA,
    )


def identity_label_and_color(
    identity: EntryIdentity | None,
) -> tuple[str, tuple[int, int, int]]:
    if identity is None:
        return "STRANGER", (0, 165, 255)

    status = identity.person_status.upper()

    if status == "REGISTERING":
        return "REGISTERING...", (255, 220, 0)

    if status == "RETURNING":
        return_count = max(0, identity.visit_count - 1)

        return (
            f"{identity.person_uid} | RETURN #{return_count}",
            (0, 255, 0),
        )

    if status == "NEW":
        return f"{identity.person_uid} | NEW", (0, 230, 255)

    if status == "REVIEW_REQUIRED":
        return f"{identity.person_uid} | REVIEW", (0, 80, 255)

    return (
        f"{identity.person_uid or 'UNKNOWN'} | {status}",
        (255, 255, 0),
    )


def draw_dashboard(frame: np.ndarray) -> np.ndarray:
    """
    과거에는 카메라 아래에 DASHBOARD_HEIGHT만큼 검은 정보 패널을 붙였지만,
    현재는 웹 스트림에서 카메라 영상만 그대로 반환한다.

    함수 이름은 기존 호출부 호환을 위해 유지한다.
    """
    return frame


# ============================================================
# Worker 결과 적용 / Track 정리
# ============================================================

def drain_face_results(
    result_queue: queue.Queue[FaceResult],
    face_candidates_by_local_id: dict[int, list[FaceCandidate]],
    last_seen_by_local_id: dict[int, float],
    face_grace_deadline_by_local_id: dict[int, float],
) -> None:
    while True:
        try:
            face_result = result_queue.get_nowait()
        except queue.Empty:
            return

        local_id = face_result.local_track_id
        now = time.monotonic()

        with identity_lock:
            already_sent = local_id in identity_by_local_id

        grace_deadline = face_grace_deadline_by_local_id.get(local_id)
        grace_active = (
            grace_deadline is not None
            and now <= grace_deadline
        )

        # ENTRY 전에는 기존과 동일하게 받는다.
        # ENTRY 후에는 유예시간 안에 도착한, 이미 진행 중이던 Face 결과만 살린다.
        if already_sent and not grace_active:
            continue

        # Track이 사라졌더라도 ENTRY 직후 유예시간 안이라면 결과를 허용한다.
        if (
            local_id not in last_seen_by_local_id
            and not grace_active
        ):
            continue

        face_list = face_candidates_by_local_id.setdefault(local_id, [])
        update_face_candidates(
            candidates=face_list,
            new_candidate=face_result.candidate,
        )


def cleanup_track_state(
    current_time: float,
    current_frame_index: int,
    last_seen_by_local_id: dict[int, float],
    previous_x_by_local_id: dict[int, int],
    body_candidates_by_local_id: dict[int, list[BodyCandidate]],
    face_candidates_by_local_id: dict[int, list[FaceCandidate]],
    body_grace_deadline_by_local_id: dict[int, float],
    body_grace_diag_by_local_id: dict[int, BodyGraceDiagnostics],
    face_grace_deadline_by_local_id: dict[int, float],
    face_task_buffer: LatestFaceTaskBuffer,
    timing_mqtt_client: mqtt.Client,
) -> None:
    expired_local_ids = [
        local_id
        for local_id, last_seen in last_seen_by_local_id.items()
        if current_time - last_seen > TRACK_STATE_TIMEOUT_SEC
    ]

    if not expired_local_ids:
        return

    # 기존 cleanup 전에 Body Grace 진단과 NODE_TIMING event를 처리한다.
    for local_id in expired_local_ids:
        body_diag = body_grace_diag_by_local_id.get(local_id)

        if body_diag is not None:
            body_diag.finish_once(
                reason="TRACK_LOST",
                end_frame=current_frame_index,
                end_monotonic=current_time,
            )

        publish_node_timing_once(
            mqtt_client=timing_mqtt_client,
            local_track_id=local_id,
            exited_monotonic=current_time,
        )

    with identity_lock:
        for local_id in expired_local_ids:
            identity = identity_by_local_id.pop(local_id, None)

            if identity is not None:
                local_id_by_request_id.pop(identity.request_id, None)

    for local_id in expired_local_ids:
        last_seen_by_local_id.pop(local_id, None)
        previous_x_by_local_id.pop(local_id, None)
        body_candidates_by_local_id.pop(local_id, None)
        face_candidates_by_local_id.pop(local_id, None)
        body_grace_deadline_by_local_id.pop(local_id, None)
        body_grace_diag_by_local_id.pop(local_id, None)
        face_grace_deadline_by_local_id.pop(local_id, None)
        face_task_buffer.remove(local_id)

        # local track 단위 timing 임시값만 정리한다.
        # Journey session은 중복 방지/ID 변경 연속성을 위해 유지한다.
        with timing_lock:
            first_seen_at_by_local_id.pop(local_id, None)
            journey_id_by_local_track_id.pop(local_id, None)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    global latest_jpeg

    ensure_log_file()
    CAPTURE_ROOT.mkdir(parents=True, exist_ok=True)
    FACE_CAPTURE_ROOT.mkdir(parents=True, exist_ok=True)

    if not YOLO_MODEL_PATH.exists():
        raise RuntimeError(f"YOLO 모델 파일이 없습니다: {YOLO_MODEL_PATH}")

    if not REID_ENGINE_PATH.exists():
        raise RuntimeError(f"Re-ID Engine이 없습니다: {REID_ENGINE_PATH}")

    if not FACE_DETECTOR_MODEL_PATH.exists():
        raise RuntimeError(
            f"YuNet 모델 파일이 없습니다: {FACE_DETECTOR_MODEL_PATH}"
        )

    if not FACE_RECOGNIZER_MODEL_PATH.exists():
        raise RuntimeError(
            f"SFace 모델 파일이 없습니다: {FACE_RECOGNIZER_MODEL_PATH}"
        )

    if not torch.cuda.is_available():
        raise RuntimeError("Jetson GPU를 사용할 수 없습니다.")

    # --------------------------------------------------------
    # YOLO는 Main 영상 Thread에서만 사용
    # --------------------------------------------------------

    yolo_model = YOLO(str(YOLO_MODEL_PATH))

    # --------------------------------------------------------
    # MQTT
    # --------------------------------------------------------

    mqtt_publisher = MqttPublisher()
    mqtt_publisher.connect()

    response_client = start_response_client()

    # --------------------------------------------------------
    # Face Worker
    # --------------------------------------------------------

    face_task_buffer = LatestFaceTaskBuffer()
    face_result_queue: queue.Queue[FaceResult] = queue.Queue()
    face_ready = threading.Event()
    face_state: dict[str, str | None] = {"error": None}

    face_thread = threading.Thread(
        target=face_worker_loop,
        args=(
            face_task_buffer,
            face_result_queue,
            face_ready,
            face_state,
        ),
        daemon=True,
        name="camera_a_face_worker",
    )
    face_thread.start()

    if not face_ready.wait(timeout=10.0):
        response_client.loop_stop()
        response_client.disconnect()
        mqtt_publisher.disconnect()
        raise RuntimeError("Face Worker 시작 Timeout")

    if face_state.get("error"):
        response_client.loop_stop()
        response_client.disconnect()
        mqtt_publisher.disconnect()
        raise RuntimeError(
            f"Face Worker 시작 실패: {face_state['error']}"
        )

    # --------------------------------------------------------
    # Entry Worker
    # --------------------------------------------------------

    entry_job_queue: queue.Queue[EntryJob | None] = queue.Queue(
        maxsize=ENTRY_QUEUE_MAXSIZE
    )
    entry_ready = threading.Event()
    entry_state: dict[str, str | None] = {"error": None}

    entry_thread = threading.Thread(
        target=entry_worker_loop,
        args=(
            entry_job_queue,
            mqtt_publisher,
            entry_ready,
            entry_state,
        ),
        daemon=True,
        name="camera_a_entry_worker",
    )
    entry_thread.start()

    if not entry_ready.wait(timeout=30.0):
        face_task_buffer.stop()
        response_client.loop_stop()
        response_client.disconnect()
        mqtt_publisher.disconnect()
        raise RuntimeError("ENTRY Worker 시작 Timeout")

    if entry_state.get("error"):
        face_task_buffer.stop()
        response_client.loop_stop()
        response_client.disconnect()
        mqtt_publisher.disconnect()
        raise RuntimeError(
            f"ENTRY Worker 시작 실패: {entry_state['error']}"
        )

    # --------------------------------------------------------
    # Camera
    # --------------------------------------------------------

    cap = cv2.VideoCapture(CAMERA_DEVICE, cv2.CAP_V4L2)

    if not cap.isOpened():
        face_task_buffer.stop()
        entry_job_queue.put(None)
        response_client.loop_stop()
        response_client.disconnect()
        mqtt_publisher.disconnect()
        raise RuntimeError(
            f"/dev/video{CAMERA_DEVICE} 카메라를 열 수 없습니다."
        )

    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(*"MJPG"),
    )
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, CAMERA_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, CAMERA_HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, CAMERA_FPS)

    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    entry_line_x = int(frame_width * ENTRY_LINE_X_RATIO)

    previous_x_by_local_id: dict[int, int] = {}
    last_seen_by_local_id: dict[int, float] = {}
    body_candidates_by_local_id: dict[int, list[BodyCandidate]] = {}
    face_candidates_by_local_id: dict[int, list[FaceCandidate]] = {}
    body_grace_deadline_by_local_id: dict[int, float] = {}
    body_grace_diag_by_local_id: dict[int, BodyGraceDiagnostics] = {}
    face_grace_deadline_by_local_id: dict[int, float] = {}

    # 실제 Camera/YOLO main-loop 처리 frame 진행량 진단용.
    frame_progress = FrameProgress()

    frame_index = 0

    server_thread = threading.Thread(
        target=start_web_server,
        daemon=True,
        name="camera_a_web_server",
    )
    server_thread.start()

    print()
    print("============================================")
    print(" Camera A - FINAL Body + Face Entry Node")
    print("============================================")
    print("GPU              :", torch.cuda.get_device_name(0))
    print(f"카메라           : /dev/video{CAMERA_DEVICE}")
    print(f"ENTRY 방향       : {ENTRY_DIRECTION}")
    print(f"ENTRY LINE       : {ENTRY_LINE_X_RATIO}")
    print("Main Loop        : YOLO + ByteTrack + ENTRY")
    print("Face Worker      : YuNet + SFace 비동기")
    print("Entry Worker     : OSNet + Capture + MQTT 비동기")
    print(f"Body TOP-K       : {BODY_TOP_K}")
    print(f"Body Frame Gap   : {BODY_MIN_FRAME_GAP}")
    print(f"Body Grace       : {BODY_ENTRY_GRACE_SEC:.2f} sec")
    print(f"Face Threshold   : {FACE_SCORE_THRESHOLD}")
    print(f"Face 검사 간격   : {FACE_CHECK_INTERVAL_FRAMES} frames")
    print(f"Face TOP-K       : {FACE_TOP_K}")
    print(f"Face Grace       : {FACE_ENTRY_GRACE_SEC:.2f} sec")
    print(f"Body Capture     : {CAPTURE_ROOT}")
    print(f"Face Capture     : {FACE_CAPTURE_ROOT}")
    print(f"Main 응답 구독   : {TOPIC_A_ENTRY_RESPONSE}")
    print(f"Timing 발행      : {TOPIC_A_TIMING} (QoS {MQTT_QOS})")
    print("Face Embedding   : Main Server 전달 준비 완료")
    print("종료             : Ctrl + C")
    print("============================================")
    print()

    try:
        while True:
            success, frame = cap.read()

            if not success:
                print("Camera A 프레임 읽기 실패")
                time.sleep(0.05)
                continue

            frame_index += 1

            if FLIP_HORIZONTAL:
                frame = cv2.flip(frame, 1)

            frame = apply_small_brightness_adjustment(frame)

            # Face Worker에서 완료된 결과를 Main Thread 상태에 반영
            drain_face_results(
                result_queue=face_result_queue,
                face_candidates_by_local_id=face_candidates_by_local_id,
                last_seen_by_local_id=last_seen_by_local_id,
                face_grace_deadline_by_local_id=(
                    face_grace_deadline_by_local_id
                ),
            )

            results = yolo_model.track(
                source=frame,
                persist=True,
                tracker="bytetrack.yaml",
                classes=[0],
                conf=0.50,
                iou=0.50,
                end2end=False,
                device=0,
                verbose=False,
            )

            result = results[0]
            annotated_frame = frame.copy()

            draw_entry_guide(
                frame=annotated_frame,
                line_x=entry_line_x,
                frame_width=frame_width,
                frame_height=frame_height,
            )

            current_time = time.monotonic()
            frame_progress.update(
                frame_index=frame_index,
                monotonic_at=current_time,
            )

            if result.boxes is not None and result.boxes.id is not None:
                local_ids = result.boxes.id.int().cpu().tolist()
                boxes = result.boxes.xyxy.int().cpu().tolist()
                confidences = result.boxes.conf.cpu().tolist()

                for local_id, box, confidence in zip(
                    local_ids,
                    boxes,
                    confidences,
                ):
                    x1, y1, x2, y2 = box
                    current_x = (x1 + x2) // 2
                    current_y = (y1 + y2) // 2

                    # NODE_TIMING entered_at:
                    # ByteTrack local id가 화면에 처음 나타난 순간
                    register_track_first_seen(
                        local_track_id=local_id,
                        monotonic_now=current_time,
                    )

                    last_seen_by_local_id[local_id] = current_time

                    # canonical Journey 연결 뒤 session last_seen 갱신
                    update_journey_timing_last_seen(
                        local_track_id=local_id,
                        monotonic_now=current_time,
                    )

                    current_crop = extract_person_crop(
                        frame=frame,
                        box=box,
                    )

                    with identity_lock:
                        already_sent = local_id in identity_by_local_id

                    # ----------------------------------------
                    # Body TOP3 후보 갱신
                    #
                    # ENTRY 전에는 기존과 동일하게 계속 수집한다.
                    # ENTRY 후에는 BODY_ENTRY_GRACE_SEC 동안만 추가 수집한다.
                    # grace가 끝난 뒤에는 같은 Track의 Body 후보를 더 만들지 않는다.
                    # ----------------------------------------

                    body_grace_deadline = (
                        body_grace_deadline_by_local_id.get(local_id)
                    )
                    body_grace_active = (
                        body_grace_deadline is not None
                        and current_time <= body_grace_deadline
                    )

                    if not already_sent or body_grace_active:
                        crop_area = max(1, (x2 - x1) * (y2 - y1))
                        crop_score = float(crop_area) * float(confidence)

                        body_list = body_candidates_by_local_id.setdefault(
                            local_id,
                            [],
                        )
                        body_update_result = update_body_candidates(
                            candidates=body_list,
                            new_candidate=BodyCandidate(
                                image=current_crop.copy(),
                                confidence=float(confidence),
                                # 기존 quality 의미와의 호환을 위해
                                # YOLO confidence를 품질값으로 사용한다.
                                quality=float(confidence),
                                selection_score=crop_score,
                                frame_index=frame_index,
                            ),
                        )

                        if body_grace_active:
                            body_diag = (
                                body_grace_diag_by_local_id.get(local_id)
                            )

                            if body_diag is not None:
                                body_diag.record_candidate_result(
                                    body_update_result
                                )

                                if len(body_list) >= BODY_TOP_K:
                                    body_diag.finish_once(
                                        reason="TARGET_REACHED",
                                        end_frame=frame_index,
                                        end_monotonic=current_time,
                                    )

                    previous_x = previous_x_by_local_id.get(local_id)
                    crossed_now = False

                    # ==================================================
                    # 1순위: ENTRY 판정
                    # ==================================================

                    if (
                        previous_x is not None
                        and not already_sent
                        and crossed_entry_line(
                            previous_x=previous_x,
                            current_x=current_x,
                            line_x=entry_line_x,
                        )
                    ):
                        crossed_now = True

                        # BODY도 Face와 같이 공유 list를 그대로 사용한다.
                        # ENTRY 직후 BODY_ENTRY_GRACE_SEC 동안 Main 영상 loop가
                        # 같은 list에 후보를 계속 추가/교체할 수 있다.
                        body_candidates = (
                            body_candidates_by_local_id.setdefault(
                                local_id,
                                [],
                            )
                        )

                        # Track이 너무 짧아서 후보가 비어있는 예외 상황 대비
                        if not body_candidates:
                            crop_area = max(1, (x2 - x1) * (y2 - y1))
                            body_candidates.append(
                                BodyCandidate(
                                    image=current_crop.copy(),
                                    confidence=float(confidence),
                                    quality=float(confidence),
                                    selection_score=(
                                        float(crop_area)
                                        * float(confidence)
                                    ),
                                    frame_index=frame_index,
                                )
                            )

                        entry_timestamp = (
                            datetime.now()
                            .astimezone()
                            .isoformat(timespec="seconds")
                        )
                        request_id = make_request_id(local_id)

                        body_grace_start_monotonic = time.monotonic()
                        body_grace_deadline = (
                            body_grace_start_monotonic
                            + BODY_ENTRY_GRACE_SEC
                        )
                        body_grace_deadline_by_local_id[local_id] = (
                            body_grace_deadline
                        )

                        body_grace_diag = BodyGraceDiagnostics(
                            request_id=request_id,
                            local_track_id=local_id,
                            initial_count=len(body_candidates),
                            target_count=BODY_TOP_K,
                            grace_sec=BODY_ENTRY_GRACE_SEC,
                            start_frame=frame_index,
                            start_monotonic=body_grace_start_monotonic,
                        )
                        body_grace_diag_by_local_id[local_id] = (
                            body_grace_diag
                        )

                        print()
                        print("[A BODY GRACE START]")
                        print(f"request_id     : {request_id}")
                        print(f"local_track_id : {local_id}")
                        print(
                            f"initial_count  : "
                            f"{len(body_candidates)}"
                        )
                        print(f"target_count   : {BODY_TOP_K}")
                        print(
                            f"grace_sec      : "
                            f"{BODY_ENTRY_GRACE_SEC:.2f}"
                        )
                        print(f"start_frame    : {frame_index}")

                        # ENTRY 순간 이미 TOP-K라면 즉시 종료 상태로 기록한다.
                        if len(body_candidates) >= BODY_TOP_K:
                            body_grace_diag.finish_once(
                                reason="TARGET_REACHED",
                                end_frame=frame_index,
                                end_monotonic=body_grace_start_monotonic,
                            )

                        # 공유 list를 그대로 사용한다. ENTRY 직전에 이미 요청된
                        # Face 작업이 늦게 완료되면 FACE_ENTRY_GRACE_SEC 동안
                        # 이 list에 후보가 추가될 수 있다.
                        face_candidates = (
                            face_candidates_by_local_id.setdefault(
                                local_id,
                                [],
                            )
                        )
                        face_grace_deadline = (
                            time.monotonic() + FACE_ENTRY_GRACE_SEC
                        )
                        face_grace_deadline_by_local_id[local_id] = (
                            face_grace_deadline
                        )

                        pending_identity = EntryIdentity(
                            local_track_id=local_id,
                            request_id=request_id,
                            person_status="REGISTERING",
                            entry_at=entry_timestamp,
                            updated_at=entry_timestamp,
                        )

                        with identity_lock:
                            identity_by_local_id[local_id] = pending_identity
                            local_id_by_request_id[request_id] = local_id

                        # 여기서는 Face Task를 제거하지 않는다.
                        # ENTRY 직전에 이미 제출된 최신 Task가 유예시간 안에
                        # 끝날 수 있도록 살려둔다.

                        # 같은 프레임 박스에 REGISTERING이 바로 표시되도록 갱신
                        already_sent = True

                        print()
                        print("===== A ENTRY 감지 =====")
                        print(f"Local ID    : {local_id}")
                        print(f"Request ID  : {request_id}")
                        print(f"Entry Time  : {entry_timestamp}")
                        print("화면 상태   : REGISTERING...")
                        print(
                            f"Body 후보   : "
                            f"{len(body_candidates)}/{BODY_TOP_K}"
                        )
                        print(
                            f"Face 후보   : "
                            f"{len(face_candidates)}/{FACE_TOP_K}"
                        )
                        print("ENTRY 처리  : Background Worker")
                        print("========================")

                        try:
                            entry_job_queue.put_nowait(
                                EntryJob(
                                    local_track_id=local_id,
                                    request_id=request_id,
                                    timestamp=entry_timestamp,
                                    body_candidates=body_candidates,
                                    body_grace_deadline=(
                                        body_grace_deadline
                                    ),
                                    body_grace_diag=body_grace_diag,
                                    frame_progress=frame_progress,
                                    enqueued_monotonic=time.monotonic(),
                                    face_candidates=face_candidates,
                                    face_grace_deadline=(
                                        face_grace_deadline
                                    ),
                                )
                            )

                        except queue.Full:
                            with identity_lock:
                                identity = identity_by_local_id.get(local_id)
                                if identity is not None:
                                    identity.person_status = "SEND_ERROR"

                            print(
                                "[Camera A] ENTRY Queue가 가득 찼습니다."
                            )

                        # Body/Face 후보는 각각 grace 종료/Track cleanup까지 유지한다.

                    # ==================================================
                    # 2순위: Face 작업 제출
                    # Main Loop에서는 YuNet을 실행하지 않는다.
                    # ==================================================

                    if (
                        not already_sent
                        and not crossed_now
                        and frame_index % FACE_CHECK_INTERVAL_FRAMES == 0
                    ):
                        face_task_buffer.submit(
                            FaceTask(
                                local_track_id=local_id,
                                frame_index=frame_index,
                                person_crop=current_crop.copy(),
                            )
                        )

                    previous_x_by_local_id[local_id] = current_x

                    # ----------------------------------------
                    # 화면 표시
                    # ----------------------------------------

                    with identity_lock:
                        identity = identity_by_local_id.get(local_id)

                    label, box_color = identity_label_and_color(identity)

                    cv2.rectangle(
                        annotated_frame,
                        (x1, y1),
                        (x2, y2),
                        box_color,
                        2,
                    )
                    cv2.circle(
                        annotated_frame,
                        (current_x, current_y),
                        6,
                        (0, 0, 255),
                        -1,
                    )
                    cv2.putText(
                        annotated_frame,
                        label,
                        (x1, max(y1 - 10, 25)),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.70,
                        box_color,
                        2,
                    )

                    if not already_sent:
                        body_list = body_candidates_by_local_id.get(
                            local_id,
                            [],
                        )
                        face_list = face_candidates_by_local_id.get(
                            local_id,
                            [],
                        )

                        if face_list:
                            best_face_quality = max(
                                item.quality
                                for item in face_list
                            )
                            face_text = (
                                f"BODY {len(body_list)}/{BODY_TOP_K} | "
                                f"FACE {len(face_list)}/{FACE_TOP_K} "
                                f"Q {best_face_quality:.2f}"
                            )
                        else:
                            face_text = (
                                f"BODY {len(body_list)}/{BODY_TOP_K} | "
                                f"FACE 0/{FACE_TOP_K}"
                            )

                        cv2.putText(
                            annotated_frame,
                            face_text,
                            (x1, min(y2 + 22, frame_height - 10)),
                            cv2.FONT_HERSHEY_SIMPLEX,
                            0.52,
                            (255, 255, 255),
                            2,
                        )

            # ENTRY 후 Body 추가 수집 유예시간 종료 처리
            expired_body_grace_ids = [
                local_id
                for local_id, deadline
                in body_grace_deadline_by_local_id.items()
                if current_time > deadline
            ]

            for local_id in expired_body_grace_ids:
                body_diag = body_grace_diag_by_local_id.get(local_id)

                if body_diag is not None:
                    body_diag.finish_once(
                        reason="GRACE_TIMEOUT",
                        end_frame=frame_index,
                        end_monotonic=current_time,
                    )

                body_grace_deadline_by_local_id.pop(local_id, None)

                # EntryJob은 공유 list 객체를 이미 들고 있으므로
                # dict에서 제거해도 Worker snapshot에는 영향이 없다.
                body_candidates_by_local_id.pop(local_id, None)

            # ENTRY 직전 진행 중이던 Face 작업의 유예시간 종료 처리
            expired_face_grace_ids = [
                local_id
                for local_id, deadline
                in face_grace_deadline_by_local_id.items()
                if current_time > deadline
            ]

            for local_id in expired_face_grace_ids:
                face_task_buffer.remove(local_id)
                face_grace_deadline_by_local_id.pop(local_id, None)

            cleanup_track_state(
                current_time=current_time,
                current_frame_index=frame_index,
                last_seen_by_local_id=last_seen_by_local_id,
                previous_x_by_local_id=previous_x_by_local_id,
                body_candidates_by_local_id=body_candidates_by_local_id,
                face_candidates_by_local_id=face_candidates_by_local_id,
                body_grace_deadline_by_local_id=(
                    body_grace_deadline_by_local_id
                ),
                body_grace_diag_by_local_id=(
                    body_grace_diag_by_local_id
                ),
                face_grace_deadline_by_local_id=(
                    face_grace_deadline_by_local_id
                ),
                face_task_buffer=face_task_buffer,
                timing_mqtt_client=response_client,
            )

            output_frame = draw_dashboard(annotated_frame)

            encode_success, buffer = cv2.imencode(
                ".jpg",
                output_frame,
                [cv2.IMWRITE_JPEG_QUALITY, 80],
            )

            if not encode_success:
                continue

            with frame_lock:
                latest_jpeg = buffer.tobytes()

    except KeyboardInterrupt:
        print()
        print("Camera A 종료")

    finally:
        cap.release()

        face_task_buffer.stop()

        try:
            entry_job_queue.put_nowait(None)
        except queue.Full:
            pass

        response_client.loop_stop()
        response_client.disconnect()
        mqtt_publisher.disconnect()


if __name__ == "__main__":
    main()
