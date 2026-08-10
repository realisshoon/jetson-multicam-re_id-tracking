from __future__ import annotations

import csv
import json
import queue
import threading
import time
import uuid

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import paho.mqtt.client as mqtt
import torch

from ultralytics import YOLO

from src.common.model_requirements import require_model_files
from src.network.mqtt_client import (
    MQTT_BROKER_HOST,
    MQTT_BROKER_PORT,
    MQTT_QOS,
    MqttPublisher,
)
from src.reid.reid_engine import ReIDTensorRTEngine


# ============================================================
# 프로젝트 경로
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

YOLO_MODEL_PATH = (
    PROJECT_ROOT
    / "yolo26n.pt"
)

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

EVENT_LOG_PATH = (
    PROJECT_ROOT
    / "logs"
    / "node_a_entry_central.csv"
)

CAPTURE_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "captures"
    / "A"
)

FACE_CAPTURE_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "captures"
    / "A_face"
)


# ============================================================
# MQTT 설정
# ============================================================

MQTT_HOST = MQTT_BROKER_HOST
MQTT_PORT = MQTT_BROKER_PORT

TOPIC_A_ENTRY_RESPONSE = (
    "cctv/responses/a/entry"
)


# ============================================================
# 카메라
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

# right:
# 왼쪽 → 오른쪽
#
# left:
# 오른쪽 → 왼쪽

ENTRY_DIRECTION = "right"


# ============================================================
# 얼굴 자동 수집
# ============================================================

FACE_SCORE_THRESHOLD = 0.60

# 얼굴 처리는 매 프레임 하지 않음
FACE_CHECK_INTERVAL_FRAMES = 5

FACE_TOP_K = 3

FACE_MIN_FRAME_GAP = 10

FACE_MIN_SIZE_PX = 28

FACE_MIN_SHARPNESS = 15.0

FACE_MIN_FRONTAL_SCORE = 0.30

FACE_UPSCALE_FACTOR = 2.0

FACE_SAVE_PADDING_RATIO = 0.20


# ============================================================
# 추적 상태
# ============================================================

TRACK_STATE_TIMEOUT_SEC = 8.0

RECENT_RESULT_LIMIT = 4


latest_jpeg: bytes | None = None


frame_lock = threading.Lock()

identity_lock = threading.Lock()

log_lock = threading.Lock()


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

    updated_at: str = ""


@dataclass
class FaceCandidate:

    image: np.ndarray

    confidence: float

    quality: float

    sharpness: float

    area_ratio: float

    frontal_score: float

    frame_index: int


@dataclass
class EntryJob:

    local_track_id: int

    request_id: str

    timestamp: str

    reid_crop: np.ndarray

    quality: float

    face_candidates: list[FaceCandidate]


identity_by_local_id: dict[
    int,
    EntryIdentity,
] = {}


local_id_by_request_id: dict[
    str,
    int,
] = {}


logged_response_request_ids: set[
    str
] = set()


recent_results: deque[
    EntryIdentity
] = deque(
    maxlen=RECENT_RESULT_LIMIT
)


# ============================================================
# 웹 서버
# ============================================================

class ReusableThreadingHTTPServer(
    ThreadingHTTPServer
):

    allow_reuse_address = True


class StreamHandler(
    BaseHTTPRequestHandler
):

    def do_GET(
        self
    ) -> None:

        if self.path == "/":

            html = """
<!DOCTYPE html>
<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>
Camera A Entrance Tracking
</title>

<style>

body {
    margin: 0;
    background: #0d1117;
    color: white;
    text-align: center;
    font-family: Arial, sans-serif;
}

h2 {
    margin: 14px 0 8px;
}

p {
    margin: 0 0 10px;
    color: #aeb6c2;
}

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

<h2>
Camera A - Entrance Management
</h2>

<p>
Person UID / Body Re-ID / Automatic Best Face Collection
</p>

<img src="/stream">

</body>

</html>
"""

            data = html.encode(
                "utf-8"
            )

            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8",
            )

            self.send_header(
                "Content-Length",
                str(
                    len(data)
                ),
            )

            self.end_headers()

            self.wfile.write(
                data
            )

            return

        if self.path == "/stream":

            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "multipart/x-mixed-replace; boundary=frame",
            )

            self.send_header(
                "Cache-Control",
                "no-cache",
            )

            self.send_header(
                "Pragma",
                "no-cache",
            )

            self.end_headers()

            try:

                while True:

                    with frame_lock:

                        frame_data = (
                            latest_jpeg
                        )

                    if frame_data is None:

                        time.sleep(
                            0.05
                        )

                        continue

                    self.wfile.write(
                        b"--frame\r\n"
                    )

                    self.wfile.write(
                        b"Content-Type: image/jpeg\r\n"
                    )

                    self.wfile.write(
                        (
                            f"Content-Length: "
                            f"{len(frame_data)}"
                            f"\r\n\r\n"
                        ).encode()
                    )

                    self.wfile.write(
                        frame_data
                    )

                    self.wfile.write(
                        b"\r\n"
                    )

                    self.wfile.flush()

                    time.sleep(
                        0.01
                    )

            except (
                BrokenPipeError,
                ConnectionResetError,
                ConnectionAbortedError,
            ):

                pass

            return

        self.send_error(
            404
        )

    def log_message(
        self,
        format,
        *args,
    ) -> None:

        return


def start_web_server() -> None:

    server = (
        ReusableThreadingHTTPServer(
            (
                "0.0.0.0",
                SERVER_PORT,
            ),
            StreamHandler,
        )
    )

    print(
        f"Camera A 웹 서버: "
        f"http://<jetson-a-ip>:{SERVER_PORT}"
    )

    server.serve_forever()


# ============================================================
# CSV
# ============================================================

def ensure_log_file() -> None:

    EVENT_LOG_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if EVENT_LOG_PATH.exists():

        return

    with EVENT_LOG_PATH.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(
            file
        )

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

    print(
        f"CSV 파일 생성: "
        f"{EVENT_LOG_PATH}"
    )


def save_central_entry_result(
    identity: EntryIdentity,
) -> None:

    with log_lock:

        with EVENT_LOG_PATH.open(
            "a",
            newline="",
            encoding="utf-8",
        ) as file:

            writer = csv.writer(
                file
            )

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
# Body Capture
# ============================================================

def make_request_id(
    local_track_id: int,
) -> str:

    timestamp = (
        datetime.now()
        .strftime(
            "%Y%m%d_%H%M%S_%f"
        )
    )

    short_uuid = (
        uuid.uuid4()
        .hex[:8]
    )

    return (
        f"A_{timestamp}_"
        f"L{local_track_id}_"
        f"{short_uuid}"
    )


def save_entry_capture(
    crop: np.ndarray,
    request_id: str,
    timestamp: str,
) -> str:

    day_folder = (
        timestamp[:10]
        .replace(
            "-",
            "",
        )
    )

    target_dir = (
        CAPTURE_ROOT
        / day_folder
    )

    target_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    capture_path = (
        target_dir
        / f"{request_id}.jpg"
    )

    success = cv2.imwrite(
        str(
            capture_path
        ),
        crop,
        [
            cv2.IMWRITE_JPEG_QUALITY,
            95,
        ],
    )

    if not success:

        raise RuntimeError(
            f"Capture 저장 실패: "
            f"{capture_path}"
        )

    return str(
        capture_path
    )


# ============================================================
# 얼굴 품질
# ============================================================

def clamp01(
    value: float,
) -> float:

    return max(
        0.0,
        min(
            1.0,
            float(
                value
            ),
        ),
    )


def calculate_face_sharpness(
    face_image: np.ndarray,
) -> float:

    if face_image.size == 0:

        return 0.0

    gray = cv2.cvtColor(
        face_image,
        cv2.COLOR_BGR2GRAY,
    )

    return float(
        cv2.Laplacian(
            gray,
            cv2.CV_64F,
        ).var()
    )


def calculate_frontal_score(
    face: np.ndarray,
) -> float:

    try:

        eye1_x = float(
            face[4]
        )

        eye1_y = float(
            face[5]
        )

        eye2_x = float(
            face[6]
        )

        eye2_y = float(
            face[7]
        )

        nose_x = float(
            face[8]
        )

        eye_mid_x = (
            eye1_x
            + eye2_x
        ) / 2.0

        eye_distance = float(
            np.hypot(
                eye2_x
                - eye1_x,
                eye2_y
                - eye1_y,
            )
        )

        if eye_distance <= 1.0:

            return 0.0

        nose_offset = (
            abs(
                nose_x
                - eye_mid_x
            )
            / eye_distance
        )

        center_score = (
            1.0
            - min(
                nose_offset
                / 0.65,
                1.0,
            )
        )

        eye_slope = (
            abs(
                eye2_y
                - eye1_y
            )
            / eye_distance
        )

        level_score = (
            1.0
            - min(
                eye_slope
                / 0.50,
                1.0,
            )
        )

        frontal_score = (
            0.70
            * center_score
            + 0.30
            * level_score
        )

        return clamp01(
            frontal_score
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

    image_height, image_width = (
        person_crop.shape[:2]
    )

    padding_x = (
        width
        * FACE_SAVE_PADDING_RATIO
    )

    padding_y = (
        height
        * FACE_SAVE_PADDING_RATIO
    )

    x1 = max(
        0,
        int(
            x
            - padding_x
        ),
    )

    y1 = max(
        0,
        int(
            y
            - padding_y
        ),
    )

    x2 = min(
        image_width,
        int(
            x
            + width
            + padding_x
        ),
    )

    y2 = min(
        image_height,
        int(
            y
            + height
            + padding_y
        ),
    )

    if (
        x2 <= x1
        or y2 <= y1
    ):

        return None

    face_crop = person_crop[
        y1:y2,
        x1:x2,
    ]

    if face_crop.size == 0:

        return None

    return face_crop.copy()


def detect_face_candidate(
    detector,
    person_crop: np.ndarray,
    frame_index: int,
) -> FaceCandidate | None:

    if person_crop.size == 0:

        return None

    original_height, original_width = (
        person_crop.shape[:2]
    )

    if (
        original_width < 20
        or original_height < 20
    ):

        return None

    enlarged = cv2.resize(
        person_crop,
        None,
        fx=FACE_UPSCALE_FACTOR,
        fy=FACE_UPSCALE_FACTOR,
        interpolation=cv2.INTER_CUBIC,
    )

    attempts = [
        (
            person_crop,
            1.0,
        ),
        (
            enlarged,
            FACE_UPSCALE_FACTOR,
        ),
    ]

    selected_face = None

    selected_scale = 1.0

    for (
        detect_image,
        scale,
    ) in attempts:

        height, width = (
            detect_image.shape[:2]
        )

        try:

            detector.setScoreThreshold(
                FACE_SCORE_THRESHOLD
            )

            detector.setInputSize(
                (
                    width,
                    height,
                )
            )

            _, faces = (
                detector.detect(
                    detect_image
                )
            )

        except cv2.error:

            faces = None

        if (
            faces is None
            or len(faces) == 0
        ):

            continue

        selected_face = max(
            faces,
            key=lambda item: float(
                item[-1]
            ),
        )

        selected_scale = scale

        break

    if selected_face is None:

        return None

    confidence = float(
        selected_face[-1]
    )

    if (
        confidence
        < FACE_SCORE_THRESHOLD
    ):

        return None

    face_x = (
        float(
            selected_face[0]
        )
        / selected_scale
    )

    face_y = (
        float(
            selected_face[1]
        )
        / selected_scale
    )

    face_width = (
        float(
            selected_face[2]
        )
        / selected_scale
    )

    face_height = (
        float(
            selected_face[3]
        )
        / selected_scale
    )

    if (
        face_width < FACE_MIN_SIZE_PX
        or face_height < FACE_MIN_SIZE_PX
    ):

        return None

    raw_x1 = max(
        0,
        int(
            face_x
        ),
    )

    raw_y1 = max(
        0,
        int(
            face_y
        ),
    )

    raw_x2 = min(
        original_width,
        int(
            face_x
            + face_width
        ),
    )

    raw_y2 = min(
        original_height,
        int(
            face_y
            + face_height
        ),
    )

    if (
        raw_x2 <= raw_x1
        or raw_y2 <= raw_y1
    ):

        return None

    raw_face = person_crop[
        raw_y1:raw_y2,
        raw_x1:raw_x2,
    ]

    if raw_face.size == 0:

        return None

    sharpness = (
        calculate_face_sharpness(
            raw_face
        )
    )

    if (
        sharpness
        < FACE_MIN_SHARPNESS
    ):

        return None

    frontal_score = (
        calculate_frontal_score(
            selected_face
        )
    )

    if (
        frontal_score
        < FACE_MIN_FRONTAL_SCORE
    ):

        return None

    face_area = (
        face_width
        * face_height
    )

    person_area = max(
        1.0,
        float(
            original_width
            * original_height
        ),
    )

    area_ratio = (
        face_area
        / person_area
    )

    size_score = clamp01(
        area_ratio
        / 0.035
    )

    sharpness_score = (
        clamp01(
            sharpness
            / 120.0
        )
    )

    quality = (
        0.45
        * confidence
        + 0.20
        * frontal_score
        + 0.20
        * size_score
        + 0.15
        * sharpness_score
    )

    saved_face = (
        crop_face_with_padding(
            person_crop=person_crop,
            x=face_x,
            y=face_y,
            width=face_width,
            height=face_height,
        )
    )

    if saved_face is None:

        return None

    saved_height, saved_width = (
        saved_face.shape[:2]
    )

    largest_side = max(
        saved_width,
        saved_height,
    )

    if largest_side < 160:

        resize_scale = (
            160.0
            / max(
                1,
                largest_side,
            )
        )

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
        quality=float(
            quality
        ),
        sharpness=float(
            sharpness
        ),
        area_ratio=float(
            area_ratio
        ),
        frontal_score=float(
            frontal_score
        ),
        frame_index=frame_index,
    )


# ============================================================
# Face TOP3
# ============================================================

def update_face_candidates(
    candidates: list[FaceCandidate],
    new_candidate: FaceCandidate,
) -> None:

    for (
        index,
        old_candidate,
    ) in enumerate(
        candidates
    ):

        frame_gap = abs(
            new_candidate.frame_index
            - old_candidate.frame_index
        )

        if (
            frame_gap
            < FACE_MIN_FRAME_GAP
        ):

            if (
                new_candidate.quality
                > old_candidate.quality
            ):

                candidates[
                    index
                ] = new_candidate

            candidates.sort(
                key=lambda item: (
                    item.quality
                ),
                reverse=True,
            )

            del candidates[
                FACE_TOP_K:
            ]

            return

    candidates.append(
        new_candidate
    )

    candidates.sort(
        key=lambda item: (
            item.quality
        ),
        reverse=True,
    )

    del candidates[
        FACE_TOP_K:
    ]


def save_face_candidates(
    candidates: list[FaceCandidate],
    request_id: str,
    timestamp: str,
) -> list[str]:

    if not candidates:

        return []

    day_folder = (
        timestamp[:10]
        .replace(
            "-",
            "",
        )
    )

    target_dir = (
        FACE_CAPTURE_ROOT
        / day_folder
        / request_id
    )

    target_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    sorted_candidates = sorted(
        candidates,
        key=lambda item: (
            item.quality
        ),
        reverse=True,
    )

    saved_paths: list[
        str
    ] = []

    for (
        rank,
        candidate,
    ) in enumerate(
        sorted_candidates[
            :FACE_TOP_K
        ],
        start=1,
    ):

        filename = (
            f"face_{rank}"
            f"_Q{candidate.quality:.3f}"
            f"_C{candidate.confidence:.3f}"
            f"_F{candidate.frame_index}"
            f".jpg"
        )

        path = (
            target_dir
            / filename
        )

        success = cv2.imwrite(
            str(
                path
            ),
            candidate.image,
            [
                cv2.IMWRITE_JPEG_QUALITY,
                95,
            ],
        )

        if success:

            saved_paths.append(
                str(
                    path
                )
            )

        else:

            print(
                "[Camera A] "
                "Face Capture 저장 실패: "
                f"{path}"
            )

    return saved_paths


# ============================================================
# ENTRY Background Worker
# ============================================================

def entry_worker_loop(
    job_queue: queue.Queue,
    reid_engine: ReIDTensorRTEngine,
    mqtt_publisher: MqttPublisher,
) -> None:

    while True:

        job = (
            job_queue.get()
        )

        if job is None:

            job_queue.task_done()

            break

        try:

            # ----------------------------------------
            # OSNet Body embedding
            # ----------------------------------------

            embedding = (
                reid_engine.extract(
                    job.reid_crop
                )
            )

            embedding = (
                embedding
                .astype(
                    np.float32
                )
                .reshape(
                    -1
                )
            )

            if (
                embedding.size
                != 512
            ):

                raise RuntimeError(
                    "Re-ID embedding 크기 오류: "
                    f"{embedding.shape}"
                )

            embedding_norm = float(
                np.linalg.norm(
                    embedding
                )
            )

            # ----------------------------------------
            # Body 이미지 저장
            # ----------------------------------------

            capture_path = (
                save_entry_capture(
                    crop=job.reid_crop,
                    request_id=job.request_id,
                    timestamp=job.timestamp,
                )
            )

            # ----------------------------------------
            # Face TOP3 저장
            # ----------------------------------------

            face_capture_paths = (
                save_face_candidates(
                    candidates=(
                        job.face_candidates
                    ),
                    request_id=(
                        job.request_id
                    ),
                    timestamp=(
                        job.timestamp
                    ),
                )
            )

            # ----------------------------------------
            # MQTT ENTRY
            # ----------------------------------------

            embedding_list = (
                embedding.tolist()
            )

            mqtt_publisher.publish_entry(
                {
                    "request_id": (
                        job.request_id
                    ),

                    "timestamp": (
                        job.timestamp
                    ),

                    "node_id": "A",

                    "event": "ENTRY",

                    "local_track_id": (
                        job.local_track_id
                    ),

                    "next_nodes": [
                        "B",
                        "C",
                    ],

                    "reid_model": (
                        "osnet_x0_25"
                    ),

                    "embedding_dim": len(
                        embedding_list
                    ),

                    "embedding": (
                        embedding_list
                    ),

                    "quality": (
                        job.quality
                    ),

                    "capture_path": (
                        capture_path
                    ),

                    "verification_status": (
                        "AUTO_MATCHED"
                    ),
                }
            )

            print()

            print(
                "===== A ENTRY 처리 완료 ====="
            )

            print(
                f"Local ID      : "
                f"{job.local_track_id}"
            )

            print(
                f"Request ID    : "
                f"{job.request_id}"
            )

            print(
                f"Embedding Dim : "
                f"{embedding.size}"
            )

            print(
                f"Embedding Norm: "
                f"{embedding_norm:.6f}"
            )

            print(
                f"Body Capture  : "
                f"{capture_path}"
            )

            print(
                f"Face Count    : "
                f"{len(face_capture_paths)}"
                f"/{FACE_TOP_K}"
            )

            if face_capture_paths:

                print(
                    "Best Faces:"
                )

                sorted_faces = sorted(
                    job.face_candidates,
                    key=lambda item: (
                        item.quality
                    ),
                    reverse=True,
                )

                for (
                    rank,
                    face_item,
                ) in enumerate(
                    sorted_faces[
                        :FACE_TOP_K
                    ],
                    start=1,
                ):

                    print(
                        f"  #{rank} "
                        f"Q={face_item.quality:.3f} "
                        f"CONF={face_item.confidence:.3f} "
                        f"FRONT={face_item.frontal_score:.3f} "
                        f"SHARP={face_item.sharpness:.1f}"
                    )

                print(
                    "Face Folder   : "
                    f"{Path(face_capture_paths[0]).parent}"
                )

            else:

                print(
                    "Best Faces    : 없음 "
                    "(Body Re-ID만 사용)"
                )

            print(
                "=============================="
            )

        except Exception as error:

            with identity_lock:

                identity = (
                    identity_by_local_id.get(
                        job.local_track_id
                    )
                )

                if identity is not None:

                    identity.person_status = (
                        "SEND_ERROR"
                    )

            print()

            print(
                "[Camera A] "
                "ENTRY Worker 오류: "
                f"{error}"
            )

        finally:

            job_queue.task_done()


# ============================================================
# MQTT Response
# ============================================================

def safe_float(
    value: Any,
) -> float | None:

    if value is None:

        return None

    try:

        return float(
            value
        )

    except (
        TypeError,
        ValueError,
    ):

        return None


def on_response_connect(
    client: mqtt.Client,
    userdata,
    flags,
    reason_code,
    properties,
) -> None:

    # Paho MQTT v2 ReasonCode 대응
    if reason_code.is_failure:

        print(
            "Camera A 응답 MQTT 연결 실패: "
            f"{reason_code}"
        )

        return

    client.subscribe(
        TOPIC_A_ENTRY_RESPONSE,
        qos=MQTT_QOS,
    )

    print(
        "Camera A MQTT 응답 구독: "
        f"{TOPIC_A_ENTRY_RESPONSE}"
    )


def on_response_message(
    client: mqtt.Client,
    userdata,
    message: mqtt.MQTTMessage,
) -> None:

    try:

        payload = json.loads(
            message.payload.decode(
                "utf-8"
            )
        )

        if (
            payload.get(
                "event"
            )
            != "ENTRY_RESULT"
        ):

            return

        request_id = (
            payload.get(
                "request_id"
            )
        )

        local_track_id = (
            payload.get(
                "local_track_id"
            )
        )

        with identity_lock:

            if (
                local_track_id is None
                and request_id
            ):

                local_track_id = (
                    local_id_by_request_id.get(
                        str(
                            request_id
                        )
                    )
                )

            if local_track_id is None:

                print(
                    "[Camera A] "
                    "ENTRY_RESULT에 "
                    "Local ID가 없습니다."
                )

                return

            local_track_id = int(
                local_track_id
            )

            request_id = str(
                request_id
                or (
                    f"A_RESPONSE_"
                    f"L{local_track_id}"
                )
            )

            identity = (
                EntryIdentity(
                    local_track_id=(
                        local_track_id
                    ),

                    request_id=(
                        request_id
                    ),

                    person_uid=(
                        payload.get(
                            "person_uid"
                        )
                    ),

                    journey_id=(
                        payload.get(
                            "journey_id"
                        )
                    ),

                    person_status=str(
                        payload.get(
                            "person_status",
                            "UNKNOWN",
                        )
                    ).upper(),

                    visit_count=int(
                        payload.get(
                            "visit_count",
                            0,
                        )
                        or 0
                    ),

                    match_score=(
                        safe_float(
                            payload.get(
                                "person_match_score"
                            )
                        )
                    ),

                    previous_last_seen_at=(
                        payload.get(
                            "previous_last_seen_at"
                        )
                    ),

                    candidate_person_uid=(
                        payload.get(
                            "candidate_person_uid"
                        )
                    ),

                    updated_at=str(
                        payload.get(
                            "timestamp",
                            datetime.now()
                            .isoformat(
                                timespec="seconds"
                            ),
                        )
                    ),
                )
            )

            identity_by_local_id[
                local_track_id
            ] = identity

            local_id_by_request_id[
                request_id
            ] = local_track_id

            recent_results.appendleft(
                identity
            )

            should_log = (
                request_id
                not in logged_response_request_ids
            )

            if should_log:

                logged_response_request_ids.add(
                    request_id
                )

        if should_log:

            save_central_entry_result(
                identity
            )

        print()

        print(
            "===== A 중앙 ID 수신 ====="
        )

        print(
            f"Local ID     : "
            f"{identity.local_track_id}"
        )

        print(
            f"Person UID   : "
            f"{identity.person_uid}"
        )

        print(
            f"Journey ID   : "
            f"{identity.journey_id}"
        )

        print(
            f"Person 상태  : "
            f"{identity.person_status}"
        )

        print(
            f"방문 횟수    : "
            f"{identity.visit_count}"
        )

        print(
            f"Match Score  : "
            f"{identity.match_score}"
        )

        print(
            "=========================="
        )

    except Exception as error:

        print(
            "[Camera A] "
            "ENTRY_RESULT 처리 오류: "
            f"{error}"
        )


def start_response_client() -> mqtt.Client:

    client_id = (
        "camera-a-response-"
        f"{uuid.uuid4().hex[:8]}"
    )

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
    )

    client.on_connect = (
        on_response_connect
    )

    client.on_message = (
        on_response_message
    )

    client.connect(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=60,
    )

    client.loop_start()

    return client


# ============================================================
# 영상 처리
# ============================================================

def apply_small_brightness_adjustment(
    frame: np.ndarray,
) -> np.ndarray:

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

    frame_height, frame_width = (
        frame.shape[:2]
    )

    x1, y1, x2, y2 = (
        box
    )

    box_width = max(
        1,
        x2 - x1,
    )

    box_height = max(
        1,
        y2 - y1,
    )

    padding_x = int(
        box_width
        * padding_ratio
    )

    padding_y = int(
        box_height
        * padding_ratio
    )

    crop_x1 = max(
        0,
        x1 - padding_x,
    )

    crop_y1 = max(
        0,
        y1 - padding_y,
    )

    crop_x2 = min(
        frame_width,
        x2 + padding_x,
    )

    crop_y2 = min(
        frame_height,
        y2 + padding_y,
    )

    crop = frame[
        crop_y1:crop_y2,
        crop_x1:crop_x2,
    ]

    if crop.size == 0:

        raise RuntimeError(
            "사람 Crop이 비어 있습니다: "
            f"{box}"
        )

    return crop.copy()


def crossed_entry_line(
    previous_x: int,
    current_x: int,
    line_x: int,
) -> bool:

    if (
        ENTRY_DIRECTION
        == "right"
    ):

        return (
            previous_x
            < line_x
            <= current_x
        )

    if (
        ENTRY_DIRECTION
        == "left"
    ):

        return (
            previous_x
            > line_x
            >= current_x
        )

    raise ValueError(
        "ENTRY_DIRECTION은 "
        "'right' 또는 'left'여야 합니다."
    )


# ============================================================
# 화면
# ============================================================

def draw_entry_guide(
    frame: np.ndarray,
    line_x: int,
    frame_width: int,
    frame_height: int,
) -> None:

    overlay = (
        frame.copy()
    )

    if (
        ENTRY_DIRECTION
        == "right"
    ):

        start_left = 0

        start_right = (
            line_x
        )

        entry_left = (
            line_x
        )

        entry_right = (
            frame_width
        )

        arrow_start = (
            max(
                40,
                line_x - 220,
            ),
            90,
        )

        arrow_end = (
            min(
                frame_width - 40,
                line_x + 220,
            ),
            90,
        )

        direction_text = (
            "MOVE RIGHT >>>"
        )

    elif (
        ENTRY_DIRECTION
        == "left"
    ):

        start_left = (
            line_x
        )

        start_right = (
            frame_width
        )

        entry_left = 0

        entry_right = (
            line_x
        )

        arrow_start = (
            min(
                frame_width - 40,
                line_x + 220,
            ),
            90,
        )

        arrow_end = (
            max(
                40,
                line_x - 220,
            ),
            90,
        )

        direction_text = (
            "<<< MOVE LEFT"
        )

    else:

        raise ValueError(
            "ENTRY_DIRECTION은 "
            "'right' 또는 'left'여야 합니다."
        )

    cv2.rectangle(
        overlay,
        (
            start_left,
            0,
        ),
        (
            start_right,
            frame_height,
        ),
        (
            0,
            140,
            255,
        ),
        -1,
    )

    cv2.rectangle(
        overlay,
        (
            entry_left,
            0,
        ),
        (
            entry_right,
            frame_height,
        ),
        (
            0,
            180,
            0,
        ),
        -1,
    )

    cv2.addWeighted(
        overlay,
        0.14,
        frame,
        0.86,
        0,
        frame,
    )

    cv2.line(
        frame,
        (
            line_x,
            0,
        ),
        (
            line_x,
            frame_height,
        ),
        (
            0,
            255,
            255,
        ),
        4,
    )

    cv2.arrowedLine(
        frame,
        arrow_start,
        arrow_end,
        (
            0,
            255,
            255,
        ),
        5,
        tipLength=0.12,
    )

    start_center_x = (
        start_left
        + start_right
    ) // 2

    entry_center_x = (
        entry_left
        + entry_right
    ) // 2

    cv2.putText(
        frame,
        "START SIDE",
        (
            max(
                10,
                start_center_x - 95,
            ),
            frame_height - 35,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (
            255,
            255,
            255,
        ),
        2,
    )

    cv2.putText(
        frame,
        "ENTRY SIDE",
        (
            max(
                10,
                entry_center_x - 95,
            ),
            frame_height - 35,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (
            255,
            255,
            255,
        ),
        2,
    )

    cv2.putText(
        frame,
        direction_text,
        (
            20,
            40,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.9,
        (
            0,
            255,
            255,
        ),
        3,
    )

    cv2.putText(
        frame,
        "CROSS THIS LINE",
        (
            max(
                10,
                line_x - 120,
            ),
            135,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.65,
        (
            0,
            255,
            255,
        ),
        2,
    )


def identity_label_and_color(
    identity: EntryIdentity | None,
) -> tuple[
    str,
    tuple[int, int, int],
]:

    if identity is None:

        return (
            "STRANGER",
            (
                0,
                165,
                255,
            ),
        )

    status = (
        identity.person_status
        .upper()
    )

    if (
        status
        == "REGISTERING"
    ):

        return (
            "REGISTERING...",
            (
                255,
                220,
                0,
            ),
        )

    if (
        status
        == "RETURNING"
    ):

        return (
            f"{identity.person_uid} | RETURNING",
            (
                0,
                255,
                0,
            ),
        )

    if (
        status
        == "NEW"
    ):

        return (
            f"{identity.person_uid} | NEW",
            (
                0,
                230,
                255,
            ),
        )

    if (
        status
        == "REVIEW_REQUIRED"
    ):

        return (
            f"{identity.person_uid} | REVIEW",
            (
                0,
                80,
                255,
            ),
        )

    return (
        (
            f"{identity.person_uid or 'UNKNOWN'}"
            f" | {status}"
        ),
        (
            255,
            255,
            0,
        ),
    )


def draw_dashboard(
    frame: np.ndarray,
) -> np.ndarray:

    frame_height, frame_width = (
        frame.shape[:2]
    )

    dashboard = np.zeros(
        (
            frame_height
            + DASHBOARD_HEIGHT,
            frame_width,
            3,
        ),
        dtype=np.uint8,
    )

    dashboard[
        :frame_height
    ] = frame

    panel_top = (
        frame_height
    )

    cv2.rectangle(
        dashboard,
        (
            0,
            panel_top,
        ),
        (
            frame_width,
            frame_height
            + DASHBOARD_HEIGHT,
        ),
        (
            20,
            24,
            31,
        ),
        -1,
    )

    cv2.line(
        dashboard,
        (
            0,
            panel_top,
        ),
        (
            frame_width,
            panel_top,
        ),
        (
            90,
            105,
            125,
        ),
        1,
    )

    cv2.putText(
        dashboard,
        "CAMERA A | ENTRY MANAGEMENT",
        (
            20,
            panel_top + 30,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.72,
        (
            255,
            255,
            255,
        ),
        2,
    )

    cv2.putText(
        dashboard,
        (
            "ROUTE: [A] -> B/C -> D "
            "| BEST FACE AUTO"
        ),
        (
            20,
            panel_top + 60,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.64,
        (
            0,
            220,
            255,
        ),
        2,
    )

    with identity_lock:

        results_snapshot = list(
            recent_results
        )

    if not results_snapshot:

        cv2.putText(
            dashboard,
            "Waiting for entry line crossing...",
            (
                20,
                panel_top + 100,
            ),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.62,
            (
                170,
                180,
                195,
            ),
            1,
        )

        return dashboard

    for (
        index,
        identity,
    ) in enumerate(
        results_snapshot[
            :3
        ]
    ):

        y = (
            panel_top
            + 95
            + index * 25
        )

        score_text = (
            f"{identity.match_score:.3f}"
            if identity.match_score
            is not None
            else "-"
        )

        line = (
            f"{identity.person_uid}"
            f" | {identity.person_status}"
            f" | {identity.journey_id}"
            f" | Visits {identity.visit_count}"
            f" | Score {score_text}"
        )

        cv2.putText(
            dashboard,
            line,
            (
                20,
                y,
            ),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.54,
            (
                220,
                226,
                235,
            ),
            1,
        )

    return dashboard


# ============================================================
# Track 정리
# ============================================================

def cleanup_track_state(
    current_time: float,

    last_seen_by_local_id:
        dict[
            int,
            float,
        ],

    previous_x_by_local_id:
        dict[
            int,
            int,
        ],

    best_crop_by_local_id:
        dict[
            int,
            np.ndarray,
        ],

    best_crop_score_by_local_id:
        dict[
            int,
            float,
        ],

    best_confidence_by_local_id:
        dict[
            int,
            float,
        ],

    face_candidates_by_local_id:
        dict[
            int,
            list[
                FaceCandidate
            ],
        ],
) -> None:

    expired_local_ids = [

        local_id

        for (
            local_id,
            last_seen,
        )

        in (
            last_seen_by_local_id
            .items()
        )

        if (
            current_time
            - last_seen
            > TRACK_STATE_TIMEOUT_SEC
        )
    ]

    if not expired_local_ids:

        return

    with identity_lock:

        for local_id in (
            expired_local_ids
        ):

            identity = (
                identity_by_local_id.pop(
                    local_id,
                    None,
                )
            )

            if identity is not None:

                local_id_by_request_id.pop(
                    identity.request_id,
                    None,
                )

    for local_id in (
        expired_local_ids
    ):

        last_seen_by_local_id.pop(
            local_id,
            None,
        )

        previous_x_by_local_id.pop(
            local_id,
            None,
        )

        best_crop_by_local_id.pop(
            local_id,
            None,
        )

        best_crop_score_by_local_id.pop(
            local_id,
            None,
        )

        best_confidence_by_local_id.pop(
            local_id,
            None,
        )

        face_candidates_by_local_id.pop(
            local_id,
            None,
        )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    global latest_jpeg

    require_model_files(
        "Camera A",
        {
            "YOLO": YOLO_MODEL_PATH,
            "Re-ID TensorRT engine": REID_ENGINE_PATH,
            "YuNet face detector": FACE_DETECTOR_MODEL_PATH,
        },
    )

    ensure_log_file()

    CAPTURE_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    FACE_CAPTURE_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # 파일 확인
    # --------------------------------------------------------

    if (
        not FACE_DETECTOR_MODEL_PATH.exists()
    ):

        raise RuntimeError(
            "YuNet 모델 파일이 없습니다: "
            f"{FACE_DETECTOR_MODEL_PATH}"
        )

    if (
        not torch.cuda.is_available()
    ):

        raise RuntimeError(
            "Jetson GPU를 사용할 수 없습니다."
        )

    # --------------------------------------------------------
    # YOLO
    # --------------------------------------------------------

    yolo_model = YOLO(
        str(
            YOLO_MODEL_PATH
        )
    )

    # --------------------------------------------------------
    # OSNet
    # --------------------------------------------------------

    reid_engine = (
        ReIDTensorRTEngine(
            REID_ENGINE_PATH
        )
    )

    # --------------------------------------------------------
    # YuNet
    # --------------------------------------------------------

    face_detector = (
        cv2.FaceDetectorYN.create(
            str(
                FACE_DETECTOR_MODEL_PATH
            ),
            "",
            (
                320,
                320,
            ),
            FACE_SCORE_THRESHOLD,
            0.3,
            5000,
        )
    )

    # --------------------------------------------------------
    # MQTT
    # --------------------------------------------------------

    mqtt_publisher = (
        MqttPublisher(client_id="camera-a")
    )

    mqtt_publisher.connect()

    response_client = (
        start_response_client()
    )

    # --------------------------------------------------------
    # ENTRY Worker
    # --------------------------------------------------------

    entry_job_queue = (
        queue.Queue(
            maxsize=16
        )
    )

    entry_worker_thread = (
        threading.Thread(
            target=entry_worker_loop,

            args=(
                entry_job_queue,
                reid_engine,
                mqtt_publisher,
            ),

            daemon=True,
        )
    )

    entry_worker_thread.start()

    # --------------------------------------------------------
    # Camera
    # --------------------------------------------------------

    cap = cv2.VideoCapture(
        CAMERA_DEVICE,
        cv2.CAP_V4L2,
    )

    if (
        not cap.isOpened()
    ):

        response_client.loop_stop()

        response_client.disconnect()

        mqtt_publisher.disconnect()

        raise RuntimeError(
            f"/dev/video{CAMERA_DEVICE} "
            "카메라를 열 수 없습니다."
        )

    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(
            *"MJPG"
        ),
    )

    cap.set(
        cv2.CAP_PROP_FRAME_WIDTH,
        CAMERA_WIDTH,
    )

    cap.set(
        cv2.CAP_PROP_FRAME_HEIGHT,
        CAMERA_HEIGHT,
    )

    cap.set(
        cv2.CAP_PROP_FPS,
        CAMERA_FPS,
    )

    frame_width = int(
        cap.get(
            cv2.CAP_PROP_FRAME_WIDTH
        )
    )

    frame_height = int(
        cap.get(
            cv2.CAP_PROP_FRAME_HEIGHT
        )
    )

    entry_line_x = int(
        frame_width
        * ENTRY_LINE_X_RATIO
    )

    # --------------------------------------------------------
    # Track별 저장
    # --------------------------------------------------------

    previous_x_by_local_id: dict[
        int,
        int,
    ] = {}

    last_seen_by_local_id: dict[
        int,
        float,
    ] = {}

    best_crop_by_local_id: dict[
        int,
        np.ndarray,
    ] = {}

    best_crop_score_by_local_id: dict[
        int,
        float,
    ] = {}

    best_confidence_by_local_id: dict[
        int,
        float,
    ] = {}

    face_candidates_by_local_id: dict[
        int,
        list[
            FaceCandidate
        ],
    ] = {}

    frame_index = 0

    # --------------------------------------------------------
    # Web
    # --------------------------------------------------------

    server_thread = threading.Thread(
        target=start_web_server,
        daemon=True,
    )

    server_thread.start()

    print()

    print(
        "=================================="
    )

    print(
        " Camera A Body + Best Face 시작"
    )

    print(
        "=================================="
    )

    print(
        "GPU            :",
        torch.cuda.get_device_name(
            0
        ),
    )

    print(
        f"카메라         : "
        f"/dev/video{CAMERA_DEVICE}"
    )

    print(
        f"ENTRY 방향     : "
        f"{ENTRY_DIRECTION}"
    )

    print(
        f"ENTRY LINE     : "
        f"{ENTRY_LINE_X_RATIO}"
    )

    print(
        "Body Re-ID     : "
        "OSNet x0.25 TensorRT"
    )

    print(
        "Face Detector  : YuNet"
    )

    print(
        f"Face Threshold : "
        f"{FACE_SCORE_THRESHOLD}"
    )

    print(
        f"Face 검사 간격 : "
        f"{FACE_CHECK_INTERVAL_FRAMES} frames"
    )

    print(
        f"Face TOP-K     : "
        f"{FACE_TOP_K}"
    )

    print(
        f"Body Capture   : "
        f"{CAPTURE_ROOT}"
    )

    print(
        f"Face Capture   : "
        f"{FACE_CAPTURE_ROOT}"
    )

    print(
        f"Main 응답 구독 : "
        f"{TOPIC_A_ENTRY_RESPONSE}"
    )

    print(
        f"밝기 보정      : "
        f"alpha={IMAGE_CONTRAST_ALPHA}, "
        f"beta={IMAGE_BRIGHTNESS_BETA}"
    )

    print(
        "ENTRY 우선     : ON"
    )

    print(
        "OSNet/MQTT     : Background Worker"
    )

    print(
        "Face는 아직 Person UID "
        "판정에 사용하지 않음"
    )

    print(
        "종료            : Ctrl + C"
    )

    print(
        "=================================="
    )

    print()

    try:

        while True:

            success, frame = (
                cap.read()
            )

            if not success:

                print(
                    "Camera A 프레임 읽기 실패"
                )

                time.sleep(
                    0.05
                )

                continue

            frame_index += 1

            if FLIP_HORIZONTAL:

                frame = cv2.flip(
                    frame,
                    1,
                )

            frame = (
                apply_small_brightness_adjustment(
                    frame
                )
            )

            # =================================================
            # YOLO + ByteTrack
            # =================================================

            results = (
                yolo_model.track(
                    source=frame,

                    persist=True,

                    tracker=(
                        "bytetrack.yaml"
                    ),

                    classes=[
                        0
                    ],

                    conf=0.50,

                    iou=0.50,

                    end2end=False,

                    device=0,

                    verbose=False,
                )
            )

            result = (
                results[0]
            )

            annotated_frame = (
                frame.copy()
            )

            draw_entry_guide(
                frame=(
                    annotated_frame
                ),

                line_x=(
                    entry_line_x
                ),

                frame_width=(
                    frame_width
                ),

                frame_height=(
                    frame_height
                ),
            )

            current_time = (
                time.monotonic()
            )

            # =================================================
            # Person Track
            # =================================================

            if (
                result.boxes is not None
                and result.boxes.id
                is not None
            ):

                local_ids = (
                    result.boxes.id
                    .int()
                    .cpu()
                    .tolist()
                )

                boxes = (
                    result.boxes.xyxy
                    .int()
                    .cpu()
                    .tolist()
                )

                confidences = (
                    result.boxes.conf
                    .cpu()
                    .tolist()
                )

                for (
                    local_id,
                    box,
                    confidence,
                ) in zip(
                    local_ids,
                    boxes,
                    confidences,
                ):

                    x1, y1, x2, y2 = (
                        box
                    )

                    current_x = (
                        x1 + x2
                    ) // 2

                    current_y = (
                        y1 + y2
                    ) // 2

                    last_seen_by_local_id[
                        local_id
                    ] = current_time

                    # -----------------------------------------
                    # 사람 Crop
                    # -----------------------------------------

                    current_crop = (
                        extract_person_crop(
                            frame=frame,
                            box=box,
                        )
                    )

                    # -----------------------------------------
                    # Body Best Crop
                    # -----------------------------------------

                    crop_area = max(
                        1,
                        (
                            x2
                            - x1
                        )
                        * (
                            y2
                            - y1
                        ),
                    )

                    crop_score = (
                        float(
                            crop_area
                        )
                        * float(
                            confidence
                        )
                    )

                    previous_best_score = (
                        best_crop_score_by_local_id.get(
                            local_id,
                            -1.0,
                        )
                    )

                    if (
                        crop_score
                        > previous_best_score
                    ):

                        best_crop_by_local_id[
                            local_id
                        ] = current_crop

                        best_crop_score_by_local_id[
                            local_id
                        ] = crop_score

                        best_confidence_by_local_id[
                            local_id
                        ] = float(
                            confidence
                        )

                    # -----------------------------------------
                    # 이미 ENTRY 처리됐는지
                    # -----------------------------------------

                    with identity_lock:

                        already_sent = (
                            local_id
                            in identity_by_local_id
                        )

                    previous_x = (
                        previous_x_by_local_id.get(
                            local_id
                        )
                    )

                    crossed_now = False

                    # =========================================
                    # ★ ENTRY가 FACE보다 우선
                    # =========================================

                    if (
                        previous_x
                        is not None

                        and not already_sent

                        and crossed_entry_line(
                            previous_x=(
                                previous_x
                            ),

                            current_x=(
                                current_x
                            ),

                            line_x=(
                                entry_line_x
                            ),
                        )
                    ):

                        crossed_now = True

                        # -------------------------------------
                        # Best Body
                        # -------------------------------------

                        reid_crop = (
                            best_crop_by_local_id.get(
                                local_id,
                                current_crop,
                            )
                        ).copy()

                        # -------------------------------------
                        # ID / Time
                        # -------------------------------------

                        entry_timestamp = (
                            datetime.now()
                            .astimezone()
                            .isoformat(
                                timespec="seconds"
                            )
                        )

                        request_id = (
                            make_request_id(
                                local_id
                            )
                        )

                        quality = float(
                            best_confidence_by_local_id.get(
                                local_id,
                                confidence,
                            )
                        )

                        # -------------------------------------
                        # 지금까지 모은 Best Face
                        # -------------------------------------

                        face_candidates = list(
                            face_candidates_by_local_id.get(
                                local_id,
                                [],
                            )
                        )

                        # =====================================
                        # ★ 즉시 REGISTERING 상태 등록
                        # =====================================

                        pending_identity = (
                            EntryIdentity(
                                local_track_id=(
                                    local_id
                                ),

                                request_id=(
                                    request_id
                                ),

                                person_status=(
                                    "REGISTERING"
                                ),

                                updated_at=(
                                    entry_timestamp
                                ),
                            )
                        )

                        with identity_lock:

                            identity_by_local_id[
                                local_id
                            ] = pending_identity

                            local_id_by_request_id[
                                request_id
                            ] = local_id

                        # 이 프레임에서도
                        # 즉시 REGISTERING 표시
                        already_sent = True

                        print()

                        print(
                            "===== A ENTRY 감지 ====="
                        )

                        print(
                            f"Local ID    : "
                            f"{local_id}"
                        )

                        print(
                            f"Request ID  : "
                            f"{request_id}"
                        )

                        print(
                            "화면 상태   : "
                            "REGISTERING..."
                        )

                        print(
                            f"Face 후보   : "
                            f"{len(face_candidates)}"
                            f"/{FACE_TOP_K}"
                        )

                        print(
                            "OSNet/MQTT  : "
                            "Background 처리"
                        )

                        print(
                            "========================"
                        )

                        # =====================================
                        # 무거운 작업은 Worker
                        # =====================================

                        try:

                            entry_job_queue.put_nowait(
                                EntryJob(
                                    local_track_id=(
                                        local_id
                                    ),

                                    request_id=(
                                        request_id
                                    ),

                                    timestamp=(
                                        entry_timestamp
                                    ),

                                    reid_crop=(
                                        reid_crop
                                    ),

                                    quality=(
                                        quality
                                    ),

                                    face_candidates=(
                                        face_candidates
                                    ),
                                )
                            )

                        except queue.Full:

                            with identity_lock:

                                identity = (
                                    identity_by_local_id.get(
                                        local_id
                                    )
                                )

                                if (
                                    identity
                                    is not None
                                ):

                                    identity.person_status = (
                                        "SEND_ERROR"
                                    )

                            print(
                                "[Camera A] "
                                "ENTRY Queue가 가득 찼습니다."
                            )

                        # -------------------------------------
                        # ENTRY 후 임시 메모리 제거
                        # -------------------------------------

                        best_crop_by_local_id.pop(
                            local_id,
                            None,
                        )

                        best_crop_score_by_local_id.pop(
                            local_id,
                            None,
                        )

                        best_confidence_by_local_id.pop(
                            local_id,
                            None,
                        )

                        face_candidates_by_local_id.pop(
                            local_id,
                            None,
                        )

                    # =========================================
                    # Face 검사는 ENTRY 검사 다음
                    # =========================================

                    if (
                        not already_sent

                        and not crossed_now

                        and (
                            frame_index
                            % FACE_CHECK_INTERVAL_FRAMES
                            == 0
                        )
                    ):

                        face_candidate = (
                            detect_face_candidate(
                                detector=(
                                    face_detector
                                ),

                                person_crop=(
                                    current_crop
                                ),

                                frame_index=(
                                    frame_index
                                ),
                            )
                        )

                        if (
                            face_candidate
                            is not None
                        ):

                            face_list = (
                                face_candidates_by_local_id
                                .setdefault(
                                    local_id,
                                    [],
                                )
                            )

                            update_face_candidates(
                                candidates=(
                                    face_list
                                ),

                                new_candidate=(
                                    face_candidate
                                ),
                            )

                    # -----------------------------------------
                    # 위치 저장
                    # -----------------------------------------

                    previous_x_by_local_id[
                        local_id
                    ] = current_x

                    # -----------------------------------------
                    # 화면 ID
                    # -----------------------------------------

                    with identity_lock:

                        identity = (
                            identity_by_local_id.get(
                                local_id
                            )
                        )

                    label, box_color = (
                        identity_label_and_color(
                            identity
                        )
                    )

                    cv2.rectangle(
                        annotated_frame,

                        (
                            x1,
                            y1,
                        ),

                        (
                            x2,
                            y2,
                        ),

                        box_color,

                        2,
                    )

                    cv2.circle(
                        annotated_frame,

                        (
                            current_x,
                            current_y,
                        ),

                        6,

                        (
                            0,
                            0,
                            255,
                        ),

                        -1,
                    )

                    cv2.putText(
                        annotated_frame,

                        label,

                        (
                            x1,

                            max(
                                y1 - 10,
                                25,
                            ),
                        ),

                        cv2.FONT_HERSHEY_SIMPLEX,

                        0.70,

                        box_color,

                        2,
                    )

                    # -----------------------------------------
                    # ENTRY 전 얼굴 후보 개수
                    # -----------------------------------------

                    if (
                        not already_sent
                    ):

                        face_list = (
                            face_candidates_by_local_id.get(
                                local_id,
                                [],
                            )
                        )

                        if face_list:

                            best_face_quality = max(
                                item.quality

                                for item

                                in face_list
                            )

                            face_text = (
                                f"FACE "
                                f"{len(face_list)}"
                                f"/{FACE_TOP_K} "
                                f"Q "
                                f"{best_face_quality:.2f}"
                            )

                        else:

                            face_text = (
                                f"FACE 0/"
                                f"{FACE_TOP_K}"
                            )

                        cv2.putText(
                            annotated_frame,

                            face_text,

                            (
                                x1,

                                min(
                                    y2 + 22,
                                    frame_height - 10,
                                ),
                            ),

                            cv2.FONT_HERSHEY_SIMPLEX,

                            0.52,

                            (
                                255,
                                255,
                                255,
                            ),

                            2,
                        )

            # =================================================
            # Track Cleanup
            # =================================================

            cleanup_track_state(
                current_time=(
                    current_time
                ),

                last_seen_by_local_id=(
                    last_seen_by_local_id
                ),

                previous_x_by_local_id=(
                    previous_x_by_local_id
                ),

                best_crop_by_local_id=(
                    best_crop_by_local_id
                ),

                best_crop_score_by_local_id=(
                    best_crop_score_by_local_id
                ),

                best_confidence_by_local_id=(
                    best_confidence_by_local_id
                ),

                face_candidates_by_local_id=(
                    face_candidates_by_local_id
                ),
            )

            # =================================================
            # Web Stream
            # =================================================

            output_frame = (
                draw_dashboard(
                    annotated_frame
                )
            )

            (
                encode_success,
                buffer,
            ) = cv2.imencode(
                ".jpg",

                output_frame,

                [
                    cv2.IMWRITE_JPEG_QUALITY,
                    80,
                ],
            )

            if (
                not encode_success
            ):

                continue

            with frame_lock:

                latest_jpeg = (
                    buffer.tobytes()
                )

    except KeyboardInterrupt:

        print()

        print(
            "Camera A 종료"
        )

    finally:

        cap.release()

        response_client.loop_stop()

        response_client.disconnect()

        mqtt_publisher.disconnect()


if __name__ == "__main__":

    main()
