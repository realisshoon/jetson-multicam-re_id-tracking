from __future__ import annotations

import csv
import json
import threading
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import cv2
import numpy as np
import paho.mqtt.client as mqtt
import torch
from ultralytics import YOLO

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

CANDIDATE_LOG_PATH = (
    PROJECT_ROOT
    / "logs"
    / "node_b_candidates.csv"
)

MATCH_LOG_PATH = (
    PROJECT_ROOT
    / "logs"
    / "node_b_matches.csv"
)


# ============================================================
# Camera B 설정
# ============================================================

# 두 번째 C270 실제 영상 장치
CAMERA_DEVICE = 2

CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720
CAMERA_FPS = 30

# A는 8000, B는 8001
SERVER_PORT = 8001

# 거울 모드
FLIP_HORIZONTAL = True


# ============================================================
# MQTT 설정
# ============================================================

# 현재 A와 B가 같은 Jetson이므로 localhost
MQTT_BROKER_HOST = "localhost"
MQTT_BROKER_PORT = 1883
MQTT_ENTRY_TOPIC = "cctv/entry"


# ============================================================
# 최초 Re-ID 매칭 설정
# ============================================================

# Global ID 최초 부여 기준
MATCH_THRESHOLD = 0.70

# 1등과 2등 후보 점수 차이
MATCH_MARGIN = 0.05

# 같은 후보가 연속으로 이 횟수만큼 선택돼야 확정
MATCH_CONFIRMATIONS = 3

# Re-ID 실행 간격
REID_INTERVAL_FRAMES = 3

# 최근 embedding 평균 개수
REID_HISTORY_SIZE = 5


# ============================================================
# 매칭 후 본인 재검증 설정
# ============================================================

# 이미 ID가 붙은 사람을 유지할 최소 유사도
VERIFY_THRESHOLD = 0.55

# 이 횟수 연속 재검증 실패 시 ID 해제
VERIFY_FAILURE_LIMIT = 2


# ============================================================
# 시간 및 Track 관리 설정
# ============================================================

# A에서 받은 후보 유효시간
CANDIDATE_TIMEOUT_SECONDS = 300.0

# B에 등장한 뒤 매칭되지 않으면 이상 판정
ANOMALY_DELAY_SECONDS = 2.0

# 이 프레임 동안 Track이 안 보이면 완전히 제거
TRACK_LOST_GRACE_FRAMES = 20


# ============================================================
# 공유 데이터
# ============================================================

latest_jpeg: bytes | None = None

frame_lock = threading.Lock()
candidate_lock = threading.Lock()

# A에서 전달받은 Re-ID 후보
candidates: dict[str, dict[str, Any]] = {}


# ============================================================
# Embedding 처리
# ============================================================

def normalize_embedding(
    embedding: np.ndarray,
) -> np.ndarray:
    embedding = np.asarray(
        embedding,
        dtype=np.float32,
    ).reshape(-1)

    if embedding.size != 512:
        raise ValueError(
            f"Embedding 크기가 512가 아닙니다: "
            f"{embedding.shape}"
        )

    if not np.all(np.isfinite(embedding)):
        raise ValueError(
            "Embedding에 NaN 또는 Inf가 있습니다."
        )

    norm = float(np.linalg.norm(embedding))

    if norm <= 1e-12:
        raise ValueError(
            "Embedding Norm이 0입니다."
        )

    return embedding / norm


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


def average_embeddings(
    history: deque[np.ndarray],
) -> np.ndarray:
    stacked = np.stack(
        list(history),
        axis=0,
    )

    average = np.mean(
        stacked,
        axis=0,
    )

    return normalize_embedding(average)


def parse_timestamp(
    timestamp: str,
) -> float:
    try:
        return datetime.fromisoformat(
            timestamp
        ).timestamp()

    except (ValueError, TypeError):
        return time.time()


# ============================================================
# 웹 영상 스트리밍
# ============================================================

class ReusableThreadingHTTPServer(
    ThreadingHTTPServer
):
    allow_reuse_address = True


class StreamHandler(BaseHTTPRequestHandler):

    def do_GET(self) -> None:
        if self.path == "/":
            html = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>Camera B Re-ID Tracking</title>

    <style>
        body {
            margin: 0;
            background: #111;
            color: white;
            text-align: center;
            font-family: Arial, sans-serif;
        }

        h2 {
            margin: 15px;
        }

        img {
            width: 95%;
            max-width: 1280px;
            border: 2px solid white;
        }
    </style>
</head>

<body>
    <h2>Camera B - Re-ID Passage Tracking</h2>
    <img src="/stream">
</body>
</html>
"""
            data = html.encode("utf-8")

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8",
            )
            self.send_header(
                "Content-Length",
                str(len(data)),
            )
            self.end_headers()

            self.wfile.write(data)
            return

        if self.path == "/stream":
            self.send_response(200)

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
                        frame_data = latest_jpeg

                    if frame_data is None:
                        time.sleep(0.05)
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
                            f"{len(frame_data)}\r\n\r\n"
                        ).encode()
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

        self.send_error(404)

    def log_message(
        self,
        format,
        *args,
    ) -> None:
        return


def start_web_server() -> None:
    server = ReusableThreadingHTTPServer(
        ("0.0.0.0", SERVER_PORT),
        StreamHandler,
    )

    print(
        f"Camera B 웹 서버: "
        f"http://10.10.20.56:{SERVER_PORT}"
    )

    server.serve_forever()


# ============================================================
# CSV 로그
# ============================================================

def ensure_log_files() -> None:
    CANDIDATE_LOG_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if not CANDIDATE_LOG_PATH.exists():
        with CANDIDATE_LOG_PATH.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:
            writer = csv.writer(file)

            writer.writerow(
                [
                    "received_at",
                    "entry_timestamp",
                    "global_person_id",
                    "source_local_track_id",
                    "status",
                ]
            )

    if not MATCH_LOG_PATH.exists():
        with MATCH_LOG_PATH.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file:
            writer = csv.writer(file)

            writer.writerow(
                [
                    "matched_at",
                    "node_id",
                    "b_local_track_id",
                    "global_person_id",
                    "similarity",
                    "status",
                ]
            )


def save_candidate_log(
    candidate: dict[str, Any],
) -> None:
    with CANDIDATE_LOG_PATH.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                candidate["received_at"],
                candidate["entry_timestamp"],
                candidate["global_person_id"],
                candidate["source_local_track_id"],
                candidate["status"],
            ]
        )


def save_match_log(
    local_track_id: int,
    global_person_id: str,
    similarity: float,
) -> None:
    matched_at = datetime.now().isoformat(
        timespec="seconds"
    )

    with MATCH_LOG_PATH.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                matched_at,
                "B",
                local_track_id,
                global_person_id,
                f"{similarity:.6f}",
                "MATCHED",
            ]
        )


# ============================================================
# 후보 등록 및 상태 관리
# ============================================================

def save_candidate(
    payload: dict[str, Any],
) -> None:
    global_person_id = payload.get(
        "global_person_id"
    )

    raw_embedding = payload.get(
        "embedding"
    )

    if not global_person_id:
        print(
            "Global ID가 없는 MQTT 메시지입니다."
        )
        return

    if not isinstance(raw_embedding, list):
        print(
            f"{global_person_id}: "
            f"Embedding이 없습니다."
        )
        return

    try:
        embedding = normalize_embedding(
            np.asarray(
                raw_embedding,
                dtype=np.float32,
            )
        )

    except ValueError as error:
        print(
            f"{global_person_id}: "
            f"Embedding 오류: {error}"
        )
        return

    received_at = datetime.now().isoformat(
        timespec="seconds"
    )

    entry_timestamp = payload.get(
        "timestamp",
        "",
    )

    candidate = {
        "received_at": received_at,
        "entry_timestamp": entry_timestamp,
        "entry_epoch": parse_timestamp(
            entry_timestamp
        ),
        "global_person_id": global_person_id,
        "source_local_track_id": payload.get(
            "local_track_id",
            "",
        ),
        "embedding": embedding,
        "status": "PENDING",
        "matched_b_local_id": None,
        "match_score": None,
    }

    with candidate_lock:
        if global_person_id in candidates:
            print(
                f"[B 중복 후보 무시] "
                f"{global_person_id}"
            )
            return

        candidates[
            global_person_id
        ] = candidate

    save_candidate_log(candidate)

    print()
    print("===== B Re-ID 후보 수신 =====")
    print(
        f"Global ID      : "
        f"{global_person_id}"
    )
    print(
        f"Embedding Dim  : "
        f"{embedding.size}"
    )
    print(
        f"Embedding Norm : "
        f"{np.linalg.norm(embedding):.6f}"
    )
    print("Status         : PENDING")
    print("=============================")


def cleanup_expired_candidates() -> None:
    now = time.time()

    with candidate_lock:
        for candidate in candidates.values():
            if candidate["status"] != "PENDING":
                continue

            elapsed = (
                now
                - candidate["entry_epoch"]
            )

            if elapsed > CANDIDATE_TIMEOUT_SECONDS:
                candidate["status"] = "EXPIRED"


def get_pending_candidates() -> list[
    dict[str, Any]
]:
    cleanup_expired_candidates()

    with candidate_lock:
        return [
            {
                **candidate,
                "embedding": candidate[
                    "embedding"
                ].copy(),
            }
            for candidate in candidates.values()
            if candidate["status"] == "PENDING"
        ]


def get_candidate_embedding(
    global_person_id: str,
) -> np.ndarray | None:
    with candidate_lock:
        candidate = candidates.get(
            global_person_id
        )

        if candidate is None:
            return None

        return candidate[
            "embedding"
        ].copy()


def mark_candidate_matched(
    global_person_id: str,
    b_local_track_id: int,
    similarity: float,
) -> bool:
    with candidate_lock:
        candidate = candidates.get(
            global_person_id
        )

        if candidate is None:
            return False

        if candidate["status"] != "PENDING":
            return False

        candidate["status"] = "MATCHED"

        candidate[
            "matched_b_local_id"
        ] = b_local_track_id

        candidate["match_score"] = similarity

        return True


def release_candidate(
    global_person_id: str,
    b_local_track_id: int,
    reason: str,
) -> None:
    """
    B의 Track과 Global ID 연결을 해제한다.

    후보 유효시간이 남아 있으면 PENDING으로 복구하고,
    시간이 초과됐으면 EXPIRED로 처리한다.
    """

    with candidate_lock:
        candidate = candidates.get(
            global_person_id
        )

        if candidate is None:
            return

        matched_local_id = candidate.get(
            "matched_b_local_id"
        )

        if (
            matched_local_id is not None
            and matched_local_id
            != b_local_track_id
        ):
            return

        elapsed = (
            time.time()
            - candidate["entry_epoch"]
        )

        if elapsed <= CANDIDATE_TIMEOUT_SECONDS:
            candidate["status"] = "PENDING"
        else:
            candidate["status"] = "EXPIRED"

        candidate["matched_b_local_id"] = None
        candidate["match_score"] = None

    print()
    print("===== B Global ID 연결 해제 =====")
    print(f"Global ID : {global_person_id}")
    print(f"B Local ID: {b_local_track_id}")
    print(f"Reason    : {reason}")
    print("새 사람은 STRANGER부터 재판정")
    print("================================")


def get_candidate_panel_snapshot() -> list[
    dict[str, Any]
]:
    cleanup_expired_candidates()

    with candidate_lock:
        return [
            {
                "global_person_id": candidate[
                    "global_person_id"
                ],
                "status": candidate["status"],
                "match_score": candidate[
                    "match_score"
                ],
            }
            for candidate in candidates.values()
        ]


# ============================================================
# MQTT
# ============================================================

def on_connect(
    client: mqtt.Client,
    userdata,
    flags,
    reason_code,
    properties,
) -> None:
    if reason_code != 0:
        print(
            f"Camera B MQTT 연결 실패: "
            f"{reason_code}"
        )
        return

    print(
        f"Camera B MQTT 연결 완료: "
        f"{MQTT_BROKER_HOST}:"
        f"{MQTT_BROKER_PORT}"
    )

    client.subscribe(
        MQTT_ENTRY_TOPIC,
        qos=1,
    )

    print(
        f"Camera B MQTT 구독: "
        f"{MQTT_ENTRY_TOPIC}"
    )


def on_message(
    client: mqtt.Client,
    userdata,
    message: mqtt.MQTTMessage,
) -> None:
    try:
        payload = json.loads(
            message.payload.decode("utf-8")
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        print(
            f"잘못된 MQTT 메시지: {error}"
        )
        return

    if payload.get("event") != "ENTRY":
        return

    next_nodes = payload.get(
        "next_nodes",
        [],
    )

    if "B" not in next_nodes:
        return

    save_candidate(payload)


def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="camera_b_reid_verify",
    )

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(
        MQTT_BROKER_HOST,
        MQTT_BROKER_PORT,
        keepalive=60,
    )

    client.loop_start()

    return client


# ============================================================
# 사람 Crop
# ============================================================

def extract_person_crop(
    frame: np.ndarray,
    box: list[int],
    padding_ratio: float = 0.04,
) -> np.ndarray:
    frame_height, frame_width = (
        frame.shape[:2]
    )

    x1, y1, x2, y2 = box

    box_width = max(
        1,
        x2 - x1,
    )

    box_height = max(
        1,
        y2 - y1,
    )

    padding_x = int(
        box_width * padding_ratio
    )

    padding_y = int(
        box_height * padding_ratio
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
            f"사람 Crop이 비어 있습니다: "
            f"{box}"
        )

    return crop.copy()


# ============================================================
# PENDING 후보 비교
# ============================================================

def find_best_candidate(
    b_embedding: np.ndarray,
) -> tuple[
    str | None,
    float,
    float,
]:
    pending_candidates = (
        get_pending_candidates()
    )

    if not pending_candidates:
        return None, -1.0, -1.0

    scores: list[
        tuple[str, float]
    ] = []

    for candidate in pending_candidates:
        score = cosine_similarity(
            b_embedding,
            candidate["embedding"],
        )

        scores.append(
            (
                candidate[
                    "global_person_id"
                ],
                score,
            )
        )

    scores.sort(
        key=lambda item: item[1],
        reverse=True,
    )

    best_global_id = scores[0][0]
    best_score = scores[0][1]

    if len(scores) >= 2:
        second_score = scores[1][1]
    else:
        second_score = -1.0

    return (
        best_global_id,
        best_score,
        second_score,
    )


# ============================================================
# 화면 우측 후보 패널
# ============================================================

def draw_candidate_panel(
    frame: np.ndarray,
    frame_width: int,
) -> None:
    snapshot = (
        get_candidate_panel_snapshot()
    )

    panel_width = 350

    panel_left = max(
        0,
        frame_width - panel_width,
    )

    overlay = frame.copy()

    cv2.rectangle(
        overlay,
        (panel_left, 0),
        (frame_width, 290),
        (0, 0, 0),
        -1,
    )

    cv2.addWeighted(
        overlay,
        0.70,
        frame,
        0.30,
        0,
        frame,
    )

    pending_count = sum(
        candidate["status"] == "PENDING"
        for candidate in snapshot
    )

    cv2.putText(
        frame,
        "A RE-ID CANDIDATES",
        (panel_left + 12, 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.62,
        (0, 255, 255),
        2,
    )

    cv2.putText(
        frame,
        f"Pending: {pending_count}",
        (panel_left + 12, 62),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.60,
        (255, 255, 255),
        2,
    )

    text_y = 98

    for candidate in snapshot[-6:]:
        global_id = candidate[
            "global_person_id"
        ]

        status = candidate["status"]

        score = candidate[
            "match_score"
        ]

        if status == "MATCHED":
            color = (0, 255, 0)

            score_text = (
                f" {score:.2f}"
                if score is not None
                else ""
            )

        elif status == "EXPIRED":
            color = (128, 128, 128)
            score_text = ""

        else:
            color = (0, 255, 255)
            score_text = ""

        cv2.putText(
            frame,
            (
                f"{global_id} "
                f"{status}{score_text}"
            ),
            (panel_left + 12, text_y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.53,
            color,
            2,
        )

        text_y += 30


# ============================================================
# 메인
# ============================================================

def main() -> None:
    global latest_jpeg

    ensure_log_files()

    if not torch.cuda.is_available():
        raise RuntimeError(
            "Jetson GPU를 사용할 수 없습니다."
        )

    yolo_model = YOLO(
        str(YOLO_MODEL_PATH)
    )

    reid_engine = ReIDTensorRTEngine(
        REID_ENGINE_PATH
    )

    cap = cv2.VideoCapture(
        CAMERA_DEVICE,
        cv2.CAP_V4L2,
    )

    if not cap.isOpened():
        raise RuntimeError(
            f"/dev/video{CAMERA_DEVICE} "
            f"카메라를 열 수 없습니다."
        )

    cap.set(
        cv2.CAP_PROP_FOURCC,
        cv2.VideoWriter_fourcc(*"MJPG"),
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

    mqtt_client: mqtt.Client | None = None

    # Track 최초 발견 시간
    first_seen_by_local_id: dict[
        int,
        float,
    ] = {}

    # Track별 최근 embedding
    embedding_history_by_local_id: dict[
        int,
        deque[np.ndarray],
    ] = {}

    # 최초 매칭 중인 Global ID
    tentative_global_id_by_local_id: dict[
        int,
        str,
    ] = {}

    tentative_count_by_local_id: dict[
        int,
        int,
    ] = {}

    # 매칭 전 최근 최고 점수
    best_score_by_local_id: dict[
        int,
        float,
    ] = {}

    # 확정된 B Local ID → Global ID
    global_id_by_local_id: dict[
        int,
        str,
    ] = {}

    match_score_by_local_id: dict[
        int,
        float,
    ] = {}

    # 매칭 후 재검증 점수
    verify_score_by_local_id: dict[
        int,
        float,
    ] = {}

    # 매칭 후 재검증 연속 실패 횟수
    verify_failure_by_local_id: dict[
        int,
        int,
    ] = {}

    # Track 마지막 발견 프레임
    last_seen_frame_by_local_id: dict[
        int,
        int,
    ] = {}

    frame_index = 0

    try:
        mqtt_client = create_mqtt_client()

        server_thread = threading.Thread(
            target=start_web_server,
            daemon=True,
        )

        server_thread.start()

        print(
            "GPU:",
            torch.cuda.get_device_name(0),
        )

        print("Camera B Re-ID 시작")
        print(
            f"카메라         : "
            f"/dev/video{CAMERA_DEVICE}"
        )
        print(
            f"웹 포트        : "
            f"{SERVER_PORT}"
        )
        print(
            f"최초 매칭 기준 : "
            f"{MATCH_THRESHOLD:.2f}"
        )
        print(
            f"ID 유지 기준   : "
            f"{VERIFY_THRESHOLD:.2f}"
        )
        print(
            f"재검증 실패    : "
            f"{VERIFY_FAILURE_LIMIT}회 시 해제"
        )
        print(
            f"Track 제거     : "
            f"{TRACK_LOST_GRACE_FRAMES}프레임"
        )
        print("새 사람은 STRANGER부터 시작")
        print("종료: Ctrl + C")

        while True:
            success, frame = cap.read()

            if not success:
                print(
                    "Camera B 프레임 읽기 실패"
                )
                time.sleep(0.05)
                continue

            frame_index += 1

            if FLIP_HORIZONTAL:
                frame = cv2.flip(
                    frame,
                    1,
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

            cv2.putText(
                annotated_frame,
                "CAMERA B - RE-ID VERIFY",
                (20, 35),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.82,
                (0, 255, 255),
                2,
            )

            cv2.putText(
                annotated_frame,
                (
                    f"MATCH {MATCH_THRESHOLD:.2f} "
                    f"/ VERIFY {VERIFY_THRESHOLD:.2f}"
                ),
                (20, 70),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.62,
                (255, 255, 255),
                2,
            )

            if (
                result.boxes is not None
                and result.boxes.id is not None
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

                for local_id, box in zip(
                    local_ids,
                    boxes,
                ):
                    last_seen_frame_by_local_id[
                        local_id
                    ] = frame_index

                    first_seen_by_local_id.setdefault(
                        local_id,
                        time.time(),
                    )

                    x1, y1, x2, y2 = box

                    should_run_reid = (
                        (
                            frame_index
                            + local_id
                        )
                        % REID_INTERVAL_FRAMES
                        == 0
                    )

                    if should_run_reid:
                        try:
                            person_crop = (
                                extract_person_crop(
                                    frame=frame,
                                    box=box,
                                )
                            )

                            embedding = (
                                reid_engine.extract(
                                    person_crop
                                )
                            )

                            embedding = (
                                normalize_embedding(
                                    embedding
                                )
                            )

                            history = (
                                embedding_history_by_local_id
                                .setdefault(
                                    local_id,
                                    deque(
                                        maxlen=(
                                            REID_HISTORY_SIZE
                                        )
                                    ),
                                )
                            )

                            history.append(embedding)

                            averaged_embedding = (
                                average_embeddings(
                                    history
                                )
                            )

                            matched_global_id = (
                                global_id_by_local_id.get(
                                    local_id
                                )
                            )

                            # ====================================
                            # 이미 Global ID가 있는 Track 재검증
                            # ====================================

                            if matched_global_id is not None:
                                reference_embedding = (
                                    get_candidate_embedding(
                                        matched_global_id
                                    )
                                )

                                if reference_embedding is None:
                                    release_candidate(
                                        global_person_id=(
                                            matched_global_id
                                        ),
                                        b_local_track_id=(
                                            local_id
                                        ),
                                        reason=(
                                            "REFERENCE_NOT_FOUND"
                                        ),
                                    )

                                    global_id_by_local_id.pop(
                                        local_id,
                                        None,
                                    )

                                    match_score_by_local_id.pop(
                                        local_id,
                                        None,
                                    )

                                    verify_failure_by_local_id.pop(
                                        local_id,
                                        None,
                                    )

                                    verify_score_by_local_id.pop(
                                        local_id,
                                        None,
                                    )

                                    first_seen_by_local_id[
                                        local_id
                                    ] = time.time()

                                else:
                                    verify_score = (
                                        cosine_similarity(
                                            averaged_embedding,
                                            reference_embedding,
                                        )
                                    )

                                    verify_score_by_local_id[
                                        local_id
                                    ] = verify_score

                                    if (
                                        verify_score
                                        >= VERIFY_THRESHOLD
                                    ):
                                        verify_failure_by_local_id[
                                            local_id
                                        ] = 0

                                    else:
                                        failure_count = (
                                            verify_failure_by_local_id
                                            .get(
                                                local_id,
                                                0,
                                            )
                                            + 1
                                        )

                                        verify_failure_by_local_id[
                                            local_id
                                        ] = failure_count

                                        print(
                                            f"[B 재검증 실패] "
                                            f"Local={local_id}, "
                                            f"Global={matched_global_id}, "
                                            f"Score={verify_score:.3f}, "
                                            f"Count="
                                            f"{failure_count}/"
                                            f"{VERIFY_FAILURE_LIMIT}"
                                        )

                                        if (
                                            failure_count
                                            >= VERIFY_FAILURE_LIMIT
                                        ):
                                            release_candidate(
                                                global_person_id=(
                                                    matched_global_id
                                                ),
                                                b_local_track_id=(
                                                    local_id
                                                ),
                                                reason=(
                                                    "REID_VERIFY_FAILED"
                                                ),
                                            )

                                            global_id_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            match_score_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            verify_failure_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            verify_score_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            tentative_global_id_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            tentative_count_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            best_score_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            embedding_history_by_local_id[
                                                local_id
                                            ].clear()

                                            first_seen_by_local_id[
                                                local_id
                                            ] = time.time()

                            # ====================================
                            # Global ID가 없는 Track 최초 매칭
                            # ====================================

                            else:
                                (
                                    best_global_id,
                                    best_score,
                                    second_score,
                                ) = find_best_candidate(
                                    averaged_embedding
                                )

                                best_score_by_local_id[
                                    local_id
                                ] = best_score

                                if best_global_id is not None:
                                    score_ok = (
                                        best_score
                                        >= MATCH_THRESHOLD
                                    )

                                    margin_ok = (
                                        second_score < 0
                                        or (
                                            best_score
                                            - second_score
                                        )
                                        >= MATCH_MARGIN
                                    )

                                    if score_ok and margin_ok:
                                        previous_candidate = (
                                            tentative_global_id_by_local_id
                                            .get(local_id)
                                        )

                                        if (
                                            previous_candidate
                                            == best_global_id
                                        ):
                                            tentative_count_by_local_id[
                                                local_id
                                            ] = (
                                                tentative_count_by_local_id
                                                .get(
                                                    local_id,
                                                    0,
                                                )
                                                + 1
                                            )

                                        else:
                                            tentative_global_id_by_local_id[
                                                local_id
                                            ] = best_global_id

                                            tentative_count_by_local_id[
                                                local_id
                                            ] = 1

                                        confirmation_count = (
                                            tentative_count_by_local_id[
                                                local_id
                                            ]
                                        )

                                        if (
                                            confirmation_count
                                            >= MATCH_CONFIRMATIONS
                                        ):
                                            matched = (
                                                mark_candidate_matched(
                                                    global_person_id=(
                                                        best_global_id
                                                    ),
                                                    b_local_track_id=(
                                                        local_id
                                                    ),
                                                    similarity=(
                                                        best_score
                                                    ),
                                                )
                                            )

                                            if matched:
                                                global_id_by_local_id[
                                                    local_id
                                                ] = best_global_id

                                                match_score_by_local_id[
                                                    local_id
                                                ] = best_score

                                                verify_failure_by_local_id[
                                                    local_id
                                                ] = 0

                                                verify_score_by_local_id[
                                                    local_id
                                                ] = best_score

                                                save_match_log(
                                                    local_track_id=(
                                                        local_id
                                                    ),
                                                    global_person_id=(
                                                        best_global_id
                                                    ),
                                                    similarity=(
                                                        best_score
                                                    ),
                                                )

                                                print()
                                                print(
                                                    "===== B Re-ID 매칭 성공 ====="
                                                )
                                                print(
                                                    f"B Local ID : "
                                                    f"{local_id}"
                                                )
                                                print(
                                                    f"Global ID  : "
                                                    f"{best_global_id}"
                                                )
                                                print(
                                                    f"Similarity : "
                                                    f"{best_score:.6f}"
                                                )
                                                print(
                                                    "지속 재검증 : 활성화"
                                                )
                                                print(
                                                    "============================="
                                                )

                                            tentative_global_id_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                            tentative_count_by_local_id.pop(
                                                local_id,
                                                None,
                                            )

                                    else:
                                        tentative_global_id_by_local_id.pop(
                                            local_id,
                                            None,
                                        )

                                        tentative_count_by_local_id.pop(
                                            local_id,
                                            None,
                                        )

                                else:
                                    tentative_global_id_by_local_id.pop(
                                        local_id,
                                        None,
                                    )

                                    tentative_count_by_local_id.pop(
                                        local_id,
                                        None,
                                    )

                        except Exception as error:
                            print(
                                f"[B Re-ID 오류] "
                                f"Local ID {local_id}: "
                                f"{error}"
                            )

                    # ============================================
                    # 화면 상태 표시
                    # ============================================

                    global_id = (
                        global_id_by_local_id.get(
                            local_id
                        )
                    )

                    if global_id is not None:
                        verify_failure = (
                            verify_failure_by_local_id.get(
                                local_id,
                                0,
                            )
                        )

                        verify_score = (
                            verify_score_by_local_id.get(
                                local_id,
                                0.0,
                            )
                        )

                        if verify_failure > 0:
                            label = (
                                f"VERIFYING: {global_id}"
                            )

                            sub_label = (
                                f"VERIFY {verify_score:.2f} "
                                f"FAIL {verify_failure}/"
                                f"{VERIFY_FAILURE_LIMIT}"
                            )

                            box_color = (
                                0,
                                255,
                                255,
                            )

                        else:
                            label = (
                                f"ID: {global_id}"
                            )

                            sub_label = (
                                f"VERIFIED "
                                f"{verify_score:.2f}"
                            )

                            box_color = (
                                0,
                                255,
                                0,
                            )

                    else:
                        elapsed = (
                            time.time()
                            - first_seen_by_local_id[
                                local_id
                            ]
                        )

                        tentative_global_id = (
                            tentative_global_id_by_local_id
                            .get(local_id)
                        )

                        tentative_count = (
                            tentative_count_by_local_id
                            .get(
                                local_id,
                                0,
                            )
                        )

                        best_score = (
                            best_score_by_local_id.get(
                                local_id,
                                -1.0,
                            )
                        )

                        if tentative_global_id is not None:
                            label = (
                                f"MATCHING: "
                                f"{tentative_global_id}"
                            )

                            sub_label = (
                                f"{best_score:.2f} "
                                f"{tentative_count}/"
                                f"{MATCH_CONFIRMATIONS}"
                            )

                            box_color = (
                                0,
                                255,
                                255,
                            )

                        elif (
                            elapsed
                            >= ANOMALY_DELAY_SECONDS
                        ):
                            label = (
                                "ANOMALY: STRANGER"
                            )

                            if best_score >= 0:
                                sub_label = (
                                    f"BEST SCORE "
                                    f"{best_score:.2f}"
                                )
                            else:
                                sub_label = (
                                    "NO A ENTRY CANDIDATE"
                                )

                            box_color = (
                                0,
                                0,
                                255,
                            )

                        else:
                            label = "STRANGER"

                            sub_label = (
                                "CHECKING RE-ID"
                            )

                            box_color = (
                                0,
                                165,
                                255,
                            )

                    cv2.rectangle(
                        annotated_frame,
                        (x1, y1),
                        (x2, y2),
                        box_color,
                        3,
                    )

                    cv2.putText(
                        annotated_frame,
                        label,
                        (
                            x1,
                            max(
                                y1 - 35,
                                25,
                            ),
                        ),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.68,
                        box_color,
                        2,
                    )

                    cv2.putText(
                        annotated_frame,
                        sub_label,
                        (
                            x1,
                            max(
                                y1 - 10,
                                50,
                            ),
                        ),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.53,
                        box_color,
                        2,
                    )

            # ================================================
            # 사라진 Track 상태 제거
            # ================================================

            stale_local_ids = [
                local_id
                for local_id, last_frame
                in last_seen_frame_by_local_id.items()
                if (
                    frame_index
                    - last_frame
                    > TRACK_LOST_GRACE_FRAMES
                )
            ]

            for local_id in stale_local_ids:
                global_id = (
                    global_id_by_local_id.get(
                        local_id
                    )
                )

                if global_id is not None:
                    release_candidate(
                        global_person_id=global_id,
                        b_local_track_id=local_id,
                        reason="TRACK_LOST",
                    )

                first_seen_by_local_id.pop(
                    local_id,
                    None,
                )

                embedding_history_by_local_id.pop(
                    local_id,
                    None,
                )

                tentative_global_id_by_local_id.pop(
                    local_id,
                    None,
                )

                tentative_count_by_local_id.pop(
                    local_id,
                    None,
                )

                best_score_by_local_id.pop(
                    local_id,
                    None,
                )

                global_id_by_local_id.pop(
                    local_id,
                    None,
                )

                match_score_by_local_id.pop(
                    local_id,
                    None,
                )

                verify_score_by_local_id.pop(
                    local_id,
                    None,
                )

                verify_failure_by_local_id.pop(
                    local_id,
                    None,
                )

                last_seen_frame_by_local_id.pop(
                    local_id,
                    None,
                )

            draw_candidate_panel(
                frame=annotated_frame,
                frame_width=frame_width,
            )

            encode_success, buffer = (
                cv2.imencode(
                    ".jpg",
                    annotated_frame,
                    [
                        cv2.IMWRITE_JPEG_QUALITY,
                        80,
                    ],
                )
            )

            if not encode_success:
                continue

            with frame_lock:
                latest_jpeg = (
                    buffer.tobytes()
                )

    except KeyboardInterrupt:
        print()
        print("Camera B Re-ID 종료")

    finally:
        cap.release()

        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()

            print(
                "Camera B MQTT 연결 종료"
            )


if __name__ == "__main__":
    main()