from __future__ import annotations

import csv
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import cv2
import numpy as np
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

EVENT_LOG_PATH = (
    PROJECT_ROOT
    / "logs"
    / "node_a_entry.csv"
)


# ============================================================
# 카메라 및 웹 서버 설정
# ============================================================

CAMERA_DEVICE = 0

CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720
CAMERA_FPS = 30

SERVER_PORT = 8000

FLIP_HORIZONTAL = True


# ============================================================
# ENTRY LINE 설정
# ============================================================

# 0.50 = 화면 중앙
ENTRY_LINE_X_RATIO = 0.50

# right = 왼쪽 START SIDE → 오른쪽 ENTRY SIDE
# left  = 오른쪽 START SIDE → 왼쪽 ENTRY SIDE
ENTRY_DIRECTION = "right"


# ============================================================
# 스트리밍 공유 데이터
# ============================================================

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()


# ============================================================
# 웹 스트리밍
# ============================================================

class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class StreamHandler(BaseHTTPRequestHandler):

    def do_GET(self) -> None:
        if self.path == "/":
            html = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>Camera A Entrance Tracking</title>

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
    <h2>Camera A - Entrance Tracking</h2>
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

                    self.wfile.write(b"--frame\r\n")
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

    def log_message(self, format, *args) -> None:
        return


def start_web_server() -> None:
    server = ReusableThreadingHTTPServer(
        ("0.0.0.0", SERVER_PORT),
        StreamHandler,
    )

    print(
        f"Camera A 웹 서버: "
        f"http://10.10.20.56:{SERVER_PORT}"
    )

    server.serve_forever()


# ============================================================
# CSV 관리
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
        writer = csv.writer(file)

        writer.writerow(
            [
                "timestamp",
                "node_id",
                "event",
                "local_track_id",
                "global_person_id",
            ]
        )

    print(
        f"CSV 파일 생성: {EVENT_LOG_PATH}"
    )


def get_next_global_number() -> int:
    maximum_number = 0

    if not EVENT_LOG_PATH.exists():
        return 1

    with EVENT_LOG_PATH.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            global_id = row.get(
                "global_person_id",
                "",
            )

            if not global_id.startswith("G"):
                continue

            try:
                number = int(global_id[1:])

                maximum_number = max(
                    maximum_number,
                    number,
                )

            except ValueError:
                continue

    return maximum_number + 1


def save_entry_event(
    local_track_id: int,
    global_person_id: str,
) -> str:
    timestamp = datetime.now().isoformat(
        timespec="seconds"
    )

    with EVENT_LOG_PATH.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                timestamp,
                "A",
                "ENTRY",
                local_track_id,
                global_person_id,
            ]
        )

    return timestamp


# ============================================================
# 사람 Crop 생성
# ============================================================

def extract_person_crop(
    frame: np.ndarray,
    box: list[int],
    padding_ratio: float = 0.04,
) -> np.ndarray:
    """
    YOLO 바운딩 박스에서 사람 이미지를 자른다.
    박스 가장자리에 약간의 여백을 추가한다.
    """

    frame_height, frame_width = frame.shape[:2]

    x1, y1, x2, y2 = box

    box_width = max(1, x2 - x1)
    box_height = max(1, y2 - y1)

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
            f"사람 Crop이 비어 있습니다: {box}"
        )

    return crop.copy()


# ============================================================
# ENTRY LINE 통과 판단
# ============================================================

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
        "ENTRY_DIRECTION은 "
        "'right' 또는 'left'여야 합니다."
    )


# ============================================================
# ENTRY 안내 화면
# ============================================================

def draw_entry_guide(
    frame: np.ndarray,
    line_x: int,
    frame_width: int,
    frame_height: int,
) -> None:
    overlay = frame.copy()

    if ENTRY_DIRECTION == "right":
        start_left = 0
        start_right = line_x

        entry_left = line_x
        entry_right = frame_width

        arrow_start = (
            max(40, line_x - 220),
            90,
        )

        arrow_end = (
            min(frame_width - 40, line_x + 220),
            90,
        )

        direction_text = "MOVE RIGHT >>>"

    elif ENTRY_DIRECTION == "left":
        start_left = line_x
        start_right = frame_width

        entry_left = 0
        entry_right = line_x

        arrow_start = (
            min(frame_width - 40, line_x + 220),
            90,
        )

        arrow_end = (
            max(40, line_x - 220),
            90,
        )

        direction_text = "<<< MOVE LEFT"

    else:
        raise ValueError(
            "ENTRY_DIRECTION은 "
            "'right' 또는 'left'여야 합니다."
        )

    # START SIDE
    cv2.rectangle(
        overlay,
        (start_left, 0),
        (start_right, frame_height),
        (0, 140, 255),
        -1,
    )

    # ENTRY SIDE
    cv2.rectangle(
        overlay,
        (entry_left, 0),
        (entry_right, frame_height),
        (0, 180, 0),
        -1,
    )

    cv2.addWeighted(
        overlay,
        0.16,
        frame,
        0.84,
        0,
        frame,
    )

    # ENTRY LINE
    cv2.line(
        frame,
        (line_x, 0),
        (line_x, frame_height),
        (0, 255, 255),
        4,
    )

    # 방향 화살표
    cv2.arrowedLine(
        frame,
        arrow_start,
        arrow_end,
        (0, 255, 255),
        5,
        tipLength=0.12,
    )

    start_center_x = (
        start_left + start_right
    ) // 2

    entry_center_x = (
        entry_left + entry_right
    ) // 2

    cv2.putText(
        frame,
        "START SIDE",
        (
            max(10, start_center_x - 95),
            frame_height - 35,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (255, 255, 255),
        2,
    )

    cv2.putText(
        frame,
        "ENTRY SIDE",
        (
            max(10, entry_center_x - 95),
            frame_height - 35,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (255, 255, 255),
        2,
    )

    cv2.putText(
        frame,
        direction_text,
        (20, 40),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.9,
        (0, 255, 255),
        3,
    )

    cv2.putText(
        frame,
        "CROSS THIS LINE",
        (
            max(10, line_x - 120),
            135,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.65,
        (0, 255, 255),
        2,
    )


# ============================================================
# 메인
# ============================================================

def main() -> None:
    global latest_jpeg

    ensure_log_file()

    next_global_number = (
        get_next_global_number()
    )

    if not torch.cuda.is_available():
        raise RuntimeError(
            "Jetson GPU를 사용할 수 없습니다."
        )

    # YOLO 모델
    yolo_model = YOLO(
        str(YOLO_MODEL_PATH)
    )

    # Re-ID TensorRT 엔진
    reid_engine = ReIDTensorRTEngine(
        REID_ENGINE_PATH
    )

    # MQTT 연결
    mqtt_publisher = MqttPublisher()
    mqtt_publisher.connect()

    # Camera A
    cap = cv2.VideoCapture(
        CAMERA_DEVICE,
        cv2.CAP_V4L2,
    )

    if not cap.isOpened():
        mqtt_publisher.disconnect()

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

    frame_height = int(
        cap.get(
            cv2.CAP_PROP_FRAME_HEIGHT
        )
    )

    entry_line_x = int(
        frame_width
        * ENTRY_LINE_X_RATIO
    )

    # Local ID별 이전 X 위치
    previous_x_by_local_id: dict[
        int,
        int,
    ] = {}

    # Local ID → Global ID
    global_id_by_local_id: dict[
        int,
        str,
    ] = {}

    # Local ID별 가장 품질이 좋은 사람 Crop
    best_crop_by_local_id: dict[
        int,
        np.ndarray,
    ] = {}

    best_crop_score_by_local_id: dict[
        int,
        float,
    ] = {}

    server_thread = threading.Thread(
        target=start_web_server,
        daemon=True,
    )

    server_thread.start()

    print(
        "GPU:",
        torch.cuda.get_device_name(0),
    )
    print("Camera A 시작")
    print(
        f"카메라: /dev/video{CAMERA_DEVICE}"
    )
    print(
        f"ENTRY 방향: {ENTRY_DIRECTION}"
    )
    print(
        f"ENTRY LINE: {ENTRY_LINE_X_RATIO}"
    )
    print("Re-ID: TensorRT FP16")
    print("MQTT embedding 전송: 활성화")
    print("종료: Ctrl + C")

    try:
        while True:
            success, frame = cap.read()

            if not success:
                print(
                    "Camera A 프레임 읽기 실패"
                )
                time.sleep(0.05)
                continue

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

            draw_entry_guide(
                frame=annotated_frame,
                line_x=entry_line_x,
                frame_width=frame_width,
                frame_height=frame_height,
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

                confidences = (
                    result.boxes.conf
                    .cpu()
                    .tolist()
                )

                for local_id, box, confidence in zip(
                    local_ids,
                    boxes,
                    confidences,
                ):
                    x1, y1, x2, y2 = box

                    current_x = (
                        x1 + x2
                    ) // 2

                    current_y = (
                        y1 + y2
                    ) // 2

                    # 현재 사람 Crop
                    current_crop = extract_person_crop(
                        frame=frame,
                        box=box,
                    )

                    # 큰 박스이면서 신뢰도가 높은 Crop을 보관
                    crop_area = max(
                        1,
                        (x2 - x1) * (y2 - y1),
                    )

                    crop_score = (
                        float(crop_area)
                        * float(confidence)
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

                    previous_x = (
                        previous_x_by_local_id.get(
                            local_id
                        )
                    )

                    # ENTRY LINE 통과
                    if (
                        previous_x is not None
                        and local_id
                        not in global_id_by_local_id
                        and crossed_entry_line(
                            previous_x=previous_x,
                            current_x=current_x,
                            line_x=entry_line_x,
                        )
                    ):
                        # 현재까지 가장 품질 좋은 Crop 사용
                        reid_crop = (
                            best_crop_by_local_id.get(
                                local_id,
                                current_crop,
                            )
                        )

                        # 512차원 Re-ID embedding 추출
                        embedding = (
                            reid_engine.extract(
                                reid_crop
                            )
                        )

                        embedding = (
                            embedding
                            .astype(np.float32)
                            .reshape(-1)
                        )

                        if embedding.size != 512:
                            raise RuntimeError(
                                f"Re-ID embedding 크기 오류: "
                                f"{embedding.shape}"
                            )

                        embedding_norm = float(
                            np.linalg.norm(
                                embedding
                            )
                        )

                        global_id = (
                            f"G{next_global_number:06d}"
                        )

                        next_global_number += 1

                        global_id_by_local_id[
                            local_id
                        ] = global_id

                        entry_timestamp = (
                            save_entry_event(
                                local_track_id=local_id,
                                global_person_id=global_id,
                            )
                        )

                        # NumPy float32는 JSON 변환이 안 되므로
                        # Python float list로 변환
                        embedding_list = (
                            embedding.tolist()
                        )

                        mqtt_publisher.publish_entry(
                            {
                                "timestamp": (
                                    entry_timestamp
                                ),
                                "node_id": "A",
                                "event": "ENTRY",
                                "local_track_id": (
                                    local_id
                                ),
                                "global_person_id": (
                                    global_id
                                ),
                                "next_nodes": [
                                    "B",
                                    "C",
                                ],
                                "reid_model": (
                                    "osnet_x0_25"
                                ),
                                "embedding_dim": (
                                    len(
                                        embedding_list
                                    )
                                ),
                                "embedding": (
                                    embedding_list
                                ),
                            }
                        )

                        print()
                        print(
                            "===== A 입장 등록 ====="
                        )
                        print(
                            f"Local ID     : "
                            f"{local_id}"
                        )
                        print(
                            f"Global ID    : "
                            f"{global_id}"
                        )
                        print(
                            f"Time         : "
                            f"{entry_timestamp}"
                        )
                        print(
                            f"Embedding Dim: "
                            f"{embedding.size}"
                        )
                        print(
                            f"Embedding Norm: "
                            f"{embedding_norm:.6f}"
                        )
                        print(
                            "MQTT 대상    : B, C"
                        )
                        print(
                            "======================="
                        )

                        # 등록 완료 후 Crop 메모리 정리
                        best_crop_by_local_id.pop(
                            local_id,
                            None,
                        )

                        best_crop_score_by_local_id.pop(
                            local_id,
                            None,
                        )

                    previous_x_by_local_id[
                        local_id
                    ] = current_x

                    global_id = (
                        global_id_by_local_id.get(
                            local_id
                        )
                    )

                    if global_id is None:
                        label = "STRANGER"
                        box_color = (
                            0,
                            165,
                            255,
                        )

                    else:
                        label = (
                            f"ID: {global_id}"
                        )
                        box_color = (
                            0,
                            255,
                            0,
                        )

                    cv2.rectangle(
                        annotated_frame,
                        (x1, y1),
                        (x2, y2),
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
                        (0, 0, 255),
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
                        0.7,
                        box_color,
                        2,
                    )

            cv2.putText(
                annotated_frame,
                (
                    f"Registered: "
                    f"{len(global_id_by_local_id)}"
                ),
                (20, 75),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.9,
                (0, 255, 255),
                2,
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
        print("Camera A 종료")

    finally:
        cap.release()
        mqtt_publisher.disconnect()


if __name__ == "__main__":
    main()