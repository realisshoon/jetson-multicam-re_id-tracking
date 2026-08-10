from __future__ import annotations

import csv
import json
import threading
import time
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

from src.common.config import load_mqtt_config
from src.common.model_requirements import require_model_files
from src.reid.reid_engine import ReIDTensorRTEngine


# ============================================================
# 기본 설정
# ============================================================

ROOT = Path(__file__).resolve().parents[2]
YOLO_MODEL = ROOT / "yolo26n.pt"
REID_ENGINE = ROOT / "models/reid/person_reid_osnet_x0_25_fp16.engine"

CAMERA_SOURCE = "http://10.10.20.22:8090/stream"

WIDTH, HEIGHT, FPS = 640, 480, 15
WEB_PORT = 8002

# PC 송출 코드에서 이미 좌우 반전하므로 D에서는 반전하지 않음
FLIP_HORIZONTAL = False

MQTT_CONFIG = load_mqtt_config()
MQTT_HOST = MQTT_CONFIG.host
MQTT_PORT = MQTT_CONFIG.port
MQTT_QOS = MQTT_CONFIG.qos
CANDIDATE_TOPIC = "cctv/candidates/d"
ARRIVAL_TOPIC = "cctv/events/d/arrival"

CAPTURE_ROOT = ROOT / "outputs" / "captures" / "D"

# A/B와 동일한 아주 약한 밝기/대비 보정
IMAGE_CONTRAST_ALPHA = 1.02
IMAGE_BRIGHTNESS_BETA = 8

MATCH_BEST_THRESHOLD = 0.70
MATCH_TOP2_THRESHOLD = 0.62
MATCH_MARGIN = 0.04
MATCH_CONFIRMATIONS = 3

VERIFY_THRESHOLD = 0.55
VERIFY_FAILURE_LIMIT = 2

REID_INTERVAL_FRAMES = 3
REID_HISTORY_SIZE = 5
CANDIDATE_TIMEOUT_SECONDS = 300.0
ANOMALY_DELAY_SECONDS = 2.0
TRACK_LOST_GRACE_FRAMES = 20

LOG_DIR = ROOT / "logs"
CANDIDATE_CSV = LOG_DIR / "node_d_candidates.csv"
ARRIVAL_CSV = LOG_DIR / "node_d_arrivals.csv"

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()
candidate_lock = threading.Lock()


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
    status: str = "PENDING"
    matched_d_local_id: int | None = None
    best_similarity: float | None = None
    top2_mean: float | None = None
    combined_score: float | None = None


candidates: dict[str, Candidate] = {}


# ============================================================
# 공통 함수
# ============================================================

def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def parse_time(value: str) -> float:
    try:
        return datetime.fromisoformat(value).timestamp()
    except (TypeError, ValueError):
        return time.time()


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

def save_candidate(payload: dict[str, Any]) -> None:
    """
    Main Server가 WAITING_D 상태에서 보낸
    A+B 또는 A+C Gallery 후보를 저장한다.
    """

    journey_id = payload.get("journey_id")
    person_uid = payload.get("person_uid")
    stage = payload.get("stage")
    raw_gallery = payload.get("gallery")

    if (
        not journey_id
        or not person_uid
        or stage != "WAITING_D"
        or not isinstance(raw_gallery, list)
        or not raw_gallery
    ):
        print(
            "[D MQTT] 잘못된 Main 후보 메시지"
        )
        return

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
        print(
            f"[D MQTT] {journey_id}: "
            f"{error}"
        )
        return

    entry_timestamp = payload.get(
        "entry_timestamp",
        now_iso(),
    )

    passage_timestamp = payload.get(
        "passage_timestamp",
        payload.get(
            "b_passage_timestamp",
            now_iso(),
        ),
    )

    route = list(
        payload.get(
            "route",
            ["A", "B"],
        )
    )

    candidate = Candidate(
        journey_id=journey_id,
        person_uid=person_uid,

        received_at=now_iso(),

        entry_timestamp=entry_timestamp,
        entry_epoch=parse_time(
            entry_timestamp
        ),

        # 기존 변수명은 호환성을 위해 유지
        b_passage_timestamp=passage_timestamp,
        b_passage_epoch=parse_time(
            passage_timestamp
        ),

        route=route,
        gallery=gallery,
        gallery_nodes=gallery_nodes,
    )

    with candidate_lock:
        old = candidates.get(
            journey_id
        )

        if (
            old
            and old.status
            in {
                "PENDING",
                "COMPLETED",
            }
        ):
            print(
                f"[D 중복 후보 무시] "
                f"{journey_id}"
            )
            return

        candidates[journey_id] = candidate

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


def cleanup_candidates() -> None:
    current = time.time()
    with candidate_lock:
        for item in candidates.values():
            if (
                item.status == "PENDING"
                and current - item.b_passage_epoch > CANDIDATE_TIMEOUT_SECONDS
            ):
                item.status = "EXPIRED"


def get_candidate(journey_id: str) -> Candidate | None:
    with candidate_lock:
        return candidates.get(journey_id)


def find_best_candidate(
    embedding: np.ndarray,
) -> tuple[str | None, float, float, float, float]:
    cleanup_candidates()
    results: list[tuple[str, float, float, float]] = []

    with candidate_lock:
        pending = [item for item in candidates.values() if item.status == "PENDING"]
        for item in pending:
            best, top2, combined = gallery_score(embedding, item.gallery)
            results.append((item.journey_id, best, top2, combined))

    if not results:
        return None, -1.0, -1.0, -1.0, -1.0

    results.sort(key=lambda item: item[3], reverse=True)
    journey_id, best, top2, combined = results[0]
    second_combined = results[1][3] if len(results) >= 2 else -1.0
    return journey_id, best, top2, combined, second_combined


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
) -> bool:
    d_embedding = normalize(
        d_embedding
    )

    with candidate_lock:
        item = candidates.get(
            journey_id
        )

        if (
            item is None
            or item.status != "PENDING"
        ):
            return False

        item.status = "COMPLETED"
        item.matched_d_local_id = local_id
        item.best_similarity = best
        item.top2_mean = top2
        item.combined_score = combined

        person_uid = item.person_uid

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

    arrival_timestamp = now_iso()
    arrival_epoch = parse_time(
        arrival_timestamp
    )

    total_duration = max(
        0.0,
        arrival_epoch - entry_epoch,
    )

    passage_to_d_duration = max(
        0.0,
        arrival_epoch - passage_epoch,
    )

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
        "embedding_dim": 512,
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

    info = client.publish(
        ARRIVAL_TOPIC,
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        qos=MQTT_QOS,
        retain=False,
    )

    if (
        info.rc
        != mqtt.MQTT_ERR_SUCCESS
    ):
        print(
            f"[D ARRIVAL MQTT 실패] "
            f"rc={info.rc}"
        )
        return False

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

def on_connect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code != 0:
        print(f"Camera D MQTT 연결 실패: {reason_code}")
        return
    print(f"Camera D MQTT 연결 완료: {MQTT_HOST}:{MQTT_PORT}")
    client.subscribe(CANDIDATE_TOPIC, qos=MQTT_QOS)
    print(f"Camera D MQTT 구독: {CANDIDATE_TOPIC}")
    print(f"Camera D MQTT 발행: {ARRIVAL_TOPIC}")


def on_message(
    client,
    userdata,
    message,
) -> None:
    try:
        payload = json.loads(
            message.payload.decode(
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
        return

    if (
        message.topic
        == CANDIDATE_TOPIC
        and payload.get("event")
        == "CANDIDATE"
        and payload.get("stage")
        == "WAITING_D"
    ):
        save_candidate(
            payload
        )


def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="camera-d",
    )
    client.on_connect = on_connect
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
) -> np.ndarray:
    """
    CCTV 영상 아래에 D Journey 후보와
    최근 도착 완료 정보를 표시한다.
    """

    cleanup_candidates()

    with candidate_lock:
        snapshot = list(candidates.values())

    frame_height, frame_width = frame.shape[:2]

    panel_height = 200
    panel_top = frame_height

    dashboard = np.full(
        (
            frame_height + panel_height,
            frame_width,
            3,
        ),
        24,
        dtype=np.uint8,
    )

    dashboard[:frame_height] = frame

    cv2.line(
        dashboard,
        (0, panel_top),
        (frame_width, panel_top),
        (110, 110, 110),
        2,
    )

    pending_items = [
        item
        for item in snapshot
        if item.status == "PENDING"
    ]

    completed_items = [
        item
        for item in snapshot
        if item.status == "COMPLETED"
    ][-4:]

    expired_count = sum(
        item.status == "EXPIRED"
        for item in snapshot
    )

    cv2.putText(
        dashboard,
        "CAMERA D - ADMINISTRATOR VIEW",
        (15, panel_top + 28),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.63,
        (0, 255, 255),
        2,
    )

    cv2.putText(
        dashboard,
        (
            f"PENDING {len(pending_items)}   "
            f"COMPLETED {len(completed_items)}   "
            f"EXPIRED {expired_count}"
        ),
        (15, panel_top + 54),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (220, 220, 220),
        1,
    )

    middle_x = frame_width // 2

    cv2.line(
        dashboard,
        (middle_x, panel_top + 65),
        (middle_x, panel_top + panel_height - 10),
        (70, 70, 70),
        1,
    )

    cv2.putText(
        dashboard,
        "WAITING JOURNEYS",
        (15, panel_top + 82),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (0, 255, 255),
        1,
    )

    cv2.putText(
        dashboard,
        "RECENT ARRIVALS",
        (middle_x + 15, panel_top + 82),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (0, 255, 0),
        1,
    )

    # 왼쪽: 도착 대기 후보
    y = panel_top + 108

    if not pending_items:
        cv2.putText(
            dashboard,
            "No pending journey",
            (15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.44,
            (145, 145, 145),
            1,
        )

    for item in pending_items[-4:]:
        cv2.putText(
            dashboard,
            (
                f"{item.person_uid} | {item.journey_id} | "
                f"PENDING | {route_text(item.route)}"
            ),
            (15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.41,
            (0, 255, 255),
            1,
        )

        y += 25

    # 오른쪽: 최근 도착 완료
    y = panel_top + 108

    if not completed_items:
        cv2.putText(
            dashboard,
            "No completed arrival",
            (middle_x + 15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.44,
            (145, 145, 145),
            1,
        )

    for item in completed_items:
        score_text = (
            f"{item.combined_score:.2f}"
            if item.combined_score is not None
            else "-"
        )

        cv2.putText(
            dashboard,
            (
                f"{item.person_uid} | {item.journey_id} | "
                f"ARRIVED | {score_text}"
            ),
            (middle_x + 15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.40,
            (0, 255, 0),
            1,
        )

        y += 25

    return dashboard

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
    verify_scores: dict[int, float] = {}
    verify_failures: dict[int, int] = {}
    best_capture_by_local_id: dict[int, np.ndarray] = {}
    best_capture_quality_by_local_id: dict[int, float] = {}

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
        print(f"도착 발행 토픽 : {ARRIVAL_TOPIC}")
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

            annotated = frame.copy()
            cv2.putText(
                annotated, "CAMERA D - ADMINISTRATOR VIEW", (20, 35),
                cv2.FONT_HERSHEY_SIMPLEX, 0.80, (0, 255, 255), 2,
            )
            cv2.putText(
                annotated,
                f"BEST {MATCH_BEST_THRESHOLD:.2f} TOP2 {MATCH_TOP2_THRESHOLD:.2f}",
                (20, 68), cv2.FONT_HERSHEY_SIMPLEX, 0.56,
                (255, 255, 255), 2,
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
                    first_seen.setdefault(local_id, time.time())
                    last_seen[local_id] = frame_index
                    x1, y1, x2, y2 = box

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

                            arrived_journey = arrived_ids.get(local_id)

                            if arrived_journey is not None:
                                candidate = get_candidate(arrived_journey)
                                if candidate is None:
                                    arrived_ids.pop(local_id, None)
                                else:
                                    _, _, verify = gallery_score(averaged, candidate.gallery)
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
                                            arrived_ids.pop(local_id, None)
                                            verify_scores.pop(local_id, None)
                                            verify_failures.pop(local_id, None)
                                            history.clear()
                                            first_seen[local_id] = time.time()

                            else:
                                candidate_id, best, top2, combined, second = (
                                    find_best_candidate(averaged)
                                )
                                best_scores[local_id] = best
                                top2_scores[local_id] = top2
                                combined_scores[local_id] = combined

                                matched = (
                                    candidate_id is not None
                                    and best >= MATCH_BEST_THRESHOLD
                                    and top2 >= MATCH_TOP2_THRESHOLD
                                    and (second < 0 or combined - second >= MATCH_MARGIN)
                                )

                                if matched:
                                    if tentative_id.get(local_id) == candidate_id:
                                        tentative_count[local_id] = (
                                            tentative_count.get(local_id, 0) + 1
                                        )
                                    else:
                                        tentative_id[local_id] = candidate_id
                                        tentative_count[local_id] = 1

                                    if tentative_count[local_id] >= MATCH_CONFIRMATIONS:
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
                                        ):
                                            arrived_ids[local_id] = candidate_id
                                            verify_scores[local_id] = combined
                                            verify_failures[local_id] = 0
                                        tentative_id.pop(local_id, None)
                                        tentative_count.pop(local_id, None)
                                else:
                                    tentative_id.pop(local_id, None)
                                    tentative_count.pop(local_id, None)

                        except Exception as error:
                            print(f"[D Re-ID 오류] Local={local_id}: {error}")

                    journey_id = arrived_ids.get(local_id)

                    if journey_id is not None:
                        candidate = get_candidate(journey_id)
                        person_uid = (
                            candidate.person_uid
                            if candidate is not None
                            else "UNKNOWN"
                        )
                        display_route = (
                            route_text(candidate.route)
                            if candidate is not None
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
                                f"{tentative_count.get(local_id, 0)}/{MATCH_CONFIRMATIONS}"
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

                    cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 3)
                    cv2.putText(
                        annotated, label, (x1, max(25, y1 - 32)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.65, color, 2,
                    )
                    cv2.putText(
                        annotated, sub, (x1, max(50, y1 - 8)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.49, color, 2,
                    )

            stale_ids = [
                local_id for local_id, seen in last_seen.items()
                if frame_index - seen > TRACK_LOST_GRACE_FRAMES
            ]
            for local_id in stale_ids:
                journey_id = arrived_ids.get(local_id)
                if journey_id:
                    print(
                        f"[D Track 종료] Local={local_id}, "
                        f"Journey={journey_id}, COMPLETED 유지"
                    )
                for mapping in (
                    first_seen, last_seen, histories, tentative_id,
                    tentative_count, best_scores, top2_scores,
                    combined_scores, arrived_ids, verify_scores,
                    verify_failures, best_capture_by_local_id,
                    best_capture_quality_by_local_id,
                ):
                    mapping.pop(local_id, None)

            output_frame = build_candidate_dashboard(
                annotated
            )

            encoded, buffer = cv2.imencode(
                ".jpg",
                output_frame,
                [
                    cv2.IMWRITE_JPEG_QUALITY,
                    80,
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
