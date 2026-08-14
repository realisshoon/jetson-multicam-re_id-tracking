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

from src.common.config import load_mqtt_config
from src.common.journey import build_passage_payload
from src.common.model_requirements import require_model_files
from src.common.stranger_detection import (
    STRANGER_DETECTION_TOPICS,
    StrangerDetectionGate,
    publish_stranger_detection,
)
from src.reid.reid_engine import ReIDTensorRTEngine


ROOT = Path(__file__).resolve().parents[2]
YOLO_MODEL = ROOT / "yolo26n.pt"
REID_ENGINE = ROOT / "models/reid/person_reid_osnet_x0_25_fp16.engine"

CAMERA_DEVICE = 2
WIDTH, HEIGHT, FPS = 1280, 720, 30
# WIDTH, HEIGHT, FPS = 640, 480, 15
WEB_PORT = 8001
FLIP_HORIZONTAL = True

MQTT_CONFIG = load_mqtt_config()
MQTT_HOST = MQTT_CONFIG.host
MQTT_PORT = MQTT_CONFIG.port
MQTT_QOS = MQTT_CONFIG.qos
CANDIDATE_TOPIC = "cctv/candidates/b"
PASSAGE_TOPIC = "cctv/events/b/passage"
DETECTION_TOPIC = STRANGER_DETECTION_TOPICS["B"]

CAPTURE_ROOT = ROOT / "outputs" / "captures" / "B"

# A와 동일하게 아주 약한 밝기/대비 보정
IMAGE_CONTRAST_ALPHA = 1.02
IMAGE_BRIGHTNESS_BETA = 8

MATCH_THRESHOLD = 0.70
MATCH_MARGIN = 0.05
MATCH_CONFIRMATIONS = 3
VERIFY_THRESHOLD = 0.55
VERIFY_FAILURE_LIMIT = 2
REID_INTERVAL_FRAMES = 3
REID_HISTORY_SIZE = 5

B_GALLERY_TARGET = 2
B_GALLERY_MAX = 2
GALLERY_MIN_FRAME_GAP = 10
GALLERY_DUPLICATE_THRESHOLD = 0.999
PASSAGE_MIN_VERIFY_SUCCESSES = 2

CANDIDATE_TIMEOUT_SECONDS = 300.0
ANOMALY_DELAY_SECONDS = 2.0
TRACK_LOST_GRACE_FRAMES = 20

LOG_DIR = ROOT / "logs"
CANDIDATE_CSV = LOG_DIR / "node_b_candidates.csv"
MATCH_CSV = LOG_DIR / "node_b_matches.csv"
PASSAGE_CSV = LOG_DIR / "node_b_passages.csv"

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()
candidate_lock = threading.Lock()
candidates: dict[str, dict[str, Any]] = {}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def parse_time(value: str) -> float:
    try:
        return datetime.fromisoformat(value).timestamp()
    except (TypeError, ValueError):
        return time.time()


def normalize(embedding: np.ndarray | list[float]) -> np.ndarray:
    array = np.asarray(embedding, dtype=np.float32).reshape(-1)
    if array.size != 512:
        raise ValueError(f"Embedding 크기 오류: {array.size}")
    if not np.all(np.isfinite(array)):
        raise ValueError("Embedding에 NaN/Inf가 있습니다.")
    norm = float(np.linalg.norm(array))
    if norm <= 1e-12:
        raise ValueError("Embedding norm이 0입니다.")
    return array / norm


def similarity(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b))


def average(history: deque[np.ndarray]) -> np.ndarray:
    return normalize(np.mean(np.stack(list(history)), axis=0))


def extract_crop(frame: np.ndarray, box: list[int]) -> np.ndarray:
    h, w = frame.shape[:2]
    x1, y1, x2, y2 = box
    bw, bh = max(1, x2 - x1), max(1, y2 - y1)
    px, py = int(bw * 0.04), int(bh * 0.04)
    x1, y1 = max(0, x1 - px), max(0, y1 - py)
    x2, y2 = min(w, x2 + px), min(h, y2 + py)
    crop = frame[y1:y2, x1:x2]
    if crop.size == 0:
        raise RuntimeError("사람 crop이 비었습니다.")
    return crop.copy()


def crop_quality(
    box: list[int], confidence: float, frame_width: int, frame_height: int
) -> tuple[bool, float]:
    x1, _, x2, y2 = box
    area_ratio = max(1, x2 - x1) * max(1, y2 - box[1])
    area_ratio /= float(frame_width * frame_height)
    side_ok = x1 >= 5 and x2 <= frame_width - 5
    ok = confidence >= 0.50 and area_ratio >= 0.03 and side_ok
    quality = 0.65 * confidence + 0.35 * min(1.0, area_ratio / 0.20)
    return ok, float(np.clip(quality, 0.0, 1.0))


def apply_small_brightness_adjustment(
    frame: np.ndarray,
) -> np.ndarray:
    return cv2.convertScaleAbs(
        frame,
        alpha=IMAGE_CONTRAST_ALPHA,
        beta=IMAGE_BRIGHTNESS_BETA,
    )


def save_match_capture(
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
        f"B_{captured_at.strftime('%H%M%S_%f')}_"
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
            f"B Capture 저장 실패: {capture_path}"
        )

    return str(capture_path)


def ensure_csv() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    files = [
        (CANDIDATE_CSV, ["received_at", "entry_timestamp", "journey_id", "a_local_id", "status"]),
        (MATCH_CSV, ["matched_at", "b_local_id", "journey_id", "similarity", "status"]),
        (PASSAGE_CSV, ["published_at", "topic", "journey_id", "b_local_id", "b_gallery", "total_gallery", "status"]),
    ]
    for path, header in files:
        if not path.exists():
            with path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(header)


def append_csv(path: Path, row: list[Any]) -> None:
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


def save_candidate(payload: dict[str, Any]) -> None:
    """
    Main Server가 B에 전달한 CANDIDATE 메시지를 저장한다.

    예상 구조:
    - event: CANDIDATE
    - stage: WAITING_B_OR_C
    - journey_id: J000001
    - person_uid: P000001
    - gallery: A 특징값
    """

    journey_id = payload.get("journey_id")
    person_uid = payload.get("person_uid", "")
    stage = payload.get("stage")
    gallery = payload.get("gallery", [])

    if (
        not journey_id
        or stage != "WAITING_B_OR_C"
        or not isinstance(gallery, list)
    ):
        print("[B MQTT] 잘못된 Main 후보 메시지")
        return

    a_gallery_item = None

    for item in gallery:
        if not isinstance(item, dict):
            continue

        raw_embedding = item.get("embedding")

        if (
            item.get("node_id") == "A"
            and isinstance(raw_embedding, list)
        ):
            a_gallery_item = item
            break

    if a_gallery_item is None:
        print(
            f"[B MQTT] {journey_id}: "
            "A 특징값이 없습니다."
        )
        return

    try:
        embedding = normalize(
            a_gallery_item["embedding"]
        )
    except ValueError as error:
        print(
            f"[B MQTT] {journey_id}: {error}"
        )
        return

    entry_timestamp = payload.get(
        "entry_timestamp",
        a_gallery_item.get(
            "captured_at",
            now_iso(),
        ),
    )

    candidate = {
        "journey_id": journey_id,
        "person_uid": person_uid,
        "person_status": payload.get("person_status", "UNKNOWN"),
        "visit_count": payload.get("visit_count"),
        "previous_last_seen_at": payload.get("previous_last_seen_at"),
        "candidate_person_uid": payload.get("candidate_person_uid"),
        "route": payload.get("route", ["A"]),

        "entry_timestamp": entry_timestamp,
        "entry_epoch": parse_time(
            entry_timestamp
        ),

        # 중앙 후보에는 A Local ID가 없어도 정상
        "a_local_id": "",

        "embedding": embedding,

        "status": "PENDING",
        "matched_b_local_id": None,
        "match_score": None,
        "passage_published": False,
    }

    with candidate_lock:
        old = candidates.get(
            journey_id
        )

        if (
            old
            and old["status"]
            in {
                "PENDING",
                "MATCHED",
                "PASSED",
            }
        ):
            print(
                f"[B 중복 후보 무시] "
                f"{journey_id}"
            )
            return

        candidates[journey_id] = candidate

    append_csv(
        CANDIDATE_CSV,
        [
            now_iso(),
            entry_timestamp,
            journey_id,
            "",
            "PENDING",
        ],
    )

    print()
    print("===== B Main 후보 수신 =====")
    print(f"Person UID    : {person_uid}")
    print(f"Journey ID    : {journey_id}")
    print(f"Stage         : {stage}")
    print(f"Embedding Dim : {embedding.size}")
    print(
        f"Norm          : "
        f"{np.linalg.norm(embedding):.6f}"
    )
    print("============================")


def cleanup_candidates() -> None:
    now = time.time()
    with candidate_lock:
        for item in candidates.values():
            if item["status"] == "PENDING":
                if now - item["entry_epoch"] > CANDIDATE_TIMEOUT_SECONDS:
                    item["status"] = "EXPIRED"


def pending_candidates() -> list[tuple[str, np.ndarray]]:
    cleanup_candidates()
    with candidate_lock:
        return [
            (item["journey_id"], item["embedding"].copy())
            for item in candidates.values()
            if item["status"] == "PENDING"
        ]


def candidate_reference(journey_id: str) -> dict[str, Any] | None:
    with candidate_lock:
        item = candidates.get(journey_id)
        if item is None:
            return None
        return {
            "journey_id": item["journey_id"],
            "person_uid": item["person_uid"],
            "person_status": item.get("person_status", "UNKNOWN"),
            "visit_count": item.get("visit_count"),
            "previous_last_seen_at": item.get("previous_last_seen_at"),
            "candidate_person_uid": item.get("candidate_person_uid"),
            "route": list(item.get("route", ["A"])),
            "entry_timestamp": item["entry_timestamp"],
            "a_local_id": item["a_local_id"],
            "embedding": item["embedding"].copy(),
            "match_score": item.get("match_score"),
            "passage_published": item["passage_published"],
        }


def mark_matched(journey_id: str, local_id: int, score: float) -> bool:
    with candidate_lock:
        item = candidates.get(journey_id)
        if item is None or item["status"] != "PENDING":
            return False
        item["status"] = "MATCHED"
        item["matched_b_local_id"] = local_id
        item["match_score"] = score
        return True


def mark_passed(journey_id: str) -> None:
    with candidate_lock:
        item = candidates.get(journey_id)
        if item:
            item["status"] = "PASSED"
            item["passage_published"] = True


def release_candidate(journey_id: str, local_id: int, reason: str) -> None:
    with candidate_lock:
        item = candidates.get(journey_id)
        if item is None:
            return
        if item.get("matched_b_local_id") not in {None, local_id}:
            return

        passed = bool(item["passage_published"])
        if passed:
            item["status"] = "PASSED"
        elif time.time() - item["entry_epoch"] <= CANDIDATE_TIMEOUT_SECONDS:
            item["status"] = "PENDING"
        else:
            item["status"] = "EXPIRED"

        item["matched_b_local_id"] = None
        if not passed:
            item["match_score"] = None

    print("\n===== B Global ID 연결 정리 =====")
    print(f"Journey ID: {journey_id}")
    print(f"B Local ID: {local_id}")
    print(f"Reason    : {reason}")
    print(f"Status    : {'PASSED 유지' if passed else '재매칭 가능'}")
    print("================================")


def find_best(b_embedding: np.ndarray) -> tuple[str | None, float, float]:
    pending = pending_candidates()
    if not pending:
        return None, -1.0, -1.0

    scores = [(jid, similarity(b_embedding, emb)) for jid, emb in pending]
    scores.sort(key=lambda item: item[1], reverse=True)
    best_id, best_score = scores[0]
    second_score = scores[1][1] if len(scores) > 1 else -1.0
    return best_id, best_score, second_score


def try_add_gallery(
    local_id: int,
    embedding: np.ndarray,
    quality: float,
    frame_index: int,
    galleries: dict[int, list[np.ndarray]],
    last_gallery_frame: dict[int, int],
) -> bool:
    gallery = galleries.setdefault(local_id, [])
    if len(gallery) >= B_GALLERY_MAX:
        return False
    if frame_index - last_gallery_frame.get(local_id, -99999) < GALLERY_MIN_FRAME_GAP:
        return False

    embedding = normalize(embedding)
    if any(
        similarity(embedding, old) >= GALLERY_DUPLICATE_THRESHOLD
        for old in gallery
    ):
        return False

    gallery.append(embedding.copy())
    last_gallery_frame[local_id] = frame_index
    print(
        f"[B Gallery 추가] Local={local_id}, "
        f"Count={len(gallery)}/{B_GALLERY_TARGET}, Quality={quality:.3f}"
    )
    return True


def publish_passage(
    client: mqtt.Client,
    local_id: int,
    journey_id: str,
    gallery: list[np.ndarray],
    capture_crop: np.ndarray,
    capture_quality: float,
    match_score: float,
) -> bool:
    reference = candidate_reference(journey_id)
    if reference is None:
        print(f"[B PASSAGE 실패] {journey_id}: A 후보 정보 없음")
        return False

    person_uid = str(reference.get("person_uid") or "UNKNOWN")
    passage_at = now_iso()

    try:
        capture_path = save_match_capture(
            crop=capture_crop,
            person_uid=person_uid,
            journey_id=journey_id,
            local_id=local_id,
            score=match_score,
        )
    except Exception as error:
        print(f"[B Capture 저장 실패] {error}")
        capture_path = ""

    payload = build_passage_payload(
        journey_id=journey_id,
        entry_timestamp=reference["entry_timestamp"],
        a_embedding=reference["embedding"],
        b_embeddings=gallery[:B_GALLERY_MAX],
        a_local_track_id=reference["a_local_id"],
        b_local_track_id=local_id,
        b_passage_timestamp=passage_at,
    )

    # Main Server의 Capture/관리자 DB 기록용 추가 필드
    payload["person_uid"] = person_uid
    payload["local_track_id"] = local_id
    payload["capture_path"] = capture_path
    payload["similarity"] = float(match_score)
    payload["quality"] = float(
        max(0.0, min(1.0, capture_quality))
    )
    payload["verification_status"] = "AUTO_MATCHED"

    info = client.publish(
        PASSAGE_TOPIC,
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        qos=MQTT_QOS,
        retain=False,
    )
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"[B PASSAGE 실패] MQTT rc={info.rc}")
        return False

    mark_passed(journey_id)
    append_csv(
        PASSAGE_CSV,
        [
            passage_at,
            PASSAGE_TOPIC,
            journey_id,
            local_id,
            len(gallery),
            payload["gallery_count"],
            "PUBLISHED",
        ],
    )

    print("\n===== B -> MAIN PASSAGE 발행 =====")
    print(f"Person UID    : {person_uid}")
    print(f"Journey ID    : {journey_id}")
    print(f"B Local ID    : {local_id}")
    print(f"Similarity    : {match_score:.6f}")
    print(f"B Gallery     : {len(gallery)}")
    print(f"Total Gallery : {payload['gallery_count']}")
    print(f"Route         : {payload['route']}")
    print(f"Capture       : {capture_path or '저장 실패'}")
    print("=================================")
    return True


def on_connect(client, userdata, flags, reason_code, properties) -> None:
    if reason_code != 0:
        print(f"Camera B MQTT 연결 실패: {reason_code}")
        return
    print(f"Camera B MQTT 연결 완료: {MQTT_HOST}:{MQTT_PORT}")
    client.subscribe(CANDIDATE_TOPIC, qos=MQTT_QOS)
    print(f"Camera B MQTT 구독: {CANDIDATE_TOPIC}")
    print(f"Camera B MQTT 발행: {PASSAGE_TOPIC}")
    print(f"Camera B MQTT 발행: {DETECTION_TOPIC}")


def on_message(client, userdata, message) -> None:
    try:
        payload = json.loads(
            message.payload.decode("utf-8")
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        print(
            f"[B MQTT] 잘못된 메시지: "
            f"{error}"
        )
        return

    if (
        message.topic == CANDIDATE_TOPIC
        and payload.get("event")
        == "CANDIDATE"
        and payload.get("stage")
        == "WAITING_B_OR_C"
    ):
        save_candidate(payload)


def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="camera-b",
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


class ReusableServer(ThreadingHTTPServer):
    allow_reuse_address = True


class StreamHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/":
            html = b"""<!doctype html><html><head><meta charset="utf-8">
<title>Camera B</title><style>
body{margin:0;background:#111;color:#fff;text-align:center;font-family:Arial}
img{width:95%;max-width:1280px;border:2px solid #fff}</style></head>
<body><h2>Camera B - Administrator View</h2><img src="/stream"></body></html>"""
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
    print(f"Camera B 웹 서버: http://<jetson-b-ip>:{WEB_PORT}")
    ReusableServer(("0.0.0.0", WEB_PORT), StreamHandler).serve_forever()

def build_candidate_dashboard(
    frame: np.ndarray,
) -> np.ndarray:
    """관리자가 Person UID와 이동 경로를 한눈에 보는 하단 패널."""

    cleanup_candidates()

    with candidate_lock:
        snapshot = [dict(item) for item in candidates.values()]

    frame_height, frame_width = frame.shape[:2]
    panel_height = 220
    panel_top = frame_height

    dashboard = np.full(
        (frame_height + panel_height, frame_width, 3),
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

    pending_count = sum(
        item["status"] == "PENDING" for item in snapshot
    )
    matched_count = sum(
        item["status"] == "MATCHED" for item in snapshot
    )
    passed_count = sum(
        item["status"] == "PASSED" for item in snapshot
    )

    cv2.putText(
        dashboard,
        "CAMERA B - ADMINISTRATOR VIEW",
        (15, panel_top + 28),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.63,
        (0, 255, 255),
        2,
    )
    cv2.putText(
        dashboard,
        "ROUTE  A  ->  [B]  ->  D",
        (15, panel_top + 55),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.55,
        (255, 220, 0),
        2,
    )
    cv2.putText(
        dashboard,
        (
            f"PENDING {pending_count}   "
            f"MATCHED {matched_count}   "
            f"PASSED {passed_count}"
        ),
        (frame_width - 390, panel_top + 32),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (220, 220, 220),
        1,
    )

    active_items = [
        item for item in snapshot
        if item["status"] in {"PENDING", "MATCHED"}
    ][-4:]
    passed_items = [
        item for item in snapshot
        if item["status"] == "PASSED"
    ][-4:]

    middle_x = frame_width // 2
    cv2.line(
        dashboard,
        (middle_x, panel_top + 72),
        (middle_x, panel_top + panel_height - 10),
        (70, 70, 70),
        1,
    )

    cv2.putText(
        dashboard,
        "ACTIVE / PENDING",
        (15, panel_top + 92),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (0, 255, 255),
        1,
    )
    cv2.putText(
        dashboard,
        "RECENT PASSED",
        (middle_x + 15, panel_top + 92),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.48,
        (0, 255, 0),
        1,
    )

    y = panel_top + 120
    if not active_items:
        cv2.putText(
            dashboard,
            "No active candidate",
            (15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.44,
            (145, 145, 145),
            1,
        )

    for item in active_items:
        person_uid = item.get("person_uid") or "UNKNOWN"
        journey_id = item["journey_id"]
        status = item["status"]
        score = item.get("match_score")
        score_text = f"{score:.2f}" if score is not None else "-"
        color = (0, 255, 0) if status == "MATCHED" else (0, 255, 255)

        cv2.putText(
            dashboard,
            f"{person_uid} | {journey_id} | {status} | {score_text}",
            (15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.45,
            color,
            1,
        )
        y += 24

    y = panel_top + 120
    if not passed_items:
        cv2.putText(
            dashboard,
            "No completed journey",
            (middle_x + 15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.44,
            (145, 145, 145),
            1,
        )

    for item in passed_items:
        person_uid = item.get("person_uid") or "UNKNOWN"
        journey_id = item["journey_id"]
        score = item.get("match_score")
        score_text = f"{score:.2f}" if score is not None else "-"

        cv2.putText(
            dashboard,
            f"{person_uid} | {journey_id} | PASSED | {score_text}",
            (middle_x + 15, y),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.45,
            (0, 255, 0),
            1,
        )
        y += 24

    return dashboard

# def draw_candidate_panel(frame: np.ndarray, frame_width: int) -> None:
#     cleanup_candidates()
#     with candidate_lock:
#         snapshot = [
#             (item["journey_id"], item["status"], item["match_score"])
#             for item in candidates.values()
#         ]

#     left = max(0, frame_width - 340)
#     overlay = frame.copy()
#     cv2.rectangle(overlay, (left, 0), (frame_width, 260), (0, 0, 0), -1)
#     cv2.addWeighted(overlay, 0.70, frame, 0.30, 0, frame)

#     cv2.putText(
#         frame, "MAIN RE-ID CANDIDATES", (left + 10, 28),
#         cv2.FONT_HERSHEY_SIMPLEX, 0.58, (0, 255, 255), 2
#     )

#     y = 60
#     for jid, status, score in snapshot[-6:]:
#         color = {
#             "PENDING": (0, 255, 255),
#             "MATCHED": (0, 255, 0),
#             "PASSED": (255, 255, 0),
#             "EXPIRED": (128, 128, 128),
#         }.get(status, (255, 255, 255))
#         score_text = f" {score:.2f}" if score is not None else ""
#         cv2.putText(
#             frame, f"{jid} {status}{score_text}", (left + 10, y),
#             cv2.FONT_HERSHEY_SIMPLEX, 0.48, color, 2
#         )
#         y += 29


def main() -> None:
    global latest_jpeg

    require_model_files(
        "Camera B",
        {
            "YOLO": YOLO_MODEL,
            "Re-ID TensorRT engine": REID_ENGINE,
        },
    )

    ensure_csv()
    CAPTURE_ROOT.mkdir(parents=True, exist_ok=True)

    if not torch.cuda.is_available():
        raise RuntimeError("Jetson GPU를 사용할 수 없습니다.")

    yolo = YOLO(str(YOLO_MODEL))
    reid = ReIDTensorRTEngine(REID_ENGINE)

    cap = cv2.VideoCapture(CAMERA_DEVICE, cv2.CAP_V4L2)
    if not cap.isOpened():
        raise RuntimeError(f"/dev/video{CAMERA_DEVICE} 카메라를 열 수 없습니다.")

    cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, FPS)

    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    first_seen: dict[int, float] = {}
    last_seen: dict[int, int] = {}
    histories: dict[int, deque[np.ndarray]] = {}

    tentative_id: dict[int, str] = {}
    tentative_count: dict[int, int] = {}
    best_scores: dict[int, float] = {}

    global_ids: dict[int, str] = {}
    verify_scores: dict[int, float] = {}
    verify_failures: dict[int, int] = {}
    verify_successes: dict[int, int] = {}

    galleries: dict[int, list[np.ndarray]] = {}
    last_gallery_frame: dict[int, int] = {}
    published_local_ids: set[int] = set()

    # PASSAGE 시 저장할 가장 품질 좋은 B Crop
    best_capture_by_local_id: dict[int, np.ndarray] = {}
    best_capture_quality_by_local_id: dict[int, float] = {}
    stranger_gate = StrangerDetectionGate("B")

    frame_index = 0
    mqtt_client: mqtt.Client | None = None

    try:
        mqtt_client = create_mqtt_client()
        threading.Thread(target=start_web, daemon=True).start()

        print("GPU:", torch.cuda.get_device_name(0))
        print("Camera B Re-ID + Gallery 시작")
        print(f"카메라         : /dev/video{CAMERA_DEVICE}")
        print(f"웹 포트        : {WEB_PORT}")
        print(f"B Gallery 목표 : {B_GALLERY_TARGET}")
        print(f"B -> Main 토픽  : {PASSAGE_TOPIC}")
        print(f"미등록 감지 토픽: {DETECTION_TOPIC}")
        print(f"Capture 저장    : {CAPTURE_ROOT}")
        print(
            f"밝기 보정       : "
            f"alpha={IMAGE_CONTRAST_ALPHA}, "
            f"beta={IMAGE_BRIGHTNESS_BETA}"
        )
        print("종료            : Ctrl + C")

        while True:
            ok, frame = cap.read()
            if not ok:
                print("Camera B 프레임 읽기 실패")
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
                annotated, "CAMERA B - PERSON RE-ID", (20, 35),
                cv2.FONT_HERSHEY_SIMPLEX, 0.80, (0, 255, 255), 2
            )

            if result.boxes is not None and result.boxes.id is not None:
                local_ids = result.boxes.id.int().cpu().tolist()
                boxes = result.boxes.xyxy.int().cpu().tolist()
                confs = result.boxes.conf.cpu().tolist()

                for local_id, box, confidence in zip(local_ids, boxes, confs):
                    first_seen.setdefault(local_id, time.time())
                    last_seen[local_id] = frame_index
                    reid_observation_valid = False
                    x1, y1, x2, y2 = box

                    if (frame_index + local_id) % REID_INTERVAL_FRAMES == 0:
                        try:
                            current_crop = extract_crop(
                                frame,
                                box,
                            )
                            current_embedding = normalize(
                                reid.extract(current_crop)
                            )
                            history = histories.setdefault(
                                local_id, deque(maxlen=REID_HISTORY_SIZE)
                            )
                            history.append(current_embedding)
                            avg_embedding = average(history)
                            reid_observation_valid = True

                            journey_id = global_ids.get(local_id)

                            if journey_id is not None:
                                reference = candidate_reference(journey_id)
                                if reference is None:
                                    release_candidate(
                                        journey_id, local_id, "REFERENCE_NOT_FOUND"
                                    )
                                    global_ids.pop(local_id, None)
                                    first_seen[local_id] = time.time()
                                else:
                                    score = similarity(
                                        avg_embedding, reference["embedding"]
                                    )
                                    verify_scores[local_id] = score

                                    if score >= VERIFY_THRESHOLD:
                                        verify_failures[local_id] = 0
                                        verify_successes[local_id] = (
                                            verify_successes.get(local_id, 0) + 1
                                        )

                                        quality_ok, quality = crop_quality(
                                            box, float(confidence),
                                            frame_width, frame_height
                                        )
                                        if quality_ok:
                                            try_add_gallery(
                                                local_id,
                                                current_embedding,
                                                quality,
                                                frame_index,
                                                galleries,
                                                last_gallery_frame,
                                            )

                                            previous_quality = (
                                                best_capture_quality_by_local_id.get(
                                                    local_id,
                                                    -1.0,
                                                )
                                            )
                                            if quality > previous_quality:
                                                best_capture_by_local_id[
                                                    local_id
                                                ] = current_crop.copy()
                                                best_capture_quality_by_local_id[
                                                    local_id
                                                ] = quality

                                        ready = (
                                            local_id not in published_local_ids
                                            and len(galleries.get(local_id, []))
                                            >= B_GALLERY_TARGET
                                            and verify_successes.get(local_id, 0)
                                            >= PASSAGE_MIN_VERIFY_SUCCESSES
                                        )
                                        if ready and mqtt_client is not None:
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

                                            if publish_passage(
                                                mqtt_client,
                                                local_id,
                                                journey_id,
                                                galleries[local_id],
                                                capture_crop,
                                                capture_quality,
                                                score,
                                            ):
                                                published_local_ids.add(local_id)

                                    else:
                                        verify_successes[local_id] = 0
                                        verify_failures[local_id] = (
                                            verify_failures.get(local_id, 0) + 1
                                        )
                                        print(
                                            f"[B 재검증 실패] Local={local_id}, "
                                            f"Journey={journey_id}, Score={score:.3f}, "
                                            f"Count={verify_failures[local_id]}/"
                                            f"{VERIFY_FAILURE_LIMIT}"
                                        )
                                        if (
                                            verify_failures[local_id]
                                            >= VERIFY_FAILURE_LIMIT
                                        ):
                                            release_candidate(
                                                journey_id,
                                                local_id,
                                                "REID_VERIFY_FAILED",
                                            )
                                            global_ids.pop(local_id, None)
                                            verify_scores.pop(local_id, None)
                                            verify_failures.pop(local_id, None)
                                            verify_successes.pop(local_id, None)
                                            tentative_id.pop(local_id, None)
                                            tentative_count.pop(local_id, None)
                                            best_scores.pop(local_id, None)
                                            galleries.pop(local_id, None)
                                            last_gallery_frame.pop(local_id, None)
                                            published_local_ids.discard(local_id)
                                            best_capture_by_local_id.pop(
                                                local_id,
                                                None,
                                            )
                                            best_capture_quality_by_local_id.pop(
                                                local_id,
                                                None,
                                            )
                                            history.clear()
                                            first_seen[local_id] = time.time()

                            else:
                                candidate_id, score, second = find_best(avg_embedding)
                                best_scores[local_id] = score

                                score_ok = (
                                    candidate_id is not None
                                    and score >= MATCH_THRESHOLD
                                )
                                margin_ok = second < 0 or score - second >= MATCH_MARGIN

                                if score_ok and margin_ok:
                                    if tentative_id.get(local_id) == candidate_id:
                                        tentative_count[local_id] = (
                                            tentative_count.get(local_id, 0) + 1
                                        )
                                    else:
                                        tentative_id[local_id] = candidate_id
                                        tentative_count[local_id] = 1

                                    if (
                                        tentative_count[local_id]
                                        >= MATCH_CONFIRMATIONS
                                    ):
                                        if mark_matched(
                                            candidate_id, local_id, score
                                        ):
                                            global_ids[local_id] = candidate_id
                                            verify_scores[local_id] = score
                                            verify_failures[local_id] = 0
                                            verify_successes[local_id] = 1
                                            galleries[local_id] = []
                                            last_gallery_frame.pop(local_id, None)

                                            quality_ok, quality = crop_quality(
                                                box, float(confidence),
                                                frame_width, frame_height
                                            )
                                            if quality_ok:
                                                try_add_gallery(
                                                    local_id,
                                                    current_embedding,
                                                    quality,
                                                    frame_index,
                                                    galleries,
                                                    last_gallery_frame,
                                                )
                                                best_capture_by_local_id[
                                                    local_id
                                                ] = current_crop.copy()
                                                best_capture_quality_by_local_id[
                                                    local_id
                                                ] = quality

                                            append_csv(
                                                MATCH_CSV,
                                                [
                                                    now_iso(),
                                                    local_id,
                                                    candidate_id,
                                                    f"{score:.6f}",
                                                    "MATCHED",
                                                ],
                                            )
                                            matched_reference = (
                                                candidate_reference(candidate_id)
                                            )
                                            matched_person_uid = (
                                                matched_reference.get("person_uid")
                                                if matched_reference
                                                else "UNKNOWN"
                                            )
                                            print("\n===== B Re-ID 매칭 성공 =====")
                                            print(f"Person UID : {matched_person_uid}")
                                            print(f"B Local ID : {local_id}")
                                            print(f"Journey ID : {candidate_id}")
                                            print(f"Similarity : {score:.6f}")
                                            print("Gallery 수집 시작")
                                            print("=============================")

                                        tentative_id.pop(local_id, None)
                                        tentative_count.pop(local_id, None)
                                else:
                                    tentative_id.pop(local_id, None)
                                    tentative_count.pop(local_id, None)

                        except Exception as error:
                            print(f"[B Re-ID 오류] Local={local_id}: {error}")

                    if reid_observation_valid and mqtt_client is not None:
                        detection_payload = stranger_gate.observe(
                            local_track_id=local_id,
                            observed_at=datetime.now().astimezone(),
                            is_unregistered=local_id not in global_ids,
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

                    journey_id = global_ids.get(local_id)
                    if journey_id is not None:
                        reference = candidate_reference(journey_id)
                        person_uid = (
                            reference.get("person_uid")
                            if reference
                            else "UNKNOWN"
                        )

                        if local_id in published_local_ids:
                            label = f"{person_uid} | PASSED"
                            sub = "A > [B] > D"
                            color = (255, 255, 0)
                        elif verify_failures.get(local_id, 0) > 0:
                            label = f"{person_uid} | VERIFYING"
                            sub = "A > [B] > D"
                            color = (0, 255, 255)
                        else:
                            label = f"{person_uid} | MATCHED"
                            sub = "A > [B] > D"
                            color = (0, 255, 0)
                    else:
                        elapsed = time.time() - first_seen[local_id]
                        temp = tentative_id.get(local_id)
                        score = best_scores.get(local_id, -1.0)

                        if temp:
                            temp_reference = candidate_reference(temp)
                            temp_person_uid = (
                                temp_reference.get("person_uid")
                                if temp_reference
                                else "UNKNOWN"
                            )
                            label = f"CHECKING: {temp_person_uid}"
                            sub = (
                                f"{score:.2f} "
                                f"{tentative_count.get(local_id, 0)}/"
                                f"{MATCH_CONFIRMATIONS}"
                            )
                            color = (0, 255, 255)
                        elif elapsed >= ANOMALY_DELAY_SECONDS:
                            label = "ANOMALY | STRANGER"
                            sub = (
                                f"BEST SCORE {score:.2f}"
                                if score >= 0
                                else "NO A ENTRY CANDIDATE"
                            )
                            color = (0, 0, 255)
                        else:
                            label = "STRANGER"
                            sub = "CHECKING RE-ID"
                            color = (0, 165, 255)

                    cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 3)
                    cv2.putText(
                        annotated, label, (x1, max(25, y1 - 32)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.65, color, 2
                    )
                    cv2.putText(
                        annotated, sub, (x1, max(50, y1 - 8)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.50, color, 2
                    )

            stale_ids = [
                local_id
                for local_id, seen_frame in last_seen.items()
                if frame_index - seen_frame > TRACK_LOST_GRACE_FRAMES
            ]

            for local_id in stale_ids:
                stranger_gate.remove_track(local_id)
                journey_id = global_ids.get(local_id)
                if journey_id:
                    release_candidate(journey_id, local_id, "TRACK_LOST")

                for mapping in (
                    first_seen, last_seen, histories, tentative_id,
                    tentative_count, best_scores, global_ids, verify_scores,
                    verify_failures, verify_successes, galleries,
                    last_gallery_frame, best_capture_by_local_id,
                    best_capture_quality_by_local_id,
                ):
                    mapping.pop(local_id, None)
                published_local_ids.discard(local_id)

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
        print("\nCamera B 종료")

    finally:
        cap.release()
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("Camera B MQTT 연결 종료")


if __name__ == "__main__":
    main()
