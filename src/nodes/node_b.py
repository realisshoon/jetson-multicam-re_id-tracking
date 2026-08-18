from __future__ import annotations

import csv
import hashlib
import json
import os
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
from src.common.model_requirements import require_model_files
from src.reid.reid_engine import ReIDTensorRTEngine


ROOT = Path(__file__).resolve().parents[2]
YOLO_MODEL = ROOT / "yolo26n.pt"
REID_ENGINE = ROOT / "models/reid/person_reid_osnet_x0_25_fp16.engine"

CAMERA_DEVICE = 0
WIDTH, HEIGHT, FPS = 1280, 720, 30
# WIDTH, HEIGHT, FPS = 640, 480, 15
OUTPUT_WIDTH, OUTPUT_HEIGHT = 1280, 720
WEB_PORT = 8001
FLIP_HORIZONTAL = True

NODE_ID = "B"
MQTT_CONFIG = load_mqtt_config()
MQTT_HOST = MQTT_CONFIG.host
MQTT_PORT = MQTT_CONFIG.port
MQTT_QOS = MQTT_CONFIG.qos
CANDIDATE_TOPIC = "cctv/candidates/b"
PASSAGE_TOPIC = "cctv/events/b/passage"

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
TEMPORAL_WINDOW_SIZE = 3
TEMPORAL_CANDIDATE_BANK_MAX = 6
OBSERVED_SAMPLE_HISTORY_MAX = 120
GALLERY_MIN_FRAME_GAP = 10
GALLERY_DUPLICATE_THRESHOLD = 0.999
PASSAGE_MIN_VERIFY_SUCCESSES = 2
PASSAGE_MIN_REID_SAMPLES = 2
B_PASSAGE_MIN_QUALITY = float(os.getenv("B_PASSAGE_MIN_QUALITY", "0.70"))
PASSAGE_MIN_BEST_SCORE = 0.75
PASSAGE_MIN_TOPK_SCORE = 0.68
PASSAGE_MIN_COMBINED_SCORE = 0.72
PASSAGE_MIN_CONSISTENT_COUNT = 2
PERSON_TOPK = 3
PERSON_BEST_WEIGHT = 0.45
PERSON_TOPK_WEIGHT = 0.55
PERSON_REVIEW_COMBINED_THRESHOLD = 0.72
WIRE_SCORE_TOLERANCE = 1e-6
DECISION_FORMULA_VERSION = "MAIN_WIRE_V1"

CANDIDATE_TIMEOUT_SECONDS = 300.0
ANOMALY_DELAY_SECONDS = 2.0
TRACK_LOST_GRACE_FRAMES = 20

LOG_DIR = ROOT / "logs"
REVISIT_RUN_ID = os.getenv(
    "REVISIT_RUN_ID", datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
)
REVISIT_LOG = LOG_DIR / "revisit" / REVISIT_RUN_ID / "camera_b_revisit.jsonl"
CANDIDATE_CSV = LOG_DIR / "node_b_candidates.csv"
MATCH_CSV = LOG_DIR / "node_b_matches.csv"
PASSAGE_CSV = LOG_DIR / "node_b_passages.csv"
PASSAGE_DIAGNOSTICS_JSONL = LOG_DIR / "node_b_passage_diagnostics.jsonl"

latest_jpeg: bytes | None = None
frame_lock = threading.Lock()
candidate_lock = threading.Lock()
revisit_log_lock = threading.Lock()
candidates: dict[str, dict[str, Any]] = {}
pending_passage_pubacks: dict[int, dict[str, Any]] = {}


def embedding_to_list(embedding: np.ndarray | list[float]) -> list[float]:
    return normalize(embedding).astype(np.float32).tolist()


def make_gallery_entry(
    node_id: str,
    embedding: np.ndarray | list[float],
    captured_at: str,
    quality: float | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "node_id": node_id,
        "captured_at": captured_at,
        "embedding_dim": 512,
        "embedding": embedding_to_list(embedding),
    }
    if quality is not None:
        item["quality"] = float(np.clip(quality, 0.0, 1.0))
    return item


def build_passage_payload(
    journey_id: str,
    person_uid: str,
    entry_timestamp: str,
    incoming_gallery: list[dict[str, Any]],
    b_embeddings: list[np.ndarray],
    a_local_track_id: int | str,
    b_local_track_id: int,
    b_passage_timestamp: str,
    selected_wire_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    gallery = [dict(item) for item in incoming_gallery]
    if len(selected_wire_samples) != len(b_embeddings):
        raise ValueError("B Gallery와 선택된 집계 표본 수가 일치하지 않습니다.")
    gallery.extend(
        make_gallery_entry(
            "B",
            embedding,
            b_passage_timestamp,
            quality=float(sample["quality"]),
        )
        for embedding, sample in zip(b_embeddings, selected_wire_samples)
    )
    if not b_embeddings:
        raise ValueError("B 특징값이 하나 이상 필요합니다.")
    diagnostics = calculate_gallery_diagnostics(gallery)
    return {
        "schema_version": 1,
        "event": "PASSAGE",
        "journey_id": journey_id,
        "person_uid": person_uid,
        "global_person_id": person_uid,
        "current_node": "B",
        "route": ["A", "B"],
        "next_nodes": ["D"],
        "entry_timestamp": entry_timestamp,
        "b_passage_timestamp": b_passage_timestamp,
        "a_local_track_id": a_local_track_id,
        "b_local_track_id": b_local_track_id,
        "gallery_count": len(gallery),
        "gallery": gallery,
        **diagnostics,
    }


def calculate_gallery_diagnostics(gallery: list[dict[str, Any]]) -> dict[str, Any]:
    """Recalculate Main's scores only from the embeddings that go on the wire."""
    a_entries = [item for item in gallery if item.get("node_id") == "A"]
    b_entries = [
        item for item in gallery
        if item.get("node_id") == "B"
        and float(item.get("quality", -1.0)) >= B_PASSAGE_MIN_QUALITY
    ]
    if not a_entries:
        raise ValueError("최종 payload에 Main A Gallery가 없습니다.")
    if len(b_entries) < PASSAGE_MIN_REID_SAMPLES:
        raise ValueError("최종 payload의 유효 B Gallery가 부족합니다.")

    a_embeddings = np.stack([
        np.asarray(item["embedding"], dtype=np.float32) for item in a_entries
    ])
    b_embeddings = np.stack([
        np.asarray(item["embedding"], dtype=np.float32) for item in b_entries
    ])
    if a_embeddings.shape[1:] != (512,) or b_embeddings.shape[1:] != (512,):
        raise ValueError("최종 payload Gallery embedding_dim은 512여야 합니다.")
    if not np.all(np.isfinite(a_embeddings)) or not np.all(np.isfinite(b_embeddings)):
        raise ValueError("최종 payload Gallery에 NaN/Inf가 있습니다.")

    matrix = b_embeddings @ a_embeddings.T
    per_frame_best = matrix.max(axis=1)
    flattened = np.sort(matrix.reshape(-1))[::-1]
    topk_values = flattened[:min(PERSON_TOPK, flattened.size)]
    best_score = float(flattened[0])
    topk_score = float(np.mean(topk_values))
    combined_score = (
        PERSON_BEST_WEIGHT * best_score
        + PERSON_TOPK_WEIGHT * topk_score
    )
    consistency_count = int(np.sum(
        per_frame_best >= PERSON_REVIEW_COMBINED_THRESHOLD
    ))
    qualities = [float(item["quality"]) for item in b_entries]
    final_quality = float(np.mean(qualities))
    return {
        "per_frame_best_scores": [float(value) for value in per_frame_best],
        "best_score": float(best_score),
        "topk_score": float(topk_score),
        "combined_score": float(combined_score),
        "multiframe_consistency": int(consistency_count),
        "consistency_count": int(consistency_count),
        "multiframe_consistency_ratio": float(consistency_count / len(b_entries)),
        "quality_samples": qualities,
        "final_quality": final_quality,
        "final_similarity": float(combined_score),
        "gallery_sample_count": len(b_entries),
    }


def validate_b_passage_evidence(
    gallery: list[dict[str, Any]],
) -> tuple[bool, str, dict[str, Any] | None]:
    """Apply the same evidence contract as Main before C publishes PASSAGE."""
    selected_entries = [
        item for item in gallery
        if item.get("node_id") == "B"
        and float(item.get("quality", -1.0)) >= B_PASSAGE_MIN_QUALITY
    ]
    if len(selected_entries) < PASSAGE_MIN_REID_SAMPLES:
        return False, "INSUFFICIENT_QUALITY", None
    diagnostics = calculate_gallery_diagnostics(gallery)
    if diagnostics["best_score"] < PASSAGE_MIN_BEST_SCORE:
        return False, "REJECTED_BEST_SCORE", diagnostics
    if diagnostics["topk_score"] < PASSAGE_MIN_TOPK_SCORE:
        return False, "REJECTED_TOPK_SCORE", diagnostics
    if diagnostics["combined_score"] < PASSAGE_MIN_COMBINED_SCORE:
        return False, "REJECTED_COMBINED_SCORE", diagnostics
    if diagnostics["consistency_count"] < PASSAGE_MIN_CONSISTENT_COUNT:
        return False, "REJECTED_CONSISTENCY", diagnostics
    return True, "ACCEPTED", diagnostics


def verify_wire_payload(
    payload: dict[str, Any],
) -> tuple[bool, dict[str, Any], dict[str, Any]]:
    """Round-trip the payload and enforce score/quality wire invariants."""
    wire_payload = json.loads(json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")
    ))
    recalculated = calculate_gallery_diagnostics(wire_payload["gallery"])
    float_invariants = {
        "combined_score": recalculated["combined_score"],
        "best_score": recalculated["best_score"],
        "topk_score": recalculated["topk_score"],
        "final_similarity": recalculated["combined_score"],
        "final_quality": recalculated["final_quality"],
        "quality": recalculated["final_quality"],
    }
    mismatches = {
        key: {"payload": wire_payload.get(key), "recalculated": expected}
        for key, expected in float_invariants.items()
        if not isinstance(wire_payload.get(key), (int, float))
        or abs(float(wire_payload[key]) - expected) >= WIRE_SCORE_TOLERANCE
    }
    if wire_payload.get("consistency_count") != recalculated["consistency_count"]:
        mismatches["consistency_count"] = {
            "payload": wire_payload.get("consistency_count"),
            "recalculated": recalculated["consistency_count"],
        }
    return not mismatches, wire_payload, mismatches


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


def make_reid_sample(
    embedding: np.ndarray,
    incoming_gallery: list[dict[str, Any]],
    quality: float,
    frame_index: int,
    gallery_selected: bool,
) -> dict[str, Any]:
    if not incoming_gallery:
        raise ValueError("Re-ID score를 계산할 incoming gallery가 없습니다.")
    best_score = max(
        similarity(embedding, normalize(item["embedding"]))
        for item in incoming_gallery
    )
    return {
        "frame_index": int(frame_index),
        "best_score": float(best_score),
        "quality": float(quality),
        "gallery_selected": bool(gallery_selected),
    }


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


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        file.write("\n")


def log_revisit_event(
    event: str,
    *,
    request_id: str | None = None,
    journey_id: str | None = None,
    person_uid: str | None = None,
    local_track_id: int | None = None,
    **fields: Any,
) -> None:
    record = {
        "at": now_iso(),
        "run_id": REVISIT_RUN_ID,
        "event": event,
        "node": "B",
        "request_id": request_id,
        "journey_id": journey_id,
        "person_uid": person_uid,
        "local_track_id": local_track_id,
        **fields,
    }
    with revisit_log_lock:
        REVISIT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with REVISIT_LOG.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
            file.write("\n")


def save_candidate(payload: dict[str, Any]) -> None:
    """
    Main Server가 C에 전달한 CANDIDATE 메시지를 저장한다.

    예상 구조:
    - event: CANDIDATE
    - stage: WAITING_B_OR_C
    - journey_id: J000001
    - person_uid: P000001
    - gallery: A 특징값
    """

    journey_id = payload.get("journey_id")
    person_uid = payload.get("person_uid", "")
    request_id = payload.get("request_id")
    stage = payload.get("stage")
    gallery = payload.get("gallery", [])

    sample_metadata: list[dict[str, Any]] = []
    if isinstance(gallery, list):
        for item in gallery:
            embedding = item.get("embedding") if isinstance(item, dict) else None
            embedding_dim = len(embedding) if isinstance(embedding, list) else None
            norm: float | None = None
            if isinstance(embedding, list):
                try:
                    array = np.asarray(embedding, dtype=np.float32)
                    if np.all(np.isfinite(array)):
                        norm = float(np.linalg.norm(array))
                except (TypeError, ValueError):
                    pass
            sample_metadata.append({
                "embedding_dim": embedding_dim,
                "norm": norm,
                "quality": item.get("quality") if isinstance(item, dict) else None,
            })
    log_revisit_event(
        "B_CANDIDATE_RECEIVED",
        request_id=request_id,
        journey_id=journey_id,
        person_uid=person_uid or None,
        gallery_count=len(gallery) if isinstance(gallery, list) else 0,
        embedding_dim=(sample_metadata[0]["embedding_dim"] if sample_metadata else None),
        samples=sample_metadata,
    )

    if (
        payload.get("event") != "CANDIDATE"
        or not journey_id
        or not person_uid
        or stage != "WAITING_B_OR_C"
        or not isinstance(gallery, list)
        or not gallery
    ):
        log_revisit_event(
            "B_CANDIDATE_ACTIVATED",
            request_id=request_id,
            journey_id=journey_id,
            person_uid=person_uid or None,
            activated=False,
            reason="INVALID_CANDIDATE_PAYLOAD",
        )
        print("[B MQTT] 잘못된 Main 후보 메시지")
        return

    validated_gallery: list[dict[str, Any]] = []
    gallery_embeddings: list[np.ndarray] = []
    try:
        for item in gallery:
            if not isinstance(item, dict) or not isinstance(item.get("embedding"), list):
                raise ValueError("Gallery 항목 형식이 잘못됐습니다.")
            if item.get("embedding_dim", 512) != 512:
                raise ValueError("Gallery embedding_dim은 512여야 합니다.")
            normalized = normalize(item["embedding"])
            normalized_item = dict(item)
            normalized_item["embedding_dim"] = 512
            normalized_item["embedding"] = normalized.astype(np.float32).tolist()
            validated_gallery.append(normalized_item)
            gallery_embeddings.append(normalized)
    except (TypeError, ValueError) as error:
        log_revisit_event(
            "B_CANDIDATE_ACTIVATED",
            request_id=request_id,
            journey_id=journey_id,
            person_uid=person_uid,
            activated=False,
            reason="INVALID_GALLERY",
            detail=str(error),
        )
        print(
            f"[B MQTT] {journey_id}: {error}"
        )
        return

    entry_timestamp = payload.get(
        "entry_timestamp",
        validated_gallery[0].get(
            "captured_at",
            now_iso(),
        ),
    )

    candidate = {
        "journey_id": journey_id,
        "request_id": request_id,
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

        "embedding": gallery_embeddings[0],
        "gallery_embeddings": gallery_embeddings,
        "incoming_gallery": validated_gallery,

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
            log_revisit_event(
                "B_CANDIDATE_ACTIVATED",
                request_id=request_id,
                journey_id=journey_id,
                person_uid=person_uid,
                activated=False,
                reason="DUPLICATE_ACTIVE_CANDIDATE",
            )
            print(
                f"[B 중복 후보 무시] "
                f"{journey_id}"
            )
            return

        candidates[journey_id] = candidate

    log_revisit_event(
        "B_CANDIDATE_ACTIVATED",
        request_id=request_id,
        journey_id=journey_id,
        person_uid=person_uid,
        activated=True,
        reason="REGISTERED",
    )

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
    print(f"Gallery Count : {len(gallery_embeddings)}")
    print(f"Embedding Dim : {gallery_embeddings[0].size}")
    print(
        f"Norm          : "
        f"{np.linalg.norm(gallery_embeddings[0]):.6f}"
    )
    print("============================")


def cleanup_candidates() -> None:
    now = time.time()
    with candidate_lock:
        for item in candidates.values():
            if item["status"] == "PENDING":
                if now - item["entry_epoch"] > CANDIDATE_TIMEOUT_SECONDS:
                    item["status"] = "EXPIRED"


def pending_candidates() -> list[tuple[str, list[np.ndarray]]]:
    cleanup_candidates()
    with candidate_lock:
        return [
            (
                item["journey_id"],
                [embedding.copy() for embedding in item["gallery_embeddings"]],
            )
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
            "request_id": item.get("request_id"),
            "person_uid": item["person_uid"],
            "person_status": item.get("person_status", "UNKNOWN"),
            "visit_count": item.get("visit_count"),
            "previous_last_seen_at": item.get("previous_last_seen_at"),
            "candidate_person_uid": item.get("candidate_person_uid"),
            "route": list(item.get("route", ["A"])),
            "entry_timestamp": item["entry_timestamp"],
            "a_local_id": item["a_local_id"],
            "embedding": item["embedding"].copy(),
            "incoming_gallery": [dict(entry) for entry in item["incoming_gallery"]],
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


def mark_track_rejected(journey_id: str, local_id: int, reason: str) -> None:
    request_id: str | None = None
    person_uid: str | None = None
    with candidate_lock:
        item = candidates.get(journey_id)
        if item is None or item.get("matched_b_local_id") not in {None, local_id}:
            return
        request_id = item.get("request_id")
        person_uid = item.get("person_uid")
        item["status"] = reason
        item["matched_b_local_id"] = None
        item["match_score"] = None
    log_revisit_event(
        "B_TRACK_CLEANUP",
        request_id=request_id,
        journey_id=journey_id,
        person_uid=person_uid,
        local_track_id=local_id,
        reason="TRACK_LOST",
        final_status=reason,
    )
    print("\n===== B Track 최종 판정 =====")
    print(f"Journey ID: {journey_id}")
    print(f"B Local ID: {local_id}")
    print(f"Status    : {reason}")
    print("============================")


def release_candidate(journey_id: str, local_id: int, reason: str) -> None:
    request_id: str | None = None
    person_uid: str | None = None
    final_status = "UNKNOWN"
    with candidate_lock:
        item = candidates.get(journey_id)
        if item is None:
            return
        request_id = item.get("request_id")
        person_uid = item.get("person_uid")
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
        final_status = item["status"]

    log_revisit_event(
        "B_TRACK_CLEANUP",
        request_id=request_id,
        journey_id=journey_id,
        person_uid=person_uid,
        local_track_id=local_id,
        reason=reason,
        final_status=final_status,
    )

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

    scores = [
        (jid, max(similarity(b_embedding, emb) for emb in gallery))
        for jid, gallery in pending
    ]
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
    if quality < B_PASSAGE_MIN_QUALITY:
        print(
            f"[B Gallery 저품질 폐기] Local={local_id}, "
            f"Quality={quality:.3f}, Required={B_PASSAGE_MIN_QUALITY:.3f}"
        )
        return False
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


def add_temporal_candidate(
    embedding: np.ndarray,
    quality: float,
    frame_index: int,
    incoming_gallery: list[dict[str, Any]],
    window: list[dict[str, Any]],
    candidate_bank: list[dict[str, Any]],
) -> bool:
    """Add an observation and finalize one non-overlapping temporal window."""
    if quality < B_PASSAGE_MIN_QUALITY:
        return False
    if any(item["frame_index"] == frame_index for item in window):
        return False
    window.append({
        "embedding": normalize(embedding).copy(),
        "quality": float(quality),
        "frame_index": int(frame_index),
    })
    if len(window) < TEMPORAL_WINDOW_SIZE:
        return False

    observations = window[:TEMPORAL_WINDOW_SIZE]
    del window[:TEMPORAL_WINDOW_SIZE]
    mean_embedding = normalize(np.mean(np.stack([
        item["embedding"] for item in observations
    ]), axis=0))
    mean_quality = float(np.mean([item["quality"] for item in observations]))
    best_score = max(
        similarity(mean_embedding, normalize(item["embedding"]))
        for item in incoming_gallery
    )
    candidate_bank.append({
        "embedding": mean_embedding,
        "quality": mean_quality,
        "best_score": float(best_score),
        "frame_index": observations[-1]["frame_index"],
        "window_start_frame": observations[0]["frame_index"],
        "window_end_frame": observations[-1]["frame_index"],
        "gallery_selected": True,
    })
    candidate_bank.sort(key=lambda item: item["best_score"], reverse=True)
    del candidate_bank[TEMPORAL_CANDIDATE_BANK_MAX:]
    return True


def selected_temporal_candidates(
    candidate_bank: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return candidate_bank[:B_GALLERY_TARGET]


def temporal_candidate_diagnostics(
    incoming_gallery: list[dict[str, Any]],
    selected_candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if len(selected_candidates) < B_GALLERY_TARGET:
        return None
    gallery = [dict(item) for item in incoming_gallery]
    gallery.extend(
        make_gallery_entry(
            "B", item["embedding"], now_iso(), quality=item["quality"]
        )
        for item in selected_candidates
    )
    return calculate_gallery_diagnostics(gallery)


def finalize_partial_temporal_window(
    incoming_gallery: list[dict[str, Any]],
    window: list[dict[str, Any]],
    candidate_bank: list[dict[str, Any]],
) -> bool:
    """Finalize remaining valid observations in temporal window into a candidate."""
    valid_observations = [
        item for item in window
        if float(item.get("quality", 0.0)) >= B_PASSAGE_MIN_QUALITY
    ]
    window.clear()
    if not valid_observations:
        return False
    mean_embedding = normalize(np.mean(np.stack([
        item["embedding"] for item in valid_observations
    ]), axis=0))
    mean_quality = float(np.mean([item["quality"] for item in valid_observations]))
    best_score = max(
        similarity(mean_embedding, normalize(item["embedding"]))
        for item in incoming_gallery
    )
    candidate_bank.append({
        "embedding": mean_embedding,
        "quality": mean_quality,
        "best_score": float(best_score),
        "frame_index": valid_observations[-1]["frame_index"],
        "window_start_frame": valid_observations[0]["frame_index"],
        "window_end_frame": valid_observations[-1]["frame_index"],
        "gallery_selected": True,
    })
    candidate_bank.sort(key=lambda item: item["best_score"], reverse=True)
    del candidate_bank[TEMPORAL_CANDIDATE_BANK_MAX:]
    return True


def promote_confirmation_observations(
    candidate_id: str,
    incoming_gallery: list[dict[str, Any]],
    confirmation_seeds: list[dict[str, Any]],
    temporal_window: list[dict[str, Any]],
    temporal_candidate_bank: list[dict[str, Any]],
    observed_samples: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    """Promote high-quality confirmation observations matching candidate_id as seed evidence."""
    promoted: list[dict[str, Any]] = []
    any_completed = False
    seen_frame_indices: set[int] = set()
    for seed in confirmation_seeds:
        frame_index = int(seed.get("frame_index", -1))
        if (
            seed.get("candidate_id") != candidate_id
            or float(seed.get("quality", 0.0)) < B_PASSAGE_MIN_QUALITY
            or frame_index < 0
            or frame_index in seen_frame_indices
        ):
            continue
        seen_frame_indices.add(frame_index)
        embedding = seed["embedding"]
        quality = float(seed["quality"])
        frame_index = int(seed["frame_index"])
        observed_sample = make_reid_sample(
            embedding,
            incoming_gallery,
            quality,
            frame_index,
            False,
        )
        observed_samples.append(observed_sample)
        completed = add_temporal_candidate(
            embedding,
            quality,
            frame_index,
            incoming_gallery,
            temporal_window,
            temporal_candidate_bank,
        )
        if completed:
            any_completed = True
        promoted.append(seed)
    return promoted, any_completed


def publish_passage(
    client: mqtt.Client,
    local_id: int,
    journey_id: str,
    gallery: list[np.ndarray],
    capture_crop: np.ndarray,
    capture_quality: float,
    match_score: float,
    observed_samples: list[dict[str, Any]],
    selected_wire_samples: list[dict[str, Any]],
    *,
    rejection_is_final: bool = False,
) -> bool:
    reference = candidate_reference(journey_id)
    if reference is None:
        log_revisit_event(
            "B_PASSAGE_DECISION",
            journey_id=journey_id,
            local_track_id=local_id,
            status="FAILED",
            reason="REFERENCE_NOT_FOUND",
            valid_samples=0,
            qualities=[],
        )
        print(f"[B PASSAGE 실패] {journey_id}: A 후보 정보 없음")
        return False

    person_uid = str(reference.get("person_uid") or "UNKNOWN")
    request_id = reference.get("request_id")
    passage_at = now_iso()

    if len(gallery) < PASSAGE_MIN_REID_SAMPLES or len(selected_wire_samples) < PASSAGE_MIN_REID_SAMPLES:
        selected_qualities = [float(sample.get("quality", 0.0)) for sample in selected_wire_samples]
        log_revisit_event(
            "B_PASSAGE_DECISION",
            request_id=request_id,
            journey_id=journey_id,
            person_uid=person_uid,
            local_track_id=local_id,
            status="REJECTED" if rejection_is_final else "COLLECTING",
            reason="INSUFFICIENT_QUALITY",
            valid_samples=len(selected_qualities),
            qualities=selected_qualities,
        )
        return False

    payload = build_passage_payload(
        journey_id=journey_id,
        person_uid=person_uid,
        entry_timestamp=reference["entry_timestamp"],
        incoming_gallery=reference["incoming_gallery"],
        b_embeddings=gallery[:B_GALLERY_MAX],
        a_local_track_id=reference["a_local_id"],
        b_local_track_id=local_id,
        b_passage_timestamp=passage_at,
        selected_wire_samples=selected_wire_samples,
    )

    accepted, reason, diagnostics = validate_b_passage_evidence(payload["gallery"])
    selected_qualities = [
        float(item["quality"])
        for item in payload["gallery"]
        if item.get("node_id") == "B"
    ]
    if not accepted:
        log_revisit_event(
            "B_PASSAGE_DECISION",
            request_id=request_id,
            journey_id=journey_id,
            person_uid=person_uid,
            local_track_id=local_id,
            status="REJECTED" if rejection_is_final else "COLLECTING",
            reason=reason,
            valid_samples=len(selected_qualities),
            qualities=selected_qualities,
        )
        if rejection_is_final:
            append_jsonl(PASSAGE_DIAGNOSTICS_JSONL, {
                "logged_at": now_iso(), "topic": PASSAGE_TOPIC,
                "journey_id": journey_id, "status": reason,
                "observed_samples": observed_samples, "payload": payload,
            })
            print(f"[B PASSAGE 최종 거부] {journey_id}: {reason}")
        return False
    if diagnostics is None:
        raise RuntimeError("B PASSAGE 진단값이 없습니다.")

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

    # Main Server의 Capture/관리자 DB 기록용 추가 필드
    payload["person_uid"] = person_uid
    payload["local_track_id"] = local_id
    payload["capture_path"] = capture_path
    payload["similarity"] = float(match_score)
    payload["quality"] = float(payload["final_quality"])
    payload["verification_status"] = "AUTO_MATCHED"

    wire_ok, wire_payload, mismatches = verify_wire_payload(payload)
    if not wire_ok:
        log_revisit_event(
            "B_PASSAGE_DECISION",
            request_id=request_id,
            journey_id=journey_id,
            person_uid=person_uid,
            local_track_id=local_id,
            status="FAILED",
            reason="LOCAL_WIRE_SCORE_MISMATCH",
            valid_samples=len(selected_qualities),
            qualities=selected_qualities,
        )
        append_jsonl(PASSAGE_DIAGNOSTICS_JSONL, {
            "logged_at": now_iso(), "topic": PASSAGE_TOPIC,
            "journey_id": journey_id,
            "status": "LOCAL_WIRE_SCORE_MISMATCH",
            "mismatches": mismatches,
            "observed_samples": observed_samples,
            "payload": wire_payload,
        })
        print(
            f"[B PASSAGE 발행 금지] {journey_id}: "
            f"LOCAL_WIRE_SCORE_MISMATCH {mismatches}"
        )
        return False
    payload = wire_payload

    log_revisit_event(
        "B_PASSAGE_DECISION",
        request_id=request_id,
        journey_id=journey_id,
        person_uid=person_uid,
        local_track_id=local_id,
        status="PASSED",
        reason="ACCEPTED",
        valid_samples=len(selected_qualities),
        qualities=selected_qualities,
    )

    print(
        "[B PASSAGE 발행 직전] "
        f"journey_id={journey_id}, valid_samples={len(selected_qualities)}, "
        f"qualities={selected_qualities}, best={diagnostics['best_score']:.6f}, "
        f"topk={diagnostics['topk_score']:.6f}, "
        f"combined={diagnostics['combined_score']:.6f}, "
        f"final_quality={diagnostics['final_quality']:.6f}"
    )

    append_jsonl(
        PASSAGE_DIAGNOSTICS_JSONL,
        {
            "logged_at": now_iso(), "topic": PASSAGE_TOPIC,
            "observed_samples": observed_samples, "payload": payload,
        },
    )

    info = client.publish(
        PASSAGE_TOPIC,
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        qos=MQTT_QOS,
        retain=False,
    )
    rc = int(info.rc)
    mid = int(info.mid)
    publish_context = {
        "request_id": request_id,
        "journey_id": journey_id,
        "person_uid": person_uid,
        "local_track_id": local_id,
        "topic": PASSAGE_TOPIC,
        "rc": rc,
    }
    log_revisit_event(
        "B_PASSAGE_PUBLISH",
        **publish_context,
        mid=mid,
        puback=False,
    )
    if rc == mqtt.MQTT_ERR_SUCCESS:
        pending_passage_pubacks[mid] = publish_context
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


def on_publish(client, userdata, mid, reason_code, properties) -> None:
    context = pending_passage_pubacks.pop(int(mid), {})
    puback_reason = getattr(reason_code, "value", reason_code)
    log_revisit_event(
        "B_PASSAGE_PUBLISH",
        request_id=context.get("request_id"),
        journey_id=context.get("journey_id"),
        person_uid=context.get("person_uid"),
        local_track_id=context.get("local_track_id"),
        topic=context.get("topic", PASSAGE_TOPIC),
        rc=context.get("rc"),
        mid=int(mid),
        puback=True,
        puback_reason=int(puback_reason),
    )


def create_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="camera-b",
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
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


def center_crop_resize(
    frame: np.ndarray,
) -> np.ndarray:
    """비율을 유지하면서 중앙 crop하여 Camera A 송출 크기로 맞춘다."""

    source_height, source_width = frame.shape[:2]
    if source_width <= 0 or source_height <= 0:
        raise ValueError("출력할 프레임 크기가 올바르지 않습니다.")

    source_ratio = source_width / source_height
    output_ratio = OUTPUT_WIDTH / OUTPUT_HEIGHT
    if source_ratio > output_ratio:
        crop_width = max(1, round(source_height * output_ratio))
        crop_left = (source_width - crop_width) // 2
        cropped = frame[:, crop_left:crop_left + crop_width]
    else:
        crop_height = max(1, round(source_width / output_ratio))
        crop_top = (source_height - crop_height) // 2
        cropped = frame[crop_top:crop_top + crop_height, :]

    interpolation = (
        cv2.INTER_AREA
        if cropped.shape[1] > OUTPUT_WIDTH or cropped.shape[0] > OUTPUT_HEIGHT
        else cv2.INTER_LINEAR
    )
    return cv2.resize(
        cropped,
        (OUTPUT_WIDTH, OUTPUT_HEIGHT),
        interpolation=interpolation,
    )


def build_placeholder(message: str = "CAMERA DISCONNECTED") -> np.ndarray:
    """연결 여부와 무관하게 스트림 규격을 유지하는 전체 화면 Placeholder."""

    frame = np.full((OUTPUT_HEIGHT, OUTPUT_WIDTH, 3), (18, 22, 28), np.uint8)
    cv2.putText(
        frame, "CAMERA B", (48, 72), cv2.FONT_HERSHEY_SIMPLEX,
        1.15, (255, 255, 255), 2,
    )
    text_size = cv2.getTextSize(message, cv2.FONT_HERSHEY_SIMPLEX, 1.0, 2)[0]
    origin = ((OUTPUT_WIDTH - text_size[0]) // 2, OUTPUT_HEIGHT // 2)
    cv2.putText(
        frame, message, origin, cv2.FONT_HERSHEY_SIMPLEX,
        1.0, (90, 170, 255), 2,
    )
    cv2.putText(
        frame, "Waiting for camera connection...", (origin[0] + 25, origin[1] + 42),
        cv2.FONT_HERSHEY_SIMPLEX, 0.55, (175, 185, 195), 1,
    )
    return frame


def publish_frame(frame: np.ndarray) -> None:
    global latest_jpeg

    encoded, buffer = cv2.imencode(
        ".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80]
    )
    if encoded:
        with frame_lock:
            latest_jpeg = buffer.tobytes()


def draw_stream_hud(frame: np.ndarray) -> None:
    """Camera A와 동일한 크기의 최소 LIVE HUD를 그린다."""

    overlay = frame.copy()
    cv2.rectangle(overlay, (18, 18), (194, 56), (20, 22, 26), -1)
    cv2.addWeighted(overlay, 0.78, frame, 0.22, 0, frame)
    cv2.circle(frame, (33, 37), 6, (75, 75, 255), -1, cv2.LINE_AA)
    cv2.putText(
        frame, "LIVE  |  CAM B", (48, 44), cv2.FONT_HERSHEY_SIMPLEX,
        0.55, (235, 235, 235), 1, cv2.LINE_AA,
    )


def build_output_frame(
    annotated_inference_frame: np.ndarray,
) -> np.ndarray:
    """추론 annotation을 고정 stream frame으로 변환하고 최소 HUD만 합성한다."""

    # Box와 라벨은 추론 frame에 먼저 그린 뒤 영상과 함께 동일 변환한다.
    # 따라서 resize/crop 후에도 좌표가 영상의 객체와 어긋나지 않는다.
    output = center_crop_resize(annotated_inference_frame)
    draw_stream_hud(output)
    return output

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

    node_b_path = Path(__file__).resolve()
    node_b_sha256 = hashlib.sha256(node_b_path.read_bytes()).hexdigest()
    print(f"node_b.py path={node_b_path}")
    print(f"node_b.py sha256={node_b_sha256}")
    print(f'decision_formula_version="{DECISION_FORMULA_VERSION}"')
    print(f"B_PASSAGE_MIN_QUALITY={B_PASSAGE_MIN_QUALITY:.2f}")

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

    cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, FPS)

    first_seen: dict[int, float] = {}
    last_seen: dict[int, int] = {}
    histories: dict[int, deque[np.ndarray]] = {}

    tentative_id: dict[int, str] = {}
    tentative_count: dict[int, int] = {}
    tentative_observations: dict[int, list[dict[str, Any]]] = {}
    best_scores: dict[int, float] = {}

    global_ids: dict[int, str] = {}
    verify_scores: dict[int, float] = {}
    verify_failures: dict[int, int] = {}
    verify_successes: dict[int, int] = {}

    galleries: dict[int, list[np.ndarray]] = {}
    observed_samples: dict[int, list[dict[str, Any]]] = {}
    selected_wire_samples: dict[int, list[dict[str, Any]]] = {}
    temporal_windows: dict[int, list[dict[str, Any]]] = {}
    temporal_candidate_banks: dict[int, list[dict[str, Any]]] = {}
    last_collection_scores: dict[int, tuple[float, float, float, int]] = {}
    last_gallery_frame: dict[int, int] = {}
    published_local_ids: set[int] = set()

    # PASSAGE 시 저장할 가장 품질 좋은 B Crop
    best_capture_by_local_id: dict[int, np.ndarray] = {}
    best_capture_quality_by_local_id: dict[int, float] = {}

    frame_index = 0
    mqtt_client: mqtt.Client | None = None

    try:
        mqtt_client = create_mqtt_client()
        publish_frame(build_placeholder())
        threading.Thread(target=start_web, daemon=True).start()

        print("GPU:", torch.cuda.get_device_name(0))
        print("Camera B Re-ID + Gallery 시작")
        print(f"카메라         : /dev/video{CAMERA_DEVICE}")
        print(f"웹 포트        : {WEB_PORT}")
        print(f"B Gallery 목표 : {B_GALLERY_TARGET}")
        print(f"B -> Main 토픽  : {PASSAGE_TOPIC}")
        print(f"Capture 저장    : {CAPTURE_ROOT}")
        print(
            f"밝기 보정       : "
            f"alpha={IMAGE_CONTRAST_ALPHA}, "
            f"beta={IMAGE_BRIGHTNESS_BETA}"
        )
        print("종료            : Ctrl + C")

        while True:
            if not cap.isOpened():
                publish_frame(build_placeholder())
                time.sleep(1.0)
                cap.open(CAMERA_DEVICE, cv2.CAP_V4L2)
                if cap.isOpened():
                    cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))
                    cap.set(cv2.CAP_PROP_FRAME_WIDTH, WIDTH)
                    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, HEIGHT)
                    cap.set(cv2.CAP_PROP_FPS, FPS)
                continue

            ok, frame = cap.read()
            if not ok:
                print("Camera B 프레임 읽기 실패")
                publish_frame(build_placeholder())
                cap.release()
                continue

            frame_index += 1
            if FLIP_HORIZONTAL:
                frame = cv2.flip(frame, 1)

            frame = apply_small_brightness_adjustment(
                frame
            )
            frame_height, frame_width = frame.shape[:2]

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

            # 추론은 camera 원본 frame에서 수행하고, UI frame은 별도로 합성한다.
            annotated = frame.copy()

            if result.boxes is not None and result.boxes.id is not None:
                local_ids = result.boxes.id.int().cpu().tolist()
                boxes = result.boxes.xyxy.int().cpu().tolist()
                confs = result.boxes.conf.cpu().tolist()

                for local_id, box, confidence in zip(local_ids, boxes, confs):
                    if local_id not in first_seen:
                        first_seen[local_id] = time.time()
                        log_revisit_event(
                            "B_TRACK_DETECTED",
                            local_track_id=local_id,
                            first_observed_at=now_iso(),
                        )
                    last_seen[local_id] = frame_index
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
                                        window_completed = False
                                        if quality_ok:
                                            observed_sample = make_reid_sample(
                                                current_embedding,
                                                reference["incoming_gallery"],
                                                quality,
                                                frame_index,
                                                False,
                                            )
                                            observed_samples.setdefault(local_id, []).append(
                                                observed_sample
                                            )
                                            del observed_samples[local_id][
                                                :-OBSERVED_SAMPLE_HISTORY_MAX
                                            ]
                                            window_completed = add_temporal_candidate(
                                                current_embedding,
                                                quality,
                                                frame_index,
                                                reference["incoming_gallery"],
                                                temporal_windows.setdefault(local_id, []),
                                                temporal_candidate_banks.setdefault(local_id, []),
                                            )
                                            if window_completed:
                                                selected = selected_temporal_candidates(
                                                    temporal_candidate_banks[local_id]
                                                )
                                                galleries[local_id] = [
                                                    item["embedding"] for item in selected
                                                ]
                                                selected_wire_samples[local_id] = selected
                                                diagnostics = temporal_candidate_diagnostics(
                                                    reference["incoming_gallery"], selected
                                                )
                                                if diagnostics is not None:
                                                    log_revisit_event(
                                                        "B_MATCH_WINDOW",
                                                        request_id=reference.get("request_id"),
                                                        journey_id=journey_id,
                                                        person_uid=reference.get("person_uid"),
                                                        local_track_id=local_id,
                                                        best=diagnostics["best_score"],
                                                        topk=diagnostics["topk_score"],
                                                        combined=diagnostics["combined_score"],
                                                        consistent_windows=diagnostics[
                                                            "consistency_count"
                                                        ],
                                                        thresholds={
                                                            "quality": B_PASSAGE_MIN_QUALITY,
                                                            "best": PASSAGE_MIN_BEST_SCORE,
                                                            "topk": PASSAGE_MIN_TOPK_SCORE,
                                                            "combined": PASSAGE_MIN_COMBINED_SCORE,
                                                            "consistent_windows": (
                                                                PASSAGE_MIN_CONSISTENT_COUNT
                                                            ),
                                                        },
                                                    )
                                                    scores = (
                                                        diagnostics["best_score"],
                                                        diagnostics["topk_score"],
                                                        diagnostics["combined_score"],
                                                        diagnostics["consistency_count"],
                                                    )
                                                    previous = last_collection_scores.get(local_id)
                                                    if previous is None or scores[2] > previous[2] + 1e-6:
                                                        print(
                                                            f"[B COLLECTING] Journey={journey_id}, "
                                                            f"Windows={len(temporal_candidate_banks[local_id])}, "
                                                            f"Best={scores[0]:.6f}, TopK={scores[1]:.6f}, "
                                                            f"Combined={scores[2]:.6f}, Consistent={scores[3]}"
                                                        )
                                                        last_collection_scores[local_id] = scores

                                            previous_quality = (
                                                best_capture_quality_by_local_id.get(
                                                    local_id,
                                                    -1.0,
                                                )
                                            )
                                            if quality >= B_PASSAGE_MIN_QUALITY and quality > previous_quality:
                                                best_capture_by_local_id[
                                                    local_id
                                                ] = current_crop.copy()
                                                best_capture_quality_by_local_id[
                                                    local_id
                                                ] = quality

                                        ready = (
                                            local_id not in published_local_ids
                                            and window_completed
                                            and len(galleries.get(local_id, []))
                                            >= B_GALLERY_TARGET
                                            and verify_successes.get(local_id, 0)
                                            >= PASSAGE_MIN_VERIFY_SUCCESSES
                                            and len(selected_wire_samples.get(local_id, []))
                                            >= PASSAGE_MIN_REID_SAMPLES
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
                                                observed_samples.get(local_id, []),
                                                selected_wire_samples.get(local_id, []),
                                                rejection_is_final=False,
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
                                            tentative_observations.pop(local_id, None)
                                            best_scores.pop(local_id, None)
                                            galleries.pop(local_id, None)
                                            observed_samples.pop(local_id, None)
                                            selected_wire_samples.pop(local_id, None)
                                            temporal_windows.pop(local_id, None)
                                            temporal_candidate_banks.pop(local_id, None)
                                            last_collection_scores.pop(local_id, None)
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
                                        tentative_observations[local_id] = []

                                    quality_ok, quality = crop_quality(
                                        box, float(confidence),
                                        frame_width, frame_height
                                    )
                                    if quality_ok and quality >= B_PASSAGE_MIN_QUALITY:
                                        tentative_observations.setdefault(local_id, []).append({
                                            "embedding": current_embedding.copy(),
                                            "quality": float(quality),
                                            "frame_index": int(frame_index),
                                            "crop": current_crop.copy(),
                                            "candidate_id": str(candidate_id),
                                            "score": float(score),
                                        })

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
                                            observed_samples[local_id] = []
                                            selected_wire_samples[local_id] = []
                                            temporal_windows[local_id] = []
                                            temporal_candidate_banks[local_id] = []
                                            last_collection_scores.pop(local_id, None)
                                            last_gallery_frame.pop(local_id, None)

                                            matched_reference = candidate_reference(
                                                candidate_id
                                            )
                                            if matched_reference is None:
                                                raise RuntimeError(
                                                    "매칭 직후 candidate reference가 없습니다."
                                                )

                                            raw_seeds = tentative_observations.pop(local_id, [])
                                            promoted_seeds, any_completed = promote_confirmation_observations(
                                                candidate_id,
                                                matched_reference["incoming_gallery"],
                                                raw_seeds,
                                                temporal_windows[local_id],
                                                temporal_candidate_banks[local_id],
                                                observed_samples[local_id],
                                            )

                                            for seed in promoted_seeds:
                                                seed_crop = seed.get("crop")
                                                seed_quality = float(seed.get("quality", 0.0))
                                                if seed_crop is not None and seed_quality >= B_PASSAGE_MIN_QUALITY:
                                                    prev_q = best_capture_quality_by_local_id.get(
                                                        local_id, -1.0
                                                    )
                                                    if seed_quality > prev_q:
                                                        best_capture_by_local_id[
                                                            local_id
                                                        ] = seed_crop.copy()
                                                        best_capture_quality_by_local_id[
                                                            local_id
                                                        ] = seed_quality

                                            if any_completed:
                                                selected = selected_temporal_candidates(
                                                    temporal_candidate_banks[local_id]
                                                )
                                                galleries[local_id] = [
                                                    item["embedding"] for item in selected
                                                ]
                                                selected_wire_samples[local_id] = selected
                                                diagnostics = temporal_candidate_diagnostics(
                                                    matched_reference["incoming_gallery"],
                                                    selected,
                                                )
                                                if diagnostics is not None:
                                                    log_revisit_event(
                                                        "B_MATCH_WINDOW",
                                                        request_id=matched_reference.get(
                                                            "request_id"
                                                        ),
                                                        journey_id=candidate_id,
                                                        person_uid=matched_reference.get(
                                                            "person_uid"
                                                        ),
                                                        local_track_id=local_id,
                                                        best=diagnostics["best_score"],
                                                        topk=diagnostics["topk_score"],
                                                        combined=diagnostics[
                                                            "combined_score"
                                                        ],
                                                        consistent_windows=diagnostics[
                                                            "consistency_count"
                                                        ],
                                                        thresholds={
                                                            "quality": B_PASSAGE_MIN_QUALITY,
                                                            "best": PASSAGE_MIN_BEST_SCORE,
                                                            "topk": PASSAGE_MIN_TOPK_SCORE,
                                                            "combined": (
                                                                PASSAGE_MIN_COMBINED_SCORE
                                                            ),
                                                            "consistent_windows": (
                                                                PASSAGE_MIN_CONSISTENT_COUNT
                                                            ),
                                                        },
                                                    )

                                            del observed_samples[local_id][
                                                :-OBSERVED_SAMPLE_HISTORY_MAX
                                            ]

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
                                            print(f"Seeds Reused: {len(promoted_seeds)}")
                                            print("Gallery 수집 시작")
                                            print("=============================")

                                        tentative_id.pop(local_id, None)
                                        tentative_count.pop(local_id, None)
                                        tentative_observations.pop(local_id, None)
                                else:
                                    tentative_id.pop(local_id, None)
                                    tentative_count.pop(local_id, None)
                                    tentative_observations.pop(local_id, None)

                        except Exception as error:
                            print(f"[B Re-ID 오류] Local={local_id}: {error}")

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
                        elif temporal_candidate_banks.get(local_id):
                            label = f"{person_uid} | COLLECTING"
                            sub = (
                                f"WINDOWS {len(temporal_candidate_banks[local_id])}/"
                                f"{TEMPORAL_CANDIDATE_BANK_MAX}"
                            )
                            color = (0, 255, 0)
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
                journey_id = global_ids.get(local_id)
                if journey_id:
                    if local_id in published_local_ids:
                        release_candidate(journey_id, local_id, "TRACK_LOST")
                    else:
                        reference = candidate_reference(journey_id)
                        if (
                            reference is not None
                            and len(selected_wire_samples.get(local_id, []))
                            < B_GALLERY_TARGET
                            and local_id in temporal_windows
                        ):
                            if finalize_partial_temporal_window(
                                reference["incoming_gallery"],
                                temporal_windows[local_id],
                                temporal_candidate_banks.setdefault(local_id, []),
                            ):
                                selected = selected_temporal_candidates(
                                    temporal_candidate_banks[local_id]
                                )
                                galleries[local_id] = [
                                    item["embedding"] for item in selected
                                ]
                                selected_wire_samples[local_id] = selected

                        final_published = False
                        final_validation_attempted = False
                        if (
                            mqtt_client is not None
                            and len(selected_wire_samples.get(local_id, []))
                            >= B_GALLERY_TARGET
                            and local_id in best_capture_by_local_id
                        ):
                            final_validation_attempted = True
                            final_published = publish_passage(
                                mqtt_client,
                                local_id,
                                journey_id,
                                galleries[local_id],
                                best_capture_by_local_id[local_id],
                                best_capture_quality_by_local_id[local_id],
                                verify_scores.get(local_id, -1.0),
                                observed_samples.get(local_id, []),
                                selected_wire_samples[local_id],
                                rejection_is_final=True,
                            )
                        if final_published:
                            published_local_ids.add(local_id)
                            release_candidate(journey_id, local_id, "TRACK_LOST")
                        else:
                            final_status = (
                                "REJECTED"
                                if len(selected_wire_samples.get(local_id, []))
                                >= B_GALLERY_TARGET
                                else "INSUFFICIENT_QUALITY"
                            )
                            if not final_validation_attempted:
                                reference = candidate_reference(journey_id)
                                final_qualities = [
                                    float(item["quality"])
                                    for item in selected_wire_samples.get(local_id, [])
                                ]
                                log_revisit_event(
                                    "B_PASSAGE_DECISION",
                                    request_id=(reference or {}).get("request_id"),
                                    journey_id=journey_id,
                                    person_uid=(reference or {}).get("person_uid"),
                                    local_track_id=local_id,
                                    status="REJECTED",
                                    reason=final_status,
                                    valid_samples=len(final_qualities),
                                    qualities=final_qualities,
                                )
                            mark_track_rejected(journey_id, local_id, final_status)
                else:
                    log_revisit_event(
                        "B_TRACK_CLEANUP",
                        local_track_id=local_id,
                        reason="TRACK_LOST",
                        final_status="UNMATCHED",
                    )

                for mapping in (
                    first_seen, last_seen, histories, tentative_id,
                    tentative_count, tentative_observations, best_scores,
                    global_ids, verify_scores,
                    verify_failures, verify_successes, galleries,
                    observed_samples, selected_wire_samples,
                    temporal_windows, temporal_candidate_banks,
                    last_collection_scores,
                    last_gallery_frame, best_capture_by_local_id,
                    best_capture_quality_by_local_id,
                ):
                    mapping.pop(local_id, None)
                published_local_ids.discard(local_id)

            output_frame = build_output_frame(
                annotated
            )
            publish_frame(output_frame)

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
