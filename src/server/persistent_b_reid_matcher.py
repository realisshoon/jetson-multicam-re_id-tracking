from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import threading
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfigError, load_mqtt_config


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
DEFAULT_DATABASE = Path("data/local_camera_a_reid_e2e.db")
DEFAULT_REQUEST_TOPIC = "reid/match/b/request"
DEFAULT_RESPONSE_PREFIX = "reid/match/b/result"

# Production src/nodes/node_b.py values.
MATCH_THRESHOLD = 0.70
MATCH_MARGIN = 0.05
OPEN_B_C_STATUSES = ("CREATED", "CANDIDATE_SENT", "PENDING")


def normalize_embedding(value: Any) -> list[float]:
    if not isinstance(value, list) or len(value) != 512:
        raise ValueError("embedding must be a 512-element list")
    converted: list[float] = []
    for index, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError(f"embedding[{index}] must be numeric")
        item = float(item)
        if not math.isfinite(item):
            raise ValueError(f"embedding[{index}] must be finite")
        converted.append(item)
    norm = math.sqrt(sum(item * item for item in converted))
    if norm <= 1e-12:
        raise ValueError("embedding norm must be positive")
    return [item / norm for item in converted]


def cosine_similarity(left: list[float], right: list[float]) -> float:
    return sum(a * b for a, b in zip(left, right))


class PersistentBMatcher:
    def __init__(
        self,
        db_path: Path,
        *,
        threshold: float = MATCH_THRESHOLD,
        margin: float = MATCH_MARGIN,
    ) -> None:
        self.db_path = db_path
        self.threshold = threshold
        self.margin = margin

    def match(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = required_text(request, "request_id")
        if request.get("node_id") != "B":
            raise ValueError("node_id must be B")
        if request.get("embedding_dim") != 512:
            raise ValueError("embedding_dim must be 512")
        query = normalize_embedding(request.get("embedding"))
        candidates = self._load_candidates()
        scores: list[tuple[str, float]] = []
        for journey_id, samples in candidates.items():
            sample_scores = [cosine_similarity(query, sample) for sample in samples]
            scores.append((journey_id, max(sample_scores)))
        scores.sort(key=lambda item: item[1], reverse=True)

        candidate_count = len(scores)
        best_journey_id = scores[0][0] if scores else None
        best_similarity = scores[0][1] if scores else None
        best_gallery_sample_count = (
            len(candidates[best_journey_id])
            if best_journey_id is not None
            else 0
        )
        second_similarity = scores[1][1] if len(scores) > 1 else None
        threshold_ok = best_similarity is not None and best_similarity >= self.threshold
        margin_ok = (
            best_similarity is not None
            and (
                second_similarity is None
                or best_similarity - second_similarity >= self.margin
            )
        )
        matched = bool(threshold_ok and margin_ok)
        if candidate_count == 0:
            reason = "no_open_candidate"
        elif not threshold_ok:
            reason = "below_threshold"
        elif not margin_ok:
            reason = "below_margin"
        else:
            reason = "matched"
        return {
            "request_id": request_id,
            "node_id": "B",
            "matched": matched,
            "reason": reason,
            "candidate_journey_count": candidate_count,
            "best_journey_id": best_journey_id if matched else None,
            "best_gallery_sample_count": best_gallery_sample_count,
            "best_similarity": round(best_similarity, 6) if best_similarity is not None else None,
            "second_similarity": round(second_similarity, 6) if second_similarity is not None else None,
            "threshold": self.threshold,
            "margin": self.margin,
        }

    def _load_candidates(self) -> dict[str, list[list[float]]]:
        placeholders = ",".join("?" for _ in OPEN_B_C_STATUSES)
        connection = sqlite3.connect(
            f"file:{self.db_path.as_posix()}?mode=ro",
            uri=True,
        )
        try:
            rows = connection.execute(
                f"""
                SELECT j.journey_id, g.embedding_json
                FROM journeys AS j
                JOIN gallery_samples AS g
                  ON g.journey_id = j.journey_id
                WHERE j.status IN ({placeholders})
                  AND g.node_id = 'A'
                  AND g.embedding_dim = 512
                  AND g.embedding_json IS NOT NULL
                ORDER BY j.journey_id, g.id
                """,
                OPEN_B_C_STATUSES,
            ).fetchall()
        finally:
            connection.close()
        candidates: dict[str, list[list[float]]] = {}
        for journey_id, embedding_json in rows:
            embedding = normalize_embedding(json.loads(embedding_json))
            candidates.setdefault(str(journey_id), []).append(embedding)
        return candidates


def required_text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value.strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persistent SQLite-backed Camera B Re-ID matcher"
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--db", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--request-topic", default=DEFAULT_REQUEST_TOPIC)
    parser.add_argument("--response-prefix", default=DEFAULT_RESPONSE_PREFIX)
    parser.add_argument("--threshold", type=float, default=MATCH_THRESHOLD)
    parser.add_argument("--margin", type=float, default=MATCH_MARGIN)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.db.is_file():
        print(f"Matcher DB not found: {args.db}", file=sys.stderr)
        return 1
    matcher = PersistentBMatcher(
        args.db,
        threshold=args.threshold,
        margin=args.margin,
    )
    try:
        config = load_mqtt_config(args.config)
        client = JsonMqttClient(
            config.broker,
            client_id="windows_persistent_b_reid_matcher",
        )

        def on_request(topic: str, request: dict[str, Any]) -> None:
            request_id = request.get("request_id")
            if not isinstance(request_id, str) or not request_id.strip():
                print("[MATCH_REJECTED] missing request_id", file=sys.stderr)
                return
            try:
                response = matcher.match(request)
            except (ValueError, OSError, sqlite3.Error, json.JSONDecodeError) as error:
                response = {
                    "request_id": request_id,
                    "node_id": "B",
                    "matched": False,
                    "reason": f"invalid_request:{error}",
                    "candidate_journey_count": 0,
                    "best_journey_id": None,
                    "best_gallery_sample_count": 0,
                    "best_similarity": None,
                    "second_similarity": None,
                    "threshold": matcher.threshold,
                    "margin": matcher.margin,
                }
            response_topic = f"{args.response_prefix}/{request_id}"
            # This handler runs on Paho's network loop thread. Waiting for the
            # PUBACK here would block the same thread that must receive it.
            client.publish_json(response_topic, response, qos=1, wait=False)
            print(
                "[MATCH_RESULT] "
                f"request_id={request_id} candidates={response['candidate_journey_count']} "
                f"best={response['best_journey_id'] or '-'} "
                f"similarity={response['best_similarity']} threshold={response['threshold']} "
                f"matched={response['matched']} reason={response['reason']}",
                flush=True,
            )

        client.subscribe_json(args.request_topic, on_request, qos=1)
        client.connect(timeout=10.0)
        print(
            f"Persistent B matcher connected: {config.broker.host}:{config.broker.port} "
            f"request={args.request_topic} db={args.db} "
            f"threshold={matcher.threshold:.2f} margin={matcher.margin:.2f}",
            flush=True,
        )
        stop = threading.Event()
        while not stop.wait(1.0):
            pass
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"Persistent B matcher failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0
    finally:
        if "client" in locals():
            client.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
