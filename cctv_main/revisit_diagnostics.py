from __future__ import annotations

import json
import math
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


COMMON_FIELDS = (
    "at",
    "run_id",
    "event",
    "request_id",
    "journey_id",
    "person_uid",
    "temporary_person_uid",
    "candidate_person_uid",
    "canonical_person_uid",
    "local_track_id",
    "reason",
)

_FORBIDDEN_KEYS = {
    "embedding",
    "embeddings",
    "raw_embedding",
    "admin_token",
    "authorization",
    "password",
    "email",
    "phone",
    "name",
    "image",
    "image_path",
    "capture_path",
}


def new_run_id() -> str:
    configured = os.environ.get("CCTV_REVISIT_RUN_ID", "").strip()
    value = configured or (
        datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%f%z")
        + f"-p{os.getpid()}"
    )
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    return safe or "revisit"


def default_log_root(project_root: Path) -> Path:
    return Path(
        os.environ.get(
            "CCTV_REVISIT_LOG_ROOT",
            project_root / "data" / "logs" / "revisit",
        )
    ).expanduser()


def _json_safe(value: Any, key: str = "") -> Any:
    normalized_key = key.strip().lower()
    if (
        normalized_key in _FORBIDDEN_KEYS
        or normalized_key.endswith("_token")
    ):
        raise ValueError(
            f"REVISIT diagnostic field is not log-safe: {key}"
        )
    if isinstance(value, dict):
        return {
            str(item_key): _json_safe(item_value, str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item, key) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item(), key)
        except (TypeError, ValueError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class RevisitDiagnosticLogger:
    def __init__(self, log_root: Path, run_id: str | None = None) -> None:
        self.log_root = Path(log_root)
        self.run_id = run_id or new_run_id()
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self.log_root / self.run_id / "main_revisit.jsonl"

    def write(self, event: str, **fields: Any) -> Path | None:
        record = {field: None for field in COMMON_FIELDS}
        record.update(
            {
                "at": fields.pop("at", None)
                or datetime.now().astimezone().isoformat(timespec="milliseconds"),
                "run_id": self.run_id,
                "event": str(event),
                "reason": fields.pop("reason", None),
            }
        )
        for field in COMMON_FIELDS:
            if field in fields:
                record[field] = fields.pop(field)
        record.update(fields)
        safe_record = _json_safe(record)
        line = json.dumps(
            safe_record,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=False,
        )
        try:
            with self._lock:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8", newline="\n") as stream:
                    stream.write(line + "\n")
                    stream.flush()
        except OSError as error:
            print(f"[REVISIT DIAGNOSTIC LOG ERROR] {error}", flush=True)
            return None
        return self.path


def sample_summaries(samples: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for sample in samples:
        values = sample.get("embedding")
        dimension = int(getattr(values, "size", len(values) if values is not None else 0))
        norm = None
        if values is not None:
            try:
                norm = math.sqrt(sum(float(value) ** 2 for value in values))
            except (TypeError, ValueError, OverflowError):
                norm = None
        summaries.append(
            {
                "dimension": dimension,
                "norm": norm,
                "quality": sample.get("quality"),
            }
        )
    return summaries


def payload_sample_diagnostics(
    payload: dict[str, Any], modality: str
) -> dict[str, Any]:
    normalized = modality.strip().lower()
    plural_key = f"{normalized}_embeddings"
    quality_key = f"{normalized}_qualities"
    supplied = payload.get(plural_key)
    vectors: list[Any]
    if isinstance(supplied, list) and supplied:
        vectors = list(supplied)
    elif normalized == "body" and isinstance(payload.get("embedding"), list):
        vectors = [payload["embedding"]]
    else:
        vectors = []

    summaries: list[dict[str, Any]] = []
    for vector in vectors:
        dimension = len(vector) if isinstance(vector, list) else None
        norm = None
        if isinstance(vector, list):
            try:
                numeric = [float(value) for value in vector]
                if all(math.isfinite(value) for value in numeric):
                    norm = math.sqrt(math.fsum(value * value for value in numeric))
            except (TypeError, ValueError, OverflowError):
                norm = None
        summaries.append({"dimension": dimension, "norm": norm})

    raw_qualities = payload.get(quality_key)
    if isinstance(raw_qualities, list):
        quality_values = raw_qualities[: len(vectors)]
    elif normalized == "body" and vectors:
        quality_values = [payload.get("quality", 1.0)]
    else:
        quality_values = []
    qualities: list[float | None] = []
    for value in quality_values:
        try:
            converted = float(value)
            qualities.append(converted if math.isfinite(converted) else None)
        except (TypeError, ValueError):
            qualities.append(None)
    return {
        "sample_count": len(vectors),
        "qualities": qualities,
        "summaries": summaries,
    }


def candidate_summaries(
    candidates: Iterable[dict[str, Any]],
    decision_reason: str | None,
) -> list[dict[str, Any]]:
    items = list(candidates)
    summaries: list[dict[str, Any]] = []
    for index, candidate in enumerate(items):
        combined = candidate.get("fused_similarity")
        next_score = (
            items[index + 1].get("fused_similarity")
            if index + 1 < len(items)
            else None
        )
        margin = (
            float(combined) - float(next_score)
            if combined is not None and next_score is not None
            else None
        )
        excluded_reasons: list[str] = []
        if index > 0:
            excluded_reasons.append("LOWER_RANK_THAN_SELECTED_CANDIDATE")
        elif decision_reason and decision_reason not in {
            "AUTO_MATCH_BODY_THRESHOLDS_MARGIN_AND_CONSISTENCY",
            "AUTO_MATCH_FACE_THRESHOLD_MARGIN_AND_CONSISTENCY",
        }:
            excluded_reasons.append(str(decision_reason))
        summaries.append(
            {
                "candidate_person_uid": candidate.get("person_uid"),
                "body_score": candidate.get("body_similarity"),
                "face_score": candidate.get("face_similarity"),
                "combined_score": combined,
                "margin_to_next": margin,
                "excluded_reasons": excluded_reasons,
            }
        )
    return summaries
