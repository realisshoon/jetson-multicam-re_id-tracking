from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any


EMBEDDING_DIM = 512
GLOBAL_PERSON_ID_PATTERN = re.compile(r"^G[0-9]{6,}$")
ALLOWED_NEXT_NODES = frozenset({"B", "C"})
EVENT_KEY_FIELDS = (
    "timestamp",
    "node_id",
    "event",
    "local_track_id",
    "global_person_id",
)


class CameraAEntryValidationError(ValueError):
    """Raised when a Camera A cctv/entry payload is invalid."""


def validate_camera_a_entry(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise CameraAEntryValidationError("payload must be a JSON object")

    normalized = dict(payload)

    timestamp = _required_string(payload, "timestamp")
    try:
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise CameraAEntryValidationError(
            "timestamp must be a valid ISO-8601 string"
        ) from error
    normalized["timestamp"] = timestamp

    node_id = _required_string(payload, "node_id")
    if node_id != "A":
        raise CameraAEntryValidationError("node_id must be A")
    normalized["node_id"] = node_id

    event = _required_string(payload, "event")
    if event != "ENTRY":
        raise CameraAEntryValidationError("event must be ENTRY")
    normalized["event"] = event

    local_track_id = payload.get("local_track_id")
    if isinstance(local_track_id, bool) or not isinstance(local_track_id, int):
        raise CameraAEntryValidationError(
            "local_track_id must be a non-negative integer"
        )
    if local_track_id < 0:
        raise CameraAEntryValidationError(
            "local_track_id must be a non-negative integer"
        )
    normalized["local_track_id"] = local_track_id

    global_person_id = _required_string(payload, "global_person_id")
    if not GLOBAL_PERSON_ID_PATTERN.fullmatch(global_person_id):
        raise CameraAEntryValidationError(
            "global_person_id must match ^G[0-9]{6,}$"
        )
    normalized["global_person_id"] = global_person_id

    next_nodes = payload.get("next_nodes")
    if not isinstance(next_nodes, list):
        raise CameraAEntryValidationError("next_nodes must be a list")
    normalized_next_nodes: list[str] = []
    for value in next_nodes:
        if not isinstance(value, str) or value not in ALLOWED_NEXT_NODES:
            raise CameraAEntryValidationError(
                "next_nodes may contain only B and C"
            )
        normalized_next_nodes.append(value)
    normalized["next_nodes"] = normalized_next_nodes

    normalized["reid_model"] = _required_string(payload, "reid_model")

    embedding_dim = payload.get("embedding_dim")
    if isinstance(embedding_dim, bool) or not isinstance(embedding_dim, int):
        raise CameraAEntryValidationError("embedding_dim must be an integer")
    if embedding_dim != EMBEDDING_DIM:
        raise CameraAEntryValidationError("embedding_dim must be 512")
    normalized["embedding_dim"] = embedding_dim

    embedding = payload.get("embedding")
    if not isinstance(embedding, list):
        raise CameraAEntryValidationError("embedding must be a list")
    if len(embedding) != embedding_dim:
        raise CameraAEntryValidationError(
            "embedding_dim must match the embedding length"
        )
    if len(embedding) != EMBEDDING_DIM:
        raise CameraAEntryValidationError(
            "embedding must contain 512 values"
        )

    normalized_embedding: list[float] = []
    for index, value in enumerate(embedding):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CameraAEntryValidationError(
                f"embedding[{index}] must be a number"
            )
        if not math.isfinite(value):
            raise CameraAEntryValidationError(
                f"embedding[{index}] must be finite"
            )
        normalized_embedding.append(float(value))
    normalized["embedding"] = normalized_embedding

    return normalized


def create_camera_a_event_key(payload: dict[str, Any]) -> str:
    canonical = {
        field: payload[field]
        for field in EVENT_KEY_FIELDS
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compact_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise CameraAEntryValidationError(
            f"{key} must be a non-empty string"
        )
    return value.strip()
