from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any


SCHEMA_VERSION = "1"


class JourneyProtocolError(ValueError):
    """Raised when a known MQTT contract cannot be normalized."""


@dataclass(frozen=True)
class CanonicalJourneyEvent:
    schema_version: str
    event_key: str
    message_id: str | None
    event_type: str
    journey_id: str
    source_node: str
    target_node: str | None
    local_track_id: int | None
    timestamp: str
    route: list[str] | None
    status: str
    similarity: float | None
    quality: float | None
    embedding_dim: int | None
    embedding: list[float] | None
    gallery_count: int | None
    gallery_nodes: list[str] | None
    raw_topic: str
    raw_payload: dict[str, Any]
    sample_index: int | None = None
    best_similarity: float | None = None
    top2_mean: float | None = None
    combined_score: float | None = None
    total_duration_sec: float | None = None
    previous_node: str | None = None
    previous_to_destination_sec: float | None = None


def adapt_known_mqtt_payload(
    topic: str,
    payload: dict[str, Any],
) -> CanonicalJourneyEvent | None:
    """Adapt only contracts confirmed in code or explicit canonical envelopes."""
    if topic == "cctv/entry":
        return _adapt_origin_main_entry(topic, payload)
    if payload.get("schema_version") and payload.get("journey_id"):
        return _adapt_canonical_envelope(topic, payload)
    return None


def create_raw_event_key(topic: str, payload: Any) -> str:
    message_id = payload.get("message_id") if isinstance(payload, dict) else None
    if isinstance(message_id, str) and message_id.strip():
        return f"message:{message_id.strip()}"
    encoded = compact_json(
        {"topic": topic, "payload": payload},
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def create_canonical_event_key(
    topic: str,
    payload: dict[str, Any],
    *,
    journey_id: str,
    event_type: str,
    source_node: str,
    local_track_id: int | None,
    timestamp: str,
) -> str:
    message_id = payload.get("message_id")
    if isinstance(message_id, str) and message_id.strip():
        return f"message:{message_id.strip()}"
    canonical = {
        "topic": topic,
        "journey_id": journey_id,
        "event_type": event_type,
        "node_id": source_node,
        "local_track_id": local_track_id,
        "timestamp": timestamp,
        "sample_index": payload.get("sample_index"),
        "passage_status": payload.get("status"),
    }
    encoded = compact_json(canonical, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compact_json(value: Any, *, sort_keys: bool = False) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=sort_keys,
        separators=(",", ":"),
    )


def _adapt_origin_main_entry(
    topic: str,
    payload: dict[str, Any],
) -> CanonicalJourneyEvent:
    event_type = _required_string(payload, "event")
    if event_type != "ENTRY":
        raise JourneyProtocolError("cctv/entry event must be ENTRY")
    source_node = _required_string(payload, "node_id")
    if source_node != "A":
        raise JourneyProtocolError("cctv/entry node_id must be A")
    journey_id = payload.get("journey_id") or payload.get("global_person_id")
    if not isinstance(journey_id, str) or not journey_id.strip():
        raise JourneyProtocolError(
            "cctv/entry requires journey_id or global_person_id"
        )
    timestamp = _required_timestamp(payload, "timestamp")
    local_track_id = _optional_integer(payload, "local_track_id")
    if local_track_id is None:
        raise JourneyProtocolError("cctv/entry requires local_track_id")
    next_nodes = _optional_string_list(payload, "next_nodes")
    if next_nodes is None:
        raise JourneyProtocolError("cctv/entry requires next_nodes")
    _required_string(payload, "reid_model")
    embedding = _optional_embedding(payload)
    embedding_dim = _optional_integer(payload, "embedding_dim")
    if embedding_dim != 512:
        raise JourneyProtocolError("cctv/entry embedding_dim must be 512")
    if embedding is None:
        raise JourneyProtocolError("cctv/entry requires embedding")
    if embedding_dim != len(embedding):
        raise JourneyProtocolError(
            "embedding_dim must match the embedding length"
        )
    message_id = _optional_string(payload, "message_id")
    event_key = create_canonical_event_key(
        topic,
        payload,
        journey_id=journey_id,
        event_type=event_type,
        source_node=source_node,
        local_track_id=local_track_id,
        timestamp=timestamp,
    )
    return CanonicalJourneyEvent(
        schema_version=SCHEMA_VERSION,
        event_key=event_key,
        message_id=message_id,
        event_type=event_type,
        journey_id=journey_id,
        source_node=source_node,
        target_node=None,
        local_track_id=local_track_id,
        timestamp=timestamp,
        route=None,
        status="CREATED",
        similarity=None,
        quality=None,
        embedding_dim=embedding_dim,
        embedding=embedding,
        gallery_count=None,
        gallery_nodes=None,
        raw_topic=topic,
        raw_payload=dict(payload),
    )


def _adapt_canonical_envelope(
    topic: str,
    payload: dict[str, Any],
) -> CanonicalJourneyEvent:
    event_type = _required_string(payload, "event_type")
    journey_id = _required_string(payload, "journey_id")
    source_node = _required_string(payload, "source_node")
    timestamp = _required_timestamp(payload, "timestamp")
    status = _required_string(payload, "status")
    local_track_id = _optional_integer(payload, "local_track_id")
    embedding = _optional_embedding(payload)
    embedding_dim = _optional_integer(payload, "embedding_dim")
    if embedding is not None and embedding_dim != len(embedding):
        raise JourneyProtocolError(
            "embedding_dim must match the embedding length"
        )
    message_id = _optional_string(payload, "message_id")
    event_key = create_canonical_event_key(
        topic,
        payload,
        journey_id=journey_id,
        event_type=event_type,
        source_node=source_node,
        local_track_id=local_track_id,
        timestamp=timestamp,
    )
    return CanonicalJourneyEvent(
        schema_version=str(payload["schema_version"]),
        event_key=event_key,
        message_id=message_id,
        event_type=event_type,
        journey_id=journey_id,
        source_node=source_node,
        target_node=_optional_string(payload, "target_node"),
        local_track_id=local_track_id,
        timestamp=timestamp,
        route=_optional_string_list(payload, "route"),
        status=status,
        similarity=_optional_number(payload, "similarity"),
        quality=_optional_number(payload, "quality"),
        embedding_dim=embedding_dim,
        embedding=embedding,
        gallery_count=_optional_integer(payload, "gallery_count"),
        gallery_nodes=_optional_string_list(payload, "gallery_nodes"),
        raw_topic=topic,
        raw_payload=dict(payload),
        sample_index=_optional_integer(payload, "sample_index"),
        best_similarity=_optional_number(payload, "best_similarity"),
        top2_mean=_optional_number(payload, "top2_mean"),
        combined_score=_optional_number(payload, "combined_score"),
        total_duration_sec=_optional_number(payload, "total_duration_sec"),
        previous_node=_optional_string(payload, "previous_node"),
        previous_to_destination_sec=_optional_number(
            payload,
            "previous_to_destination_sec",
        ),
    )


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise JourneyProtocolError(f"{key} must be a non-empty string")
    return value.strip()


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise JourneyProtocolError(f"{key} must be a non-empty string")
    return value.strip()


def _required_timestamp(payload: dict[str, Any], key: str) -> str:
    value = _required_string(payload, key)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise JourneyProtocolError(f"{key} must be ISO-8601") from error
    return value


def _optional_integer(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise JourneyProtocolError(f"{key} must be an integer")
    return value


def _optional_number(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise JourneyProtocolError(f"{key} must be a number")
    if not math.isfinite(value):
        raise JourneyProtocolError(f"{key} must be finite")
    return float(value)


def _optional_string_list(
    payload: dict[str, Any],
    key: str,
) -> list[str] | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item
        for item in value
    ):
        raise JourneyProtocolError(f"{key} must be a string list")
    return list(value)


def _optional_embedding(payload: dict[str, Any]) -> list[float] | None:
    value = payload.get("embedding")
    if value is None:
        return None
    if not isinstance(value, list):
        raise JourneyProtocolError("embedding must be a list")
    embedding: list[float] = []
    for index, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise JourneyProtocolError(
                f"embedding[{index}] must be a number"
            )
        if not math.isfinite(item):
            raise JourneyProtocolError(
                f"embedding[{index}] must be finite"
            )
        embedding.append(float(item))
    return embedding
