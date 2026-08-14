from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.server.journey_protocol import CanonicalJourneyEvent, compact_json


A_ENTRY_TOPIC = "cctv/events/a/entry"
B_PASSAGE_TOPIC = "cctv/events/b/passage"
D_ARRIVAL_TOPIC = "cctv/events/d/arrival"
B_CANDIDATE_TOPIC = "cctv/candidates/b"
D_CANDIDATE_TOPIC = "cctv/candidates/d"
A_RESPONSE_TOPIC = "cctv/responses/a/entry"
COMPLETED_TOPIC = "cctv/main/journey/completed"

INBOUND_TOPICS = (A_ENTRY_TOPIC, B_PASSAGE_TOPIC, D_ARRIVAL_TOPIC)


class TeamAProtocolError(ValueError):
    """Raised when a Team-A payload does not satisfy the confirmed contract."""


@dataclass(frozen=True)
class NormalizedIdentity:
    journey_id: str
    person_uid: str | None
    legacy_global_person_id: str | None
    local_track_id: int


@dataclass(frozen=True)
class EmbeddingSummary:
    embedding_dim: int
    embedding_count: int
    l2_norm: float


@dataclass(frozen=True)
class TeamAAdaptedEvent:
    canonical: CanonicalJourneyEvent
    identity: NormalizedIdentity
    gallery_samples: tuple[CanonicalJourneyEvent, ...]
    request_id: str | None
    capture_path: str | None
    embedding_summary: EmbeddingSummary | None


@dataclass(frozen=True)
class TeamACandidateValidation:
    topic: str
    journey_id: str
    person_uid: str
    stage: str
    route: tuple[str, ...]
    gallery_count: int
    gallery_summaries: tuple[EmbeddingSummary, ...]


def adapt_team_a_payload(topic: str, payload: dict[str, Any]) -> TeamAAdaptedEvent:
    if not isinstance(payload, dict):
        raise TeamAProtocolError("payload must be a JSON object")
    if topic == A_ENTRY_TOPIC:
        return _adapt_a_entry(topic, payload)
    if topic == B_PASSAGE_TOPIC:
        return _adapt_b_passage(topic, payload)
    if topic == D_ARRIVAL_TOPIC:
        return _adapt_d_arrival(topic, payload)
    raise TeamAProtocolError(f"unsupported Team-A inbound topic: {topic}")


def validate_team_a_candidate(
    topic: str,
    payload: dict[str, Any],
) -> TeamACandidateValidation:
    if topic not in {B_CANDIDATE_TOPIC, D_CANDIDATE_TOPIC}:
        raise TeamAProtocolError(f"unsupported Team-A candidate topic: {topic}")
    _expect(payload, "event", "CANDIDATE")
    expected_stage = "WAITING_B_OR_C" if topic == B_CANDIDATE_TOPIC else "WAITING_D"
    _expect(payload, "stage", expected_stage)
    journey_id = _required_string(payload, "journey_id")
    person_uid = _required_string(payload, "person_uid")
    route = _required_string_list(payload, "route")
    expected_route = ["A"] if topic == B_CANDIDATE_TOPIC else ["A", "B"]
    if route != expected_route:
        raise TeamAProtocolError(f"route must be {expected_route}")
    _required_timestamp(payload, "entry_timestamp")
    if topic == D_CANDIDATE_TOPIC:
        _required_timestamp(payload, "passage_timestamp")
    summaries = _validate_gallery(payload)
    for name in (
        "person_match_score",
        "second_match_score",
        "person_best_score",
        "person_topk_score",
        "person_combined_score",
        "person_match_margin",
    ):
        _optional_number(payload, name)
    return TeamACandidateValidation(
        topic=topic,
        journey_id=journey_id,
        person_uid=person_uid,
        stage=expected_stage,
        route=tuple(route),
        gallery_count=len(summaries),
        gallery_summaries=tuple(summaries),
    )


def _adapt_a_entry(topic: str, payload: dict[str, Any]) -> TeamAAdaptedEvent:
    _expect(payload, "event", "ENTRY")
    _expect(payload, "node_id", "A")
    request_id = _required_string(payload, "request_id")
    timestamp = _required_timestamp(payload, "timestamp")
    local_track_id = _required_integer(payload, "local_track_id")
    next_nodes = _required_string_list(payload, "next_nodes")
    if not next_nodes or not set(next_nodes).issubset({"B", "C"}):
        raise TeamAProtocolError("A next_nodes must contain only B and/or C")
    _required_string(payload, "reid_model")
    embedding, summary = _validate_embedding_container(payload)
    quality = _required_quality(payload, "quality")
    _required_string(payload, "verification_status")
    capture_path = _optional_string(payload, "capture_path")

    # The confirmed A payload has no journey_id. This provisional key keeps
    # request dedupe stable without pretending that request_id is a person ID.
    explicit_journey_id = _optional_string(payload, "journey_id")
    journey_id = explicit_journey_id or f"team-a-request:{request_id}"
    person_uid = _optional_string(payload, "person_uid")
    legacy_global_person_id = _optional_string(payload, "global_person_id")
    identity = NormalizedIdentity(
        journey_id=journey_id,
        person_uid=person_uid,
        legacy_global_person_id=legacy_global_person_id,
        local_track_id=local_track_id,
    )
    event_key = f"message:{request_id}"
    canonical = CanonicalJourneyEvent(
        schema_version="team-a-1",
        event_key=event_key,
        message_id=request_id,
        event_type="ENTRY",
        journey_id=journey_id,
        source_node="A",
        target_node=None,
        local_track_id=local_track_id,
        timestamp=timestamp,
        route=["A"],
        status="CREATED",
        similarity=None,
        quality=quality,
        embedding_dim=summary.embedding_dim,
        embedding=embedding,
        gallery_count=1,
        gallery_nodes=["A"],
        raw_topic=topic,
        raw_payload=dict(payload),
    )
    gallery = _gallery_event(
        journey_id=journey_id,
        node_id="A",
        local_track_id=local_track_id,
        sample_index=1,
        captured_at=timestamp,
        quality=quality,
        embedding=embedding,
        raw_topic=topic,
    )
    return TeamAAdaptedEvent(
        canonical=canonical,
        identity=identity,
        gallery_samples=(gallery,),
        request_id=request_id,
        capture_path=capture_path,
        embedding_summary=summary,
    )


def _adapt_b_passage(topic: str, payload: dict[str, Any]) -> TeamAAdaptedEvent:
    schema_version = _required_string(payload, "schema_version")
    _expect(payload, "event", "PASSAGE")
    _expect(payload, "current_node", "B")
    journey_id = _required_string(payload, "journey_id")
    person_uid = _required_string(payload, "person_uid")
    legacy_global_person_id = _optional_string(payload, "global_person_id")
    route = _required_string_list(payload, "route")
    if route != ["A", "B"]:
        raise TeamAProtocolError("B route must be ['A', 'B']")
    next_nodes = _required_string_list(payload, "next_nodes")
    if next_nodes != ["D"]:
        raise TeamAProtocolError("B next_nodes must be ['D']")
    _required_timestamp(payload, "entry_timestamp")
    timestamp = _required_timestamp(payload, "b_passage_timestamp")
    a_local_track_id = _required_integer(payload, "a_local_track_id")
    b_local_track_id = _required_integer(payload, "b_local_track_id")
    _optional_integer(payload, "local_track_id")
    similarity = _required_number(payload, "similarity")
    quality = _required_quality(payload, "quality")
    _required_string(payload, "verification_status")
    capture_path = _optional_string(payload, "capture_path")
    summaries = _validate_gallery(payload)
    gallery_payloads = payload["gallery"]

    identity = NormalizedIdentity(
        journey_id=journey_id,
        person_uid=person_uid,
        legacy_global_person_id=legacy_global_person_id,
        local_track_id=b_local_track_id,
    )
    event_key = _event_key(
        "b",
        [topic, journey_id, "PASSAGE", timestamp, b_local_track_id],
    )
    canonical = CanonicalJourneyEvent(
        schema_version=schema_version,
        event_key=event_key,
        message_id=None,
        event_type="PASSAGE",
        journey_id=journey_id,
        source_node="B",
        target_node="D",
        local_track_id=b_local_track_id,
        timestamp=timestamp,
        route=route,
        status="PASSED",
        similarity=similarity,
        quality=quality,
        embedding_dim=None,
        embedding=None,
        gallery_count=len(summaries),
        gallery_nodes=[_required_string(sample, "node_id") for sample in gallery_payloads],
        raw_topic=topic,
        raw_payload=dict(payload),
    )
    samples: list[CanonicalJourneyEvent] = []
    for index, sample in enumerate(gallery_payloads, start=1):
        node_id = _required_string(sample, "node_id")
        inferred_track = a_local_track_id if node_id == "A" else b_local_track_id
        sample_track = _optional_integer(sample, "local_track_id") or inferred_track
        sample_embedding, _ = _validate_embedding_container(sample)
        samples.append(
            _gallery_event(
                journey_id=journey_id,
                node_id=node_id,
                local_track_id=sample_track,
                sample_index=index,
                captured_at=_required_timestamp(sample, "captured_at"),
                quality=_required_quality(sample, "quality"),
                embedding=sample_embedding,
                raw_topic=topic,
            )
        )
    return TeamAAdaptedEvent(
        canonical=canonical,
        identity=identity,
        gallery_samples=tuple(samples),
        request_id=None,
        capture_path=capture_path,
        embedding_summary=None,
    )


def _adapt_d_arrival(topic: str, payload: dict[str, Any]) -> TeamAAdaptedEvent:
    schema_version = _required_string(payload, "schema_version")
    _expect(payload, "event", "ARRIVAL")
    _expect(payload, "node_id", "D")
    _expect(payload, "current_node", "D")
    _expect(payload, "status", "COMPLETED")
    journey_id = _required_string(payload, "journey_id")
    person_uid = _required_string(payload, "person_uid")
    legacy_global_person_id = _optional_string(payload, "global_person_id")
    route = _required_string_list(payload, "route")
    if route != ["A", "B", "D"]:
        raise TeamAProtocolError("D route must be ['A', 'B', 'D']")
    _required_timestamp(payload, "entry_timestamp")
    _required_timestamp(payload, "passage_timestamp")
    timestamp = _required_timestamp(payload, "d_arrival_timestamp")
    total_duration = _required_nonnegative_number(payload, "total_duration_seconds")
    passage_to_d = _required_nonnegative_number(
        payload,
        "passage_to_d_duration_seconds",
    )
    d_local_track_id = _required_integer(payload, "d_local_track_id")
    _optional_integer(payload, "local_track_id")
    gallery_count = _required_nonnegative_integer(payload, "gallery_count")
    embedding, summary = _validate_embedding_container(payload)
    quality = _required_quality(payload, "quality")
    _required_quality(payload, "capture_quality")
    _required_string(payload, "quality_source")
    _required_string(payload, "verification_status")
    capture_path = _optional_string(payload, "capture_path")
    similarity = _required_number(payload, "similarity")
    best_similarity = _required_number(payload, "best_similarity")
    top2_mean = _required_number(payload, "top2_mean")
    combined_score = _required_number(payload, "combined_score")
    match = payload.get("match")
    if not isinstance(match, dict):
        raise TeamAProtocolError("match must be an object")
    for name, expected in (
        ("best_similarity", best_similarity),
        ("top2_mean", top2_mean),
        ("combined_score", combined_score),
    ):
        actual = _required_number(match, name)
        if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9):
            raise TeamAProtocolError(f"match.{name} must equal top-level {name}")

    identity = NormalizedIdentity(
        journey_id=journey_id,
        person_uid=person_uid,
        legacy_global_person_id=legacy_global_person_id,
        local_track_id=d_local_track_id,
    )
    event_key = _event_key(
        "d",
        [topic, journey_id, "ARRIVAL", timestamp, d_local_track_id],
    )
    canonical = CanonicalJourneyEvent(
        schema_version=schema_version,
        event_key=event_key,
        message_id=None,
        event_type="ARRIVAL",
        journey_id=journey_id,
        source_node="D",
        target_node=None,
        local_track_id=d_local_track_id,
        timestamp=timestamp,
        route=route,
        status="COMPLETED",
        similarity=similarity,
        quality=quality,
        embedding_dim=summary.embedding_dim,
        embedding=embedding,
        gallery_count=gallery_count,
        gallery_nodes=None,
        raw_topic=topic,
        raw_payload=dict(payload),
        best_similarity=best_similarity,
        top2_mean=top2_mean,
        combined_score=combined_score,
        total_duration_sec=total_duration,
        previous_node="B",
        previous_to_destination_sec=passage_to_d,
    )
    gallery = _gallery_event(
        journey_id=journey_id,
        node_id="D",
        local_track_id=d_local_track_id,
        sample_index=gallery_count + 1,
        captured_at=timestamp,
        quality=quality,
        embedding=embedding,
        raw_topic=topic,
    )
    return TeamAAdaptedEvent(
        canonical=canonical,
        identity=identity,
        gallery_samples=(gallery,),
        request_id=None,
        capture_path=capture_path,
        embedding_summary=summary,
    )


def _validate_gallery(payload: dict[str, Any]) -> list[EmbeddingSummary]:
    count = _required_nonnegative_integer(payload, "gallery_count")
    gallery = payload.get("gallery")
    if not isinstance(gallery, list):
        raise TeamAProtocolError("gallery must be a list")
    if len(gallery) != count:
        raise TeamAProtocolError("gallery_count must equal gallery length")
    summaries: list[EmbeddingSummary] = []
    for index, sample in enumerate(gallery):
        if not isinstance(sample, dict):
            raise TeamAProtocolError(f"gallery[{index}] must be an object")
        _required_string(sample, "node_id")
        _required_timestamp(sample, "captured_at")
        _required_quality(sample, "quality")
        _, summary = _validate_embedding_container(sample, prefix=f"gallery[{index}].")
        summaries.append(summary)
    return summaries


def _validate_embedding_container(
    payload: dict[str, Any],
    *,
    prefix: str = "",
) -> tuple[list[float], EmbeddingSummary]:
    embedding_dim = _required_integer(payload, "embedding_dim", prefix=prefix)
    if embedding_dim != 512:
        raise TeamAProtocolError(f"{prefix}embedding_dim must be 512")
    value = payload.get("embedding")
    if not isinstance(value, list):
        raise TeamAProtocolError(f"{prefix}embedding must be a list")
    if len(value) != embedding_dim:
        raise TeamAProtocolError(
            f"{prefix}embedding length must equal embedding_dim"
        )
    embedding: list[float] = []
    for index, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise TeamAProtocolError(
                f"{prefix}embedding[{index}] must be numeric"
            )
        number = float(item)
        if not math.isfinite(number):
            raise TeamAProtocolError(
                f"{prefix}embedding[{index}] must be finite"
            )
        embedding.append(number)
    norm = math.sqrt(sum(item * item for item in embedding))
    return embedding, EmbeddingSummary(embedding_dim, len(embedding), norm)


def _gallery_event(
    *,
    journey_id: str,
    node_id: str,
    local_track_id: int,
    sample_index: int,
    captured_at: str,
    quality: float,
    embedding: list[float],
    raw_topic: str,
) -> CanonicalJourneyEvent:
    sample_payload = {
        "node_id": node_id,
        "local_track_id": local_track_id,
        "captured_at": captured_at,
        "quality": quality,
        "embedding_dim": len(embedding),
        "embedding": embedding,
    }
    event_key = _event_key(
        "gallery",
        [journey_id, node_id, local_track_id, captured_at, quality, embedding],
    )
    return CanonicalJourneyEvent(
        schema_version="team-a-1",
        event_key=event_key,
        message_id=None,
        event_type="GALLERY_SAMPLE",
        journey_id=journey_id,
        source_node=node_id,
        target_node=None,
        local_track_id=local_track_id,
        timestamp=captured_at,
        route=None,
        status="GALLERY_COLLECTING",
        similarity=None,
        quality=quality,
        embedding_dim=len(embedding),
        embedding=embedding,
        gallery_count=None,
        gallery_nodes=None,
        raw_topic=raw_topic,
        raw_payload=sample_payload,
        sample_index=sample_index,
    )


def _event_key(kind: str, parts: list[Any]) -> str:
    digest = hashlib.sha256(compact_json(parts).encode("utf-8")).hexdigest()
    return f"team-a-{kind}:{digest}"


def _expect(payload: dict[str, Any], key: str, expected: str) -> None:
    actual = _required_string(payload, key)
    if actual != expected:
        raise TeamAProtocolError(f"{key} must be {expected}")


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TeamAProtocolError(f"{key} must be a non-empty string")
    return value.strip()


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise TeamAProtocolError(f"{key} must be a non-empty string")
    return value.strip()


def _required_string_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or not value:
        raise TeamAProtocolError(f"{key} must be a non-empty string list")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise TeamAProtocolError(f"{key} must be a non-empty string list")
    return [item.strip() for item in value]


def _required_timestamp(payload: dict[str, Any], key: str) -> str:
    value = _required_string(payload, key)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise TeamAProtocolError(f"{key} must be ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TeamAProtocolError(f"{key} must include a timezone")
    return parsed.isoformat()


def _required_integer(
    payload: dict[str, Any],
    key: str,
    *,
    prefix: str = "",
) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TeamAProtocolError(f"{prefix}{key} must be an integer")
    return value


def _optional_integer(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TeamAProtocolError(f"{key} must be an integer")
    return value


def _required_nonnegative_integer(payload: dict[str, Any], key: str) -> int:
    value = _required_integer(payload, key)
    if value < 0:
        raise TeamAProtocolError(f"{key} must be nonnegative")
    return value


def _required_number(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TeamAProtocolError(f"{key} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise TeamAProtocolError(f"{key} must be finite")
    return number


def _optional_number(payload: dict[str, Any], key: str) -> float | None:
    if payload.get(key) is None:
        return None
    return _required_number(payload, key)


def _required_nonnegative_number(payload: dict[str, Any], key: str) -> float:
    value = _required_number(payload, key)
    if value < 0:
        raise TeamAProtocolError(f"{key} must be nonnegative")
    return value


def _required_quality(payload: dict[str, Any], key: str) -> float:
    value = _required_number(payload, key)
    if not 0.0 <= value <= 1.0:
        raise TeamAProtocolError(f"{key} must be between 0 and 1")
    return value
