from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any

from src.network.mqtt_config import NODE_ID_PATTERN


EMBEDDING_DIM = 512
GLOBAL_ID_PATTERN = re.compile(r"^G[0-9]{6,}$")
NODE_DATA_TOPIC_PATTERN = re.compile(
    r"^nodes/(?P<node_id>[A-Za-z0-9_-]+)/data$"
)
SUPPORTED_MESSAGE_TYPES = frozenset(
    {
        "entry_candidate",
        "reid_candidate",
        "match_result",
        "unknown",
        "heartbeat",
        "timeout",
    }
)


class ProtocolValidationError(ValueError):
    """Raised when a central-server message violates the Re-ID protocol."""


def validate_incoming_message(
    topic: str,
    message: dict[str, Any],
) -> str:
    topic_node = node_id_from_data_topic(topic)
    if not isinstance(message, dict):
        raise ProtocolValidationError("payload는 JSON object여야 합니다.")

    message_type = required_string(message, "message_type")
    if message_type not in SUPPORTED_MESSAGE_TYPES:
        raise ProtocolValidationError(
            f"지원하지 않는 message_type입니다: {message_type}"
        )
    if message_type == "reid_candidate":
        raise ProtocolValidationError(
            "reid_candidate는 Windows에서 Jetson으로 보내는 메시지입니다."
        )

    payload_node = validate_node_id(required_string(message, "node_id"))
    if topic_node != payload_node:
        raise ProtocolValidationError(
            "Topic의 Node ID와 payload.node_id가 다릅니다: "
            f"{topic_node}/{payload_node}"
        )
    validate_timestamp(message)

    if message_type == "entry_candidate":
        validate_entry_candidate(message)
    elif message_type == "match_result":
        validate_match_result(message)
    elif message_type == "unknown":
        validate_optional_global_id(message)
    elif message_type == "heartbeat":
        validate_heartbeat(message)
    elif message_type == "timeout":
        validate_global_id(message)

    return message_type


def validate_entry_candidate(message: dict[str, Any]) -> None:
    if message.get("node_id") != "A":
        raise ProtocolValidationError(
            "entry_candidate는 Node A에서만 받을 수 있습니다."
        )
    required_integer(message, "local_id")
    validate_global_id(message)

    embedding_dim = required_integer(message, "embedding_dim")
    if embedding_dim != EMBEDDING_DIM:
        raise ProtocolValidationError(
            f"embedding_dim은 {EMBEDDING_DIM}이어야 합니다."
        )
    embedding = message.get("embedding")
    if not isinstance(embedding, list):
        raise ProtocolValidationError("embedding은 숫자 배열이어야 합니다.")
    if len(embedding) != embedding_dim:
        raise ProtocolValidationError(
            "embedding_dim과 embedding 배열 길이가 다릅니다."
        )
    if len(embedding) != EMBEDDING_DIM:
        raise ProtocolValidationError(
            f"embedding 배열 길이는 {EMBEDDING_DIM}이어야 합니다."
        )
    for index, value in enumerate(embedding):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ProtocolValidationError(
                f"embedding[{index}]은 숫자여야 합니다."
            )
        if not math.isfinite(value):
            raise ProtocolValidationError(
                f"embedding[{index}]은 유한한 숫자여야 합니다."
            )


def validate_match_result(message: dict[str, Any]) -> None:
    node_id = message["node_id"]
    if node_id not in {"B", "C", "D"}:
        raise ProtocolValidationError(
            "match_result는 Node B, C 또는 D에서만 받을 수 있습니다."
        )
    required_integer(message, "local_id")
    validate_global_id(message)
    similarity = required_number(message, "similarity")
    if not 0.0 <= similarity <= 1.0:
        raise ProtocolValidationError("similarity는 0.0~1.0 범위여야 합니다.")
    required_string(message, "status")


def validate_heartbeat(message: dict[str, Any]) -> None:
    status = message.get("status", "online")
    if not isinstance(status, str) or not status.strip():
        raise ProtocolValidationError("heartbeat.status는 문자열이어야 합니다.")


def build_reid_candidate(
    entry_message: dict[str, Any],
    target_node: str,
) -> dict[str, Any]:
    return {
        "message_type": "reid_candidate",
        "source_node": entry_message["node_id"],
        "target_node": validate_node_id(target_node),
        "global_id": entry_message["global_id"],
        "timestamp": entry_message["timestamp"],
        "embedding_dim": entry_message["embedding_dim"],
        "embedding": list(entry_message["embedding"]),
    }


def node_id_from_data_topic(topic: str) -> str:
    if not isinstance(topic, str):
        raise ProtocolValidationError("MQTT Topic은 문자열이어야 합니다.")
    match = NODE_DATA_TOPIC_PATTERN.fullmatch(topic)
    if match is None:
        raise ProtocolValidationError(f"잘못된 Node data Topic입니다: {topic}")
    return match.group("node_id")


def validate_timestamp(message: dict[str, Any]) -> None:
    timestamp = required_string(message, "timestamp")
    try:
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise ProtocolValidationError(
            "timestamp는 ISO-8601 문자열이어야 합니다."
        ) from error


def validate_global_id(message: dict[str, Any]) -> str:
    global_id = required_string(message, "global_id")
    if not GLOBAL_ID_PATTERN.fullmatch(global_id):
        raise ProtocolValidationError(
            "global_id는 G와 6자리 이상의 숫자로 구성되어야 합니다."
        )
    return global_id


def validate_optional_global_id(message: dict[str, Any]) -> None:
    if "global_id" in message and message["global_id"] is not None:
        validate_global_id(message)


def validate_node_id(node_id: str) -> str:
    if not NODE_ID_PATTERN.fullmatch(node_id):
        raise ProtocolValidationError(f"잘못된 node_id입니다: {node_id}")
    return node_id


def required_string(message: dict[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ProtocolValidationError(
            f"{key} 필드가 없거나 문자열이 아닙니다."
        )
    return value.strip()


def required_integer(message: dict[str, Any], key: str) -> int:
    value = message.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolValidationError(f"{key} 필드는 정수여야 합니다.")
    return value


def required_number(message: dict[str, Any], key: str) -> float:
    value = message.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProtocolValidationError(f"{key} 필드는 숫자여야 합니다.")
    if not math.isfinite(value):
        raise ProtocolValidationError(f"{key} 필드는 유한한 숫자여야 합니다.")
    return float(value)
