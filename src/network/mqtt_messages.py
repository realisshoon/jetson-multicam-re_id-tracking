from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.network.mqtt_config import NODE_ID_PATTERN


class MessageValidationError(ValueError):
    """Raised when a roundtrip message does not match the protocol."""


def build_node_message(
    node_id: str,
    local_id: int,
    test_value: int | float,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "local_id": local_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {"test_value": test_value},
    }


def process_node_message(message: dict[str, Any]) -> dict[str, Any]:
    node_id = _required_string(message, "node_id")
    if not NODE_ID_PATTERN.fullmatch(node_id):
        raise MessageValidationError("node_id 형식이 잘못되었습니다.")
    local_id = _required_integer(message, "local_id")
    timestamp = _required_string(message, "timestamp")
    try:
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise MessageValidationError(
            "timestamp는 ISO-8601 문자열이어야 합니다."
        ) from error

    payload = message.get("payload")
    if not isinstance(payload, dict):
        raise MessageValidationError("payload는 JSON object여야 합니다.")

    test_value = payload.get("test_value")
    if isinstance(test_value, bool) or not isinstance(test_value, (int, float)):
        raise MessageValidationError("payload.test_value는 숫자여야 합니다.")

    return {
        "node_id": node_id,
        "local_id": local_id,
        "status": "accepted",
        "result": {"processed_value": test_value * 2},
    }


def _required_string(message: dict[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MessageValidationError(f"{key} 필드가 없거나 문자열이 아닙니다.")
    return value


def _required_integer(message: dict[str, Any], key: str) -> int:
    value = message.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MessageValidationError(f"{key} 필드가 없거나 정수가 아닙니다.")
    return value
