from __future__ import annotations

import argparse
import math
import re
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfig, MqttConfigError, load_mqtt_config


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
ENTRY_TOPIC = "nodes/A/data"
RESULT_TOPIC_NODE = "A"
EMBEDDING_DIM = 512
NODE_DATA_TOPIC_PATTERN = re.compile(
    r"^nodes/(?P<node_id>[A-Za-z0-9_-]+)/data$"
)


class CameraARequestError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class CameraARoundtripHandler:
    """Validate Camera A entry events and issue process-local global IDs."""

    def __init__(self) -> None:
        self._next_global_id = 1
        self._counter_lock = threading.Lock()

    def handle(
        self,
        topic: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        message_id = self._response_message_id(payload)
        try:
            self._validate(topic, payload)
        except CameraARequestError as error:
            return self._rejected_response(
                message_id=message_id,
                error_code=error.code,
                message=str(error),
            )

        global_id = self._issue_global_id()
        return {
            "message_id": payload["message_id"],
            "message_type": "entry_ack",
            "target_node": "A",
            "accepted": True,
            "global_id": global_id,
            "server_timestamp": self._server_timestamp(),
            "message": "entry candidate accepted",
        }

    def _validate(self, topic: str, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise CameraARequestError(
                "INVALID_PAYLOAD",
                "payload must be a JSON object",
            )

        message_id = payload.get("message_id")
        if not isinstance(message_id, str) or not message_id.strip():
            raise CameraARequestError(
                "INVALID_MESSAGE_ID",
                "message_id must be a non-empty string",
            )

        topic_match = NODE_DATA_TOPIC_PATTERN.fullmatch(topic)
        if topic_match is None:
            raise CameraARequestError(
                "INVALID_TOPIC",
                "topic must match nodes/{node_id}/data",
            )
        topic_node = topic_match.group("node_id")
        payload_node = payload.get("node_id")
        if topic_node != payload_node:
            raise CameraARequestError(
                "NODE_MISMATCH",
                "topic node and payload node must match",
            )
        if topic != ENTRY_TOPIC or payload_node != "A":
            raise CameraARequestError(
                "INVALID_NODE",
                "only Camera A entry candidates are supported",
            )

        if payload.get("message_type") != "entry_candidate":
            raise CameraARequestError(
                "INVALID_MESSAGE_TYPE",
                "message_type must be entry_candidate",
            )

        local_id = payload.get("local_id")
        if isinstance(local_id, bool) or not isinstance(local_id, int):
            raise CameraARequestError(
                "INVALID_LOCAL_ID",
                "local_id must be an integer",
            )

        timestamp = payload.get("timestamp")
        if not isinstance(timestamp, str) or not timestamp.strip():
            raise CameraARequestError(
                "INVALID_TIMESTAMP",
                "timestamp must be an ISO-8601 string",
            )
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as error:
            raise CameraARequestError(
                "INVALID_TIMESTAMP",
                "timestamp must be an ISO-8601 string",
            ) from error

        embedding_dim = payload.get("embedding_dim")
        if (
            isinstance(embedding_dim, bool)
            or not isinstance(embedding_dim, int)
            or embedding_dim != EMBEDDING_DIM
        ):
            raise CameraARequestError(
                "INVALID_EMBEDDING",
                "embedding must contain 512 finite values",
            )

        embedding = payload.get("embedding")
        if not isinstance(embedding, list) or len(embedding) != EMBEDDING_DIM:
            raise CameraARequestError(
                "INVALID_EMBEDDING",
                "embedding must contain 512 finite values",
            )
        if len(embedding) != embedding_dim:
            raise CameraARequestError(
                "INVALID_EMBEDDING",
                "embedding_dim must match the embedding length",
            )

        for value in embedding:
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise CameraARequestError(
                    "INVALID_EMBEDDING",
                    "embedding must contain 512 finite values",
                )

    def _issue_global_id(self) -> str:
        with self._counter_lock:
            global_id = f"G{self._next_global_id:06d}"
            self._next_global_id += 1
        return global_id

    @staticmethod
    def _response_message_id(payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        value = payload.get("message_id")
        return value if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _server_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _rejected_response(
        self,
        message_id: str | None,
        error_code: str,
        message: str,
    ) -> dict[str, Any]:
        return {
            "message_id": message_id,
            "message_type": "entry_ack",
            "target_node": "A",
            "accepted": False,
            "error_code": error_code,
            "message": message,
            "server_timestamp": self._server_timestamp(),
        }


class CameraARoundtripServer:
    def __init__(
        self,
        config: MqttConfig,
        timeout: float = 10.0,
        handler: CameraARoundtripHandler | None = None,
    ) -> None:
        self.config = config
        self.timeout = timeout
        self.handler = handler or CameraARoundtripHandler()
        self.client = JsonMqttClient(
            config.broker,
            client_id="windows_camera_a_roundtrip_server",
        )
        self.client.subscribe_json(
            config.topics.all_node_data(),
            self._on_node_data,
        )

    def connect(self) -> None:
        self.client.connect(timeout=self.timeout)

    def disconnect(self) -> None:
        self.client.disconnect()

    def _on_node_data(self, topic: str, payload: dict[str, Any]) -> None:
        request_id = payload.get("message_id")
        print(f"[RECEIVED] topic={topic} message_id={request_id}")

        response = self.handler.handle(topic, payload)
        response_topic = self.config.topics.node_result(RESULT_TOPIC_NODE)
        self.client.publish_json(
            response_topic,
            response,
            timeout=self.timeout,
            wait=False,
        )

        if response["accepted"]:
            print(
                f"[ACCEPTED] message_id={response['message_id']} "
                f"global_id={response['global_id']}"
            )
        else:
            print(
                f"[REJECTED] message_id={response['message_id']} "
                f"error_code={response['error_code']}"
            )
        print(
            f"[RESPONSE] topic={response_topic} "
            f"message_id={response['message_id']}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Camera A entry_candidate MQTT 왕복 서버",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = load_mqtt_config(args.config)
        server = CameraARoundtripServer(config, timeout=args.timeout)
        server.connect()
        print(
            f"Camera A 왕복 서버 연결: "
            f"{config.broker.host}:{config.broker.port} / "
            f"구독: {config.topics.all_node_data()}"
        )

        stop = threading.Event()
        while not stop.wait(1.0):
            pass
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"Camera A 왕복 서버 실행 실패: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("사용자 요청으로 종료합니다.")
        return 0
    finally:
        if "server" in locals():
            server.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
