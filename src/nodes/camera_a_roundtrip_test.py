from __future__ import annotations

import argparse
import sys
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfigError, load_mqtt_config


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
NODE_ID = "A"
EMBEDDING_DIM = 512


def build_entry_candidate(
    message_id: str,
    local_id: int = 3,
) -> dict[str, Any]:
    return {
        "message_id": message_id,
        "message_type": "entry_candidate",
        "node_id": NODE_ID,
        "local_id": local_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "embedding_dim": EMBEDDING_DIM,
        "embedding": [0.01] * EMBEDDING_DIM,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="가짜 Jetson A entry_candidate 왕복 시험 클라이언트",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--local-id", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    message_id = str(uuid.uuid4())
    response_received = threading.Event()
    matched_response: dict[str, Any] | None = None

    try:
        config = load_mqtt_config(args.config, node_id_override=NODE_ID)
        client = JsonMqttClient(
            config.broker,
            client_id=f"camera_a_roundtrip_test_{message_id}",
        )

        def on_result(topic: str, payload: dict[str, Any]) -> None:
            nonlocal matched_response
            response_id = payload.get("message_id")
            if response_id != message_id:
                print(
                    f"[IGNORED] topic={topic} message_id={response_id}"
                )
                return
            matched_response = payload
            print(
                f"[ENTRY_ACK] topic={topic} message_id={response_id} "
                f"accepted={payload.get('accepted')} "
                f"global_id={payload.get('global_id')}"
            )
            response_received.set()

        result_topic = config.topics.node_result(NODE_ID)
        client.subscribe_json(result_topic, on_result)
        client.connect(timeout=args.timeout)

        request = build_entry_candidate(message_id, local_id=args.local_id)
        entry_topic = config.topics.node_data(NODE_ID)
        client.publish_json(entry_topic, request, timeout=args.timeout)
        print(f"[PUBLISH] topic={entry_topic} message_id={message_id}")

        if not response_received.wait(args.timeout):
            print(
                f"entry_ack timeout: message_id={message_id}",
                file=sys.stderr,
            )
            return 1
        if matched_response is None or not matched_response.get("accepted"):
            print(
                f"entry_candidate rejected: message_id={message_id}",
                file=sys.stderr,
            )
            return 1
        global_id = matched_response.get("global_id")
        if not isinstance(global_id, str) or not global_id:
            print("entry_ack global_id가 없습니다.", file=sys.stderr)
            return 1
        return 0
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"Camera A 시험 클라이언트 실행 실패: {error}", file=sys.stderr)
        return 1
    finally:
        if "client" in locals():
            client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
