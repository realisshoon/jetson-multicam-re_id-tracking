from __future__ import annotations

import argparse
import sys
import threading
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfig, MqttConfigError, load_mqtt_config
from src.network.mqtt_messages import (
    MessageValidationError,
    process_node_message,
)


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")


def build_response_for_topic(
    config: MqttConfig,
    topic: str,
    message: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    response = process_node_message(message)
    node_id = response["node_id"]
    expected_topic = config.topics.node_data(node_id)
    if topic != expected_topic:
        raise MessageValidationError(
            f"Topic의 Node와 payload.node_id가 다릅니다: {topic}"
        )
    return config.topics.node_result(node_id), response


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Windows MQTT 중앙 왕복 시험 서버",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = load_mqtt_config(args.config)
        client = JsonMqttClient(
            config.broker,
            client_id="windows_roundtrip_server",
        )

        def on_node_data(topic: str, message: dict[str, Any]) -> None:
            try:
                result_topic, response = build_response_for_topic(
                    config,
                    topic,
                    message,
                )
            except MessageValidationError as error:
                print(f"[REJECTED] {topic}: {error}", file=sys.stderr)
                return

            # MQTT network loop callback 안에서는 PUBACK를 동기 대기하지 않는다.
            client.publish_json(
                result_topic,
                response,
                timeout=args.timeout,
                wait=False,
            )
            print(f"[RECEIVED] {topic}: {message}")
            print(f"[RESPONSE] {result_topic}: {response}")

        data_filter = config.topics.all_node_data()
        client.subscribe_json(data_filter, on_node_data)
        client.connect(timeout=args.timeout)
        print(
            f"Windows 서버 연결: {config.broker.host}:{config.broker.port} / "
            f"구독: {data_filter}"
        )

        stop = threading.Event()
        while not stop.wait(1.0):
            pass

    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"MQTT 서버 실행 실패: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("사용자 요청으로 종료합니다.")
        return 0
    finally:
        if "client" in locals():
            client.disconnect()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
