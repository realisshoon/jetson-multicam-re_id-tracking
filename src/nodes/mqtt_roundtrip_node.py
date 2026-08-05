from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfigError, load_mqtt_config
from src.network.mqtt_messages import build_node_message


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Jetson MQTT JSON 왕복 통신 시험 노드",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--node-id", help="설정 파일의 Node ID를 덮어씁니다.")
    parser.add_argument("--local-id", type=int, default=1)
    parser.add_argument("--value", type=float, default=100)
    parser.add_argument(
        "--interval",
        type=float,
        help="지정하면 해당 초 간격으로 계속 전송합니다.",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = load_mqtt_config(args.config, args.node_id)
        client = JsonMqttClient(
            config.broker,
            client_id=f"jetson_{config.node_id}_roundtrip",
        )
        result_received = threading.Event()

        def on_result(topic: str, message: dict[str, Any]) -> None:
            print(f"[RESULT] {topic}: {message}")
            result_received.set()

        def on_command(topic: str, message: dict[str, Any]) -> None:
            print(f"[COMMAND] {topic}: {message}")

        result_topic = config.topics.node_result(config.node_id)
        client.subscribe_json(result_topic, on_result)
        client.subscribe_json(config.topics.broadcast, on_command)
        client.connect(timeout=args.timeout)

        print(
            f"Broker 연결: {config.broker.host}:{config.broker.port} / "
            f"Node {config.node_id}"
        )

        local_id = args.local_id
        while True:
            message = build_node_message(config.node_id, local_id, args.value)
            data_topic = config.topics.node_data(config.node_id)
            client.publish_json(data_topic, message, timeout=args.timeout)
            print(f"[PUBLISH] {data_topic}: {message}")

            if args.interval is None:
                if result_received.wait(args.timeout):
                    return 0
                print("응답 대기 시간이 초과되었습니다.", file=sys.stderr)
                return 2

            local_id += 1
            time.sleep(args.interval)

    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"MQTT 노드 실행 실패: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("사용자 요청으로 종료합니다.")
        return 0
    finally:
        if "client" in locals():
            client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
