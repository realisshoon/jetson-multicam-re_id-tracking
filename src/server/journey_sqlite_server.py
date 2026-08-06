from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfigError, load_mqtt_config
from src.server.journey_protocol import (
    JourneyProtocolError,
    adapt_known_mqtt_payload,
    create_raw_event_key,
)
from src.server.journey_repository import JourneySQLiteRepository, JourneyStoreResult


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
DEFAULT_DATABASE = Path("data/central_tracking.db")


def decode_object(payload: bytes) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"비표준 숫자는 허용되지 않습니다: {value}")

    decoded = json.loads(
        payload.decode("utf-8"),
        parse_constant=reject_constant,
    )
    if not isinstance(decoded, dict):
        raise ValueError("최상위 JSON 값이 object가 아닙니다.")
    return decoded


class JourneyMessageProcessor:
    """Convert only known contracts and retain every other MQTT message raw."""

    def __init__(self, repository: JourneySQLiteRepository) -> None:
        self.repository = repository

    def process(self, topic: str, payload: bytes) -> JourneyStoreResult:
        try:
            message = decode_object(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            wrapped = {
                "decode_error": str(error),
                "raw_text": payload.decode("utf-8", errors="replace"),
            }
            return self.repository.store_raw_message(
                topic,
                wrapped,
                event_key=create_raw_event_key(topic, wrapped),
            )

        try:
            event = adapt_known_mqtt_payload(topic, message)
        except JourneyProtocolError:
            return self.repository.store_raw_message(
                topic,
                message,
                journey_id=_optional_text(message.get("journey_id")),
                source_node=_optional_text(message.get("node_id")),
                event_key=create_raw_event_key(topic, message),
            )
        if event is None:
            return self.repository.store_raw_message(
                topic,
                message,
                journey_id=_optional_text(message.get("journey_id")),
                source_node=_optional_text(message.get("node_id")),
                event_key=create_raw_event_key(topic, message),
            )
        return self.repository.store_event(event)


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MQTT Journey SQLite 수집 서버")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--db", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = load_mqtt_config(args.config)
        repository = JourneySQLiteRepository(args.db)
        processor = JourneyMessageProcessor(repository)
        client = JsonMqttClient(config.broker, client_id="windows_journey_sqlite")

        def on_message(topic: str, payload: bytes) -> None:
            try:
                result = processor.process(topic, payload)
                print(
                    f"[{result.status.upper()}] {topic} "
                    f"category={result.category} journey={result.journey_id or '-'}"
                )
            except OSError as error:
                print(f"[REJECTED] {topic}: {error}", file=sys.stderr)

        client.subscribe_raw("cctv/#", on_message, qos=1)
        client.connect(timeout=args.timeout)
        print(
            f"Journey 서버 연결: {config.broker.host}:{config.broker.port} / "
            f"구독: cctv/# / DB: {args.db}"
        )
        stop = threading.Event()
        while not stop.wait(1.0):
            pass
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"Journey 서버 실행 실패: {error}", file=sys.stderr)
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
