from __future__ import annotations

import argparse
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfig, MqttConfigError, load_mqtt_config
from src.server.message_protocol import (
    ProtocolValidationError,
    build_reid_candidate,
    validate_incoming_message,
)
from src.server.pending_manager import PendingCandidateError, PendingManager
from src.server.persistence import EventRepository, MemoryEventRepository
from src.server.route_manager import RouteError, RouteManager


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
PublishJson = Callable[[str, dict[str, Any]], None]
LogMessage = Callable[[str], None]


class CentralServer:
    """Protocol and routing core independent of MQTT and Django storage."""

    def __init__(
        self,
        config: MqttConfig,
        repository: EventRepository,
        publish_json: PublishJson,
        route_manager: RouteManager | None = None,
        pending_manager: PendingManager | None = None,
        logger: LogMessage = print,
    ) -> None:
        self.config = config
        self.repository = repository
        self.publish_json = publish_json
        self.routes = route_manager or RouteManager()
        self.pending = pending_manager or PendingManager()
        self.logger = logger

    def handle_message(self, topic: str, message: dict[str, Any]) -> bool:
        """Validate and process one decoded MQTT message without raising."""
        try:
            message_type = validate_incoming_message(topic, message)
            node_id = message["node_id"]
            if node_id not in self.routes.nodes:
                raise RouteError(f"지원하지 않는 Node입니다: {node_id}")

            handlers = {
                "entry_candidate": self._handle_entry_candidate,
                "match_result": self._handle_match_result,
                "unknown": self._handle_unknown,
                "heartbeat": self._handle_heartbeat,
                "timeout": self._handle_timeout,
            }
            handler = handlers.get(message_type)
            if handler is None:
                raise ProtocolValidationError(
                    f"수신 방향에서 처리할 수 없는 메시지입니다: {message_type}"
                )
            handler(message)
            return True
        except (
            KeyError,
            PendingCandidateError,
            ProtocolValidationError,
            RouteError,
        ) as error:
            self.logger(f"[REJECTED] {topic}: {error}")
            return False

    def _handle_entry_candidate(self, message: dict[str, Any]) -> None:
        candidate = self.pending.register(message)
        self.repository.record_entry(message)
        for target_node in self.routes.targets_for(candidate.source_node):
            if self.pending.mark_forwarded(candidate.global_id, target_node):
                self._publish_candidate(message, target_node)

    def _handle_match_result(self, message: dict[str, Any]) -> None:
        node_id = message["node_id"]
        global_id = message["global_id"]
        candidate = self.pending.record_match(global_id, node_id)
        self.repository.record_match(message)

        if node_id == "D":
            self.logger(f"[COMPLETED] {global_id}")
            return

        for target_node in self.routes.targets_for(node_id):
            if self.pending.mark_forwarded(global_id, target_node):
                self._publish_candidate(candidate.entry_message, target_node)

    def _handle_unknown(self, message: dict[str, Any]) -> None:
        self.repository.record_unknown(message)

    def _handle_heartbeat(self, message: dict[str, Any]) -> None:
        normalized = dict(message)
        normalized.setdefault("status", "online")
        self.repository.update_node_status(normalized)

    def _handle_timeout(self, message: dict[str, Any]) -> None:
        self.pending.timeout(message["global_id"])
        self.repository.record_timeout(message)

    def _publish_candidate(
        self,
        entry_message: dict[str, Any],
        target_node: str,
    ) -> None:
        topic = self.config.topics.node_result(target_node)
        payload = build_reid_candidate(entry_message, target_node)
        self.publish_json(topic, payload)
        self.logger(
            f"[ROUTED] {entry_message['global_id']} -> {target_node} ({topic})"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Windows Re-ID MQTT 중앙 라우팅 서버",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = load_mqtt_config(args.config)
        repository = MemoryEventRepository()
        client = JsonMqttClient(
            config.broker,
            client_id="windows_central_routing_server",
        )
        server = CentralServer(
            config=config,
            repository=repository,
            publish_json=lambda topic, message: client.publish_json(
                topic,
                message,
                timeout=args.timeout,
                wait=False,
            ),
        )

        data_filter = config.topics.all_node_data()
        client.subscribe_json(data_filter, server.handle_message)
        client.connect(timeout=args.timeout)
        print(
            f"중앙 서버 연결: {config.broker.host}:{config.broker.port} / "
            f"구독: {data_filter}"
        )

        stop = threading.Event()
        while not stop.wait(1.0):
            pass
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"중앙 서버 실행 실패: {error}", file=sys.stderr)
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
