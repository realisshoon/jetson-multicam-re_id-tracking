from __future__ import annotations

import argparse
import json
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfig, MqttConfigError, load_mqtt_config
from src.server.camera_a_message import (
    CameraAEntryValidationError,
    validate_camera_a_entry,
)
from src.server.persistence import EventRepository, SQLiteEventRepository


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
DEFAULT_DB = Path("data/central_tracking.db")


@dataclass(frozen=True)
class CameraAProcessingResult:
    status: str
    global_person_id: str | None = None
    event_key: str | None = None
    reason: str | None = None
    node_id: str | None = None
    local_track_id: int | None = None
    embedding_dim: int | None = None


class CameraASqliteMessageHandler:
    """Decode, validate, and persist one raw cctv/entry MQTT payload."""

    def __init__(
        self,
        repository: EventRepository,
        expected_topic: str = "cctv/entry",
    ) -> None:
        self.repository = repository
        self.expected_topic = expected_topic

    def handle_raw(
        self,
        topic: str,
        payload: bytes,
    ) -> CameraAProcessingResult:
        try:
            if topic != self.expected_topic:
                raise CameraAEntryValidationError(
                    f"topic must be {self.expected_topic}"
                )
            decoded = payload.decode("utf-8")
            raw_message = json.loads(decoded)
            if not isinstance(raw_message, dict):
                raise CameraAEntryValidationError(
                    "payload must be a JSON object"
                )
            message = validate_camera_a_entry(raw_message)
            stored = self.repository.record_camera_a_entry(message)
            return CameraAProcessingResult(
                status=stored.status,
                global_person_id=stored.global_person_id,
                event_key=stored.event_key,
                node_id=message["node_id"],
                local_track_id=message["local_track_id"],
                embedding_dim=message["embedding_dim"],
            )
        except (
            CameraAEntryValidationError,
            json.JSONDecodeError,
            UnicodeDecodeError,
        ) as error:
            return CameraAProcessingResult(
                status="rejected",
                reason=str(error),
            )
        except Exception as error:
            return CameraAProcessingResult(
                status="error",
                reason=str(error),
            )


class CameraASqliteServer:
    def __init__(
        self,
        config: MqttConfig,
        repository: SQLiteEventRepository,
        timeout: float = 10.0,
    ) -> None:
        self.config = config
        self.repository = repository
        self.timeout = timeout
        self.handler = CameraASqliteMessageHandler(
            repository,
            expected_topic=config.topics.camera_a_entry,
        )
        self.client = JsonMqttClient(
            config.broker,
            client_id="windows_camera_a_sqlite_server",
        )
        self.client.subscribe_raw(
            config.topics.camera_a_entry,
            self._on_camera_a_entry,
            qos=1,
        )

    def connect(self) -> None:
        self.client.connect(timeout=self.timeout)

    def disconnect(self) -> None:
        self.client.disconnect()

    def _on_camera_a_entry(self, topic: str, payload: bytes) -> None:
        result = self.handler.handle_raw(topic, payload)
        if result.status == "inserted":
            print("[ENTRY RECEIVED]")
            print(f"node_id={result.node_id}")
            print(f"local_track_id={result.local_track_id}")
            print(f"global_person_id={result.global_person_id}")
            print(f"embedding_dim={result.embedding_dim}")
            print("[SQLITE INSERTED]")
            print(f"person={result.global_person_id}")
            print(f"event_key={result.event_key}")
            print(f"db={self.repository.db_path}")
        elif result.status == "duplicate":
            print("[SQLITE DUPLICATE IGNORED]")
            print(f"global_person_id={result.global_person_id}")
            print(f"event_key={result.event_key}")
        else:
            print("[ENTRY REJECTED]")
            print(f"reason={result.reason}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist origin/main Camera A ENTRY MQTT events to SQLite",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = load_mqtt_config(args.config, node_id_override="A")
        repository = SQLiteEventRepository(args.db)
        server = CameraASqliteServer(
            config,
            repository,
            timeout=args.timeout,
        )
        server.connect()
        print("[MQTT CONNECTED]")
        print(f"broker={config.broker.host}:{config.broker.port}")
        print(f"topic={config.topics.camera_a_entry}")
        print(f"db={repository.db_path}")

        stop = threading.Event()
        while not stop.wait(1.0):
            pass
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"Camera A SQLite 서버 실행 실패: {error}", file=sys.stderr)
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
