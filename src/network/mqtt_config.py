from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


NODE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class MqttConfigError(ValueError):
    """Raised when the MQTT configuration is missing or invalid."""


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    port: int
    keepalive: int
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True)
class TopicConfig:
    publish: str
    subscribe: str
    broadcast: str

    def node_data(self, node_id: str) -> str:
        return _render_topic(self.publish, node_id)

    def node_result(self, node_id: str) -> str:
        return _render_topic(self.subscribe, node_id)

    def all_node_data(self) -> str:
        return self.publish.replace("{node_id}", "+")


@dataclass(frozen=True)
class MqttConfig:
    broker: BrokerConfig
    node_id: str
    topics: TopicConfig


def load_mqtt_config(
    path: str | Path,
    node_id_override: str | None = None,
) -> MqttConfig:
    config_path = Path(path)

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise MqttConfigError(
            f"MQTT 설정 파일을 찾을 수 없습니다: {config_path}"
        ) from error
    except yaml.YAMLError as error:
        raise MqttConfigError(f"잘못된 YAML 설정입니다: {error}") from error

    if not isinstance(raw, dict):
        raise MqttConfigError("MQTT 설정의 최상위 값은 mapping이어야 합니다.")

    broker = _mapping(raw, "broker")
    node = _mapping(raw, "node")
    topics = _mapping(raw, "topics")

    host = _string(broker, "host")
    port = _integer(broker, "port", default=1883)
    keepalive = _integer(broker, "keepalive", default=60)

    if not 1 <= port <= 65535:
        raise MqttConfigError("broker.port는 1~65535 범위여야 합니다.")
    if keepalive <= 0:
        raise MqttConfigError("broker.keepalive는 양수여야 합니다.")

    username = _optional_environment_value(broker, "username_env")
    password = _optional_environment_value(broker, "password_env")

    topic_config = TopicConfig(
        publish=_topic_template(topics, "publish"),
        subscribe=_topic_template(topics, "subscribe"),
        broadcast=_string(topics, "broadcast"),
    )

    node_id = node_id_override or _string(node, "id")

    return MqttConfig(
        broker=BrokerConfig(
            host=host,
            port=port,
            keepalive=keepalive,
            username=username,
            password=password,
        ),
        node_id=_validate_node_id(node_id),
        topics=topic_config,
    )


def _mapping(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise MqttConfigError(f"{key} 항목은 mapping이어야 합니다.")
    return value


def _string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MqttConfigError(f"{key} 항목은 비어 있지 않은 문자열이어야 합니다.")
    return value.strip()


def _integer(data: dict[str, Any], key: str, default: int) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MqttConfigError(f"{key} 항목은 정수여야 합니다.")
    return value


def _topic_template(data: dict[str, Any], key: str) -> str:
    value = _string(data, key)
    if "{node_id}" not in value:
        raise MqttConfigError(f"topics.{key}에는 {{node_id}}가 필요합니다.")
    try:
        value.format(node_id="A")
    except (KeyError, ValueError) as error:
        raise MqttConfigError(
            f"topics.{key}의 Topic 템플릿이 잘못되었습니다: {error}"
        ) from error
    return value


def _render_topic(template: str, node_id: str) -> str:
    return template.format(node_id=_validate_node_id(node_id))


def _validate_node_id(node_id: str) -> str:
    if not NODE_ID_PATTERN.fullmatch(node_id):
        raise MqttConfigError(
            "node.id는 영문자, 숫자, 밑줄, 하이픈만 사용할 수 있습니다."
        )
    return node_id


def _optional_environment_value(
    data: dict[str, Any],
    key: str,
) -> str | None:
    variable_name = data.get(key)
    if variable_name is None:
        return None
    if not isinstance(variable_name, str) or not variable_name.strip():
        raise MqttConfigError(f"broker.{key}는 환경 변수 이름이어야 합니다.")
    value = os.getenv(variable_name)
    if value is None:
        raise MqttConfigError(
            f"환경 변수 {variable_name}가 설정되지 않았습니다."
        )
    return value
