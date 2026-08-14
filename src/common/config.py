from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "mqtt.example.yaml"
LOCAL_CONFIG_PATH = PROJECT_ROOT / "configs" / "mqtt.yaml"
CONFIG_ENV_VAR = "JETSON_MQTT_CONFIG"


@dataclass(frozen=True)
class MqttConfig:
    host: str
    port: int
    qos: int
    source: Path


def _config_path() -> Path:
    configured_path = os.environ.get(CONFIG_ENV_VAR)
    if configured_path:
        return Path(configured_path).expanduser().resolve()
    if LOCAL_CONFIG_PATH.is_file():
        return LOCAL_CONFIG_PATH
    return DEFAULT_CONFIG_PATH


def _broker_section(document: Any, source: Path) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValueError(f"MQTT config must be a mapping: {source}")

    broker = document.get("broker")
    if not isinstance(broker, dict):
        raise ValueError(f"MQTT config is missing the 'broker' mapping: {source}")
    return broker


def load_mqtt_config() -> MqttConfig:
    source = _config_path()
    if not source.is_file():
        raise FileNotFoundError(
            "MQTT config not found: "
            f"{source}. Copy configs/mqtt.example.yaml to configs/mqtt.yaml "
            f"or set {CONFIG_ENV_VAR}."
        )

    with source.open("r", encoding="utf-8") as config_file:
        broker = _broker_section(yaml.safe_load(config_file), source)

    host = str(broker.get("host", "")).strip()
    try:
        port = int(broker.get("port"))
        qos = int(broker.get("qos"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"MQTT port/qos must be integers: {source}") from error

    if not host:
        raise ValueError(f"MQTT host is empty: {source}")
    if not 1 <= port <= 65535:
        raise ValueError(f"MQTT port is out of range: {port}")
    if qos not in {0, 1, 2}:
        raise ValueError(f"MQTT qos must be 0, 1, or 2: {qos}")

    return MqttConfig(host=host, port=port, qos=qos, source=source)
