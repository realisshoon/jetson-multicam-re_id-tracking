from __future__ import annotations

import json
from typing import Any

import paho.mqtt.client as mqtt

from src.common.config import load_mqtt_config


MQTT_CONFIG = load_mqtt_config()
MQTT_BROKER_HOST = MQTT_CONFIG.host
MQTT_BROKER_PORT = MQTT_CONFIG.port
MQTT_QOS = MQTT_CONFIG.qos
MQTT_ENTRY_TOPIC = "cctv/events/a/entry"


class MqttPublisher:
    def __init__(
        self,
        broker_host: str = MQTT_BROKER_HOST,
        broker_port: int = MQTT_BROKER_PORT,
        qos: int = MQTT_QOS,
        client_id: str = "camera-a",
    ) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.qos = qos

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )

        self.connected = False

    def connect(self) -> None:
        self.client.connect(
            self.broker_host,
            self.broker_port,
            keepalive=60,
        )

        self.client.loop_start()
        self.connected = True

        print(
            f"MQTT 연결 완료: "
            f"{self.broker_host}:{self.broker_port}"
        )

    def publish_entry(
        self,
        message: dict[str, Any],
    ) -> bool:
        if not self.connected:
            print("MQTT가 연결되지 않았습니다.")
            return False

        payload = json.dumps(
            message,
            ensure_ascii=False,
        )

        result = self.client.publish(
            topic=MQTT_ENTRY_TOPIC,
            payload=payload,
            qos=self.qos,
        )

        result.wait_for_publish()

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"MQTT 전송 실패: rc={result.rc}")
            return False

        print(f"MQTT 전송 완료: {payload}")
        return True

    def disconnect(self) -> None:
        if not self.connected:
            return

        self.client.loop_stop()
        self.client.disconnect()
        self.connected = False

        print("MQTT 연결 종료")
