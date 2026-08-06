from __future__ import annotations

import json
import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt

from src.network.mqtt_config import BrokerConfig


MQTT_BROKER_HOST = "localhost"
MQTT_BROKER_PORT = 1883
MQTT_ENTRY_TOPIC = "cctv/entry"


JsonMessageHandler = Callable[[str, dict[str, Any]], None]
RawMessageHandler = Callable[[str, bytes], None]


class JsonMqttClient:
    """Configurable MQTT client for JSON publish and subscribe."""

    def __init__(
        self,
        broker: BrokerConfig,
        client_id: str,
    ) -> None:
        self.broker = broker
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
        if broker.username is not None:
            self.client.username_pw_set(broker.username, broker.password)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self._connected = threading.Event()
        self._subscriptions: dict[str, tuple[int, JsonMessageHandler]] = {}
        self._raw_subscriptions: dict[
            str,
            tuple[int, RawMessageHandler],
        ] = {}

    def subscribe_json(
        self,
        topic: str,
        handler: JsonMessageHandler,
        qos: int = 1,
    ) -> None:
        self._subscriptions[topic] = (qos, handler)
        if self._connected.is_set():
            self.client.subscribe(topic, qos=qos)

    def subscribe_raw(
        self,
        topic: str,
        handler: RawMessageHandler,
        qos: int = 1,
    ) -> None:
        self._raw_subscriptions[topic] = (qos, handler)
        if self._connected.is_set():
            self.client.subscribe(topic, qos=qos)

    def connect(self, timeout: float = 10.0) -> None:
        self.client.connect(
            self.broker.host,
            self.broker.port,
            keepalive=self.broker.keepalive,
        )
        self.client.loop_start()

        if not self._connected.wait(timeout):
            self.client.loop_stop()
            self.client.disconnect()
            raise TimeoutError(
                "MQTT Broker 연결 확인 시간이 초과되었습니다: "
                f"{self.broker.host}:{self.broker.port}"
            )

    def publish_json(
        self,
        topic: str,
        message: dict[str, Any],
        qos: int = 1,
        timeout: float = 10.0,
        wait: bool = True,
    ) -> None:
        if not self._connected.is_set():
            raise RuntimeError("MQTT Broker에 연결되지 않았습니다.")

        payload = json.dumps(message, ensure_ascii=False)
        result = self.client.publish(topic, payload=payload, qos=qos)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT Publish 요청 실패: rc={result.rc}")

        if wait:
            result.wait_for_publish(timeout=timeout)
            if not result.is_published():
                raise TimeoutError(f"MQTT Publish 확인 시간 초과: {topic}")

    def disconnect(self) -> None:
        if self._connected.is_set():
            self.client.disconnect()
        self.client.loop_stop()
        self._connected.clear()

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if reason_code != 0:
            print(f"MQTT 연결 실패: {reason_code}")
            return

        self._connected.set()
        for topic, (qos, _) in self._subscriptions.items():
            client.subscribe(topic, qos=qos)
        for topic, (qos, _) in self._raw_subscriptions.items():
            client.subscribe(topic, qos=qos)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        self._connected.clear()

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        for topic_filter, (_, handler) in self._raw_subscriptions.items():
            if mqtt.topic_matches_sub(topic_filter, message.topic):
                try:
                    handler(message.topic, bytes(message.payload))
                except Exception as error:
                    print(f"MQTT 메시지 처리 실패 ({message.topic}): {error}")
                return

        try:
            decoded = json.loads(message.payload.decode("utf-8"))
            if not isinstance(decoded, dict):
                raise ValueError("최상위 JSON 값이 object가 아닙니다.")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            print(f"[REJECTED] 잘못된 MQTT JSON ({message.topic}): {error}")
            return

        for topic_filter, (_, handler) in self._subscriptions.items():
            if mqtt.topic_matches_sub(topic_filter, message.topic):
                try:
                    handler(message.topic, decoded)
                except Exception as error:
                    print(f"MQTT 메시지 처리 실패 ({message.topic}): {error}")
                return


class MqttPublisher:
    def __init__(
        self,
        broker_host: str = MQTT_BROKER_HOST,
        broker_port: int = MQTT_BROKER_PORT,
    ) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2
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
            qos=1,
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
