from __future__ import annotations

import json
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import (
    BrokerConfig,
    MqttConfigError,
    load_mqtt_config,
)
from src.network.mqtt_messages import (
    MessageValidationError,
    build_node_message,
    process_node_message,
)
from src.server.mqtt_roundtrip_server import build_response_for_topic


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG = PROJECT_ROOT / "configs" / "mqtt_config.example.yaml"


class MqttConfigTests(unittest.TestCase):
    def test_example_config_and_topics(self) -> None:
        config = load_mqtt_config(EXAMPLE_CONFIG)

        self.assertEqual(config.broker.host, "BROKER_PC_IP")
        self.assertEqual(config.broker.port, 1883)
        self.assertEqual(config.topics.node_data("D"), "nodes/D/data")
        self.assertEqual(config.topics.node_result("D"), "server/D/result")
        self.assertEqual(config.topics.all_node_data(), "nodes/+/data")

    def test_node_id_override_is_validated(self) -> None:
        with self.assertRaises(MqttConfigError):
            load_mqtt_config(EXAMPLE_CONFIG, "invalid/node")


class MqttMessageTests(unittest.TestCase):
    def test_json_roundtrip_and_processing(self) -> None:
        outgoing = build_node_message("A", 1, 100)
        decoded = json.loads(json.dumps(outgoing))
        response = process_node_message(decoded)

        self.assertEqual(response["node_id"], "A")
        self.assertEqual(response["local_id"], 1)
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["result"]["processed_value"], 200)

    def test_missing_required_field_is_rejected(self) -> None:
        with self.assertRaises(MessageValidationError):
            process_node_message(
                {
                    "node_id": "A",
                    "local_id": 1,
                    "timestamp": "2026-08-05T00:00:00+00:00",
                    "payload": {},
                }
            )

    def test_boolean_test_value_is_rejected(self) -> None:
        with self.assertRaises(MessageValidationError):
            process_node_message(
                {
                    "node_id": "A",
                    "local_id": 1,
                    "timestamp": "2026-08-05T00:00:00+00:00",
                    "payload": {"test_value": True},
                }
            )

    def test_invalid_node_id_is_rejected(self) -> None:
        with self.assertRaises(MessageValidationError):
            process_node_message(
                {
                    "node_id": "invalid/node",
                    "local_id": 1,
                    "timestamp": "2026-08-05T00:00:00+00:00",
                    "payload": {"test_value": 100},
                }
            )

    def test_invalid_timestamp_is_rejected(self) -> None:
        with self.assertRaises(MessageValidationError):
            process_node_message(
                {
                    "node_id": "A",
                    "local_id": 1,
                    "timestamp": "not-a-timestamp",
                    "payload": {"test_value": 100},
                }
            )


class JsonMqttClientTests(unittest.TestCase):
    def test_subscription_decodes_and_routes_json(self) -> None:
        client = JsonMqttClient(
            BrokerConfig(host="localhost", port=1883, keepalive=60),
            client_id="unit_test_client",
        )
        received: list[dict[str, object]] = []
        client.subscribe_json(
            "server/A/result",
            lambda topic, message: received.append(message),
        )
        mqtt_message = SimpleNamespace(
            topic="server/A/result",
            payload=b'{"status":"accepted"}',
        )

        client._on_message(client.client, None, mqtt_message)

        self.assertEqual(received, [{"status": "accepted"}])

    def test_raw_subscription_preserves_exact_payload_bytes(self) -> None:
        client = JsonMqttClient(
            BrokerConfig(host="localhost", port=1883, keepalive=60),
            client_id="raw_unit_test_client",
        )
        received: list[tuple[str, bytes]] = []
        client.subscribe_raw(
            "cctv/#",
            lambda topic, payload: received.append((topic, payload)),
        )
        mqtt_message = SimpleNamespace(
            topic="cctv/passage/b",
            payload=b"not-even-json\x00",
        )

        client._on_message(client.client, None, mqtt_message)

        self.assertEqual(
            received,
            [("cctv/passage/b", b"not-even-json\x00")],
        )


class ServerProcessingTests(unittest.TestCase):
    def test_response_uses_sending_node_topic(self) -> None:
        config = load_mqtt_config(EXAMPLE_CONFIG)
        result_topic, response = build_response_for_topic(
            config,
            "nodes/B/data",
            build_node_message("B", 7, 12.5),
        )

        self.assertEqual(result_topic, "server/B/result")
        self.assertEqual(response["result"]["processed_value"], 25.0)

    def test_topic_and_payload_node_mismatch_is_rejected(self) -> None:
        config = load_mqtt_config(EXAMPLE_CONFIG)
        with self.assertRaises(MessageValidationError):
            build_response_for_topic(
                config,
                "nodes/B/data",
                build_node_message("A", 1, 100),
            )


if __name__ == "__main__":
    unittest.main()
