from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path
from typing import Any

import paho.mqtt.client as mqtt


def load_json(path: Path) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number is not allowed: {value}")

    payload = json.loads(
        path.read_text(encoding="utf-8"),
        parse_constant=reject_constant,
    )
    if not isinstance(payload, dict):
        raise ValueError("top-level JSON value must be an object")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay one Team-A MQTT JSON fixture")
    parser.add_argument("--broker-host", required=True)
    parser.add_argument("--broker-port", required=True, type=int)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--json", required=True, type=Path)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = load_json(args.json)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        print(f"fixture load failed: {error}", file=sys.stderr)
        return 2

    connected = threading.Event()
    connect_error: list[str] = []
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="team_a_fixture_replay",
    )

    def on_connect(
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        if reason_code != 0:
            connect_error.append(str(reason_code))
        connected.set()

    client.on_connect = on_connect
    try:
        client.connect(args.broker_host, args.broker_port, keepalive=60)
        client.loop_start()
        if not connected.wait(args.timeout):
            raise TimeoutError("MQTT connection timed out")
        if connect_error:
            raise RuntimeError(f"MQTT connection rejected: {connect_error[0]}")
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        result = client.publish(args.topic, payload=body, qos=args.qos)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"MQTT publish failed: rc={result.rc}")
        result.wait_for_publish(timeout=args.timeout)
        if not result.is_published():
            raise TimeoutError("MQTT publish acknowledgement timed out")
        embedding = payload.get("embedding")
        gallery = payload.get("gallery")
        print(
            f"PUBLISHED topic={args.topic} qos={args.qos} "
            f"event={payload.get('event', '-')} "
            f"request_id={payload.get('request_id', '-')} "
            f"journey_id={payload.get('journey_id', '-')} "
            f"embedding_count={len(embedding) if isinstance(embedding, list) else 0} "
            f"gallery_count={len(gallery) if isinstance(gallery, list) else 0}"
        )
    except (OSError, RuntimeError, TimeoutError) as error:
        print(f"replay failed: {error}", file=sys.stderr)
        return 1
    finally:
        client.disconnect()
        client.loop_stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
