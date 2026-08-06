from __future__ import annotations

import argparse
import json
import math
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import MqttConfigError, load_mqtt_config


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")


def summarize_payload(value: Any) -> Any:
    """Return a log-safe payload summary without dumping vector contents."""
    if isinstance(value, dict):
        return {
            key: _summarize_embedding(item)
            if "embedding" in key.lower()
            else summarize_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [summarize_payload(item) for item in value]
    return value


def _summarize_embedding(value: Any) -> dict[str, Any]:
    if not isinstance(value, list):
        return {"type": type(value).__name__}
    numeric = [float(item) for item in value if isinstance(item, (int, float))]
    norm = math.sqrt(sum(item * item for item in numeric))
    return {
        "length": len(value),
        "norm": round(norm, 6),
        "preview": value[:3],
    }


def format_capture(topic: str, payload: bytes) -> str:
    received_at = datetime.now(timezone.utc).isoformat()
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        text = payload.decode("utf-8", errors="replace")
        return (
            f"[CAPTURE] received_at={received_at} topic={topic} "
            f"invalid_json={error} raw={text!r}"
        )
    summary = summarize_payload(decoded)
    return (
        f"[CAPTURE] received_at={received_at} topic={topic} "
        f"{json.dumps(summary, ensure_ascii=False)}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="cctv/# MQTT 원문 캡처 도구")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = load_mqtt_config(args.config)
        client = JsonMqttClient(config.broker, client_id="windows_mqtt_capture")
        client.subscribe_raw("cctv/#", lambda topic, body: print(format_capture(topic, body)))
        client.connect(timeout=args.timeout)
        print(
            f"MQTT 캡처 연결: {config.broker.host}:{config.broker.port} / "
            "구독: cctv/#"
        )
        stop = threading.Event()
        while not stop.wait(1.0):
            pass
    except (MqttConfigError, OSError, RuntimeError, TimeoutError) as error:
        print(f"MQTT 캡처 실행 실패: {error}")
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
