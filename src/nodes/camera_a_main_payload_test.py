from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.network.mqtt_client import MqttPublisher
from src.network.mqtt_config import MqttConfigError, load_mqtt_config


DEFAULT_CONFIG = Path("configs/mqtt_config.yaml")
TEST_GLOBAL_ID = "G900001"
EMBEDDING_DIM = 512


def build_main_compatible_payload(
    global_person_id: str = TEST_GLOBAL_ID,
    local_track_id: int = 1,
) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node_id": "A",
        "event": "ENTRY",
        "local_track_id": local_track_id,
        "global_person_id": global_person_id,
        "next_nodes": ["B", "C"],
        "reid_model": "osnet_x0_25",
        "embedding_dim": EMBEDDING_DIM,
        "embedding": [0.01] * EMBEDDING_DIM,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish an origin/main-compatible Camera A ENTRY payload",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--global-id", default=TEST_GLOBAL_ID)
    parser.add_argument("--local-id", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.repeat <= 0:
        print("--repeat must be positive")
        return 1

    try:
        config = load_mqtt_config(args.config, node_id_override="A")
        publisher = MqttPublisher(
            broker_host=config.broker.host,
            broker_port=config.broker.port,
            keepalive=config.broker.keepalive,
            entry_topic=config.topics.camera_a_entry,
        )
        publisher.connect()
        payload = build_main_compatible_payload(
            global_person_id=args.global_id,
            local_track_id=args.local_id,
        )
        for attempt in range(1, args.repeat + 1):
            if not publisher.publish_entry(payload):
                return 1
            print(
                f"Camera A test ENTRY sent: attempt={attempt} "
                f"global_person_id={args.global_id} "
                f"embedding_dim={EMBEDDING_DIM}"
            )
        return 0
    except (MqttConfigError, OSError, RuntimeError) as error:
        print(f"Camera A main payload test failed: {error}")
        return 1
    finally:
        if "publisher" in locals():
            publisher.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
