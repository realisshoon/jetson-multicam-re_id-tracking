from __future__ import annotations

import argparse
import json
import math
import sys
import threading
from pathlib import Path
from typing import Any

from src.network.mqtt_client import JsonMqttClient
from src.network.mqtt_config import BrokerConfig
from src.server.journey_protocol import create_raw_event_key
from src.server.journey_repository import JourneyStoreResult
from src.server.team_a_protocol_adapter import (
    INBOUND_TOPICS,
    TeamAAdaptedEvent,
    TeamAProtocolError,
    adapt_team_a_payload,
)
from src.server.team_a_protocol_repository import (
    TeamAProtocolRepository,
    TeamAStoreResult,
)


DEFAULT_DATABASE = Path("data/team_a_protocol_replay_test.db")


def decode_object(payload: bytes) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number is not allowed: {value}")

    decoded = json.loads(
        payload.decode("utf-8"),
        parse_constant=reject_constant,
    )
    if not isinstance(decoded, dict):
        raise ValueError("top-level JSON value must be an object")
    return decoded


class TeamAProtocolProcessor:
    def __init__(self, repository: TeamAProtocolRepository) -> None:
        self.repository = repository

    def process(
        self,
        topic: str,
        payload: bytes,
    ) -> tuple[TeamAStoreResult | JourneyStoreResult, TeamAAdaptedEvent | None]:
        try:
            message = decode_object(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            wrapped = {
                "decode_error": str(error),
                "raw_text": payload.decode("utf-8", errors="replace"),
            }
            result = self.repository.store_raw_message(
                topic,
                wrapped,
                event_key=create_raw_event_key(topic, wrapped),
            )
            return result, None
        try:
            adapted = adapt_team_a_payload(topic, message)
        except TeamAProtocolError as error:
            wrapped = dict(message)
            wrapped["adapter_error"] = str(error)
            result = self.repository.store_raw_message(
                topic,
                wrapped,
                journey_id=_optional_text(message.get("journey_id")),
                source_node=_optional_text(
                    message.get("node_id") or message.get("current_node")
                ),
                event_key=create_raw_event_key(topic, wrapped),
            )
            return result, None
        return self.repository.store_adapted(adapted), adapted


def format_result(
    result: TeamAStoreResult | JourneyStoreResult,
    adapted: TeamAAdaptedEvent | None,
) -> str:
    if adapted is None:
        return f"[{result.status.upper()}] raw-only event_key={result.event_key}"
    event = adapted.canonical
    fields = [
        f"[{result.status.upper()}]",
        f"topic={event.raw_topic}",
        f"event={event.event_type}",
        f"journey_id={event.journey_id}",
        f"local_track_id={event.local_track_id}",
        f"gallery_count={event.gallery_count}",
    ]
    if adapted.embedding_summary is not None:
        summary = adapted.embedding_summary
        fields.extend(
            [
                f"embedding_dim={summary.embedding_dim}",
                f"embedding_count={summary.embedding_count}",
                f"l2_norm={summary.l2_norm:.6f}",
            ]
        )
    elif adapted.gallery_samples:
        norms = [
            math.sqrt(sum(value * value for value in sample.embedding or []))
            for sample in adapted.gallery_samples
        ]
        fields.append(
            "gallery_l2_norms=" + ",".join(f"{value:.6f}" for value in norms)
        )
    return " ".join(fields)


def _optional_text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Team-A MQTT protocol adapter server")
    parser.add_argument("--broker-host", default="10.10.20.33")
    parser.add_argument("--broker-port", type=int, default=1883)
    parser.add_argument("--db", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--connect-timeout", type=float, default=10.0)
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--run-seconds", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_messages < 0 or args.run_seconds < 0:
        print("max-messages and run-seconds must be nonnegative", file=sys.stderr)
        return 2
    repository = TeamAProtocolRepository(args.db)
    processor = TeamAProtocolProcessor(repository)
    client = JsonMqttClient(
        BrokerConfig(args.broker_host, args.broker_port, 60),
        client_id="windows_team_a_protocol_adapter",
    )
    stopped = threading.Event()
    received = 0
    lock = threading.Lock()

    def on_message(topic: str, payload: bytes) -> None:
        nonlocal received
        result, adapted = processor.process(topic, payload)
        print(format_result(result, adapted), flush=True)
        with lock:
            received += 1
            if args.max_messages and received >= args.max_messages:
                stopped.set()

    for topic in INBOUND_TOPICS:
        client.subscribe_raw(topic, on_message, qos=args.qos)
    try:
        client.connect(timeout=args.connect_timeout)
        print(
            f"Team-A adapter connected: {args.broker_host}:{args.broker_port} "
            f"qos={args.qos} db={args.db}",
            flush=True,
        )
        for topic in INBOUND_TOPICS:
            print(f"SUBSCRIBE {topic}", flush=True)
        stopped.wait(args.run_seconds if args.run_seconds else None)
    except (OSError, RuntimeError, TimeoutError) as error:
        print(f"Team-A adapter failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 0
    finally:
        client.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
