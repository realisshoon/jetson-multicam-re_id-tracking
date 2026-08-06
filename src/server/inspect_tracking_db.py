from __future__ import annotations

import argparse
from pathlib import Path

from src.server.persistence import SQLiteEventRepository


DEFAULT_DB = Path("data/central_tracking.db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect Camera A persons and tracking events in SQLite",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--global-id")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit <= 0:
        raise SystemExit("--limit must be positive")

    repository = SQLiteEventRepository(args.db)
    persons = repository.fetch_persons(args.global_id, args.limit)
    events = repository.fetch_tracking_events(args.global_id, args.limit)
    integrity = repository.integrity_summary()

    print("PERSONS")
    if not persons:
        print("(none)")
    for person in persons:
        print(
            f"global_person_id={person['global_person_id']} "
            f"source_node={person['source_node']} "
            f"first_local_track_id={person['first_local_track_id']} "
            f"first_seen_at={person['first_seen_at']} "
            f"reid_model={person['reid_model']} "
            f"embedding_dim={person['embedding_dim']}"
        )

    print("TRACKING EVENTS")
    if not events:
        print("(none)")
    for event in events:
        print(
            f"id={event['id']} "
            f"global_person_id={event['global_person_id']} "
            f"node_id={event['node_id']} "
            f"event_type={event['event_type']} "
            f"local_track_id={event['local_track_id']} "
            f"event_timestamp={event['event_timestamp']} "
            f"server_received_at={event['server_received_at']} "
            f"event_key={event['event_key'][:12]}"
        )

    print("INTEGRITY")
    for key, value in integrity.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
