from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.server.journey_repository import JourneySQLiteRepository


DEFAULT_DATABASE = Path("data/central_tracking.db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="저장된 Journey 조회")
    parser.add_argument("--db", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--journey-id", required=True)
    return parser.parse_args()


def _print_section(title: str, value: Any) -> None:
    print(f"\n{title}")
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main() -> int:
    args = parse_args()
    repository = JourneySQLiteRepository(args.db)
    journey = repository.fetch_journey(args.journey_id)
    if journey is None:
        print(f"Journey를 찾을 수 없습니다: {args.journey_id}")
        return 1

    _print_section("Journey", journey)
    _print_section("Node matches", repository.fetch_node_matches(args.journey_id))
    _print_section("Gallery samples", repository.fetch_gallery_samples(args.journey_id))
    _print_section("Passage events", repository.fetch_passages(args.journey_id))
    _print_section("Completion", repository.fetch_completions(args.journey_id))
    _print_section("Table counts", repository.counts())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
