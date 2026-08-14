from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from cctv_main.capture_cache import (  # noqa: E402
    cache_capture,
    choose_automatic_representative,
    insert_capture_rows,
    insert_failed_capture_rows,
    parse_capture_specs,
    settings_from_document,
)


def table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,)
    ).fetchone() is not None


def connect_write(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def scan(
    db_path: Path,
    config_path: Path,
    apply: bool,
) -> dict[str, Any]:
    document = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    settings = settings_from_document(document, PROJECT_ROOT)
    uri = "file:" + db_path.resolve().as_posix() + ("?mode=rw" if apply else "?mode=ro")
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    capture_table_ready = table_exists(connection, "captures")
    journey_columns = {
        str(row["name"]) for row in connection.execute("PRAGMA table_info(journeys)")
    }
    identity_expression = (
        "j.identity_result"
        if "identity_result" in journey_columns
        else "CASE WHEN j.person_status = 'NEW' THEN 'NEW' "
        "WHEN j.person_status IN ('RETURNING', 'REVISIT', 'MERGED') "
        "THEN 'RETURNING' ELSE 'UNKNOWN' END"
    )
    canonical_expression = (
        "j.canonical_person_uid"
        if "canonical_person_uid" in journey_columns
        else "j.person_uid"
    )
    rows = connection.execute(
        f"""
        SELECT e.journey_id, e.event_at, e.payload_json, j.request_id,
               j.person_uid, {identity_expression} identity_result,
               {canonical_expression} canonical_person_uid
        FROM journey_events e JOIN journeys j ON j.journey_id = e.journey_id
        WHERE e.node_id = 'A' AND e.event_type = 'ENTRY'
        ORDER BY e.event_id
        """
    ).fetchall()
    report: dict[str, Any] = {
        "mode": "apply" if apply else "dry-run",
        "database": str(db_path.resolve()),
        "capture_table_ready": capture_table_ready,
        "a_entry_events": len(rows),
        "events_with_request_id": 0,
        "valid_capture_candidates": 0,
        "invalid_capture_candidates": 0,
        "already_registered": 0,
        "would_insert": 0,
        "known_person_links": 0,
        "pending_journey_only_links": 0,
        "invalid_reasons": {},
        "cached": 0,
        "failed": 0,
    }
    reason_counts: Counter[str] = Counter()
    pending_download_keys: list[str] = []
    request_person_pairs: set[tuple[str, str]] = set()
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            reason_counts["INVALID_EVENT_JSON"] += 1
            continue
        request_id = str(payload.get("request_id") or row["request_id"] or "").strip()
        if not request_id:
            reason_counts["REQUEST_ID_REQUIRED"] += 1
            continue
        report["events_with_request_id"] += 1
        specs, errors = parse_capture_specs(
            payload, request_id, str(row["event_at"]), settings
        )
        report["valid_capture_candidates"] += len(specs)
        report["invalid_capture_candidates"] += len(errors)
        reason_counts.update(str(error.get("reason")) for error in errors)
        known_uid = (
            str(row["canonical_person_uid"] or row["person_uid"])
            if row["identity_result"] in {"NEW", "RETURNING"}
            else None
        )
        if known_uid:
            report["known_person_links"] += len(specs)
            request_person_pairs.add((request_id, known_uid))
        else:
            report["pending_journey_only_links"] += len(specs)
        existing_keys: set[str] = set()
        if capture_table_ready:
            existing_keys = {
                str(item["capture_key"])
                for item in connection.execute(
                    "SELECT capture_key FROM captures WHERE request_id = ?",
                    (request_id,),
                )
            }
        report["already_registered"] += sum(
            1 for spec in specs if spec.capture_key in existing_keys
        )
        report["would_insert"] += sum(
            1 for spec in specs if spec.capture_key not in existing_keys
        )
        if apply:
            if not capture_table_ready:
                raise RuntimeError(
                    "captures table is missing; run updated Main Server migration first"
                )
            insert_capture_rows(
                connection,
                specs,
                str(row["journey_id"]),
                known_uid,
                str(row["event_at"]),
            )
            insert_failed_capture_rows(
                connection,
                errors,
                request_id,
                str(row["journey_id"]),
                str(row["event_at"]),
                str(row["event_at"]),
            )
            pending_download_keys.extend(spec.capture_key for spec in specs)
    report["invalid_reasons"] = dict(sorted(reason_counts.items()))
    if apply:
        connection.commit()
    connection.close()

    if apply:
        factory = lambda: connect_write(db_path)  # noqa: E731
        for key in dict.fromkeys(pending_download_keys):
            result = cache_capture(factory, key, settings)
            report["cached" if result.get("cache_status") == "CACHED" else "failed"] += 1
        with factory() as write_connection:
            for request_id, person_uid in request_person_pairs:
                choose_automatic_representative(
                    write_connection,
                    person_uid,
                    request_id,
                    datetime.now().astimezone().isoformat(timespec="seconds"),
                )
            write_connection.commit()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill cached Camera A captures; dry-run is the default"
    )
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data/main_server.db")
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "configs/capture_cache.yaml",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    if args.apply and not args.yes:
        parser.error("--apply requires --yes; omit both for a safe dry-run")
    print(
        json.dumps(
            scan(args.db, args.config, args.apply),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
