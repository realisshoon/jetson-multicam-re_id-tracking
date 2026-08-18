from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        return json.loads(str(value))
    except (json.JSONDecodeError, TypeError, ValueError):
        return default


def table_exists(connection: sqlite3.Connection, name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def rows(connection: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(sql).fetchall()]


def export_people(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(connection, "persons"):
        return []
    has_embeddings = table_exists(connection, "person_embeddings")
    if not has_embeddings:
        return rows(
            connection,
            "SELECT person_uid,status,visit_count FROM persons ORDER BY person_uid",
        )
    return rows(
        connection,
        """
        SELECT
            p.person_uid,
            p.status,
            p.merged_into_person_uid,
            p.visit_count,
            COUNT(pe.embedding_id) AS embedding_count,
            SUM(CASE WHEN pe.modality='BODY' THEN 1 ELSE 0 END)
                AS body_embedding_count,
            SUM(CASE WHEN pe.modality='FACE' THEN 1 ELSE 0 END)
                AS face_embedding_count
        FROM persons AS p
        LEFT JOIN person_embeddings AS pe ON pe.person_uid=p.person_uid
        GROUP BY p.person_uid
        ORDER BY p.person_uid
        """,
    )


def export_journeys(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(connection, "journeys"):
        return []
    has_review = table_exists(connection, "review_cases")
    review_join = (
        "LEFT JOIN review_cases AS r ON r.journey_id=j.journey_id"
        if has_review
        else ""
    )
    review_columns = (
        "r.initial_decision AS review_initial_decision,"
        "r.final_review_result,r.final_candidate_person_uid,"
        "r.canonical_person_uid AS review_canonical_person_uid,"
        "r.status AS review_status_detail"
        if has_review
        else "NULL AS review_initial_decision,NULL AS final_review_result,"
        "NULL AS final_candidate_person_uid,"
        "NULL AS review_canonical_person_uid,NULL AS review_status_detail"
    )
    result = rows(
        connection,
        f"""
        SELECT
            j.journey_id,j.request_id,j.person_uid,j.status,j.route_json,
            j.person_status,j.identity_result,j.review_status,
            j.candidate_person_uid,j.canonical_person_uid,
            j.decision_reason,j.score_margin,j.person_combined_score,
            {review_columns}
        FROM journeys AS j
        {review_join}
        ORDER BY j.entry_at,j.journey_id
        """,
    )
    for item in result:
        item["route"] = json_value(item.pop("route_json"), [])
        item["initial_decision"] = (
            item.pop("review_initial_decision") or item["person_status"]
        )
        final = item.get("final_review_result")
        if final is None and item["status"] == "COMPLETED":
            final = (
                "REVISIT"
                if item["identity_result"] == "RETURNING"
                else "NEW"
                if item["identity_result"] == "NEW"
                else "MANUAL_REVIEW_REQUIRED"
            )
        item["final_decision"] = final
        item["canonical_person_uid"] = (
            item.pop("review_canonical_person_uid")
            or item["canonical_person_uid"]
        )
    return result


def export_gallery(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(connection, "journey_gallery"):
        return []
    result = rows(
        connection,
        """
        SELECT
            journey_id,node_id,modality,
            COUNT(*) AS sample_count,
            MIN(quality) AS quality_min,
            AVG(quality) AS quality_avg,
            MAX(quality) AS quality_max,
            GROUP_CONCAT(quality, ',') AS quality_values_csv
        FROM journey_gallery
        GROUP BY journey_id,node_id,modality
        ORDER BY journey_id,node_id,modality
        """,
    )
    for item in result:
        values = str(item.pop("quality_values_csv") or "")
        item["qualities"] = [float(value) for value in values.split(",") if value]
    return result


def export_database_events(connection: sqlite3.Connection) -> dict[str, Any]:
    b_events: list[dict[str, Any]] = []
    if table_exists(connection, "journey_events"):
        b_events = rows(
            connection,
            """
            SELECT journey_id,event_at,1 AS approved,'PASSAGE_ACCEPTED' AS reason
            FROM journey_events
            WHERE node_id='B' AND event_type='PASSAGE'
            ORDER BY event_id
            """,
        )
    d_events: list[dict[str, Any]] = []
    if table_exists(connection, "d_arrival_attempts"):
        d_events = rows(
            connection,
            """
            SELECT
                journey_id,d_local_track_id,arrival_at,received_at,
                accepted AS approved,reason_code,reason_json
            FROM d_arrival_attempts
            ORDER BY attempt_id
            """,
        )
        for item in d_events:
            item["approved"] = bool(item["approved"])
            item["reason_codes"] = json_value(item.pop("reason_json"), [])
    return {"b": b_events, "d": d_events}


def export_reviews(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(connection, "review_cases"):
        return []
    reviews = rows(
        connection,
        """
        SELECT
            review_id,journey_id,provisional_person_uid,candidate_person_uid,
            initial_decision,initial_scores_json,status,action,target_person_uid,
            final_review_result,final_candidate_person_uid,canonical_person_uid,
            final_scores_json,resolution_source,created_at,resolved_at,
            final_reviewed_at
        FROM review_cases
        ORDER BY created_at,review_id
        """,
    )
    candidate_rows: list[dict[str, Any]] = []
    if table_exists(connection, "identity_review_candidates"):
        candidate_rows = rows(
            connection,
            """
            SELECT
                review_id,candidate_person_uid,rank,body_similarity,
                face_similarity,fused_similarity,score_margin
            FROM identity_review_candidates
            ORDER BY review_id,rank
            """,
        )
    by_review: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidate_rows:
        by_review.setdefault(str(candidate.pop("review_id")), []).append(candidate)
    for review in reviews:
        review["initial_scores"] = json_value(
            review.pop("initial_scores_json"), {}
        )
        review["final_scores"] = json_value(
            review.pop("final_scores_json"), {}
        )
        review["candidates"] = by_review.get(str(review["review_id"]), [])
    return reviews


def latest_log(log_root: Path) -> Path | None:
    candidates = sorted(
        log_root.glob("*/main_revisit.jsonl"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def export_log_events(path: Path | None) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {"b": [], "d": []}
    if path is None or not path.exists():
        return result
    allowed = {
        "at",
        "run_id",
        "event",
        "request_id",
        "journey_id",
        "person_uid",
        "temporary_person_uid",
        "candidate_person_uid",
        "canonical_person_uid",
        "local_track_id",
        "reason",
        "approved",
        "duplicate",
        "reason_codes",
        "score",
        "best_journey_score",
        "journey_margin",
        "threshold",
        "gallery_count",
        "accepted_gallery_count",
    }
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        event = record.get("event")
        target = (
            "b" if event == "B_PASSAGE_RECEIVED"
            else "d" if event == "D_ARRIVAL_RECEIVED"
            else None
        )
        if target is not None:
            safe = {key: record.get(key) for key in allowed if key in record}
            safe["line_number"] = line_number
            result[target].append(safe)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export privacy-safe REVISIT diagnostics as JSON."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=PROJECT_ROOT / "data" / "main_server.db",
    )
    parser.add_argument("--log", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.db.exists():
        raise SystemExit(f"database not found: {args.db}")
    log_root = PROJECT_ROOT / "data" / "logs" / "revisit"
    log_path = args.log
    if log_path is None and args.run_id:
        log_path = log_root / args.run_id / "main_revisit.jsonl"
    if log_path is None:
        log_path = latest_log(log_root)

    connection = sqlite3.connect(args.db)
    connection.row_factory = sqlite3.Row
    try:
        report = {
            "generated_at": datetime.now().astimezone().isoformat(),
            "database": str(args.db.resolve()),
            "diagnostic_log": str(log_path.resolve()) if log_path else None,
            "persons": export_people(connection),
            "journeys": export_journeys(connection),
            "journey_gallery": export_gallery(connection),
            "events": {
                "database": export_database_events(connection),
                "diagnostic_log": export_log_events(log_path),
            },
            "review_cases": export_reviews(connection),
        }
    finally:
        connection.close()

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
