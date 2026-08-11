"""
B의 중앙서버(feature/journey-sqlite-e2e 브랜치, journey_sqlite_server.py)가
쌓는 central_tracking.db 를 읽기 전용으로 연동한다.

그쪽 스키마(journey_repository.py 기준)는 journeys/node_matches/
gallery_samples/passage_events/journey_completions/raw_mqtt_messages 로,
우리 쪽 Person/Tracklet/Event 와는 완전히 별개다. 중요한 차이 하나 —
이 DB 에는 "등록(허가)된 인물인지" 라는 개념이 아예 없다. journeys 는
그냥 "어떤 사람이 어느 노드까지 추적됐는지" 만 담고 있어서, 등록 여부
판단은 여전히 Django 쪽 Person.confirmed(관리자 수동 확인)로 한다.
이 모듈은 "감지된 사람"(journeys/journey_completions) 목록을 보강하는
용도로만 쓴다.

jetson 레포 코드를 절대 수정하지 않는 것과 같은 원칙으로, 이 DB 도
절대 쓰지 않는다 — journey_sqlite_server.py 가 계속 쓰고 있는 파일이라
동시 쓰기는 손상 위험이 있다. sqlite3 의 URI 읽기전용 모드(mode=ro)로만
연다.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from django.conf import settings


def _connect() -> sqlite3.Connection | None:
    path = settings.CENTRAL_DB_PATH
    if not path or not Path(path).exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def is_available() -> bool:
    """CENTRAL_DB_PATH 가 설정돼 있고 실제로 열 수 있는 파일인지."""
    conn = _connect()
    if conn is None:
        return False
    conn.close()
    return True


def recent_journeys(limit: int = 50) -> list[dict[str, Any]]:
    """최근 갱신된 journey 목록. journey_id/status/route/시각 정보만 담는다."""
    conn = _connect()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT journey_id, status, route_json, created_at,
                   updated_at, completed_at
            FROM journeys
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def journey_completions(limit: int = 50) -> list[dict[str, Any]]:
    """완료(COMPLETED)까지 간 journey 들 — 전체 경로를 끝까지 추적 성공한 사람."""
    conn = _connect()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT journey_id, destination_node, route_json, best_similarity,
                   total_duration_sec, status, completed_at
            FROM journey_completions
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def counts() -> dict[str, int]:
    """테이블별 총 행 수 — 연동 상태를 한눈에 확인할 때 쓴다."""
    conn = _connect()
    if conn is None:
        return {}
    try:
        tables = (
            "journeys", "node_matches", "gallery_samples",
            "passage_events", "journey_completions", "raw_mqtt_messages",
        )
        return {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
    finally:
        conn.close()
