"""
메인 서버(B, 10.10.20.33:8080) REST API 응답을 DB 레코드로 적재한다.

2026-08-11 확정된 구조:
    Jetson A/B/C/D → MQTT → 메인 서버(Windows) → main_server.db → REST API
    → 이 Django. Jetson MQTT 를 여기서 직접 구독하던 mqtt_worker.py/
    mqtt_ingest.py 는 더 이상 쓰지 않는다(main_server_worker.py 로 대체).

신원(person_uid)·여정(journey_id)의 source of truth 는 메인 서버다 —
Camera A 는 스스로 신원을 정하지 않고, 메인 서버가 Re-ID/DB 조회 후
person_uid 를 배정한다. journey_id 는 방문 1회짜리 세션이라, 반복
방문자를 하나로 묶을 땐 person_uid 를 키로 써야 한다(Person.external_id).

2026-08-11 오후 갱신(B): `GET /api/events` 는 Main 에 없는 엔드포인트라
404 가 나고 있었다 — Main API 는 건드리지 않고 우리 쪽을 실제 계약에
맞춘다. 이제 폴링은 전부 `/api/journeys` 계열이다:

    GET /api/journeys?limit=<n>          — 목록, 필드가 얕다(요약용)
    GET /api/journeys/{journey_id}       — 상세, identity/timing/person
                                            중첩 객체 포함(Final Review 전체)

실제로 curl 로 확인한 응답 스키마(최초 안내 문서의 필드명과 다른 부분
있음 — 실제 값 기준으로 아래에 정리):

목록 항목(요약, identity 세부 없음):
    {
      "journey_id": "J000104", "person_uid": "P000006",
      "person_status": "RETURNING", "visit_count": 15,
      "journey_status": "COMPLETED", "route": ["A", "C", "D"],
      "entry_at": "...", "d_exit_at": "...",
      "journey_elapsed_seconds": 18.141,
      "initial_decision": "IDENTITY_PENDING",
      "final_review_result": "REVISIT"
    }
    ⚠ 목록의 person_uid 는 신뢰하면 안 된다 — MANUAL_REVIEW_REQUIRED 인
    동안은 여기에도 temporary_person_uid 값이 그대로 나온다(실측 확인).
    그래서 목록만으로는 canonical 여부를 판단하지 않고, ingest_journey_summary()
    는 identity 관련 필드를 아예 건드리지 않는다.

상세(신원 판단 전체, canonical 여부는 여기서만 확정):
    {
      "journey_id": "J000104",
      "person": {"person_uid": "P000006", "status": "ACTIVE", "visit_count": 15},
      "journey_status": "COMPLETED", "person_status": "RETURNING",
      "route": ["A", "C", "D"], "entry_at": "...",
      "timing": {"d_exit": "...", "elapsed_seconds": 18.141, ...},
      "identity": {
        "initial_decision": "IDENTITY_PENDING",
        "temporary_person_uid": "P000072",
        "initial_candidate_person_uid": "P000006",
        "final_result": "REVISIT",
        "final_candidate_person_uid": "P000006",
        "canonical_person_uid": "P000006",
        "final_score": 0.7978605687618257,
        "final_scores": {"body_all": {...}, "face": {...}, ...}
      },
      "nodes": [...], "captures": [...]
    }

`ingest_journey()` 는 이 상세 스키마를 받는다. temporary/initial_candidate/
final_candidate 는 참고용이고, `Journey.person` 을 실제로 연결하는 건
`identity.canonical_person_uid` 가 있고 `identity.final_result` 가
MANUAL_REVIEW_REQUIRED 가 아닐 때뿐이다 — 검토 대기 중인 여정은 Person
을 새로 만들지도, 기존 Person 에 붙이지도 않는다.

2026-08-11 저녁 갱신: `/api/events` 가 없어지면서 카메라별 "오늘 감지
횟수"·이벤트 기록·TTS 알림(A 등록완료 차임/B·C·D 미등록 경고)이 전부
멎어 있었다 — 그 기능들은 전부 Event/Tracklet 레코드가 새로 생겨야
동작하는데, ingest_event() 를 없앤 뒤로 아무것도 안 만들고 있었다.
상세 응답의 `nodes`(카메라별 통과 기록: node_id/local_track_id/
matched_at 등)가 옛 /api/events 스트림과 같은 역할을 할 수 있어서,
`ingest_journey()` 가 identity 처리 뒤에 이걸로 Tracklet/Event 를
다시 채운다. person 이 확정된 경우(=MANUAL_REVIEW_REQUIRED 아님)에만
한다 — 신원 미확정 상태로 카메라 알림을 울리면 임시 UID 를 사실상
노출하는 셈이라 B 지시사항(임시 UID 최종 노출 금지)에 어긋난다.
"""
from __future__ import annotations

import zlib
from datetime import datetime
from typing import Any

from django.utils import timezone

from .models import Camera, Event, Journey, Person, Tracklet
from .mqtt_ingest import NODE_CAMERAS


def _parse_at(value: str | None):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return timezone.make_aware(dt) if timezone.is_naive(dt) else dt


def _get_camera(node_id: str) -> Camera:
    info = NODE_CAMERAS.get(node_id, {"index": 999, "name": f"Camera {node_id}"})
    cam, _ = Camera.objects.get_or_create(
        index=info["index"],
        defaults={"name": info["name"], "source": "",
                  "note": "메인 서버 API 자동 등록"},
    )
    return cam


def _node_local_id(journey_id: str, node_id: str, local_track_id: Any) -> int:
    """(journey_id, node_id, local_track_id) 조합을 Tracklet.local_id(정수)
    로 안정적으로 바꾼다. local_track_id 는 노드 하나 안에서만 의미 있는
    작은 롤링 번호라(B 경고: "D Local Track=13 과 Person ID=P000006 은
    완전히 다른 값") journey_id 를 반드시 같이 섞어야 한다 — 안 그러면
    다른 날 다른 사람이 같은 카메라에서 같은 local_track_id 를 받았을 때
    같은 트랙렛으로 잘못 합쳐진다."""
    return zlib.crc32(f"{journey_id}:{node_id}:{local_track_id}".encode("utf-8"))


def _ingest_nodes(person: Person, journey: "Journey", nodes: list[dict[str, Any]]) -> None:
    """상세 응답의 `nodes`(카메라별 통과 기록)로 Tracklet/Event 를 채운다
    — 카메라별 "오늘 감지 횟수"·이벤트 로그·TTS 알림이 전부 이 Event
    레코드를 보고 동작한다(dashboard.html 의 진입 이벤트 처리 참고).
    같은 journey 를 재처리해도(예: 판정이 나중에 바뀌어 다시 부를 때)
    Tracklet get_or_create 가 중복 생성을 막는다.

    2026-08-12: Event.journey 를 여기서 채운다 — journey 는 호출부에서
    이미 update_or_create 로 만들어진 뒤의 실제 객체를 받는다(journey_id
    문자열만으로는 나중에 "이 감지가 어느 journey 캡처 사진을 쓰는지"
    못 찾는다, 사진은 Journey.body_images/face_images 에 있다)."""
    for node in nodes or []:
        node_id = node.get("node_id")
        if not node_id:
            continue
        cam = _get_camera(node_id)
        entered_at = _parse_at(node.get("entered_at"))
        matched_at = _parse_at(node.get("matched_at")) or entered_at or timezone.now()
        exited_at = _parse_at(node.get("exited_at"))
        local_track_id = node.get("local_track_id")

        _tracklet, created = Tracklet.objects.get_or_create(
            person=person, camera=cam,
            local_id=_node_local_id(journey.journey_id, node_id, local_track_id),
            defaults={"start_at": entered_at or matched_at,
                     "end_at": exited_at or matched_at, "frames": 1},
        )
        if not created:
            continue   # 이미 이 노드 통과를 기록했다 — 이벤트도 이미 만들어져 있다

        Event.objects.create(
            person=person, camera=cam, kind=Event.ENTER, at=matched_at,
            detail=f"{node_id} journey={journey.journey_id}",
            was_unregistered=not person.confirmed,
            journey=journey,
        )


def _route_str(route: Any) -> str:
    """실API는 route 를 배열로 준다(["A","C","D"]) — 화면 표시용으로
    "A -> C -> D" 문자열 하나로 합친다."""
    if isinstance(route, list):
        return " -> ".join(str(n) for n in route)
    return route or ""


def _sync_person(canonical_uid: str, entry_at, visit_count) -> Person | None:
    """canonical_person_uid 로만 Person 을 만들거나 갱신한다. 호출부에서
    이미 MANUAL_REVIEW_REQUIRED 가 아님을 확인한 뒤에만 불러야 한다."""
    if not canonical_uid:
        return None
    person, _created = Person.objects.get_or_create(
        external_id=canonical_uid,
        defaults={"created_at": entry_at or timezone.now(),
                 "last_seen": entry_at or timezone.now()})
    update_fields = []
    if visit_count is not None and person.visit_count != visit_count:
        person.visit_count = visit_count
        update_fields.append("visit_count")
    if entry_at and entry_at > person.last_seen:
        person.last_seen = entry_at
        update_fields.append("last_seen")
    if update_fields:
        person.save(update_fields=update_fields)
    return person


def ingest_journey_summary(item: dict[str, Any]) -> Journey | None:
    """`/api/journeys?limit=` 목록의 항목 1건 — identity 세부가 없는
    얕은 요약이다. 여기 있는 person_uid 는 신뢰할 수 없으므로(검토
    대기 중이면 temporary 값이 그대로 나옴, 실측 확인) identity/canonical
    관련 필드는 절대 건드리지 않는다 — 그냥 진행상황 필드만 최신화한다.
    아직 한 번도 못 본 journey_id 면 새로 만들되 identity 필드는 빈 채로
    둔다(다음 상세 조회가 채운다)."""
    journey_id = item.get("journey_id")
    if not journey_id:
        return None
    journey, _ = Journey.objects.update_or_create(
        journey_id=journey_id,
        defaults={
            # CharField 는 NOT NULL 이라 .get(key, "") 로는 안 된다 — 실API가
            # 값이 없는 필드를 키 자체를 생략하는 대신 JSON null 로 명시적으로
            # 채워 보낸다(get_or_create 크래시로 실측 확인, 2026-08-11) —
            # `.get(key, "")` 는 "키가 아예 없을 때"만 기본값을 쓰고 값이
            # None 이면 그대로 None 을 돌려주므로 `or ""` 로 다시 걸러야 한다.
            "person_status": item.get("person_status") or "",
            "journey_status": item.get("journey_status") or "",
            "route": _route_str(item.get("route")),
            "entry_at": _parse_at(item.get("entry_at")),
            "d_exit_at": _parse_at(item.get("d_exit_at")),
            "journey_elapsed_seconds": item.get("journey_elapsed_seconds"),
            "visit_count": item.get("visit_count"),
            "initial_decision": item.get("initial_decision") or "",
        },
    )
    return journey


def _extract_captures(data: dict[str, Any]) -> tuple[list, list]:
    """상세 응답의 `capture_groups.A.body`/`.face` → [{rank, quality, url}, ...]
    그대로 뽑는다(원래 요청: "각 최대 3장"). url 없는 항목은 버리고 rank 순
    정렬 후 3장까지만 자른다 — Main 이 이미 순서대로 주더라도 방어적으로."""
    groups = (data.get("capture_groups") or {}).get("A") or {}

    def _clean(items):
        out = [{"rank": im.get("rank"), "quality": im.get("quality"), "url": im.get("url")}
               for im in (items or []) if im.get("url")]
        out.sort(key=lambda x: x.get("rank") or 999)
        return out[:3]

    return _clean(groups.get("body")), _clean(groups.get("face"))


def ingest_journey(data: dict[str, Any]) -> Journey | None:
    """`/api/journeys/{journey_id}` 상세 1건(Final Identity Review 전체
    포함)을 저장한다. journey_id 가 없으면 무시.

    B 지시사항(2026-08-11): `temporary_person_uid`/`initial_candidate_person_uid`/
    `final_candidate_person_uid` 는 참고용이고 절대 최종 Person ID 로 쓰면
    안 된다. `Journey.person` 을 실제로 연결하는 건 canonical_person_uid
    가 있고 최종 판정이 MANUAL_REVIEW_REQUIRED 가 아닐 때뿐.

    2026-08-13 DB 초기화 이후 갱신(재현/실측 확인): Main 이 admin API 배포와
    같이 재시작되면서, identity 판정 필드들이 이 문서가 처음 정리했던
    `identity.{canonical_person_uid,final_result,...}` 중첩 위치에서 최상위
    (`data.canonical_person_uid`/`data.identity_result`/`data.candidate_person_uid`)
    로 옮겨갔다 — 그런데 `identity` 객체 자체는 응답에서 안 없어지고
    필드가 전부 null 로만 남아 있어서(옛 스키마를 그대로 믿고 있던 코드가
    에러 없이 조용히 "판정 없음"으로만 보였다), 리셋 후 첫 실데이터
    (J000001)가 실제로는 identity_result="NEW"/canonical_person_uid="P000001"
    로 이미 확정됐는데도 "감지 리스트"에 하나도 안 뜨는 회귀가 있었다.
    최상위 필드를 먼저 보고, 없으면(구버전 Main 이거나 정말 값이 없는
    경우) 예전 중첩 `identity.*` 로 폴백한다 — 어느 스키마로 와도 받는다
    (§_resolve_capture_url 에서 쓴 것과 같은 신구 호환 전략)."""
    journey_id = data.get("journey_id")
    if not journey_id:
        return None

    identity = data.get("identity") or {}
    timing = data.get("timing") or {}
    person_obj = data.get("person") or {}

    canonical_uid = data.get("canonical_person_uid") or identity.get("canonical_person_uid") or ""
    review_result = data.get("identity_result") or identity.get("final_result") or ""
    entry_at = _parse_at(data.get("entry_at"))
    d_exit_at = _parse_at(timing.get("d_exit"))
    elapsed = timing.get("elapsed_seconds")
    visit_count = person_obj.get("visit_count")

    person = None
    if canonical_uid and review_result != Journey.MANUAL_REVIEW:
        person = _sync_person(canonical_uid, entry_at, visit_count)

    body_images, face_images = _extract_captures(data)

    final_score = data.get("final_score")
    if final_score is None:
        final_score = identity.get("final_score")
    final_scores = data.get("final_scores") or identity.get("final_scores")

    journey, _ = Journey.objects.update_or_create(
        journey_id=journey_id,
        defaults={
            "person": person,
            "person_status": data.get("person_status") or "",
            "journey_status": data.get("journey_status") or "",
            "route": _route_str(data.get("route")),
            "entry_at": entry_at,
            "d_exit_at": d_exit_at,
            "journey_elapsed_seconds": elapsed,
            "visit_count": visit_count,
            "initial_decision": data.get("initial_decision") or identity.get("initial_decision") or "",
            "temporary_person_uid": data.get("temporary_person_uid") or identity.get("temporary_person_uid") or "",
            "candidate_person_uid": (data.get("candidate_person_uid") or person_obj.get("candidate_person_uid")
                                     or identity.get("initial_candidate_person_uid") or ""),
            "final_candidate_person_uid": (data.get("final_candidate_person_uid")
                                           or identity.get("final_candidate_person_uid") or ""),
            "final_score": final_score,
            "canonical_person_uid": canonical_uid,
            "final_review_result": review_result,
            "final_scores": final_scores,
            "body_images": body_images,
            "face_images": face_images,
        },
    )

    # Journey 가 이미 만들어진 뒤에 노드를 채워야 Event.journey FK 를
    # 걸 수 있다(사진 연결용) — 그래서 update_or_create 뒤로 옮겼다.
    if person:
        _ingest_nodes(person, journey, data.get("nodes"))

    return journey
