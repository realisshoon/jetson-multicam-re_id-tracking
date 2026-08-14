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
당시엔 상세 응답의 `nodes`/`captures`(카메라별 통과·캡처 기록)로
대신 Tracklet/Event 를 채웠었다(`_ingest_nodes()`, 이제 삭제됨 — 아래
2026-08-14 항목 참고).

2026-08-14: B가 `/api/events?since=<ISO timestamp>` 를 실제로 부활시켰다
(HANDOFF_TO_MAIN_SERVER.md §8 요청에 대한 응답, 임시 포트 8081에서 확인).
응답 1건 모양(실측):
    {"event_id": 168, "at": "2026-08-14T13:00:01+09:00",
     "journey_id": "J000052", "node": "A", "kind": "ENTRY",
     "person_uid": "P000039", "canonical_person_uid": "P000039",
     "identity_status": "NEW"}
    (kind: ENTRY/PASSAGE/ARRIVAL, node_id 별로 하나씩 옴)
`since` 는 필수고(없으면 400), 응답에 `next_since` 커서가 같이 온다
(`main_server_worker.py::poll_events()` 가 이어서 유지).

이게 `nodes`/`captures` 를 훑던 옛 방식보다 훨씬 낫다: (1) 카메라 B가
`nodes`/`captures` 어디에도 안 잡히던 문제와 무관하게 Main이 직접
이벤트로 쏴준다, (2) journey 상세를 매번 다시 조회할 필요 없이 이
스트림 하나로 카메라별 실시간 감지를 바로 안다, (3) `event_id` 가
Main이 보장하는 유일 값이라 중복 방지가 훨씬 단순하다. 그래서 Event
생성은 이제 이 스트림(`ingest_event_item()`)이 유일한 경로다 —
`ingest_journey()` 는 Journey 자체 필드(캡처/점수/최종판정)만 채우고
더 이상 Event 를 안 만든다(두 경로가 같이 만들면 카메라당 소리가
두 번 나는 등 중복이 생긴다).
"""
from __future__ import annotations

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


def ingest_event_item(item: dict[str, Any]) -> None:
    """`/api/events` 스트림 1건 → Tracklet/Event(카메라별 감지 이벤트,
    등록완료/미등록자감지 알림음이 전부 이걸 보고 동작한다).

    B 지시사항 그대로 적용: `person_uid` 는 못 믿고(임시값일 수 있음)
    `canonical_person_uid` 만 최종 신원으로 쓴다 — 그 값이 있고
    `identity_status` 가 MANUAL_REVIEW_REQUIRED 가 아닐 때만 Person 을
    연결한다(신원 미확정 상태로 카메라 알림을 울리면 임시 UID 를 사실상
    노출하는 셈이라 안 된다).

    `event_id` 는 Main 이 보장하는 유일 값이라 그대로 Tracklet.local_id
    로 써서 중복을 막는다(카메라+event_id 조합은 재발급되지 않는다) —
    옛 nodes/captures 스크래핑 방식(§_node_local_id, CRC32 해시)보다
    훨씬 단순하고 확실하다."""
    event_id = item.get("event_id")
    node_id = item.get("node")
    if event_id is None or not node_id:
        return

    canonical_uid = item.get("canonical_person_uid") or ""
    identity_status = item.get("identity_status") or ""
    if not canonical_uid or identity_status == Journey.MANUAL_REVIEW:
        return   # 아직 신원 미확정 — 임시 UID 로 Event 안 만든다(B 지시사항)

    at = _parse_at(item.get("at")) or timezone.now()
    person = _sync_person(canonical_uid, at, None)
    if not person:
        return

    cam = _get_camera(node_id)
    journey = Journey.objects.filter(journey_id=item.get("journey_id")).first()

    _tracklet, created = Tracklet.objects.get_or_create(
        person=person, camera=cam, local_id=event_id,
        defaults={"start_at": at, "end_at": at, "frames": 1},
    )
    if not created:
        return   # 이미 이 이벤트를 처리했다

    Event.objects.create(
        person=person, camera=cam, kind=Event.ENTER, at=at,
        detail=f"{node_id} journey={item.get('journey_id')} (events API, kind={item.get('kind')})",
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

    return journey
