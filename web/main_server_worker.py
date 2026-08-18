#!/usr/bin/env python
"""
메인 서버(B, 10.10.20.33:8080) REST API 를 폴링해서 감지 이벤트를 DB 에
적재하는 프로세스. 2026-08-11 확정된 구조에 따라 mqtt_worker.py(Jetson
MQTT 직접 구독)를 대체한다 — Django 는 이제 MQTT 를 전혀 구독하지 않고,
메인 서버가 이미 정리한 REST API 만 호출한다.

    Jetson A/B/C/D → MQTT → 메인 서버(Windows) → main_server.db
    → REST API(:8080) → 이 워커 → 로컬 DB → 대시보드

주소(IP/포트)는 RuntimeConfig(DB, Django admin 에서 편집)가 기준이라
이 워커가 주기적으로 확인해서 바뀌면 알아서 반영한다. "감지 on/off" 도
같은 RuntimeConfig.detection_enabled 를 봐서, 꺼져 있으면 폴링 자체를
쉰다(메인 서버에 요청을 안 보낸다).

메인 서버가 아직 없거나(API 미기동) 응답이 없어도 죽지 않고 계속
재시도한다 — Jetson 이 나중에 켜지는 경우를 기다리던 mqtt_worker.py 의
connect_async 패턴과 같은 태도.

2026-08-11 오후 갱신(B): `GET /api/events`·`GET /api/persons`(목록) 는
Main 에 없는 엔드포인트였다(404) — Main API 는 그대로 두고 이 워커가
실제 계약에 맞춘다. 이제 호출하는 건 이것뿐:
    GET /api/health
    GET /api/journeys?limit=<n>
    GET /api/journeys/{journey_id}
목록은 얕은 요약이라 매 틱 가볍게 갱신하고, Final Identity Review
전체(temporary/canonical/final_scores 등)가 필요한 상세 조회는 "처음
보는 journey_id" 이거나 "final_review_result 값이 방금 바뀐" 경우에만
한다 — 검토 대기(MANUAL_REVIEW_REQUIRED) 였던 게 나중에 확정되는 걸
놓치지 않으면서도, 매 틱 N건 전부를 상세 조회하는 낭비를 피한다.

2026-08-14: B가 `/api/events?since=<ISO timestamp>` 를 다시 살렸다
(임시로 8081 포트에서 확인, RuntimeConfig 로 반영). 카메라별 감지
이벤트(Event/Tracklet, 알림음의 데이터 소스)는 이제 `/api/journeys`
상세의 nodes/captures 를 훑던 방식 대신 이 스트림 하나로 받는다 —
poll_events() 가 커서(`since`/`next_since`) 기반으로 폴링한다. 재시작
때마다 과거 이벤트를 전부 재생하면 오래된 감지들이 전부 새 알림음으로
다시 울리니, 워커가 처음 뜨는 순간의 시각을 시작 커서로 잡는다(그
이전 이벤트는 무시).

실행:  python main_server_worker.py
"""
import os
import time

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()                                    # ← ORM 쓰려면 반드시 먼저

import requests                                    # noqa: E402
from django.utils import timezone                  # noqa: E402

from tracking import bus                            # noqa: E402
from tracking.main_api_ingest import (               # noqa: E402
    ingest_event_item, ingest_journey, ingest_journey_summary)
from tracking.models import RuntimeConfig            # noqa: E402

STATE_KEY = "state:main"     # 로컬 파이프라인의 state:live 와 분리
POLL_SEC = 1.0
JOURNEY_LIMIT = 50           # B 계약 예시(GET /api/journeys?limit=50)와 동일
REQUEST_TIMEOUT = 3.0
# 이 상태가 아니면 journey 가 아직 진행 중이라고 본다 — 진행 중인 동안은
# 노드가 조용히 완결될 수 있어(§poll_journeys 2026-08-14) 신호 변화와
# 무관하게 매 틱 상세를 다시 받는다.
TERMINAL_JOURNEY_STATUSES = {"EXPIRED", "COMPLETED"}

worker_state = {"connected": False, "entries_total": 0}

# journey_id → 마지막으로 확인한 (identity_result, journey_status). 둘 중
# 하나라도 바뀔 때만 상세(identity+nodes)를 다시 받는다 — 프로세스 생존
# 기간 동안만 유지되면 충분하다(재시작하면 첫 폴링에서 전부 "처음 보는
# journey" 로 다시 채움).
_known_review_state: dict[str, tuple[str | None, str | None]] = {}

# /api/events 커서. None 이면 아직 한 번도 안 불렀다는 뜻 — 그때 "지금
# 시각"으로 초기화해서, 워커가 뜨기 전의 오래된 이벤트를 다시 재생해
# 알림음이 몰아서 울리는 걸 막는다.
_events_since: str | None = None


def _base_url(cfg: RuntimeConfig) -> str:
    return f"http://{cfg.main_server_host}:{cfg.main_server_port}"


def poll_health(base_url: str) -> None:
    resp = requests.get(f"{base_url}/api/health", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()


def poll_journeys(base_url: str) -> None:
    """`/api/journeys?limit=` 목록을 받아 요약 필드를 갱신하고, identity
    가 아직 없거나 방금 바뀐 journey 만 `/api/journeys/{id}` 상세를 추가
    조회해서 Final Identity Review 전체를 반영한다.

    2026-08-11 저녁 갱신: main_connected(health 기준)와 journey polling
    실패를 분리해달라는 요청 — 이 함수는 자기 실패를 자기가 처리하고
    (예외를 밖으로 던지지 않는다) worker_state["journeys_ok"] 에만 반영,
    main() 의 main_connected 판정에는 관여하지 않는다."""
    try:
        resp = requests.get(f"{base_url}/api/journeys",
                            params={"limit": JOURNEY_LIMIT}, timeout=REQUEST_TIMEOUT)
        http_status = resp.status_code
        resp.raise_for_status()
        items = resp.json().get("items", [])
    except requests.RequestException as error:
        worker_state["journeys_ok"] = False
        print(f"[MAIN JOURNEY POLL] HTTP=FAIL error={error}")
        return

    worker_state["journeys_ok"] = True
    latest = items[0].get("journey_id") if items else None   # API 가 최신순으로 준다(실측)
    ingested = skipped = errors = 0

    for item in items:
        journey_id = item.get("journey_id")
        if not journey_id:
            continue
        ingest_journey_summary(item)

        # 2026-08-13: Main 이 admin API 배포와 같이 재시작되면서 판정 필드가
        # final_review_result(항상 null 로만 옴, 실측 확인)에서 identity_result
        # 로 옮겨갔다 — 옛 필드만 보면 이 값이 절대 안 바뀌어서(항상 None)
        # "판정이 바뀌었을 때만 상세 재조회"가 처음 한 번 뒤로 다시는 안
        # 돌아 검토 대기(MANUAL_REVIEW_REQUIRED)가 나중에 확정돼도 못 잡는다.
        #
        # 2026-08-13 추가 실측: identity_result 만 보고 재조회하면, 신원이
        # 아주 빨리(예: A 통과 직후) 확정돼서 첫 상세 조회 시점엔 아직
        # journey 가 안 끝나 nodes(카메라별 통과 기록)가 일부만 들어있는
        # 경우를 놓친다 — identity_result 는 그 뒤로 안 바뀌니 다시는
        # 상세를 안 불러서, D 통과 등 나중에 닫힌 노드의 Event/Tracklet
        # (카메라별 감지 횟수·확인음/경고음이 여기서 나온다)이 영원히
        # 안 생겼다(실측: Event 0건). journey_status(WAITING_D→EXPIRED/
        # COMPLETED 등)도 같이 봐서, 여정이 더 진행되면 다시 상세를 받는다.
        #
        # 2026-08-14 추가 실측(라이브로 직접 확인): journey_status 가 안
        # 바뀐 채로도(예: WAITING_B_OR_C 그대로) 그 사이에 카메라 A 노드
        # 자체가 matched_at/exited_at 로 조용히 완결되는 경우가 있다 —
        # "다음 카메라로 넘어가야만 상태가 바뀐다"는 가정이 틀렸다. 그래서
        # journey_status 가 아직 끝난 상태(EXPIRED/COMPLETED)가 아닌 동안은
        # 신호가 안 바껴도 매 틱 그냥 다시 상세를 받는다 — 진행 중인
        # journey 수가 많지 않아서(JOURNEY_LIMIT=50, REQUEST_TIMEOUT=3s)
        # 비용이 크지 않다. 끝난 뒤에는 기존처럼 신호가 바뀔 때만 받는다.
        cur_signal = (item.get("identity_result") or item.get("final_review_result"),
                     item.get("journey_status"))
        first_seen = journey_id not in _known_review_state
        still_active = item.get("journey_status") not in TERMINAL_JOURNEY_STATUSES
        if not (first_seen or still_active or _known_review_state.get(journey_id) != cur_signal):
            skipped += 1
            continue   # identity/진행상태 그대로(이미 끝난 여정) — 요약만 갱신하고 다음 항목으로

        try:
            detail = requests.get(f"{base_url}/api/journeys/{journey_id}",
                                  timeout=REQUEST_TIMEOUT)
            detail.raise_for_status()
            ingest_journey(detail.json())
        except requests.RequestException as error:
            errors += 1
            print(f"[main-api] {journey_id} 상세 조회 실패(다음 폴링에 재시도): {error}")
            continue   # _known_review_state 갱신 안 함 → 다음 틱에 재시도

        if first_seen:
            worker_state["entries_total"] += 1
        ingested += 1
        _known_review_state[journey_id] = cur_signal

    print(f"[MAIN JOURNEY POLL] HTTP={http_status} received={len(items)} "
          f"latest={latest or '-'} ingested={ingested} skipped={skipped} errors={errors} "
          f"(누적 적재 {worker_state['entries_total']}건)")


def poll_events(base_url: str) -> None:
    """`/api/events?since=` 커서 폴링 — 카메라별 Event/Tracklet(알림음
    데이터 소스)은 이제 전부 여기서 만든다. `since` 는 필수 파라미터라
    (없으면 400) 첫 호출 전에 반드시 `_events_since` 를 채워둬야 한다.
    실패해도(연결 끊김 등) 커서를 그대로 둬서 다음 틱에 같은 지점부터
    이어받는다 — 이벤트를 건너뛰지 않는다."""
    global _events_since
    if _events_since is None:
        _events_since = timezone.now().isoformat()
        print(f"[MAIN EVENTS POLL] 시작 커서={_events_since}")

    try:
        resp = requests.get(f"{base_url}/api/events",
                            params={"since": _events_since}, timeout=REQUEST_TIMEOUT)
        http_status = resp.status_code
        resp.raise_for_status()
        body = resp.json()
        items = body.get("items", [])
        next_since = body.get("next_since")
    except requests.RequestException as error:
        print(f"[MAIN EVENTS POLL] HTTP=FAIL error={error}")
        return

    for item in items:
        try:
            ingest_event_item(item)
        except Exception as error:                   # noqa: BLE001
            print(f"[main-api] event_id={item.get('event_id')} 적재 실패(무시): {error!r}")

    if next_since:
        _events_since = next_since

    print(f"[MAIN EVENTS POLL] HTTP={http_status} received={len(items)} "
          f"since={_events_since}")


def main():
    print("[main-api] 폴링 시작")

    while True:
        try:
            cfg = RuntimeConfig.get()

            if not cfg.detection_enabled:
                worker_state["connected"] = False
                bus.publish_state({
                    "main_connected": False,
                    "entries_total": worker_state["entries_total"],
                }, key=STATE_KEY)
                time.sleep(POLL_SEC)
                continue

            base_url = _base_url(cfg)
            try:
                poll_health(base_url)
                if not worker_state["connected"]:
                    print(f"[main-api] 연결 완료: {base_url}")
                worker_state["connected"] = True
            except requests.RequestException as error:
                if worker_state["connected"]:
                    print(f"[main-api] 연결 끊김(health): {error}")
                worker_state["connected"] = False

            # journey/events polling 은 health 가 살아있을 때만 시도하되, 그
            # 성패가 main_connected 를 건드리지 않는다(각자 자기 실패를
            # 자기가 처리) — health 는 되는데 journeys/events 엔드포인트만
            # 잠깐 흔들리는 경우를 "연결 끊김"으로 잘못 보여주지 않기 위함.
            if worker_state["connected"]:
                poll_journeys(base_url)
                poll_events(base_url)

            bus.publish_state({
                "main_connected": worker_state["connected"],
                "entries_total": worker_state["entries_total"],
            }, key=STATE_KEY)
        except Exception as error:                      # noqa: BLE001
            # 이 워커는 사람이 안 봐도 계속 돌아야 한다 — bus 파일 레이스
            # (Windows PermissionError) 처럼 예상 못한 오류로 프로세스 전체가
            # 죽은 채 방치돼 있었다(2026-08-11 밤 실제 사고). 여기서 잡아서
            # 로그만 남기고 다음 틱에 다시 시도한다.
            print(f"[main-api] 예상 못한 오류(무시하고 계속): {error!r}")

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print("[main-api] 종료")
