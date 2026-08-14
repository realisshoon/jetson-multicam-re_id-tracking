import asyncio
import hashlib
from datetime import timedelta
from urllib.parse import quote, urlparse

import requests
from django.contrib.auth import logout as auth_logout
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.core.paginator import Paginator
from django.db.models import Count, Q
from django.db.models.functions import TruncDate
from django.http import Http404, HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import bus, central_db, services
from .models import AiMatchFeedback, Camera, Event, Journey, Person, RuntimeConfig

MAIN_PERSONS_CACHE_KEY = "main_persons_list_v1"
MAIN_PERSONS_CACHE_SEC = 4
MAIN_PERSONS_STATUS_LABEL = {
    "ACTIVE": "활성", "IDENTITY_PENDING": "신원 확인 중", "REVIEW_REQUIRED": "검토 필요",
}

CAPTURE_FETCH_TIMEOUT = 3.0                            # main_server_worker.py 와 동일한 관례
CAPTURE_ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png"}

BOUNDARY = "reidframe"
DETECTIONS_PER_CAMERA = 30   # "미등록자 감지 기록" 카메라별 캐러셀에 쥐고 있을 최근 건수


def _camera_letter(camera) -> str:
    """Camera.name 은 "Camera A · 입장" 식 전체 설명이다 — 거기서 A/B/C/D
    한 글자만 뽑는다."""
    if not camera:
        return ""
    parts = camera.name.split()
    return parts[1] if len(parts) > 1 else camera.name


def _short_cam_label(camera) -> str:
    letter = _camera_letter(camera)
    return f"CAMERA-{letter}" if letter else "—"


def _thumb_url(journey) -> str | None:
    """Journey 1건의 대표 썸네일(FACE rank1 우선, 없으면 BODY rank1)을
    인증 프록시 URL로 — 실제 Jetson 주소는 절대 브라우저에 안 준다."""
    if not journey:
        return None
    thumb = journey.thumb_capture()
    return f"/journeys/{journey.journey_id}/capture/{thumb[0]}/{thumb[1]}/" if thumb else None


def _local_thumb_for_uid(uid: str) -> str | None:
    """canonical_person_uid 로 걸린 로컬 Journey 중 사진 있는 걸 찾는다.
    Person 행이 아직 없어도(§main_api_ingest._sync_person, MANUAL_REVIEW_
    REQUIRED 동안은 Person 을 안 만듦) Journey.canonical_person_uid 텍스트
    필드는 먼저 채워지는 경우가 있어 이걸로도 찾아본다.

    2026-08-13: Main 대표사진(representative_image_url)이 있으면 그게
    1순위다 — §_representative_thumb_url. 없을 때만 이 Journey 캡처
    폴백을 쓴다."""
    main_thumb = _representative_thumb_url(uid)
    if main_thumb:
        return main_thumb
    for journey in Journey.objects.filter(canonical_person_uid=uid).order_by("-entry_at")[:5]:
        url = _thumb_url(journey)
        if url:
            return url
    return None


def _main_base_url() -> str:
    cfg = RuntimeConfig.get()
    return f"http://{cfg.main_server_host}:{cfg.main_server_port}"


def _fetch_main_persons() -> list | None:
    """메인 서버 /api/persons 목록을 페이지 넘겨가며 끌어온다.

    2026-08-12: 로컬 Person 테이블은 canonical identity 가 확정된 극소수만
    있다(MANUAL_REVIEW_REQUIRED 동안은 Person 을 아예 안 만듦, §_sync_person)
    — "전체 인물" 페이지가 로컬 테이블만 보면 거의 비어 보인다. Main 이
    2026-08-12 부로 이 목록 엔드포인트를 새로 열어서(어제까진 404였음,
    main_server_worker.py 상단 주석 참고) 이제 여기서 직접 끌어온다 —
    신원의 source of truth 는 항상 Main 이라는 설계 원칙 그대로.

    실패하면(Main 오프라인 등) None — 호출부가 로컬 테이블로 폴백한다."""
    base_url = _main_base_url()
    items, offset = [], 0
    try:
        while True:
            resp = requests.get(f"{base_url}/api/persons",
                                params={"limit": 200, "offset": offset},
                                timeout=CAPTURE_FETCH_TIMEOUT)
            resp.raise_for_status()
            page = resp.json().get("items", [])
            items.extend(page)
            if len(page) < 200 or len(items) >= 1000:   # 안전장치 — 무한 루프 방지
                break
            offset += 200
    except requests.RequestException:
        return None
    return items


def _fetch_main_person_detail(uid: str) -> dict | None:
    """메인 서버 /api/persons/{uid} 상세 — 로컬에 이 사람 Journey 기록이
    거의/전혀 없을 때(위와 같은 이유) 상세 페이지를 채우는 용도."""
    try:
        resp = requests.get(f"{_main_base_url()}/api/persons/{uid}",
                            timeout=CAPTURE_FETCH_TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException:
        return None


# ------------------------------------------------------------------
# 2026-08-13: Main 이 Camera A 캡처를 중앙 저장하고 Person 단위 대표사진을
# 직접 고른 뒤 representative_image_url 로 내려주기로 했다 — 대표사진
# "선정 로직"(FACE/BODY 품질 비교 등)은 Web 에서 다시 만들지 않는다,
# Main 이 이미 고른 결과를 그대로 가져와서 보여줄 뿐이다. 우선순위는
# 항상 1) Main representative_image_url 2) 기존 Journey 캡처 폴백
# 3) NO IMG 순서 — 아래 헬퍼들이 이 순서를 담당한다.
#
# 2026-08-13 실측 확인: 지금 Main(GET /api/persons, GET /api/persons/{uid})
# 은 아직 이 필드를 안 내려준다(스키마 그대로) — 그래서 지금은 전부
# 조용히 기존 Journey 폴백으로 넘어간다. Main 이 필드를 채우기 시작하는
# 순간 아래 로직이 자동으로 그걸 우선 쓰기 시작한다(추가 배포 불필요).
def _main_representative_urls() -> dict[str, str]:
    """캐시된 Main persons 목록(§_fetch_main_persons, 4초 캐시)에서
    person_uid → representative_image_url 매핑만 뽑는다. 대시보드가
    1초마다 폴링하는 api_state() 처럼 한 번에 수십~수백 건을 다루는
    곳에서 인물별로 Main 상세를 또 조회하면 안 된다(N+1 로 폴링 부하가
    터진다) — 이미 목록 조회 때 캐시된 응답만 재사용한다."""
    items = cache.get(MAIN_PERSONS_CACHE_KEY)
    if items is None:
        items = _fetch_main_persons()
        if items is not None:
            cache.set(MAIN_PERSONS_CACHE_KEY, items, MAIN_PERSONS_CACHE_SEC)
    if not items:
        return {}
    return {it["person_uid"]: it["representative_image_url"]
            for it in items if it.get("person_uid") and it.get("representative_image_url")}


def _main_capture_proxy_url(person_uid: str) -> str:
    return f"/captures/main-person/{quote(person_uid, safe='')}/"


def _representative_thumb_url(person_uid: str) -> str | None:
    """person_uid 의 Main 대표사진 프록시 URL(있으면) — 대량 목록/폴링
    경로에서 쓰는 저비용 버전. 캐시된 Main persons 목록만 보고, 없으면
    None 을 돌려줘서 호출부가 기존 Journey 기반 폴백으로 넘어가게 한다."""
    if not person_uid:
        return None
    return _main_capture_proxy_url(person_uid) if _main_representative_urls().get(person_uid) else None


def _resolve_capture_url(value: str | None) -> str | None:
    """캡처 이미지 URL(상대경로 또는 절대 URL)을 실제로 요청 가능한 절대
    URL로 만든다 — Journey.body_images/face_images(§_extract_captures)와
    Main 의 representative_image_url 이 공통으로 쓴다.

    2026-08-13 실측: Main 이 캡처 서빙 방식을 바꿨다 — 예전엔 Camera A
    Jetson 이 직접 서빙하는 절대 URL(예: http://10.10.20.56:8000/captures/...)
    이었는데, 이제 Main 이 직접 서빙하는 상대경로(예: /api/captures/123/image)
    로 내려준다. 이미 저장된 옛 데이터엔 절대 URL이 남아있을 수 있어서
    두 형태를 다 받는다:
      - 상대경로 → 설정된 Main REST API base URL 기준으로 해석
      - 절대 URL → host:port 가 Main 설정이거나, 우리가 아는 Camera
        (jetson_host/jetson_port) 중 하나와 실제로 일치할 때만 신뢰한다
        (다른 host 로 유도되는 것 방지 — SSRF 방지)."""
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        if parsed.scheme != "http" and parsed.scheme != "https":
            return None
        cfg = RuntimeConfig.get()
        default_port = 443 if parsed.scheme == "https" else 80
        is_main = (parsed.hostname == cfg.main_server_host
                  and (parsed.port or default_port) == cfg.main_server_port)
        is_camera = Camera.objects.filter(jetson_host=parsed.hostname,
                                          jetson_port=parsed.port, enabled=True).exists()
        return value if (is_main or is_camera) else None
    return _main_base_url().rstrip("/") + "/" + value.lstrip("/")


@login_required
def api_main_person_capture_proxy(request, person_uid):
    """Main 이 결정한 Person 대표사진(representative_image_url)을 안전하게
    가져와 스트리밍한다 — §api_capture_proxy 와 같은 원칙: 브라우저에는
    이 프록시 주소만 노출되고 Main 의 실제 주소는 절대 노출되지 않는다.
    대표사진 "선정" 은 여기서 하지 않는다 — Main 상세를 다시 조회해서
    (요청 시점 최신값 유지) representative_image_url 을 그대로 가져올
    뿐이다. 목록 캐시에 없으면(예: Main 이 상세에만 채우는 경우) 상세
    조회 결과도 같이 확인한다."""
    detail = _fetch_main_person_detail(person_uid)
    rel = ((detail or {}).get("representative_image_url")
           or _main_representative_urls().get(person_uid))
    url = _resolve_capture_url(rel)
    if not url:
        raise Http404

    try:
        resp = requests.get(url, timeout=CAPTURE_FETCH_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException:
        raise Http404   # Main 이 꺼져있거나 그 사이 파일이 지워진 경우 등 — 500 대신 404

    content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
    if content_type not in CAPTURE_ALLOWED_CONTENT_TYPES:
        raise Http404

    response = HttpResponse(resp.content, content_type=content_type)
    response["Cache-Control"] = "private, max-age=300"
    return response


def _detection_dict(e: Event) -> dict:
    """미등록자 감지 이벤트 1건 → 화면용 dict. review_status 가 있으면
    확인/오류 처리된 것 — 화면에서 회색으로 표시하는 기준이 된다."""
    return {
        "id": e.pk,
        "kind": "event",
        "journey_id": e.journey.journey_id if e.journey_id else None,
        "at": timezone.localtime(e.at).strftime("%H:%M:%S"),
        "person": e.person.external_id or str(e.person),
        # person_uid 가 없으면(external_id 빈 값) 상세 페이지 링크를 못 만드니
        # 프론트에서 이 값이 있을 때만 클릭 가능하게 만든다.
        "person_uid": e.person.external_id or None,
        "review_status": e.review_status,
        "review_status_label": e.get_review_status_display() if e.review_status else "",
        "resolved_person": e.resolved_person.external_id if e.resolved_person else None,
        # 2026-08-12: "어떤 사람인지 볼 수 있게" 요청 — 이 감지가 속한
        # journey 의 Camera A 캡처 썸네일. journey 가 없거나(예: 아주 옛날
        # 데이터) 캡처가 없으면 None — 화면에서 NO IMG 로 표시한다.
        # 2026-08-13: Main 대표사진이 있으면 1순위(§_representative_thumb_url)
        # — 이 Event 는 항상 신원 확정된 사람만 생기므로 external_id 가 있다.
        "thumb_url": _representative_thumb_url(e.person.external_id) or _thumb_url(e.journey),
        # 2026-08-14: 이 Event 는 신원까지 이미 확정된 뒤에만 생긴다(Main
        # 이 candidate_person_uid 를 준 적도 없다 — 애매해서 헷갈린 게
        # 아니라 그냥 아직 관리자 검수 전이라 "미등록"으로만 보이는
        # 것뿐). 예전엔 이 경우 candidates 를 비워서 "데이터 선택" 모달을
        # 열어도 아무 후보도 안 떴는데, Main 이 이미 알려준 신원
        # (e.person)을 그냥 숨기는 셈이라 관리자가 헷갈렸다("후보가 떠야
        # 하는데 안 뜬다" 보고, 2026-08-14) — 이제 그 확정 신원 자체를
        # 유일한 candidate 로 채워서 모달이 그대로 기본 선택해준다.
        # is_known=True 로 표시해서 프런트가 "메인 서버 추정 후보"(진짜
        # 애매한 추정)와 "메인 서버 확정 인물"(이미 확실함)을 다른
        # 문구로 보여줄 수 있게 한다.
        "candidates": ([{"person_uid": e.person.external_id,
                        "thumb_url": _representative_thumb_url(e.person.external_id),
                        "is_known": True}]
                      if e.person.external_id else []),
        "final_score": None,
    }


def _journey_review_dict(j: Journey) -> dict:
    """"헷갈리는" 여정(MANUAL_REVIEW_REQUIRED) 1건 → "보류 리스트"용 dict.
    _detection_dict 와 같은 모양을 맞춰서 프런트가 카드 종류와 무관하게
    같은 컴포넌트로 렌더링한다. person_uid 는 절대 채우지 않는다(신원
    미확정, 임시 UID 를 최종 사람 ID 처럼 노출하지 말라는 B 지시사항).

    candidates 는 Main Re-ID 가 스스로 의심한 후보(참고용, 확정 아님) —
    "데이터 선택" 모달에 사진과 함께 보여준다. final_candidate_person_uid
    (Main 이 최종 처리한 후보)를 candidate_person_uid(초기 후보)보다 먼저
    두어서, 모달이 "AI 추천"으로 기본 선택하는 첫 번째 항목이 더 정제된
    쪽이 되게 한다."""
    candidate_uids = [uid for uid in
                      dict.fromkeys([j.final_candidate_person_uid, j.candidate_person_uid])
                      if uid]
    return {
        "id": f"j{j.pk}",
        "kind": "journey",
        "journey_id": j.journey_id,
        "at": timezone.localtime(j.entry_at).strftime("%H:%M:%S") if j.entry_at else "",
        "person": "검토 대기",
        "person_uid": None,
        "review_status": j.review_status,
        "review_status_label": j.get_review_status_display() if j.review_status else "",
        "resolved_person": j.local_match_person.external_id if j.local_match_person_id else None,
        "thumb_url": _thumb_url(j),
        "candidates": [{"person_uid": uid, "thumb_url": _local_thumb_for_uid(uid), "is_known": False}
                       for uid in candidate_uids],
        "final_score": j.final_score,
    }


def _camera_dicts():
    """Camera 관리자 화면(트래킹 > 카메라)에서 편집한 값을 그대로 카메라 월에
    쓴다. jetson_host 가 채워진 카메라는 그 Jetson 보드가 직접 서빙하는
    MJPEG 스트림을 브라우저가 곧바로 embed 하고(Django 를 거치지 않는다),
    비어있는 카메라는 이 PC에 연결된 로컬 소스로 보고 내부 프록시
    (/video/<index>/, mjpeg 뷰)를 쓴다. 보드가 여러 대(A/B/C/D 각각 다른
    IP)라도 카메라마다 IP를 따로 저장하니 각자 자기 주소로 스트리밍된다.
    jetson 카메라를 앞에 둬야 카메라 월의 A/B/C/D 라벨이 노드 이름과
    순서가 맞는다."""
    jetson_cams = (Camera.objects.filter(enabled=True)
                   .exclude(jetson_host="").exclude(jetson_host__isnull=True)
                   .order_by("index"))
    local_cams = (Camera.objects.filter(enabled=True)
                  .filter(Q(jetson_host="") | Q(jetson_host__isnull=True))
                  .order_by("index"))
    cams = []
    for c in jetson_cams:
        url = f"http://{c.jetson_host}:{c.jetson_port}/stream" if c.jetson_port else ""
        cams.append({"index": c.index, "name": c.name, "url": url,
                     "source": url or "미가동", "is_jetson": True})
    for c in local_cams:
        cams.append({"index": c.index, "name": c.name, "url": "",
                     "source": c.source, "is_jetson": False})
    return cams


# ------------------------------------------------------------------ (A) 화면
@login_required
def dashboard(request):
    people = (Person.objects.filter(is_active=True)
              .prefetch_related("snapshots", "tracklets__camera")
              .annotate(n_track=Count("tracklets"))[:24])
    return render(request, "tracking/dashboard.html", {
        "cameras": _camera_dicts(),
        "people": people,
        "config": RuntimeConfig.get(),
        "have_redis": bus.HAVE_REDIS,
    })


@login_required
async def mjpeg(request, cam_index: int):
    """multipart/x-mixed-replace 스트림.
    ASGI(daphne/uvicorn)에서 돌려야 한다. WSGI 면 워커가 통째로 잠긴다."""
    async def gen():
        last_hash = None
        idle = 0
        while True:
            jpeg = await asyncio.to_thread(bus.get_frame, cam_index)
            if jpeg:
                h = hashlib.md5(jpeg).digest()
                if h != last_hash:                 # 같은 프레임 재전송 안 함
                    last_hash = h
                    idle = 0
                    yield (f"--{BOUNDARY}\r\n"
                           f"Content-Type: image/jpeg\r\n"
                           f"Content-Length: {len(jpeg)}\r\n\r\n").encode()
                    yield jpeg
                    yield b"\r\n"
                else:
                    idle += 1
            else:
                idle += 1
            # 프레임이 안 들어오면 점점 느리게 폴링 (CPU 절약)
            await asyncio.sleep(0.02 if idle < 10 else 0.2)

    resp = StreamingHttpResponse(
        gen(), content_type=f"multipart/x-mixed-replace; boundary={BOUNDARY}")
    resp["Cache-Control"] = "no-store, no-cache, must-revalidate"
    resp["X-Accel-Buffering"] = "no"               # nginx 뒤에 둘 경우 버퍼링 방지
    return resp


# ------------------------------------------------------------------ API
@login_required
def api_state(request):
    """대시보드가 폴링하는 실시간 상태. WebSocket 없이 이걸로 충분하다.

    워커가 2개(로컬 카메라 파이프라인 tracker_worker.py, 메인 서버 API
    폴링 main_server_worker.py) 라 상태도 각자 다른 bus 키에 쌓인다.
    여기서 합친다."""
    live = bus.get_state()                       # 로컬 파이프라인 (state:live)
    main_live = bus.get_state(key="state:main")   # 메인 서버 API 연동
    people = (Person.objects.filter(is_active=True)
              .prefetch_related("snapshots", "tracklets__camera")
              .order_by("-last_seen")[:24])

    gallery = []
    for p in people:
        snap = p.best_snapshot()
        gallery.append({
            "id": p.pk,
            "label": p.label or f"미확인 #{p.pk}",
            "named": bool(p.label),
            "confirmed": p.confirmed,
            "thumb": snap.image.url if (snap and snap.image) else None,
            "cams": [c.name for c in p.cameras_seen()],
            "last_seen": timezone.localtime(p.last_seen).strftime("%H:%M:%S"),
        })

    today_start = timezone.localtime(timezone.now()).replace(
        hour=0, minute=0, second=0, microsecond=0)

    # 카메라별 감지 횟수(오늘) — "감지 인물" 패널에서 카메라 박스가 몇 번
    # 인식했는지 한눈에 보려고.
    # 2026-08-12 수정: 원래 Event(신원까지 확정된 사람만 생성됨) 기준으로
    # 셌는데, 그러면 그날 신원이 하나도 안 끝난 날은(실제로 있었음 —
    # 오늘 journey 31건 중 확정 0건) 전부 0회로 보인다. 카메라가 "몇 번
    # 인식했는지"는 신원 확정 여부와 무관해야 맞아서, 신원 상태와 무관하게
    # 항상 채워지는 Journey.route(예: "A -> C -> D")를 기준으로 센다.
    today_routes = Journey.objects.filter(
        entry_at__gte=today_start).values_list("route", flat=True)
    letter_counts: dict[str, int] = {}
    for route in today_routes:
        for letter in (route or "").split("->"):
            letter = letter.strip()
            if letter:
                letter_counts[letter] = letter_counts.get(letter, 0) + 1
    camera_counts = {
        str(cam.index): {"name": cam.name, "count": letter_counts.get(_camera_letter(cam), 0)}
        for cam in Camera.objects.filter(enabled=True)
    }

    # 2026-08-11 저녁: 대시보드 오른쪽 "감지 리스트" 패널을 메인 서버
    # Journey 기준으로 바꿔달라는 요청 — 최근 여정 몇 건을 그대로 내려준다.
    # 2026-08-12 수정: "검토 필요" 항목이 절반 넘게 섞여 나온다는 지적 —
    # 여기는 "신원이 분명한 최근 방문"만 보여주는 자리로 좁힌다(신규/재방문
    # 만). 헷갈리는(MANUAL_REVIEW_REQUIRED) 여정은 아래 cam_detections
    # ("보류 리스트")로 옮겼다 — 없어지는 게 아니라 자리가 바뀐 것.
    recent_journeys = [_journey_dict(j) for j in
                       Journey.objects.select_related("person", "local_match_person")
                       .filter(final_review_result__in=[Journey.NEW, Journey.REVISIT])
                       .order_by("-entry_at")[:10]]

    # 2026-08-12 재작업: "보류 리스트"(구 미등록자 감지 기록) 데이터가 거의
    # 안 채워진다는 지적 — 원인은 Event 가 신원이 "확정"된 여정에만 생기고
    # (§main_api_ingest.ingest_event_item, MANUAL_REVIEW_REQUIRED 인 동안은
    # Tracklet/Event 자체를 안 만듦) 정작 절대다수인 "헷갈리는" 여정은 한
    # 번도 안 잡혔기 때문이다.
    # 이제 MANUAL_REVIEW_REQUIRED Journey 도 같이 합쳐서 보여준다 — Camera A
    # 가 유일한 캡처 지점이라 route 의 첫 글자로 카메라 줄을 매긴다(실측:
    # 저장된 route 가 전부 "A"로 시작). "보류"(=error, 실제 인물과 연결됨)로
    # 처리된 건만 여기서 빼고 상세 페이지에서만 보이게 한다 — "확인"은
    # 그대로 두고 회색 처리만 한다(체크 해제로 되돌릴 수 있어야 해서, 이건
    # 기존 동작 그대로). "보류"는 이미 실제 인물이 연결됐으니 되돌릴 일이
    # 거의 없고, 계속 떠 있으면 실시간 화면이 지저분해진다는 지적.
    enabled_cams = list(Camera.objects.filter(enabled=True))
    cam_by_letter = {_camera_letter(cam): str(cam.index) for cam in enabled_cams}
    combined: dict[str, list] = {str(cam.index): [] for cam in enabled_cams}

    for cam in enabled_cams:
        idx = str(cam.index)
        events = (Event.objects.select_related("person", "journey")
                  .filter(camera=cam, kind=Event.ENTER, was_unregistered=True)
                  .exclude(review_status=Event.REVIEW_ERROR)
                  .order_by("-at")[:DETECTIONS_PER_CAMERA])
        combined[idx].extend((e.at, _detection_dict(e)) for e in events)

    manual_journeys = (Journey.objects
                       .filter(final_review_result=Journey.MANUAL_REVIEW)
                       .exclude(review_status=Event.REVIEW_ERROR)
                       .order_by("-entry_at")[:DETECTIONS_PER_CAMERA * max(len(enabled_cams), 1)])
    for j in manual_journeys:
        first_letter = (j.route or "").split("->")[0].strip()
        idx = cam_by_letter.get(first_letter)
        if idx is None or not j.entry_at:
            continue
        combined[idx].append((j.entry_at, _journey_review_dict(j)))

    cam_detections = {}
    for idx, items in combined.items():
        items.sort(key=lambda pair: pair[0], reverse=True)
        cam_detections[idx] = [d for _, d in items[:DETECTIONS_PER_CAMERA]]

    return JsonResponse({
        "running": live.get("running", False),
        "fps": round(live.get("fps", 0.0), 1),
        "main_connected": main_live.get("main_connected", False),
        "detection_enabled": RuntimeConfig.get().detection_enabled,
        "main_events_total": main_live.get("entries_total", 0),
        "live_tracks": live.get("tracks", []),
        "system": live.get("system", {}),
        "totals": {
            # "누적 인물" 은 전체 누적이 아니라 오늘 하루 기준으로 끊는다.
            "people": Person.objects.filter(
                is_active=True, created_at__gte=today_start).count(),
            "now": len(live.get("tracks", [])),
            "cameras": Camera.objects.filter(enabled=True).count(),
        },
        "gallery": gallery,
        "camera_counts": camera_counts,
        "recent_journeys": recent_journeys,
        # 이 events 는 카메라별 확인중/등록/미등록 상태 전환과 TTS(A 등록완료
        # 차임 포함)를 몰기 위한 전체 이벤트다 — confirmed=True 인 사람도
        # 반드시 포함해야 A 카메라 "등록완료" 차임이 계속 동작한다. 화면
        # "이벤트 기록" 패널은 이걸 안 쓰고 아래 unregistered_events 를 쓴다.
        #
        # 2026-08-13: confirmed 와 main_resolved 는 서로 다른 질문에 답한다.
        #   confirmed     = 관리자가 이 사람을 직접 검수/등록 확정했는지(로컬
        #                    전용, 기본 False — "보류 리스트" 필터링 기준 그대로).
        #   main_resolved = Main 이 이 Journey 를 NEW/REVISIT 으로 확실히
        #                    식별했는지. Event 는 애초에 canonical_person_uid
        #                    가 있고 MANUAL_REVIEW_REQUIRED 가 아닐 때만
        #                    만들어지므로(§main_api_ingest.ingest_event_item)
        #                    사실상 항상 True 지만, journey 가 나중에(관리자가
        #                    이동 목록에서 직접) 삭제돼 FK 가 끊기는 경우를
        #                    대비해 매번 다시 계산한다.
        #   A 카메라 "등록완료" 차임은 main_resolved 기준(요청: "A 카메라
        #   데이터로 매번 동작"), B·C·D "미등록자 감지" 경고음은 여전히
        #   confirmed 기준(관리자가 아직 검수 안 한 사람)이다.
        "events": [
            # person 표시는 항상 person_uid(P000006) 우선 — Person.__str__ 은
            # label 없으면 "미확인 #2" 식 내부 PK 를 보여주는데, 이건 메인
            # 서버가 모르는 우리 내부 번호라 여기서 보이면 안 된다.
            #
            # 2026-08-14: 프런트의 "이미 처리한 이벤트" 중복제거 키가
            # at|person|cam|kind 조합이었는데, at 이 초 단위(HH:MM:SS)
            # 라 같은 사람이 같은 카메라에서 1초 안에 두 번 잡히면(실측:
            # nodes+captures 양쪽에서 각각 Event 가 생겨 1초 차이로 남는
            # 경우 있음) 두 번째가 "이미 본 것"으로 조용히 씹혀서 소리가
            # 안 났다 — Event 의 실제 PK 를 내려줘서 그걸로 구분하게 한다
            # (초 단위로 절대 충돌 안 함).
            {"id": e.pk,
             "at": timezone.localtime(e.at).strftime("%H:%M:%S"),
             "person": e.person.external_id or str(e.person),
             "confirmed": e.person.confirmed,
             "main_resolved": bool(e.journey and e.journey.final_review_result
                                   in (Journey.NEW, Journey.REVISIT)),
             "cam": e.camera.name if e.camera else "—",
             "kind": e.get_kind_display()}
            for e in Event.objects.select_related("person", "camera", "journey")[:12]
        ],
        # 2026-08-12 개편: "보류 리스트"(구 미등록자 감지 기록) 패널이 카메라
        # 4대 고정 줄(A/B/C/D) + 카드형 </> 넘기기로 돼 있어서, 위에서 미리
        # 계산한 카메라별 리스트를 그대로 내려준다(Event + Journey 합친 것).
        "cam_detections": cam_detections,
    })


def _log_ai_feedback(*, journey=None, event=None, suggested_uid, confirmed_uid, user):
    """"데이터 선택" 모달에서 관리자가 확정한 결과를 Main 이 미리 제안한
    후보와 비교해서 기록한다(2026-08-13 요청) — AI/Main 이 애초에 아무것도
    제안 안 했으면(suggested_uid 없음) 평가할 대상이 없으니 기록하지
    않는다. Main 에 전송하는 코드는 아직 없다 — 로컬에 수집만 해둔다."""
    suggested_uid = (suggested_uid or "").strip()
    if not suggested_uid:
        return
    AiMatchFeedback.objects.create(
        journey=journey, event=event,
        suggested_person_uid=suggested_uid,
        confirmed_person_uid=confirmed_uid or "",
        was_correct=(suggested_uid == confirmed_uid),
        reviewed_by=user,
    )


@require_POST
@login_required
def api_event_review(request, event_id):
    """"미등록자 감지" 건 확인/오류 처리.

    - status=confirmed  → 미등록자로 뜬 게 맞다고 검토 완료 처리만 한다.
    - status=error&person_uid=P000006 → 오탐이었다는 뜻. 관리자가 찾아낸
      실제 인물을 resolved_person 으로 연결하고, 그 Person.confirmed 를
      True 로 바꾼다 — 임베딩 자동 매칭은 지금 파이프라인에 저장되는
      임베딩이 없어서(Snapshot.embedding 전부 빈 값, 2026-08-12 확인)
      할 수 없다. 대신 관리자가 person_uid 로 직접 지정하는 수동 매칭이다.
      confirmed=True 가 되고 나면 이 사람의 다음 감지부터는
      was_unregistered=False 로 찍혀서 이 로그에 아예 안 나타난다.
    - status=pending → 체크 해제(되돌리기). 이 건의 검토 기록만 지운다 —
      "오류" 처리로 Person.confirmed 가 True 된 걸 여기서 자동으로 다시
      False 로 되돌리진 않는다(다른 건도 그 사람을 참조했을 수 있어서
      안전하게 admin 에서 수동으로만 되돌리게 한다)."""
    event = get_object_or_404(Event, pk=event_id)
    status = request.POST.get("status")
    if status not in (Event.REVIEW_CONFIRMED, Event.REVIEW_ERROR, "pending"):
        return JsonResponse({"ok": False, "error": "알 수 없는 상태"}, status=400)

    if status == "pending":
        event.review_status = ""
        event.reviewed_at = None
        event.reviewed_by = None
        event.resolved_person = None
        event.save(update_fields=["review_status", "reviewed_at", "reviewed_by", "resolved_person"])
        return JsonResponse({"ok": True, "review_status": "", "review_status_label": "",
                             "resolved_person": None})

    resolved_person = None
    if status == Event.REVIEW_ERROR:
        person_uid = request.POST.get("person_uid", "").strip()
        if not person_uid:
            return JsonResponse(
                {"ok": False, "error": "보류 처리하려면 실제 인물(person_uid)을 지정해야 한다"},
                status=400)
        try:
            resolved_person = Person.objects.get(external_id=person_uid)
        except Person.DoesNotExist:
            return JsonResponse(
                {"ok": False, "error": f"'{person_uid}' 인물을 찾을 수 없다"}, status=404)
        if not resolved_person.confirmed:
            resolved_person.confirmed = True
            resolved_person.save(update_fields=["confirmed"])

    event.review_status = status
    event.reviewed_at = timezone.now()
    event.reviewed_by = request.user
    event.resolved_person = resolved_person
    event.save(update_fields=["review_status", "reviewed_at", "reviewed_by", "resolved_person"])

    _log_ai_feedback(event=event, journey=event.journey,
                     suggested_uid=request.POST.get("ai_suggested_uid", ""),
                     confirmed_uid=resolved_person.external_id if resolved_person else "",
                     user=request.user)

    return JsonResponse({
        "ok": True,
        "review_status": event.review_status,
        "review_status_label": event.get_review_status_display(),
        "resolved_person": resolved_person.external_id if resolved_person else None,
    })


@require_POST
@login_required
def api_journey_review(request, journey_id):
    """"헷갈리는" 여정(MANUAL_REVIEW_REQUIRED) 1건을 확인/보류 처리한다
    — §api_event_review 와 같은 사진-보고-직접-고르기 방식(임베딩 자동매칭
    없음)이고, 상태값도 그대로 재사용한다(Event.REVIEW_CONFIRMED/ERROR).

    - status=confirmed → 특정 인물을 못 찾았지만 "미등록자 감지가 맞다"고
      확인만 한다(신규 인물일 수 있다는 뜻).
    - status=error&person_uid=P000006 → "이 사람과 헷갈렸다"는 뜻. 관리자가
      찾은 실제 인물을 local_match_person 으로 지정한다.
    - status=pending → 되돌리기(검토 전 상태로).

    canonical_person_uid 는 여기서 손대지 않는다 — 그건 Main 만의 판정이라
    다음 폴링에서 그대로 되돌아온다(Journey.local_match_person 은 순수
    로컬 참고용). review_status 가 채워지면 대시보드 실시간 "보류 리스트"
    위젯에서는 빠지고(§api_state cam_detections) journeys.html 의 "검토
    필요" 표에서만 계속 보인다 — 기록이 없어지는 게 아니라 실시간 화면
    에서만 안 보이는 것뿐이다."""
    journey = get_object_or_404(Journey, journey_id=journey_id)
    status = request.POST.get("status")
    if status not in (Event.REVIEW_CONFIRMED, Event.REVIEW_ERROR, "pending"):
        return JsonResponse({"ok": False, "error": "알 수 없는 상태"}, status=400)

    if status == "pending":
        journey.review_status = ""
        journey.local_match_person = None
        journey.local_match_at = None
        journey.local_match_by = None
        journey.save(update_fields=["review_status", "local_match_person",
                                    "local_match_at", "local_match_by"])
        return JsonResponse({"ok": True, "review_status": "", "review_status_label": "",
                             "resolved_person": None})

    person = None
    if status == Event.REVIEW_ERROR:
        person_uid = request.POST.get("person_uid", "").strip()
        if not person_uid:
            return JsonResponse(
                {"ok": False, "error": "보류 처리하려면 실제 인물(person_uid)을 지정해야 한다"},
                status=400)
        try:
            person = Person.objects.get(external_id=person_uid)
        except Person.DoesNotExist:
            return JsonResponse({"ok": False, "error": f"'{person_uid}' 인물을 찾을 수 없다"},
                                status=404)
        if not person.confirmed:
            person.confirmed = True
            person.save(update_fields=["confirmed"])
        # 같은 여정에 걸린 아직 미확인인 Event 들도 같이 정리해서, 카메라별
        # 감지 기록 쪽에서도 같은 사람이 일관되게 처리된 걸로 보이게 한다.
        Event.objects.filter(journey=journey, review_status="").update(
            review_status=Event.REVIEW_ERROR, reviewed_at=timezone.now(),
            reviewed_by=request.user, resolved_person=person)

    journey.review_status = status
    journey.local_match_person = person
    journey.local_match_at = timezone.now()
    journey.local_match_by = request.user
    journey.save(update_fields=["review_status", "local_match_person",
                                "local_match_at", "local_match_by"])

    _log_ai_feedback(journey=journey,
                     suggested_uid=request.POST.get("ai_suggested_uid", ""),
                     confirmed_uid=person.external_id if person else "",
                     user=request.user)

    return JsonResponse({
        "ok": True,
        "review_status": journey.review_status,
        "review_status_label": journey.get_review_status_display(),
        "resolved_person": person.external_id if person else None,
    })


@require_POST
@login_required
def api_journey_delete(request, journey_id):
    """"현재 · 최근 이동 목록"/"검토 필요" 표의 행 단위 삭제.

    §api_person_delete 와 같은 성격 — 이 Journey 는 Main 이 원본이고
    main_api_ingest.ingest_journey()가 주기적으로 update_or_create 로
    동기화한다. 여기서 지우는 건 우리 로컬 사본뿐이라, Main 이 이
    journey_id 를 계속 돌려주는 동안은 다음 폴링에서 다시 생길 수 있다
    (§api_person_delete 와 동일한 한계 — Main 에 진짜 삭제 API가 없다)."""
    journey = get_object_or_404(Journey, journey_id=journey_id)
    journey.delete()
    return JsonResponse({"ok": True})


def _person_thumb_url(person: Person) -> str | None:
    """이 사람의 대표 사진. 2026-08-13: Main 이 고른 representative_image_url
    이 1순위(§_representative_thumb_url) — 대표사진 "선정" 로직은 여기서
    새로 만들지 않는다. 없으면 최근 journey 중 캡처 사진이 있는 첫 번째
    것으로 폴백(기존 로직)."""
    main_thumb = _representative_thumb_url(person.external_id)
    if main_thumb:
        return main_thumb
    for journey in person.journeys.order_by("-entry_at")[:5]:
        url = _thumb_url(journey)
        if url:
            return url
    return None


@login_required
def api_person_search(request):
    """"보류" 처리할 때 실제 인물을 찾는 검색 — 임베딩 기반 자동매칭이
    없어서(§api_event_review 주석 참고) person_uid/이름 텍스트 검색 +
    사진으로 관리자가 직접 보고 고르는 방식이다. q 가 비어있으면(모달을
    막 열었을 때) "비슷한 데이터"로 최근 방문자 순 목록을 기본으로
    보여준다 — 아예 빈 결과보다 훑어볼 후보가 있는 게 낫다.

    2026-08-13: "다른 인물 검색은 메인 서버 데이터를 가져와서 보여달라"는
    요청 — 로컬 Person 테이블은 신원 확정 극소수만 있어서(§_fetch_main_persons)
    검색 대상이 너무 좁았다. 이제 캐시된 Main persons 목록(§_main_representative_urls
    와 같은 4초 캐시 재사용)에서 검색하고, 로컬에 매칭되는 사람이 있으면
    이름/확인여부/사진을 얹는다. Main 이 응답 안 하면 예전처럼 로컬
    테이블로 폴백한다.

    2026-08-14: "가장 비슷한 순으로 정렬해달라" 요청 — 진짜 얼굴 유사도
    검색은 지금 구조로 불가능하다(임베딩 인프라 자체가 없음, Snapshot.
    embedding 이 항상 비어있음 확인됨). 대신 있는 정보로 실제로 도움이
    되는 순서를 만든다: 사진이 아예 없는 후보는 봐도 비교가 안 되니
    맨 뒤로 밀고, 사진 있는 후보끼리는 최근 방문 순으로 둔다 — "훑어보며
    비교"에 실질적으로 쓸모있는 순서."""
    q = request.GET.get("q", "").strip().upper()
    items = cache.get(MAIN_PERSONS_CACHE_KEY)
    if items is None:
        items = _fetch_main_persons()
        if items is not None:
            cache.set(MAIN_PERSONS_CACHE_KEY, items, MAIN_PERSONS_CACHE_SEC)

    if items is not None:
        local = {p.external_id: p for p in
                Person.objects.exclude(external_id="").exclude(external_id__isnull=True)}

        def _matches(uid):
            if q in uid.upper():
                return True
            local_p = local.get(uid)
            return bool(local_p and local_p.label and q in local_p.label.upper())

        filtered = [it for it in items if it.get("person_uid")
                   and (not q or _matches(it["person_uid"]))]
        filtered.sort(key=lambda it: (bool(it.get("representative_image_url")),
                                      it.get("last_seen_at") or ""), reverse=True)
        results = []
        for it in filtered[:20]:
            uid = it["person_uid"]
            local_p = local.get(uid)
            results.append({
                "person_uid": uid,
                "label": local_p.label if local_p else "",
                "confirmed": local_p.confirmed if local_p else False,
                "visit_count": it.get("visit_count"),
                "thumb_url": (_person_thumb_url(local_p) if local_p else None) or _local_thumb_for_uid(uid),
            })
        return JsonResponse({"results": results})

    qs = Person.objects.exclude(external_id="").exclude(external_id__isnull=True)
    if q:
        qs = qs.filter(Q(external_id__icontains=q) | Q(label__icontains=q))
    qs = qs.order_by("-last_seen")[:20]
    return JsonResponse({"results": [
        {"person_uid": p.external_id, "label": p.label, "confirmed": p.confirmed,
         "visit_count": p.visit_count, "thumb_url": _person_thumb_url(p)}
        for p in qs
    ]})


@login_required
def detections_view(request):
    """미등록자 감지 전체 기록 — "미등록자 감지 기록" 패널 헤더의 "상세"
    버튼으로 들어온다. 카메라/검토상태/날짜로 필터링, 50건씩 페이지네이션.

    2026-08-12 추가: 대시보드 위젯은 카메라당 최근 30건만 캐러셀로 보여줘서
    오래된 감지가 "삭제된 것처럼" 보인다는 지적 — 실제로는 여기(전체 기록)
    에 전부 남아있다. 그걸 체감할 수 있게 날짜 필터 + 날짜별 구분선을
    붙였다(오래된 기록도 날짜만 골라서 계속 볼 수 있다)."""
    qs = (Event.objects
          .select_related("person", "camera", "resolved_person", "reviewed_by", "journey")
          .filter(kind=Event.ENTER, was_unregistered=True)
          .order_by("-at"))

    cam_filter = request.GET.get("camera", "")
    if cam_filter:
        qs = qs.filter(camera__index=cam_filter)

    status_filter = request.GET.get("status", "")
    if status_filter == "pending":
        qs = qs.filter(review_status="")
    elif status_filter in (Event.REVIEW_CONFIRMED, Event.REVIEW_ERROR):
        qs = qs.filter(review_status=status_filter)

    # 카메라/상태 필터까지만 반영한 날짜 목록 — "이 조건에 실제로 기록이
    # 있는 날"만 드롭다운에 보여준다. TruncDate 는 USE_TZ=True 일 때
    # settings.TIME_ZONE(Asia/Seoul) 기준으로 잘라서, 자정 근처 기록이
    # 엉뚱한 날짜로 묶이는 일이 없다(이 세션에서 이미 겪은 UTC/KST 문제).
    available_dates = list(
        qs.annotate(d=TruncDate("at")).values_list("d", flat=True).distinct().order_by("-d"))

    date_filter = request.GET.get("date", "")
    if date_filter:
        qs = qs.filter(at__date=date_filter)

    page_obj = Paginator(qs, 50).get_page(request.GET.get("page"))
    # 템플릿에서 바로 URL 문자열을 쓸 수 있게 미리 계산해서 얹어둔다
    # (thumb_capture() 가 튜플을 돌려줘서 템플릿에서 직접 조립하기 번거롭다).
    for e in page_obj:
        e.thumb_url = _thumb_url(e.journey)
        e.local_date = timezone.localtime(e.at).date()   # 날짜 구분선용
    cameras = [{"index": c.index, "label": _short_cam_label(c)}
              for c in Camera.objects.filter(enabled=True).order_by("index")]

    return render(request, "tracking/detections.html", {
        "page_obj": page_obj,
        "cameras": cameras,
        "cam_filter": cam_filter,
        "status_filter": status_filter,
        "date_filter": date_filter,
        "available_dates": available_dates,
    })


def _period_stats(days):
    """2026-08-11 저녁: 기존엔 Person.confirmed(로컬 "검수 완료" 체크)
    기준 등록/미등록 통계였는데, 이건 메인 서버의 신원 판정(Final Review)
    개념과 안 맞는다는 지적으로 Journey.final_review_result 기준
    신규/재방문/검토 필요로 바꾼다.

    2026-08-12 갱신: "재방문+검토 합이 총과 안 맞는다"는 문의가 있었는데,
    확인해보니 데이터 유실이 아니라 정상 — 카메라 A만 찍히고 트랙이
    끊긴(journey_status=EXPIRED) 등 신원 판정 자체가 아직 안 끝난
    journey 가 실제로 많다(Main 쪽 실측 확인: EXPIRED/COMPLETED 여러 건이
    final_review_result=null). 그동안 "총"에는 이런 미확정 건도 포함
    됐는데 화면엔 안 보여서 마치 데이터가 빈 것처럼 보였다 — pending 을
    따로 계산해서 신규+재방문+검토+미확정 = 총 이 되게 노출한다.

    2026-08-14 갱신: "지금 시각 기준 최근 N일"(rolling window)이었더니,
    어제 오후에 쌓인 데이터가 24시간이 안 지나서 "1일" 통계에 그대로
    남아있어 "오늘 아무 것도 안 했는데 1일에 쌓여있다"는 혼란을 줬다 —
    "매일 00:00마다 새로 집계해달라" 요청으로, 자정(로컬 타임존) 기준
    캘린더 일수로 바꾼다: "1일"=오늘 00:00부터, "3일"=오늘 포함 최근
    3일(그저께 00:00부터), 이런 식으로 매일 자정에 리셋된다."""
    today_local_midnight = timezone.localtime().replace(
        hour=0, minute=0, second=0, microsecond=0)
    since = today_local_midnight - timedelta(days=days - 1)
    qs = Journey.objects.filter(entry_at__gte=since)
    new = qs.filter(final_review_result=Journey.NEW).count()
    revisit = qs.filter(final_review_result=Journey.REVISIT).count()
    review = qs.filter(final_review_result=Journey.MANUAL_REVIEW).count()
    total = qs.count()
    return {
        "new": new,
        "revisit": revisit,
        "review": review,
        "pending": total - new - revisit - review,  # 아직 신원 판정 안 끝난 journey
        "total": total,
    }


@login_required
def api_stats(request):
    """기간별(1일/3일/5일) 여정 판정 통계 — 신규(NEW)/재방문(REVISIT)/
    검토 필요(MANUAL_REVIEW_REQUIRED)/미확정(pending). 신규+재방문+검토+
    미확정 = 총 이 항상 성립한다.

    2026-08-13: 기준일을 7/14/30일에서 1/3/5일로 바꿨다 — 이 시스템은
    아직 데이터가 며칠 안 쌓여서(2026-08-13 기준 약 2~3일치) 긴 기간
    창이 다 똑같은 숫자만 보여줘서 의미가 없었다."""
    return JsonResponse({
        "d1": _period_stats(1),
        "d3": _period_stats(3),
        "d5": _period_stats(5),
    })


@login_required
def api_central(request):
    """B의 중앙서버(central_tracking.db) 연동 상태. CENTRAL_DB_PATH 가
    비어있거나 파일을 못 찾으면 available=False 로 조용히 응답한다 —
    아직 그 파일이 실제로 어디 있는지 확정 전이라 기본값이 비어있다."""
    return JsonResponse({
        "available": central_db.is_available(),
        "counts": central_db.counts(),
        "recent_journeys": central_db.recent_journeys(20),
    })


def _journey_dict(j: Journey) -> dict:
    """Journey 1건 → 화면용 dict. person_uid 는 반드시 canonical 값만
    담는다(display_person_uid) — temporary_person_uid 는 별도 필드로만
    참고용으로 넘긴다. B 지시사항: 임시 UID 를 최종 사람 ID로 쓰면 안 됨."""
    return {
        "journey_id": j.journey_id,
        "person_uid": j.display_person_uid(),
        "temporary_person_uid": j.temporary_person_uid,
        "candidate_person_uid": j.candidate_person_uid,
        "final_candidate_person_uid": j.final_candidate_person_uid,
        "initial_decision": j.initial_decision,
        "person_status": j.person_status,
        "journey_status": j.journey_status,
        "route": j.route,
        "entry_at": timezone.localtime(j.entry_at).strftime("%H:%M:%S") if j.entry_at else None,
        "d_exit_at": timezone.localtime(j.d_exit_at).strftime("%H:%M:%S") if j.d_exit_at else None,
        "journey_elapsed_seconds": j.journey_elapsed_seconds,
        "visit_count": j.visit_count,
        "final_review_result": j.final_review_result,
        "final_review_result_label": (j.get_final_review_result_display()
                                      if j.final_review_result else ""),
        "final_score": j.final_score,
        "final_scores": j.final_scores,
        # 2026-08-12: 관리자가 "검토 필요" 카드에서 직접 지정한 로컬 매칭 —
        # Main 의 canonical_person_uid 와 별개(§Journey.local_match_person 참고).
        "local_match_person_uid": (j.local_match_person.external_id
                                   if j.local_match_person_id else None),
        "review_status": j.review_status,
        "review_status_label": j.get_review_status_display() if j.review_status else "",
        # 2026-08-12: Camera A 캡처 대표 썸네일 — FACE rank1 있으면 그거,
        # 없으면 BODY rank1(Journey.thumb_capture). 실제 이미지 바이트는
        # 여기서 안 주고, 인증 프록시 URL 만 준다(원본 Jetson 주소를
        # 브라우저에 직접 노출 안 함).
        # 2026-08-13: 신원이 확정된 여정(canonical_person_uid 있음)이면
        # Main 대표사진이 1순위 — 검토 필요(MANUAL_REVIEW_REQUIRED)라
        # canonical_person_uid 가 아직 없으면 자동으로 이 캡처 폴백만 쓴다.
        "thumb_url": ((_representative_thumb_url(j.canonical_person_uid) if j.canonical_person_uid else None)
                     or _thumb_url(j)),
    }


@login_required
def journeys_view(request):
    """현재/최근 이동 목록 화면 — 조회 전용(2026-08-11 B 지시사항).
    실데이터는 이 화면이 폴링하는 api_journeys 가 채운다."""
    return render(request, "tracking/journeys.html", {})


@login_required
def api_journeys(request):
    """메인 서버의 Final Identity Review 결과 기준 여정 목록.
    검토 대기(MANUAL_REVIEW_REQUIRED)는 상단에 따로 분리해서 관리자가
    바로 눈에 띄게 한다 — 지금은 조회만, 액션 버튼은 다음 단계."""
    qs = list(Journey.objects.select_related("person", "local_match_person")[:100])
    manual = [j for j in qs if j.needs_review()]
    recent = [j for j in qs if not j.needs_review()]
    return JsonResponse({
        "manual_review": [_journey_dict(j) for j in manual],
        "recent": [_journey_dict(j) for j in recent],
    })


@login_required
def api_capture_proxy(request, journey_id, modality, rank):
    """Journey 캡처 이미지 인증 프록시.

    브라우저는 이 주소(우리 도메인)만 보고, 실제 Camera A/Main 주소는
    서버끼리만 오간다 — ①로그인 안 한 사람은 여기 자체가 막힌다
    (login_required) ②사용자가 임의 URL 을 넣을 수 있는 여지가 없다
    (journey_id/modality/rank 만 받고, 실제 URL 은 우리가 이미 저장해둔
    Journey.body_images/face_images 에서만 찾는다 — SSRF 방지) ③그 URL의
    host:port 가 Main 이거나 우리가 아는 Camera 설정과 실제로 일치하는지
    검증한다(§_resolve_capture_url). 이미지 자체는 저장하지 않고 매번
    그대로 흘려보낸다(프록시일 뿐).

    2026-08-13: Main 이 캡처 서빙 방식을 Camera A 직접 URL 에서 Main
    상대경로(/api/captures/{id}/image)로 바꾸면서, 옛 방식만 가정하던
    코드가 전부 404 나던 걸 발견해서 고쳤다 — 이제 두 형태 다 받는다."""
    if modality not in ("body", "face"):
        raise Http404
    journey = get_object_or_404(Journey, journey_id=journey_id)
    images = journey.face_images if modality == "face" else journey.body_images
    match = next((im for im in (images or []) if im.get("rank") == rank), None)
    if not match or not match.get("url"):
        raise Http404

    url = _resolve_capture_url(match["url"])
    if not url:
        raise Http404   # Main/Camera 어느 쪽 주소도 아니면 절대 안 따라간다(SSRF 방지)

    try:
        resp = requests.get(url, timeout=CAPTURE_FETCH_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException:
        raise Http404   # 카메라/Main 이 꺼져있거나 그 사이 파일이 지워진 경우 등 — 500 대신 404

    content_type = resp.headers.get("Content-Type", "").split(";")[0].strip()
    if content_type not in CAPTURE_ALLOWED_CONTENT_TYPES:
        raise Http404

    response = HttpResponse(resp.content, content_type=content_type)
    response["Cache-Control"] = "private, max-age=300"   # 같은 장면 반복 요청 줄이기(5분)
    return response


@login_required
def person_detail_view(request, person_uid):
    """사람 상세 — Local Track ID 가 아니라 person_uid(canonical) 로만
    조회한다(B 지시사항).

    2026-08-12 갱신: 로컬 Person 행이 없는 사람도(Main 은 아는데 우리
    MANUAL_REVIEW_REQUIRED 게이트 때문에 로컬엔 없는 경우, §_fetch_main_persons
    주석 참고) Main 상세 API 로 보여줄 수 있게 했다 — 로컬/Main 어느 쪽도
    이 person_uid 를 모르면만 404.

    2026-08-12 추가: "사진이 없어도 수정/삭제 버튼은 활성화해달라"는 요청 —
    이 버튼들은 로컬 전용 개념(label/confirmed)이라 로컬 Person 행이 있어야
    동작한다. Main 이 아는 사람인데 로컬 행이 아직 없으면(사진/캡처가 없는
    것과 같은 이유 — MANUAL_REVIEW_REQUIRED 동안 로컬에 아예 안 만들어짐)
    여기서 최소한으로 하나 만들어서 사진 유무와 무관하게 버튼이 항상 뜨게 한다."""
    person = Person.objects.filter(external_id=person_uid).first()
    main_detail = _fetch_main_person_detail(person_uid)
    if not person and not main_detail:
        raise Http404("인물을 찾을 수 없다")

    if not person and main_detail:
        from .main_api_ingest import _parse_at
        person, _ = Person.objects.get_or_create(
            external_id=person_uid,
            defaults={
                "created_at": _parse_at(main_detail.get("created_at")) or timezone.now(),
                "last_seen": _parse_at(main_detail.get("last_seen_at")) or timezone.now(),
                "visit_count": main_detail.get("visit_count") or 1,
            },
        )

    local_journeys = list(person.journeys.all()) if person else []
    # 2026-08-13: 이미 이 상세 조회(main_detail)로 representative_image_url
    # 을 확인했으면 그걸 최우선으로 쓴다(추가 조회 불필요, §_person_thumb_url
    # 도 목록 캐시 기준으로 같은 걸 한 번 더 확인하지만 여긴 상세가 더
    # 확실한 최신값이라 먼저 본다) — 없으면 기존 폴백 체인 그대로.
    main_repr = (main_detail or {}).get("representative_image_url")
    thumb_url = ((_main_capture_proxy_url(person_uid) if main_repr else None)
                or (_person_thumb_url(person) if person else None)
                or _local_thumb_for_uid(person_uid))

    if main_detail:
        status = main_detail.get("status") or ""
        display = {
            "person_uid": main_detail.get("person_uid") or person_uid,
            "visit_count": main_detail.get("visit_count") if main_detail.get("visit_count") is not None
                          else (person.visit_count if person else 0),
            "status_label": MAIN_PERSONS_STATUS_LABEL.get(status, status or "—"),
            "created_at": main_detail.get("created_at"),
            "last_seen": main_detail.get("last_seen_at"),
            "is_first_visit": (main_detail.get("visit_count") or 0) <= 1,
        }
        # Main 상세의 journeys 는 요약뿐이라(캡처/스코어 없음) 로컬 Journey
        # 테이블이 비어있을 때만 대체용으로 쓴다 — route 가 배열로 오는 등
        # 필드 모양이 로컬 테이블과 달라서 템플릿이 편하게 쓸 수 있게 여기서 정리한다.
        main_journeys = [{
            "journey_id": mj.get("journey_id"),
            "journey_status": mj.get("journey_status"),
            "route": (" -> ".join(mj["route"]) if isinstance(mj.get("route"), list)
                     else (mj.get("route") or "")),
            "entry_at": (mj.get("entry_at") or "").replace("T", " ")[:19],
            "d_exit_at": (mj.get("d_exit_at") or "").replace("T", " ")[:19],
            "elapsed": mj.get("elapsed_seconds"),
        } for mj in (main_detail.get("journeys") or [])]
    else:
        display = {
            "person_uid": person.external_id,
            "visit_count": person.visit_count,
            "status_label": "등록됨" if person.confirmed else "미등록",
            "created_at": timezone.localtime(person.created_at).strftime("%Y-%m-%d %H:%M:%S"),
            "last_seen": timezone.localtime(person.last_seen).strftime("%Y-%m-%d %H:%M:%S"),
            "is_first_visit": person.is_first_visit(),
        }
        main_journeys = []

    return render(request, "tracking/person_detail.html", {
        "person": person,
        "display": display,
        "journeys": local_journeys,
        "main_journeys": main_journeys if not local_journeys else [],
        "thumb_url": thumb_url,
    })


@login_required
def db_admin_view(request):
    """"DB 관리" 페이지 — Main 관리자 API(admin/database/*)를 대신 호출하는
    화면. superuser 가 아니면 대시보드로 돌려보낸다 — 일반 로그인 사용자에게는
    설정 메뉴에도 안 보이지만, URL을 직접 알아도 못 들어오게 막는다.
    실데이터/버튼 동작은 전부 tracking/admin_db_proxy.py 의 5개 API를
    프런트에서 fetch() 로 호출해서 채운다(이 뷰는 빈 틀만 그린다)."""
    if not request.user.is_superuser:
        return render(request, "tracking/db_admin.html", {"forbidden": True})
    return render(request, "tracking/db_admin.html", {"forbidden": False})


@login_required
def persons_index_view(request):
    """"펄스널 아이디를 모아둔 페이지" — 설정 드롭다운에서 들어온다.
    실데이터는 이 화면이 폴링하는 api_persons_list 가 채운다."""
    return render(request, "tracking/persons_index.html", {})


@login_required
def api_persons_list(request):
    """전체 인물 디렉토리 — P000000부터 순서대로, 사진과 함께.

    2026-08-12 재작업: 로컬 Person 테이블(신원 확정된 극소수만 있음, 위
    _fetch_main_persons 주석 참고) 대신 Main 의 /api/persons 를 기준
    소스로 쓴다 — 로컬 데이터(사진/이름/확인여부)는 있으면 얹어서
    보강만 한다. Main 이 응답 안 하면 예전처럼 로컬 테이블로 폴백해서
    화면이 완전히 비지는 않게 한다."""
    main_items = cache.get(MAIN_PERSONS_CACHE_KEY)
    if main_items is None:
        main_items = _fetch_main_persons()
        if main_items is not None:
            cache.set(MAIN_PERSONS_CACHE_KEY, main_items, MAIN_PERSONS_CACHE_SEC)

    if main_items is not None:
        local = {p.external_id: p for p in
                Person.objects.exclude(external_id="").exclude(external_id__isnull=True)}
        results = []
        for item in main_items:
            uid = item.get("person_uid")
            if not uid:
                continue
            local_p = local.get(uid)
            status = item.get("status") or ""
            thumb = (_person_thumb_url(local_p) if local_p else None) or _local_thumb_for_uid(uid)
            results.append({
                "person_uid": uid,
                "label": local_p.label if local_p else "",
                "confirmed": local_p.confirmed if local_p else False,
                "status": status,
                "status_label": MAIN_PERSONS_STATUS_LABEL.get(status, status or "—"),
                "visit_count": item.get("visit_count"),
                "created_at": (item.get("created_at") or "").replace("T", " ")[:19],
                "last_seen": (item.get("last_seen_at") or "").replace("T", " ")[:19],
                "thumb_url": thumb,
            })
        results.sort(key=lambda r: r["person_uid"])
        return JsonResponse({"results": results, "source": "main"})

    qs = (Person.objects.exclude(external_id="").exclude(external_id__isnull=True)
          .order_by("external_id")[:300])
    return JsonResponse({"results": [
        {
            "person_uid": p.external_id,
            "label": p.label,
            "confirmed": p.confirmed,
            "status": "",
            "status_label": "등록됨" if p.confirmed else "미등록",
            "visit_count": p.visit_count,
            "created_at": timezone.localtime(p.created_at).strftime("%Y-%m-%d %H:%M:%S"),
            "last_seen": timezone.localtime(p.last_seen).strftime("%Y-%m-%d %H:%M:%S"),
            "thumb_url": _person_thumb_url(p),
        }
        for p in qs
    ], "source": "local"})


@require_POST
@login_required
def api_person_merge(request, person_uid):
    """인물 상세 "수정" 패널의 병합 기능 — 사진 후보 모달(§api_person_search)
    로 찾은 다른 person_uid 와 이 인물을 하나로 합친다. services.merge_persons()
    가 pk 가 더 작은(더 오래된) 쪽을 남기고 나머지를 흡수하는데, 지금 보고
    있는 페이지 쪽이 흡수당할 수도 있어서 응답에 살아남은 person_uid 를
    같이 돌려줘 프런트가 그쪽 상세 페이지로 옮겨갈 수 있게 한다."""
    person = get_object_or_404(Person, external_id=person_uid)
    target_uid = request.POST.get("target_person_uid", "").strip()
    if not target_uid:
        return JsonResponse({"ok": False, "error": "합칠 인물(person_uid)을 지정해야 한다"},
                            status=400)
    if target_uid == person_uid:
        return JsonResponse({"ok": False, "error": "같은 인물끼리는 합칠 수 없다"}, status=400)
    try:
        target = Person.objects.get(external_id=target_uid)
    except Person.DoesNotExist:
        return JsonResponse({"ok": False, "error": f"'{target_uid}' 인물을 찾을 수 없다"},
                            status=404)

    keep, dropped = services.merge_persons([person, target])
    return JsonResponse({"ok": True, "person_uid": keep.external_id, "dropped": dropped})


@require_POST
@login_required
def api_person_delete(request, person_uid):
    """인물 상세 "삭제" 버튼 — 확인/취소 판단은 프런트(JS confirm)에서
    끝내고, 여기는 확정된 삭제 요청만 받는다."""
    person = get_object_or_404(Person, external_id=person_uid)
    services.delete_persons([person])
    return JsonResponse({"ok": True})


@require_POST
@login_required
def api_toggle_detection(request):
    """감지 on/off. Jetson 장비 자체는 원격으로 못 끄니, 우리 쪽 MQTT
    수신을 끊는 걸로 흉내낸다 — mqtt_worker.py 가 1초 안에 반영한다."""
    cfg = RuntimeConfig.get()
    cfg.detection_enabled = request.POST.get("enabled") == "1"
    cfg.save(update_fields=["detection_enabled"])
    return JsonResponse({"ok": True, "detection_enabled": cfg.detection_enabled})


@require_POST
@login_required
def api_control(request):
    """(C) 파이프라인 제어. 명령만 큐에 넣고 즉시 반환한다."""
    cmd = request.POST.get("cmd", "")
    allowed = {"start", "stop", "reload_cameras", "reload_gallery",
               "set_config", "snapshot_now"}
    if cmd not in allowed:
        return JsonResponse({"ok": False, "error": f"알 수 없는 명령: {cmd}"},
                            status=400)

    args = {k: v for k, v in request.POST.items() if k != "cmd"}
    if cmd == "set_config":
        cfg = RuntimeConfig.get()
        for f in ("det_conf", "reid_threshold"):
            if f in args:
                setattr(cfg, f, float(args[f]))
        cfg.save()
        args = {"det_conf": cfg.det_conf, "reid_threshold": cfg.reid_threshold}

    bus.send_command(cmd, **args)
    return JsonResponse({"ok": True, "cmd": cmd})


@login_required
@require_POST
def api_rename(request, person_id: int):
    """(B) 대시보드에서 바로 이름 붙이기."""
    try:
        p = Person.objects.get(pk=person_id)
    except Person.DoesNotExist:
        return JsonResponse({"ok": False, "error": "없는 인물"}, status=404)
    p.label = request.POST.get("label", "").strip()[:50]
    p.save(update_fields=["label"])
    return JsonResponse({"ok": True, "label": p.label})


@require_POST
@login_required
def logout_view(request):
    """설정 드롭다운 "로그아웃" — POST 폼으로만 받는다(GET 로그아웃은
    제3자 페이지가 <img src="/logout/"> 같은 걸로 강제 로그아웃시킬 수
    있는 CSRF 취약점이라 Django 도 4.1부터 막았다)."""
    auth_logout(request)
    # next= 없이 그냥 /admin/login/ 으로만 보내면 로그인 후 대시보드(/)가
    # 아니라 Django admin 홈(/admin/)으로 떨어진다 — 다른 곳들처럼(§det
    # 401/302 처리) next= 를 명시해서 로그인하면 카메라 화면으로 바로
    # 오게 한다.
    return redirect("/admin/login/?next=/")


def healthz(request):
    return HttpResponse("ok")
