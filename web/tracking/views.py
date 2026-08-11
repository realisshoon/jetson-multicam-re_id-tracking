import asyncio
import hashlib
from datetime import timedelta

from django.contrib.auth.decorators import login_required
from django.db.models import Count, Q
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import bus, central_db
from .models import Camera, Event, Person, RuntimeConfig

BOUNDARY = "reidframe"


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
def api_state(request):
    """대시보드가 폴링하는 실시간 상태. WebSocket 없이 이걸로 충분하다.

    워커가 2개(로컬 카메라 파이프라인 tracker_worker.py, jetson MQTT 연동
    mqtt_worker.py) 라 상태도 각자 다른 bus 키에 쌓인다. 여기서 합친다."""
    live = bus.get_state()                       # 로컬 파이프라인 (state:live)
    jetson_live = bus.get_state(key="state:mqtt")  # jetson MQTT 연동
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
            "last_seen": p.last_seen.strftime("%H:%M:%S"),
        })

    today_start = timezone.localtime(timezone.now()).replace(
        hour=0, minute=0, second=0, microsecond=0)

    # 카메라별 감지 횟수(오늘) — "감지 인물" 패널에서 카메라 박스가 몇 번
    # 인식했는지 한눈에 보려고. 진입(ENTER) 이벤트 1건 = 그 카메라가 새로
    # 인식한 사람 1명(같은 사람이 여러 카메라에 잡히면 카메라마다 따로 잡힌다).
    camera_counts = {
        str(row["camera__index"]): {"name": row["camera__name"], "count": row["n"]}
        for row in (Event.objects
                    .filter(kind=Event.ENTER, at__gte=today_start, camera__isnull=False)
                    .values("camera__index", "camera__name")
                    .annotate(n=Count("id")))
    }

    return JsonResponse({
        "running": live.get("running", False),
        "fps": round(live.get("fps", 0.0), 1),
        "mqtt_connected": jetson_live.get("mqtt_connected", False),
        "detection_enabled": RuntimeConfig.get().detection_enabled,
        "jetson_entries_total": jetson_live.get("entries_total", 0),
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
        "events": [
            {"at": e.at.strftime("%H:%M:%S"),
             "person": str(e.person),
             "confirmed": e.person.confirmed,
             "cam": e.camera.name if e.camera else "—",
             "kind": e.get_kind_display()}
            for e in Event.objects.select_related("person", "camera")[:12]
        ],
    })


def _period_stats(days):
    since = timezone.now() - timedelta(days=days)
    qs = Person.objects.filter(created_at__gte=since)
    total = qs.count()
    registered = qs.filter(confirmed=True).count()
    return {"registered": registered, "unregistered": total - registered,
            "total": total}


def api_stats(request):
    """기간별(7일/14일/30일) 등록/미등록/총 인원 통계.
    '등록' 은 Person.confirmed(관리자가 검수 완료 처리한 인물)로 판단한다."""
    return JsonResponse({
        "week": _period_stats(7),
        "two_weeks": _period_stats(14),
        "month": _period_stats(30),
    })


def api_central(request):
    """B의 중앙서버(central_tracking.db) 연동 상태. CENTRAL_DB_PATH 가
    비어있거나 파일을 못 찾으면 available=False 로 조용히 응답한다 —
    아직 그 파일이 실제로 어디 있는지 확정 전이라 기본값이 비어있다."""
    return JsonResponse({
        "available": central_db.is_available(),
        "counts": central_db.counts(),
        "recent_journeys": central_db.recent_journeys(20),
    })


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


def healthz(request):
    return HttpResponse("ok")
