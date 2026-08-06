import asyncio
import hashlib

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db.models import Count
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import bus
from .models import Camera, Event, Person, RuntimeConfig
from .mqtt_ingest import NODE_CAMERAS

BOUNDARY = "reidframe"

# NODE_CAMERAS(mqtt_ingest.py)의 node_id → settings.JETSON 안의 스트림 URL 키.
# 카메라 월(대시보드 통합 그리드)이 로컬 카메라와 jetson 카메라를 하나의
# 목록으로 같이 보여준다. jetson 쪽은 Django 를 거치지 않고 브라우저가
# 직접 스트림 URL 에 접근하므로 data-stream 값이 로컬(내부 프록시)과 다르다.
JETSON_URL_KEYS = {
    "A": "CAM_A_STREAM_URL", "B": "CAM_B_STREAM_URL",
    "C": "CAM_C_STREAM_URL", "D": "CAM_D_STREAM_URL",
}


def _jetson_cams():
    cams = []
    for node_id, info in NODE_CAMERAS.items():
        url = settings.JETSON.get(JETSON_URL_KEYS[node_id], "")
        cams.append({
            "index": info["index"],
            "name": info["name"],
            "url": url,
            "source": url or "미가동",
            "is_jetson": True,
        })
    return cams


# ------------------------------------------------------------------ (A) 화면
def dashboard(request):
    # index 900+ 는 jetson MQTT 워커가 자동 등록하는 가상 카메라
    # (mqtt_ingest.NODE_CAMERAS). jetson 카메라를 앞에 둬야 카메라 월의
    # A/B/C/D 라벨이 실제 jetson 노드 이름과 순서가 맞는다.
    local_cams = list(Camera.objects.filter(enabled=True, index__lt=900))
    people = (Person.objects.filter(is_active=True)
              .prefetch_related("snapshots", "tracklets__camera")
              .annotate(n_track=Count("tracklets"))[:24])
    return render(request, "tracking/dashboard.html", {
        "cameras": _jetson_cams() + local_cams,
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

    return JsonResponse({
        "running": live.get("running", False),
        "fps": round(live.get("fps", 0.0), 1),
        "mqtt_connected": jetson_live.get("mqtt_connected", False),
        "jetson_entries_total": jetson_live.get("entries_total", 0),
        "live_tracks": live.get("tracks", []),
        "system": live.get("system", {}),
        "totals": {
            "people": Person.objects.filter(is_active=True).count(),
            "now": len(live.get("tracks", [])),
            "cameras": (Camera.objects.filter(enabled=True, index__lt=900).count()
                       + len(NODE_CAMERAS)),
        },
        "gallery": gallery,
        "events": [
            {"at": e.at.strftime("%H:%M:%S"),
             "person": str(e.person),
             "cam": e.camera.name if e.camera else "—",
             "kind": e.get_kind_display()}
            for e in Event.objects.select_related("person", "camera")[:12]
        ],
    })


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
