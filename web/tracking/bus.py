"""
tracker_worker 프로세스와 Django 프로세스 사이의 유일한 통신 창구.

  frame:cam:<index>   최신 JPEG 바이트   (tracker → web)
  state:live          현재 트랙 상태 JSON (tracker → web)
  cmd:tracker         명령 큐 (LPUSH/RPOP) (web → tracker)

Redis 가 없으면 /tmp 파일로 자동 폴백한다. 개발 중에 Redis 안 띄워도
화면은 뜨게 하려는 목적이고, 실사용에서는 Redis 를 쓰는 게 맞다.
"""
import json
import os
import tempfile
import time
from pathlib import Path

from django.conf import settings

_FALLBACK_DIR = Path(tempfile.gettempdir()) / "reid_bus"

try:
    import redis as _redis
    _client = _redis.from_url(settings.REDIS_URL, socket_connect_timeout=1)
    _client.ping()
    HAVE_REDIS = True
except Exception:                                   # noqa: BLE001
    _client = None
    HAVE_REDIS = False
    _FALLBACK_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------------ 내부
def _fpath(key: str) -> Path:
    return _FALLBACK_DIR / key.replace(":", "_")


def _set_raw(key: str, value: bytes) -> None:
    if HAVE_REDIS:
        _client.set(key, value)
        return
    p = _fpath(key)
    tmp = p.with_suffix(".tmp")
    tmp.write_bytes(value)
    # Windows 에서는 대상 파일을 다른 프로세스(백신 실시간 검사, 동시 읽기 등)가
    # 순간적으로 잠그고 있으면 os.replace 가 PermissionError(WinError 5)로 실패
    # 할 수 있다 — 2026-08-11 밤 main_server_worker.py 가 이걸로 죽은 채 방치돼
    # 있었다(untreated exception). 로직 오류가 아니라 파일시스템 레이스라
    # 짧게 재시도하면 거의 바로 풀린다.
    for attempt in range(5):
        try:
            os.replace(tmp, p)                      # 원자적 교체
            return
        except PermissionError:
            if attempt == 4:
                raise
            time.sleep(0.02 * (attempt + 1))


def _get_raw(key: str):
    if HAVE_REDIS:
        return _client.get(key)
    p = _fpath(key)
    return p.read_bytes() if p.exists() else None


# ------------------------------------------------------------------ 프레임
def publish_frame(cam_index: int, jpeg: bytes) -> None:
    """tracker 가 호출. 큐에 쌓지 않고 항상 덮어쓴다 (지연 누적 방지)."""
    _set_raw(f"frame:cam:{cam_index}", jpeg)


def get_frame(cam_index: int):
    return _get_raw(f"frame:cam:{cam_index}")


# ------------------------------------------------------------------ 상태
# 로컬 카메라 파이프라인(tracker_worker.py) 과 jetson MQTT 워커(mqtt_worker.py) 가
# 별도 프로세스로 동시에 돈다. 같은 "state:live" 키를 같이 쓰면 서로 덮어써서
# 화면이 깜빡이므로 key 로 분리한다. 기본값은 기존 로컬 파이프라인과 호환.
def publish_state(state: dict, key: str = "state:live") -> None:
    _set_raw(key, json.dumps(state, ensure_ascii=False).encode())


def get_state(key: str = "state:live") -> dict:
    raw = _get_raw(key)
    if not raw:
        return {"running": False, "fps": 0.0, "cameras": [], "tracks": []}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"running": False, "fps": 0.0, "cameras": [], "tracks": []}


# ------------------------------------------------------------------ 명령
def send_command(cmd: str, **args) -> None:
    """Django 가 호출. tracker 는 루프마다 논블로킹으로 꺼내 쓴다."""
    payload = json.dumps({"cmd": cmd, "args": args}, ensure_ascii=False)
    if HAVE_REDIS:
        _client.lpush("cmd:tracker", payload)
        return
    p = _fpath("cmd:tracker")
    with p.open("a", encoding="utf-8") as f:
        f.write(payload + "\n")


def pop_command():
    """tracker 가 호출. 없으면 None. 절대 블로킹하지 않는다."""
    if HAVE_REDIS:
        raw = _client.rpop("cmd:tracker")
        return json.loads(raw) if raw else None

    p = _fpath("cmd:tracker")
    if not p.exists():
        return None
    lines = p.read_text(encoding="utf-8").splitlines()
    if not lines:
        return None
    first, rest = lines[0], lines[1:]
    p.write_text("\n".join(rest) + ("\n" if rest else ""), encoding="utf-8")
    return json.loads(first)
