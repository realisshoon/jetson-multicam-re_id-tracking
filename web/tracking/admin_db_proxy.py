"""
Main(B)의 DB 관리자 API(GET/POST /api/admin/database/*)를 대신 호출하는
고정 경로 Proxy — "DB 관리" 페이지 전용.

설계 원칙(요청 사항 그대로):
  - 임의 URL/SQL을 받는 범용 프록시가 아니다. 아래 5개 경로만 존재하고,
    각각 Main 의 정확히 대응되는 고정 경로 하나만 호출한다.
  - MAIN_ADMIN_TOKEN(관리자 토큰)은 여기(Web 백엔드 프로세스 메모리) 밖으로
    절대 안 나간다 — 프론트엔드 JS에도, 우리 API 응답 바디에도 안 실린다.
    브라우저는 우리 세션 쿠키로만 인증하고, 우리가 대신 Main에 Bearer 토큰을
    붙여 호출한다.
  - Main 이 실제로 답한 응답(상태 코드 + JSON 바디)은 그대로 통과시킨다 —
    401/403/409 계열은 전부 Main 자신이 판단해서 내리는 응답이라, 우리가
    임의로 재해석하지 않고 그대로 전달한다. 우리가 직접 만들어 내는 오류는
    Main 에 아예 요청을 보낼 수 없었던 두 가지 경우뿐이다:
      · MAIN_ADMIN_TOKEN 미설정            → 503 ADMIN_API_DISABLED
      · Main 관리자 API 서버 자체가 응답 없음 → 503 MAIN_ADMIN_CONTROL_UNAVAILABLE
  - reset/execute 는 여기서 막지 않는다(호출 자체는 정상 프록시) — 실제로
    운영 DB에 대고 누르는 건 화면/운영 절차 쪽에서 별도로 통제할 일이다.
"""
import requests
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

ADMIN_REQUEST_TIMEOUT = 15.0  # 백업/초기화는 캡처 이미지 프록시보다 오래 걸릴 수 있다


def _superuser_required(view):
    def wrapped(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return JsonResponse(
                {"error": "ADMIN_AUTH_REQUIRED", "message": "로그인이 필요하다."},
                status=401)
        if not request.user.is_superuser:
            return JsonResponse(
                {"error": "ADMIN_AUTH_REQUIRED", "message": "관리자(superuser) 권한이 필요하다."},
                status=401)
        return view(request, *args, **kwargs)
    return wrapped


def _call_main_admin(method: str, path: str, *, json_body=None):
    """Main 관리자 API 호출 공통 처리.
    반환: (status_code, json_body_or_none) — 그대로 JsonResponse 에 실어 돌려주면 된다."""
    if not settings.MAIN_ADMIN_TOKEN:
        return 503, {
            "error": "ADMIN_API_DISABLED",
            "message": "Web 백엔드에 MAIN_ADMIN_TOKEN 이 설정되지 않아 관리자 API를 쓸 수 없다.",
        }
    url = settings.MAIN_API_BASE_URL.rstrip("/") + path
    headers = {"Authorization": f"Bearer {settings.MAIN_ADMIN_TOKEN}"}
    try:
        resp = requests.request(
            method, url, headers=headers, json=json_body, timeout=ADMIN_REQUEST_TIMEOUT)
    except requests.RequestException:
        return 503, {
            "error": "MAIN_ADMIN_CONTROL_UNAVAILABLE",
            "message": "Main 관리자 제어 서버(10.10.20.33:8080)에 연결할 수 없다.",
        }
    try:
        body = resp.json()
    except ValueError:
        body = {
            "error": "MAIN_ADMIN_BAD_RESPONSE",
            "message": f"Main 이 JSON 이 아닌 응답을 돌려줬다(status={resp.status_code}).",
        }
    return resp.status_code, body


@require_GET
@_superuser_required
def api_admin_db_status(request):
    status_code, body = _call_main_admin("GET", "/api/admin/database/status")
    return JsonResponse(body, status=status_code, safe=isinstance(body, dict))


@require_POST
@csrf_protect
@_superuser_required
def api_admin_db_backup(request):
    status_code, body = _call_main_admin("POST", "/api/admin/database/backup")
    return JsonResponse(body, status=status_code, safe=isinstance(body, dict))


@require_POST
@csrf_protect
@_superuser_required
def api_admin_db_reset_preview(request):
    status_code, body = _call_main_admin("POST", "/api/admin/database/reset/preview")
    return JsonResponse(body, status=status_code, safe=isinstance(body, dict))


@require_POST
@csrf_protect
@_superuser_required
def api_admin_db_reset_execute(request):
    import json
    try:
        payload = json.loads(request.body or b"{}")
    except ValueError:
        payload = {}
    forward = {
        "confirmation_id": payload.get("confirmation_id"),
        "confirmation_text": payload.get("confirmation_text"),
    }
    status_code, body = _call_main_admin(
        "POST", "/api/admin/database/reset/execute", json_body=forward)
    return JsonResponse(body, status=status_code, safe=isinstance(body, dict))


@require_GET
@_superuser_required
def api_admin_db_job_status(request, job_id):
    status_code, body = _call_main_admin("GET", f"/api/admin/database/jobs/{job_id}")
    return JsonResponse(body, status=status_code, safe=isinstance(body, dict))
