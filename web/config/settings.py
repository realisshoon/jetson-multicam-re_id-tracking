"""
Django settings — jetson-multicam-reid 관리자 대시보드
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------- 기본
SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-only-change-me")
DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"

# Jetson IP로 접속하므로 열어둠. 외부 노출 시 반드시 좁힐 것.
ALLOWED_HOSTS = ["*"]
CSRF_TRUSTED_ORIGINS = [
    o for o in os.environ.get("DJANGO_CSRF_ORIGINS", "").split(",") if o
]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "tracking",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    # daphne 는 runserver 와 달리 static 을 자동으로 서빙하지 않는다.
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "config.urls"
ASGI_APPLICATION = "config.asgi.application"
WSGI_APPLICATION = "config.wsgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# ---------------------------------------------------------------- DB
# tracker_worker.py 와 Django 가 같은 SQLite 를 동시에 쓴다.
# WAL 모드가 아니면 'database is locked' 로 죽는다. 필수.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "OPTIONS": {
            "timeout": 20,
            "init_command": "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
        },
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
]

LANGUAGE_CODE = "ko-kr"
TIME_ZONE = "Asia/Seoul"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND":
        "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}

MEDIA_URL = "media/"
MEDIA_ROOT = BASE_DIR / "media"          # 크롭 이미지 저장 위치

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ---------------------------------------------------------------- 프로젝트 설정
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

# jetson-multicam-re_id-tracking (별도 네트워크의 Jetson 장비) 연동.
# 이 레포는 그쪽 코드를 절대 수정하지 않는다 — 그쪽이 이미 만들어 내는
# 산출물(MQTT cctv/entry 토픽, node_a/node_b 의 MJPEG 스트림)만 읽는다.
# src/network/mqtt_client.py 의 기본값과 동일한 호스트/포트/토픽.
JETSON = {
    "MQTT_HOST": os.environ.get("JETSON_MQTT_HOST", "127.0.0.1"),
    "MQTT_PORT": int(os.environ.get("JETSON_MQTT_PORT", "1883")),
    "MQTT_TOPIC": os.environ.get("JETSON_MQTT_TOPIC", "cctv/entry"),
    # node_a.py / node_b.py 가 각각 :8000 / :8001 에서 직접 서빙하는 스트림.
    "CAM_A_STREAM_URL": os.environ.get(
        "JETSON_CAM_A_URL", "http://127.0.0.1:8000/stream"),
    "CAM_B_STREAM_URL": os.environ.get(
        "JETSON_CAM_B_URL", "http://127.0.0.1:8001/stream"),
    # Camera C 는 아직 가동 전이라 기본값을 비워둔다 — 대시보드가 빈 슬롯으로
    # 보여준다. 켜지면 JETSON_CAM_C_URL 환경변수로 채우면 된다.
    "CAM_C_STREAM_URL": os.environ.get("JETSON_CAM_C_URL", ""),
    # Camera D 는 node_a/b 와 같은 장비의 :8002 에서 서빙 중.
    "CAM_D_STREAM_URL": os.environ.get(
        "JETSON_CAM_D_URL", "http://127.0.0.1:8002/stream"),
}

TRACKER = {
    # tracker_worker 가 참고하는 기본값. Camera 모델이 우선한다.
    "REID_THRESHOLD": 0.55,      # 코사인 유사도 컷
    "DET_CONF": 0.40,
    "SNAPSHOTS_PER_TRACKLET": 5, # 트랙렛당 저장할 대표 크롭 수
    "STREAM_JPEG_QUALITY": 70,
    "STREAM_WIDTH": 640,
}
