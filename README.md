# reid-admin-web

Multicam Re-ID 관리자 대시보드 (Django). `jetson-multicam-re_id-tracking`
(Jetson 장비들)이 만들어내는 데이터를 받아서 보여주는 웹.
**jetson 쪽 리포는 이 프로젝트에서 일절 수정하지 않는다.**

아키텍처와 데이터 흐름, 모델 필드, API 계획 등 자세한 내용은
[`HANDOFF_TO_MAIN_SERVER.md`](HANDOFF_TO_MAIN_SERVER.md) 와
[`web/README_WEB.md`](web/README_WEB.md) 참고.

## 구조

```
reid-admin-web/
├─ web/                       Django 프로젝트 루트
│  ├─ manage.py
│  ├─ config/                 settings / urls / asgi
│  ├─ tracking/                Django 앱 (models, views, admin, migrations,
│  │                            templates/tracking/, static/tracking/)
│  ├─ mqtt_worker.py           jetson MQTT 구독 → DB 적재 (개발용, API 준비되면 교체 예정)
│  ├─ tracker_worker.py        로컬 카메라 파이프라인
│  └─ requirements-web.txt
├─ HANDOFF_TO_MAIN_SERVER.md   메인 서버 연동 인계 문서 (아키텍처/API 스펙)
├─ .env.example                환경변수 예시 (복사해서 .env 로 사용)
├─ .gitignore
└─ README.md                   이 파일
```

`templates/`, `static/`는 Django 표준 관례대로 앱(`tracking/`) 안에 있다
(top-level에 별도로 안 두는 이유: Django 의 app static/template loader 가
앱 디렉토리 기준으로 자동으로 찾게 하기 위함).

## 요구 사항

- Python 3.10 (개발 환경 기준: **3.10.11**)
- pip

## 설치

```powershell
python -m venv .venv
.venv\Scripts\pip install -r web\requirements-web.txt

cd web
..\.venv\Scripts\python manage.py migrate
..\.venv\Scripts\python manage.py createsuperuser
```

## 환경변수

`.env.example` 을 복사해서 `.env` 로 만들고 값을 채운다 (지금 코드는 `.env`
파일을 자동으로 읽지 않으므로, 실행 전에 셸에서 값을 직접 export 하거나
프로세스 매니저/서비스 설정에 넣어야 한다). 전체 목록과 설명은
[`HANDOFF_TO_MAIN_SERVER.md`](HANDOFF_TO_MAIN_SERVER.md) 의 "환경변수 목록"
섹션 참고.

## 실행

`web/` 디렉토리에서, 터미널 3개(또는 필요한 것만):

```powershell
$env:JETSON_MQTT_HOST="10.10.20.56"
$env:JETSON_CAM_A_URL="http://10.10.20.56:8000/stream"
$env:JETSON_CAM_B_URL="http://10.10.20.56:8001/stream"
$env:JETSON_CAM_D_URL="http://10.10.20.56:8002/stream"

..\.venv\Scripts\daphne.exe -b 0.0.0.0 -p 8000 config.asgi:application   # 대시보드
..\.venv\Scripts\python.exe mqtt_worker.py       # jetson MQTT 연동
..\.venv\Scripts\python.exe tracker_worker.py    # 로컬 카메라 파이프라인
```

`http://localhost:8000` 접속.

> **`manage.py runserver` 는 쓰지 말 것.** WSGI 라 MJPEG 스트리밍이 워커를
> 통째로 잠근다. 반드시 `daphne`(ASGI) 로 띄운다.

Linux/Mac 에서는 `web/run_dev.sh` 참고 (대시보드 프로세스만 해당,
워커 2개는 별도 실행 필요).

## 현재 상태 / 다음 단계

- `mqtt_worker.py` 는 Jetson A 보드(`10.10.20.56`)의 Mosquitto 에 **직접**
  연결해서 개발/테스트 중인 임시 구성이다.
- 최종 데이터 흐름은 `Jetson A/B/D → 메인 서버 Mosquitto →
  journey_sqlite_server.py → central_tracking.db → 메인 서버 API →
  Django` 로 확정되어 있고, 메인 서버 API 가 준비되면 `mqtt_worker.py` 를
  그 API 를 호출하는 클라이언트로 교체한다. 자세한 내용과 제안 API 스펙은
  [`HANDOFF_TO_MAIN_SERVER.md`](HANDOFF_TO_MAIN_SERVER.md) 참고.
