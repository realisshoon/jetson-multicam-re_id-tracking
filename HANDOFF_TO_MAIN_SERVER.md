# reid-admin-web → 메인 서버(B) 인계 문서

작성 시점: 2026-08-06. C(Django/웹) 담당자가 B(메인 서버) 담당자에게 현재
상태를 전달하기 위한 문서. 최종 구조 확정 내용(MAIN 서버 = 10.10.20.33,
central_tracking.db, journey_sqlite_server.py)에 맞춰 이후 변경할 부분을
명시했다.

---

## 1. 지금 이 코드가 뭘 하고 있는지 (요약)

Django 대시보드 1개 + 백그라운드 워커 2개, 이렇게 3개 프로세스로 돈다.

```
tracker_worker.py   로컬 카메라 파이프라인 (지금은 미사용/placeholder)
mqtt_worker.py       jetson A보드 MQTT 를 직접 구독 → 이 PC의 SQLite 에 저장
                      ⚠ 최종 구조에서는 이 프로세스가 없어지고, Django 가
                        central_tracking.db 또는 메인 서버 API 를 읽는
                        구조로 바뀔 예정 (8번 항목 참고)
daphne (Django)       위 DB 를 읽어서 웹 화면 렌더링
```

디렉토리: `D:\20260728\reid-admin-web\` (jetson 리포와는 완전히 분리된
별도 디렉토리 — jetson 쪽 코드는 일절 수정하지 않음)

---

## 2. Django 전체 코드

리포 루트: `D:\20260728\reid-admin-web\web\`

```
web/
├── manage.py
├── run_dev.sh                      (Linux/Mac용, Windows에선 daphne 직접 실행)
├── tracker_worker.py               로컬 카메라 파이프라인 워커
├── mqtt_worker.py                  jetson MQTT 구독 워커 ⚠ 최종엔 대체 예정
├── requirements-web.txt
├── README_WEB.md                   전체 아키텍처 설명 문서
├── config/
│   ├── settings.py                 환경변수·DB·JETSON 연동 설정
│   ├── urls.py
│   └── asgi.py / wsgi.py
└── tracking/                       Django 앱 본체
    ├── models.py                   DB 스키마 (아래 3번)
    ├── views.py                    화면 렌더링 + API
    ├── urls.py
    ├── admin.py                    /admin/ 관리 화면
    ├── bus.py                      Django ↔ 워커 프로세스 간 상태 전달(Redis/파일)
    ├── mqtt_ingest.py              MQTT 페이로드 → DB 저장 로직 (아래 6번)
    ├── services.py                 인물 병합/분리 로직
    ├── management/commands/seed_demo.py   더미 데이터 생성용(현재 안 씀)
    └── templates/tracking/dashboard.html  화면 전체(HTML+CSS+JS 한 파일)
```

전체 코드는 `D:\20260728\reid-admin-web\reid-admin-web-handoff.zip` 로
압축해 두었다 (`.venv`, `db.sqlite3`, `media/`, `staticfiles/`, `__pycache__`
제외). 이 zip 을 그대로 전달하면 된다.

---

## 3. Django Model (`tracking/models.py`)

| 모델 | 용도 | 주요 필드 |
|---|---|---|
| `Camera` | 카메라 1대 | `index`(unique, 900+ 는 jetson 전용 예약), `name`, `source`, `enabled` |
| `Person` | 사람 1명(Global ID) | `external_id`(jetson 의 `global_person_id`, 예: `G000001`, unique), `label`(이름), `created_at`, `last_seen`, `is_active`, `confirmed` |
| `Tracklet` | 카메라 1대에서의 궤적 1개 | `person`(FK), `camera`(FK), `local_id`(ByteTrack local id), `start_at`, `end_at`, `frames` |
| `Snapshot` | 크롭 이미지 + Re-ID 임베딩 | `person`(FK), `tracklet`(FK, null 가능), `image`(선택), `embedding`(512-d, BinaryField), `score` |
| `Event` | 타임라인 이벤트 | `person`(FK), `camera`(FK), `kind`(enter/exit/merge/split), `at`, `detail` |
| `RuntimeConfig` | 파이프라인 파라미터(싱글톤) | `det_conf`, `reid_threshold`, `max_gallery`, `draw_boxes`, `draw_labels` |

DB: SQLite, WAL 모드 필수(`config/settings.py` 의 `DATABASES` 에 이미 설정됨,
여러 프로세스가 동시에 쓰기 때문).

---

## 4. 현재 화면(대시보드)에서 실제로 쓰는 필드

`/api/state/` (1초 폴링) 응답 기준 — `tracking/views.py` 의 `api_state()`:

```json
{
  "running": false,              // 로컬 파이프라인 가동 여부
  "fps": 0.0,                    // 로컬 파이프라인 FPS
  "mqtt_connected": true,        // jetson MQTT 연결 상태
  "jetson_entries_total": 3,     // MQTT 로 받은 누적 입장 수
  "live_tracks": [],
  "totals": { "people": 3, "now": 0, "cameras": 0 },
  "gallery": [                   // 인물 카드 목록
    { "id": 1, "label": "", "named": false, "confirmed": false,
      "thumb": null, "cams": ["Camera A · 입장"], "last_seen": "15:24:10" }
  ],
  "events": [                    // 이벤트 로그
    { "at": "15:24:10", "person": "미확인 #1", "cam": "Camera A · 입장",
      "kind": "진입" }
  ]
}
```

영상은 이 API 를 안 타고, 대시보드 HTML 에 `<img src="jetson MJPEG URL">` 로
직접 박혀 있다 (아래 7번).

---

## 5. mqtt_worker.py 가 저장하는 필드 (⚠ 최종엔 이 워커 자체가 바뀜)

`tracking/mqtt_ingest.py` 의 `ingest_entry_payload()` 가 처리하는 입력
페이로드(현재 jetson A보드, 토픽 `cctv/entry` 기준):

```json
{
  "timestamp": "2026-08-06T10:00:00",
  "node_id": "A",
  "event": "ENTRY",
  "local_track_id": 3,
  "global_person_id": "G000001",
  "next_nodes": ["B", "C"],
  "reid_model": "osnet_x0_25",
  "embedding_dim": 512,
  "embedding": [0.01, -0.02, "..."]
}
```

→ 저장 매핑:

| MQTT 필드 | 저장되는 곳 |
|---|---|
| `global_person_id` | `Person.external_id` (이 값으로 중복 방지) |
| `node_id` | `Camera` 조회/생성 (A→index 900, B→index 901) |
| `local_track_id` | `Tracklet.local_id` |
| `embedding`(512-d) | `Snapshot.embedding` |
| `event != "ENTRY"` 또는 `global_person_id` 없음 | 무시 |
| 같은 `(camera, local_id)` 재수신(MQTT 재전송) | 무시 (Event/Snapshot 중복 생성 안 함) |

**B/D 토픽(`cctv/passage/b`, `cctv/completion/d`) 은 아직 파싱 로직이
없다** — 페이로드 스키마 문서가 나오면 바로 추가 가능.

---

## 6. 영상 URL 설정 위치

`web/config/settings.py` 의 `JETSON` 딕셔너리 (환경변수로 오버라이드):

```python
JETSON = {
    "CAM_A_STREAM_URL": os.environ.get("JETSON_CAM_A_URL", "http://127.0.0.1:8000/stream"),
    "CAM_B_STREAM_URL": os.environ.get("JETSON_CAM_B_URL", "http://127.0.0.1:8001/stream"),
    "CAM_C_STREAM_URL": os.environ.get("JETSON_CAM_C_URL", ""),   # 미가동, 빈 값이면 대시보드가 빈 슬롯 표시
    "CAM_D_STREAM_URL": os.environ.get("JETSON_CAM_D_URL", "http://127.0.0.1:8002/stream"),
}
```

대시보드는 이 URL 을 그대로 `<img src>` 로 박아 넣는다 — Django 서버를
경유하지 않고 **브라우저가 각 Jetson 에 직접** 접속한다
(`web/tracking/views.py` 의 `dashboard()` → `web/tracking/templates/tracking/dashboard.html`).

확정된 구조("영상은 우선 각 Jetson HTTP Stream 을 브라우저가 직접 접근")와
일치하므로 이 부분은 그대로 유지하면 된다.

---

## 7. 환경변수 목록

| 변수 | 기본값 | 용도 |
|---|---|---|
| `DJANGO_SECRET_KEY` | `dev-only-change-me` | 운영 배포 전 반드시 변경 |
| `DJANGO_DEBUG` | `1` | 운영은 `0` 으로 |
| `DJANGO_CSRF_ORIGINS` | (빈 값) | 콤마 구분 origin 목록 |
| `REDIS_URL` | `redis://127.0.0.1:6379/0` | 없으면 파일 폴백(느림) |
| `JETSON_MQTT_HOST` | `127.0.0.1` | ⚠ 지금은 A보드(`10.10.20.56`)로 테스트 중. **최종은 메인 서버(`10.10.20.33`)** |
| `JETSON_MQTT_PORT` | `1883` | |
| `JETSON_MQTT_TOPIC` | `cctv/entry` | B/D 용 토픽 추가 필요 (`cctv/passage/b`, `cctv/completion/d`) |
| `JETSON_CAM_A_URL` | `http://127.0.0.1:8000/stream` | 지금 `http://10.10.20.56:8000/stream` |
| `JETSON_CAM_B_URL` | `http://127.0.0.1:8001/stream` | 지금 `http://10.10.20.56:8001/stream` |
| `JETSON_CAM_C_URL` | (빈 값) | 미가동 |
| `JETSON_CAM_D_URL` | `http://127.0.0.1:8002/stream` | 지금 `http://10.10.20.56:8002/stream` |

---

## 8. Django 실행 명령

Windows PowerShell 기준, `D:\20260728\reid-admin-web\web` 에서
(venv: `D:\20260728\reid-admin-web\.venv`):

```powershell
# 최초 1회
& "D:\20260728\reid-admin-web\.venv\Scripts\pip.exe" install -r requirements-web.txt
& "D:\20260728\reid-admin-web\.venv\Scripts\python.exe" manage.py migrate
& "D:\20260728\reid-admin-web\.venv\Scripts\python.exe" manage.py createsuperuser

# 실행 (터미널 3개, 또는 필요한 것만)
$env:JETSON_MQTT_HOST="10.10.20.56"; $env:JETSON_CAM_A_URL="http://10.10.20.56:8000/stream"
$env:JETSON_CAM_B_URL="http://10.10.20.56:8001/stream"; $env:JETSON_CAM_D_URL="http://10.10.20.56:8002/stream"

& "D:\20260728\reid-admin-web\.venv\Scripts\daphne.exe" -b 0.0.0.0 -p 8000 config.asgi:application
& "D:\20260728\reid-admin-web\.venv\Scripts\python.exe" mqtt_worker.py       # jetson 연동
& "D:\20260728\reid-admin-web\.venv\Scripts\python.exe" tracker_worker.py    # 로컬 카메라(현재 미사용)
```

`manage.py runserver` 는 절대 쓰지 말 것 — WSGI 라 MJPEG 스트리밍이 워커를
통째로 잠근다. 반드시 `daphne`.

---

## 9. 결정: 메인 서버 API 방식으로 간다

`central_tracking.db` 직접 연결이 아니라 **B 가 API 를 열고 Django 가
그걸 호출하는 방식**으로 확정. 이유:

- B 가 메인 서버 담당이니 API 를 여는 게 원래 역할과도 맞음
- Django 가 DB 파일을 직접 안 건드리니 `journey_sqlite_server.py` 의
  쓰기와 절대 안 부딪힘 (SQLite 동시쓰기 문제 원천 차단)
- B 가 스키마를 바꿔도 API 응답 모양만 유지하면 Django 는 영향 없음
- Django 를 나중에 다른 PC로 옮기거나 인스턴스를 늘려도 자유로움

`mqtt_worker.py` + 로컬 `db.sqlite3` 는 B 의 API 가 준비될 때까지의
임시 구성이고, API 가 오면 `mqtt_ingest.py`/`mqtt_worker.py` 자리를
API 를 호출하는 클라이언트 모듈로 교체할 예정.

## 10. B 에게 제안하는 API 스펙 (초안)

지금 Django 대시보드가 실제로 쓰는 데이터(4번 섹션) 기준으로 짜 본
제안이다. B 가 이대로 하지 않아도 되고, 필요하면 이 문서 보면서 같이
조정하면 된다. **핵심은 셋 다 GET, 인증 없이 사내망에서만 호출된다는
전제.**

### `GET /api/persons`
현재 대시보드의 "인물" 목록 카드 데이터. A(입장)/B(통과)/D(도착) 세
체크포인트를 `global_person_id` 기준으로 이미 합쳐서 내려주면 Django
쪽에서 다시 합칠 필요가 없어서 제일 좋음.

```json
{
  "persons": [
    {
      "global_person_id": "G000001",
      "label": null,
      "first_seen": "2026-08-06T10:00:00+09:00",
      "last_seen": "2026-08-06T10:03:12+09:00",
      "checkpoints": ["A", "B"],
      "status": "in_transit"
    }
  ]
}
```

- `checkpoints`: 이 사람이 실제로 통과한 노드 배열 (A/B/D 중 있는 것만) —
  대시보드의 "카메라를 가로질러 매칭됐을 때 배지 사이 선" 표시에 그대로 씀
- `status`: `entered`(A만) / `in_transit`(A+B) / `completed`(A+B+D) 같은
  형태 제안. B 가 정의하는 대로 맞춰서 받으면 됨

### `GET /api/events?since=<ISO timestamp>`
이벤트 로그용. Django 가 1초마다 폴링하면서 `since` 로 마지막으로 받은
시각 이후 것만 요청 (매번 전체를 다시 안 받아도 되게).

```json
{
  "events": [
    {"at": "2026-08-06T10:03:12+09:00", "global_person_id": "G000001",
     "node": "B", "kind": "passage"}
  ]
}
```

### `GET /api/status`
브로커/서버 상태. 지금 대시보드 상단 "MQTT 연결됨/끊김" 표시를 그대로
대체.

```json
{"mqtt_connected": true, "persons_total": 42, "last_event_at": "2026-08-06T10:03:12+09:00"}
```

### 확인 필요 (B 답변 대기)

1. 위 3개 엔드포인트 모양, B 가 그대로 가도 되는지 / 어디를 바꾸고 싶은지
2. `journey_sqlite_server.py` 가 A/B/D 세 이벤트를 `global_person_id` 로
   이미 합쳐서 저장하는지, 아니면 Django 쪽(`GET /api/persons`)에서
   합치는 로직까지 API 서버가 대신 해줘야 하는지
3. 인증 필요 여부 (지금은 "사내망이라 없어도 됨" 가정)
4. API 서버 기술 스택 (Flask/FastAPI/Django 등) — Django 쪽 클라이언트
   코드 짜는 데는 영향 없지만 참고용

이 4개 답이 오면 바로 `mqtt_worker.py` 자리를 API 클라이언트로 교체하는
작업 시작하겠음.
