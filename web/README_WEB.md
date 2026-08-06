# Multicam Re-ID 관리자 대시보드

Django 관리 웹. 두 가지 소스를 동시에 보여준다:

1. **로컬 카메라 파이프라인** — 이 서버(또는 이 서버가 접근 가능한 장비)에 직접
   붙은 카메라를 `tracker_worker.py` 가 열어서 검출/추적/Re-ID 를 돌린다.
2. **jetson-multicam-re_id-tracking 연동** — 별도 네트워크의 Jetson 장비가
   이미 만들어내는 산출물을 `mqtt_worker.py` 가 받아서 보여준다.
   **jetson 쪽 리포는 절대 수정하지 않는다.** 그쪽이 이미 밖으로 내보내는
   MQTT/MJPEG 만 소비한다.

## jetson 이 실제로 내보내는 것 (jetson 쪽 코드 기준)

| 산출물 | 내용 | jetson 쪽 소스 |
|---|---|---|
| MJPEG 스트림 | Camera A 영상, `http://<jetson-ip>:8000/stream` | `src/nodes/node_a.py` |
| MJPEG 스트림 | Camera B 영상, `http://<jetson-ip>:8001/stream` | `src/nodes/node_b.py` |
| MQTT 이벤트 | 브로커 `<jetson-ip>:1883`, 토픽 `cctv/entry`. Camera A 입장 시 `global_person_id` + 512-d Re-ID embedding 발행 | `src/network/mqtt_client.py` |

Camera B 의 재식별(매칭) 결과는 jetson 쪽 로컬 CSV(`logs/node_b_matches.csv`)
에만 기록되고 네트워크로는 나가지 않는다. 별도 PC/서버에 떠 있는 이 웹에서는
그 파일에 접근할 수 없으므로, **"누가 B 를 통과하며 재식별됐는지"는 현재
구조로는 확인할 수 없다.** (jetson 쪽에 MQTT publish 를 추가해야 풀리는데,
그건 jetson 리포 수정이 필요하므로 이 프로젝트 범위 밖이다.)

## 구조

```
로컬 카메라 ──► tracker_worker.py ──┐                   Django (daphne, ASGI)
                (YOLO/ByteTrack/     │                     /            대시보드
                 Re-ID, bus 로       ├── SQLite (WAL) ──►  /admin/      인물 관리
                 프레임/상태 전달)    │      (ORM)          /api/state/  상태 폴링
                                     │                     /api/control/ 제어(로컬만)
jetson (별도 네트워크)                │
  node_a.py → MQTT cctv/entry ──► mqtt_worker.py ──────────┘
  node_a/b  → MJPEG :8000/:8001 ─────────────────────────► 대시보드가 <img> 로 직접 임베드
```

- `tracker_worker.py` : 로컬 카메라를 열고 검출/추적/Re-ID 를 돌린다. 대시보드
  상단의 **시작/정지/카메라 다시 읽기** 버튼으로 제어한다. 실제 검출기·트래커·
  Re-ID 모델은 파일 안 "여기에 기존 파이프라인을 꽂는다" 자리에 연결한다
  (지금은 placeholder라 트랙이 안 잡힌다 — 붙일 모델이 정해지면 채운다).
- `mqtt_worker.py` : jetson 이 브로커로 보내는 `cctv/entry` 메시지를 구독하는
  '제3의 구독자'일 뿐이다(node_b.py 가 같은 토픽을 구독하는 것과 동일한
  방식). 카메라를 열거나 YOLO 를 돌리지 않는다. 받은 이벤트를 바로 DB 에
  적재한다. 영상은 이 프로세스를 거치지 않고, 대시보드가 jetson 의 MJPEG
  URL 을 직접 띄운다.
- 둘 다 `django.setup()` 으로 같은 ORM 을 쓴다. 상태는 `tracking/bus.py` 를
  통해 Django 로 전달되는데, 워커가 2개라 키를 분리해서 쓴다
  (`state:live` = 로컬 파이프라인, `state:mqtt` = jetson 연동). 서로 안 섞인다.
- Camera 모델의 `index` 900 이상은 jetson 이 자동 등록하는 가상 카메라용으로
  예약돼 있다(`tracking/mqtt_ingest.py`). 로컬 카메라 그리드에는 안 섞인다.

## 설치

```bash
cd web
pip install -r requirements-web.txt

python manage.py migrate
python manage.py createsuperuser
```

## 설정

`config/settings.py` 의 `JETSON` 딕셔너리, 또는 환경변수로 jetson 장비
주소를 지정한다 (기본값은 localhost — 실사용 시 반드시 바꿀 것):

```bash
export JETSON_MQTT_HOST=10.10.20.60
export JETSON_MQTT_PORT=1883
export JETSON_CAM_A_URL=http://10.10.20.60:8000/stream
export JETSON_CAM_B_URL=http://10.10.20.60:8001/stream
```

로컬 카메라는 코드가 아니라 `/admin/tracking/camera/` 에서 등록한다
(`index`는 0~899 범위로 쓸 것 — 900 이상은 jetson 전용 예약).

## 실행

터미널 3개:

```bash
./run_dev.sh                  # 대시보드 (daphne, :8000)
python tracker_worker.py      # 로컬 카메라 파이프라인
python mqtt_worker.py         # jetson MQTT 구독 → DB 적재
```

`http://<이 서버 IP>:8000` 접속.
- 로컬 카메라를 쓰려면 `/admin/tracking/camera/` 에서 카메라를 추가하고
  대시보드 상단의 **시작** 버튼을 누른다.
- jetson 장비가 켜져서 MQTT 로 입장 이벤트를 보내기 시작하면 별도 조작
  없이 대시보드의 "인물" 목록과 "이벤트" 로그에 자동으로 쌓인다. 상단 바의
  **MQTT** 표시가 "연결됨"이어야 정상 수신 중.

> `manage.py runserver` 로는 MJPEG 가 안 된다. runserver 는 WSGI 라
> 스트리밍 응답 하나가 워커를 통째로 잠근다. 반드시 daphne 를 써라.

Redis 가 없으면 `/tmp` 파일 폴백으로 돌아간다(대시보드 ↔ 워커 간 상태
전달용). 화면은 뜨지만 느리다. 실사용에서는 Redis 를 쓰는 게 맞다.

## 기능

**(A) 실시간 모니터링** — `/`
jetson 의 Camera A/B 영상, 로컬 카메라 그리드, 누적 인원/FPS, 인물 명단,
이벤트 로그. 이름은 카드에서 바로 클릭해 입력할 수 있다. (카메라를
가로질러 매칭됐을 때만 표시되는 배지 연결선은, jetson 쪽 Camera B 매칭
정보가 밖으로 안 나오는 현재 구조상 jetson 인물은 항상 단일 카메라로만
뜬다 — 위 "jetson 이 실제로 내보내는 것" 참고. 로컬 파이프라인에서 여러
카메라에 잡힌 인물은 정상적으로 연결선이 뜬다.)

**(B) 데이터 관리** — `/admin/tracking/person/`

| 작업 | 방법 |
|---|---|
| 이름 붙이기 | 목록에서 바로 입력 → 저장 |
| 잘못 합쳐진 ID 분리 | 트랙렛 목록 → 선택 → "새 인물로 분리" |
| 나뉜 ID 병합 | 인물 목록 → 선택 → "하나로 병합" |
| 갤러리 삭제 | 스냅샷 목록에서 삭제 (파일도 같이 지워짐) |

병합/분리하면 자동으로 `reload_gallery` 명령이 로컬 tracker 에 전달된다.
**이게 없으면 병합해도 다음 프레임에 도로 갈라진다.** (jetson 인물은 이
갤러리 매칭 로직을 안 타므로 영향 없음.)

**(C) 시스템 제어**
대시보드 상단 버튼(시작/정지/카메라 다시 읽기) — 로컬 파이프라인 전용.
jetson 쪽은 그쪽 장비에서 독립적으로 계속 돌기 때문에 여기서 제어하지
않는다. `/admin/tracking/camera/` 에서 로컬 카메라 추가,
`/admin/tracking/runtimeconfig/` 에서 threshold 조정.

## 주의

- **jetson 리포 수정 금지.** jetson 연동 부분이 하는 일은 그쪽이 이미
  내보내는 MQTT/MJPEG 를 읽는 것뿐이다. jetson 쪽 코드를 고쳐야 풀리는
  문제(예: Camera B 매칭 결과를 받아오고 싶다)는 이 프로젝트 범위 밖이다.
- **SQLite WAL 필수.** Django, `tracker_worker.py`, `mqtt_worker.py` 세
  프로세스가 같은 DB 를 쓴다. `settings.py` 에 이미 걸어뒀다. 빼면
  `database is locked` 로 죽는다.
- **DB write 빈도(로컬 파이프라인).** 스냅샷은 트랙렛당 15프레임에 1장,
  인물당 5장까지만 저장한다. 이벤트는 진입/이탈 전이에만 기록한다.
- **MQTT 재전송 대비.** QoS 1 은 "최소 1회" 배달이라 같은 ENTRY 메시지가
  중복 도착할 수 있다. `tracking/mqtt_ingest.py` 가 (camera, local_id)
  트랙렛 존재 여부로 걸러서 중복 Event/Snapshot 을 만들지 않는다.
- **DEBUG=False 로 운영.** True 면 쿼리 로그가 메모리에 계속 쌓여
  장시간 데모 중에 부풀어 오른다. `DJANGO_DEBUG=0` 환경변수.
- **라이선스.** jetson 레포는 AGPL-3.0(Ultralytics YOLO) 이다. 이 웹은
  그 코드를 import 하지 않으므로 별개지만, 같은 조직에서 함께 배포한다면
  참고할 것.
