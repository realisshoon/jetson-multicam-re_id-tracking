# reid-admin-web → 메인 서버(B) API 연동 요청

작성 시점: 2026-08-10. C(Django/웹) 담당자가 B(메인 서버) 담당자에게, 대시보드가
API로 어떤 데이터를 받아야 하는지 정리해서 전달하는 문서. 8/6 작성했던 이전
버전은 경로·프로토콜이 다 바뀌어서 이 문서로 대체한다.

---

## 1. 지금 상태 (요약)

- 대시보드 리포: `jetson-multicam-re_id-tracking` 의 `reid-admin-web` 브랜치
  (`web/` 디렉토리). jetson 쪽 코드(`src/`, `configs/` 등)는 절대 안 건드림.
- 지금은 Django가 **Jetson A보드(10.10.20.56)의 MQTT를 직접 구독**해서
  자체 SQLite에 저장하는 임시 구성으로 돌아간다 (`mqtt_worker.py` +
  `tracking/mqtt_ingest.py`, 토픽 `cctv/entry`, 필드 `global_person_id` 기준 —
  **이건 구 프로토콜이고, B의 메인 서버 브랜치(`release/multinode-baseline`)를
  보니 이미 다른 스키마로 바뀌어 있었다.** 아래 3번 참고.
- 결론(8/6에 이미 정함, 유지): **central_tracking.db를 Django가 직접 열지
  않는다.** SQLite는 네트워크 파일 공유가 없으면 원격 PC에서 못 열고, 연다 해도
  `journey_sqlite_server.py`의 동시 쓰기와 부딪힐 위험이 있다. **B가 API를
  열고 Django가 그걸 호출하는 방식**으로 간다.

---

## 2. 대시보드가 실제로 화면에서 쓰는 데이터 (기능별)

B가 API를 설계할 때 "이 화면 기능에 이 데이터가 필요하다"를 바로 알 수 있게
기능 단위로 정리했다. (`web/tracking/templates/tracking/dashboard.html`,
`web/tracking/views.py` 의 `api_state()` 기준)

| 화면 기능 | 필요한 데이터 |
|---|---|
| 카메라 월 A/B/C/D 영상 | (API 무관) 각 Jetson 보드가 직접 서빙하는 MJPEG URL — `Camera.jetson_host`/`jetson_port`로 Django admin에서 보드별로 이미 지정 가능하게 해둠 |
| 카메라 테두리 색(확인중/등록됨/미등록) + 알림음 | 카메라별 **최근 진입 이벤트**와, 그 사람이 **등록(확인)된 인물인지 아닌지** 플래그 — 이게 제일 중요한 실시간 데이터 |
| 상단 "CAM X 미등록 인물 감지" 문구 | 위와 동일한 이벤트에서 파생 |
| "감지 상태" 배지(빨강/검정) | 메인 서버·MQTT 연결이 살아있는지 여부 (헬스체크성) |
| 감지 인물 패널(카드 목록) | 인물별: id, 이름(있으면), 등록 여부, 대표 썸네일, **어느 카메라들에서 잡혔는지**, 마지막 감지 시각 |
| 감지 인물 패널 상단 "카메라별 인식 횟수" | 카메라별로 오늘 몇 번 진입 이벤트가 있었는지 카운트 |
| 이벤트 기록(로그) | 시각, 인물, 카메라, 종류(진입 등) — 최근 N건 |
| 인원 통계(7일/14일/30일) | 기간별 등록/미등록/총 인원 수 |
| **(신규 요청) journey_id-person_id 매칭 횟수** | 아래 4번 참고 — 아직 스키마 확인이 안 돼서 구체적인 필드를 못 정했다 |

지금 `/api/state/` 가 실제로 내려주는 모양(로컬 구현 기준, 참고용):

```json
{
  "mqtt_connected": true,
  "detection_enabled": true,
  "jetson_entries_total": 0,
  "totals": { "people": 0, "cameras": 4 },
  "gallery": [
    { "id": 1, "label": "", "named": false, "confirmed": false,
      "thumb": null, "cams": ["Camera A · 입장"], "last_seen": "15:24:10" }
  ],
  "camera_counts": {
    "900": { "name": "Camera A · 입장", "count": 3 }
  },
  "events": [
    { "at": "15:24:10", "person": "미확인 #1", "confirmed": false,
      "cam": "Camera A · 입장", "kind": "진입" }
  ]
}
```

---

## 3. 새로 확인한 프로토콜 (release/multinode-baseline 브랜치 + B 확인)

`git fetch` 로 이 브랜치를 봤을 때 우리가 지금 파싱하는 구 프로토콜과
완전히 다른 걸 발견했고, **B에게 실제 A 진입(ENTRY) 페이로드를 받아서
아래 내용을 확정**했다.

- **브로커 주소**: `configs/mqtt.example.yaml` 기본값이 `10.10.20.33` (중앙
  브로커). 실제로 그 IP의 1883 포트는 열려있는 것 확인함(연결은 됨, 다만
  20초 구독해봐도 실시간 트래픽은 없었음 — 노드가 꺼져있거나 아무도 안
  지나간 상태였을 뿐).
- **토픽이 노드/단계별로 분리됨** (코드 기준, B 확인 필요):
  - `cctv/events/a/entry` — A 입장
  - `cctv/responses/a/entry` — A가 구독하는 응답 토픽 (중앙 서버가
    `request_id`에 대응하는 식별자를 돌려주는 곳으로 추정)
  - `cctv/events/b/passage` — B 재식별 통과
  - `cctv/events/d/arrival` — D 도착
  - `cctv/candidates/b`, `cctv/candidates/d` — 노드 간 후보 전달용으로 추정

### A 진입(ENTRY) 페이로드 — B에게 실제로 받아서 확정함

```json
{
    "request_id": "...",
    "timestamp": "...",
    "node_id": "A",
    "event": "ENTRY",
    "local_track_id": 15,
    "next_nodes": ["B", "C"],

    "reid_model": "osnet_x0_25",
    "embedding_dim": 512,
    "embedding": [ /* 512-d, 몸통 Re-ID */ ],
    "quality": 0.91,
    "capture_path": "...",

    "face_available": true,
    "face_detector_model": "yunet_2023mar",
    "face_reid_model": "sface_2021dec",
    "face_embedding_dim": "...",
    "face_embeddings": [ [ /* ... */ ], [ /* ... */ ], [ /* ... */ ] ],
    "face_qualities": ["..."],
    "face_confidences": ["..."],
    "face_frontal_scores": ["..."],
    "face_sharpness": ["..."],
    "face_capture_paths": ["..."]
}
```

- 예상 못 했던 부분: **얼굴 인식 데이터가 통째로 붙어 있다** — 몸통
  Re-ID(`embedding`) 하나만 오던 구 프로토콜과 다르게, 얼굴 임베딩을
  여러 장(`face_embeddings`) 품질 지표(`face_qualities` 등)와 함께
  같이 보낸다.
- **여전히 `global_person_id`/`journey_id`/`person_uid` 가 없다** — A
  혼자만으로는 신원이 없다는 게 다시 확인됐다. 즉 4번(journey_id-person_id
  매칭)에 필요한 식별자는 이 이벤트가 아니라 **B(재식별)/D(도착) 페이로드나
  중앙 서버 응답 쪽에서 나올 것** — 아직 그쪽 실제 페이로드는 못 받았다.

---

## 4. 신규 요청: journey_id ↔ person_id 매칭 횟수

지금 대시보드에 "이 사람이 실제로 몇 번 매칭됐는지"를 보여주고 싶다는
요청이 있었다. 근데 예전에 확인했던 `central_tracking.db` 스키마
(`feature/journey-sqlite-e2e` 브랜치의 `journey_repository.py`)에는
`journeys` 테이블에 `journey_id`만 있고 **`person_id`/`person_uid` 컬럼이
없었다** — 3번 항목에서 본 것처럼 `person_uid`는 최근 프로토콜(passage
페이로드)에만 등장한다.

**B에게 확인 요청**: 지금 실제로 쓰는 DB(또는 서버 내부 상태)에
- `journey_id` 하나에 `person_uid`(또는 동등한 "등록된 사람" 식별자)가
  몇 번 매칭됐는지 셀 수 있는 테이블/필드가 있는지
- 있다면 그 테이블 이름과 컬럼 구성

이게 확인되면 아래 API에 엔드포인트 하나 추가해서 받으면 된다.

---

## 5. 제안하는 API 스펙 (개정판)

전제: GET 전용, 사내망이라 인증 없음(필요하면 조정). 아래는 **대시보드가
실제로 쓰는 화면 단위**로 다시 짠 제안이다 — B가 그대로 안 가도 되고,
같이 조정하면 된다.

### `GET /api/persons`
감지 인물 패널용. A/B/D 체크포인트를 이미 하나의 인물로 합쳐서 내려주면
Django 쪽에서 다시 합칠 필요가 없다.

```json
{
  "persons": [
    {
      "person_id": "P000001",
      "journey_ids": ["J000001", "J000045"],
      "label": null,
      "confirmed": true,
      "first_seen": "2026-08-10T10:00:00+09:00",
      "last_seen": "2026-08-10T10:03:12+09:00",
      "checkpoints": ["A", "B"],
      "match_count": 2,
      "thumb_url": null
    }
  ]
}
```

- `person_id`: 등록된(반복 방문 가능한) 사람의 영구 식별자 — `journey_id`는
  방문(여정) 1회 단위라 사람 단위 식별자와는 다를 걸로 추정. **B 쪽 실제
  명칭 확인 필요.**
- `confirmed`: 등록(허가)된 인물인지 — 지금 대시보드의 초록/빨강 판정 기준
- `match_count`: 4번 항목의 "journey_id-person_id 매칭 횟수"

### `GET /api/events?since=<ISO timestamp>`
이벤트 로그 + 카메라 테두리 색/알림음 트리거용. `since` 이후 것만 요청해서
매번 전체를 안 받는다.

```json
{
  "events": [
    {"at": "2026-08-10T10:03:12+09:00", "node": "B", "kind": "passage",
     "journey_id": "J000045", "person_id": "P000001", "confirmed": true}
  ]
}
```

- `kind`: `entry`(A) / `passage`(B) / `arrival`(D) 정도로 제안
- `confirmed`가 여기 있어야 카메라별로 등록/미등록 알림을 실시간으로
  띄울 수 있다 — **제일 중요한 필드.**

### `GET /api/stats?days=7|14|30`
인원 통계 패널용.

```json
{"registered": 12, "unregistered": 3, "total": 15}
```

### `GET /api/status`
헬스체크 + "감지 상태" 배지용.

```json
{"broker_connected": true, "persons_total": 42, "last_event_at": "2026-08-10T10:03:12+09:00"}
```

---

## 6. B에게 확인 요청 (정리)

1. ~~A 진입 페이로드~~ → **확인 완료** (3번). **B(재식별)/D(도착) 페이로드도
   같은 식으로 실제 예시를 받고 싶다** — `journey_id`/`person_uid`가 정확히
   어느 이벤트에서 처음 생기는지 이걸 봐야 확정된다.
2. `journey_id`와 별개로 "등록된 사람" 단위 식별자(`person_id` 같은 것)가
   있는지, 있다면 그 테이블/컬럼 이름 (4번)
3. 위 API 4개(5번) 그대로 가도 되는지 / 바꾸고 싶은 부분
4. API 서버 기술 스택 (Flask/FastAPI/Django 등) — Django 클라이언트 코드
   작성에 영향은 없지만 참고용
5. 테스트용으로 노드 하나만이라도 잠깐 켜서 이벤트를 흘려줄 수 있는지 —
   지금 10.10.20.33 브로커를 직접 구독해봤는데 실시간 트래픽이 전혀 없어서
   실제 페이로드 필드를 못 봤다

이 답이 오면 바로 `mqtt_worker.py`/`mqtt_ingest.py` 자리를 이 API를 호출하는
클라이언트 모듈로 교체하는 작업 시작하겠음.
