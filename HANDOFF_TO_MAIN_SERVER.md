# reid-admin-web ↔ 메인 서버(B) API 연동

작성 시점: 2026-08-11. 8/10에 C가 B에게 요청한 내용에 B가 답변을 줘서
구조가 확정됐다. 이 문서는 그 확정 내용 + C(Django) 쪽 구현 현황을
반영해서 갱신한 버전이다.

**2026-08-11 추가 갱신**: B가 "웹 연동 기준"을 다시 명확히 정리해서
줬다 — 최종 Re-ID/Journey/NEW·REVISIT 판정은 전부 메인 서버가 처리하고,
웹은 Jetson 로컬 트랙 ID가 아니라 `person_uid`/`journey_id` 기준으로만
사람을 관리해야 한다는 것, 그리고 `temporary_person_uid`(Final Review
확정 전 임시값)를 절대 최종 Person ID로 보여주면 안 되고 확정 후
`canonical_person_uid`만 최종 ID로 써야 한다는 것. 이 내용을 반영해서
Django 쪽에 `Journey` 모델 + "이동 목록"/"사람 상세" 조회 전용 화면을
오늘 바로 구현했다 — §2-1, §4-2, §5 참고.

---

## 1. 확정된 구조

```
Jetson A/B/C/D
      │ MQTT
      ▼
메인 서버 (Windows, B 담당, 10.10.20.33)
      │ main_server.db 에 적재
      ▼
REST API (B 가 오늘 구현, :8080)
      │ GET, 인증 없음 (같은 교육장 LAN)
      ▼
Django 대시보드 (C 담당, 10.10.20.26)
```

- Django 는 **더 이상 MQTT 를 전혀 구독하지 않는다.** Jetson 이든 중앙
  브로커(10.10.20.33:1883)든 직접 안 붙는다 — 전부 메인 서버가 처리하고,
  Django 는 메인 서버의 REST API 만 호출한다.
- `central_tracking.db`(SQLite) 를 Django가 직접 여는 방식은 쓰지
  않는다(8/6에 정한 것 유지) — SQLite 동시쓰기 문제, 네트워크 파일공유
  필요 등의 이유.
- 영상은 그대로: 브라우저가 각 Jetson 보드의 MJPEG URL 에 직접 접근
  (`Camera.jetson_host`/`jetson_port`, Django admin 에서 카메라마다 지정).
  API 나 메인 서버를 거치지 않는다.

---

## 2. 사람 식별자 — person_uid vs journey_id (확정)

- **`person_uid`** = 사람 1명의 **영구 식별자** (예: `P000002`). 반복
  방문해도 이 값은 안 바뀐다. Django 쪽에서 인물을 묶는 키는 **반드시
  이걸 써야 한다** (`Person.external_id` 에 저장).
- **`journey_id`** = 방문 **1회짜리 세션** (예: `J000002`, `J000004`,
  `J000010` — 같은 `person_uid` 가 여러 개 가질 수 있음).
- **`visit_count`**: 메인 서버가 관리하는 방문 횟수. `Person.visit_count`
  로 그대로 받아 저장한다.
- **Identity/Journey 의 source of truth 는 메인 서버다.** Camera A 는
  스스로 신원을 정하지 않는다 — A 가 `request_id`+임베딩을 보내면,
  메인 서버가 Re-ID/DB 조회 후 `person_uid`/`journey_id` 를 배정하고
  A에게 `cctv/responses/a/entry` 로 돌려준다.
- **알려진 버그(B 인지, 수정 예정)**: 지금 B(재식별) 로그에
  `"global_person_id": "J000002"` 처럼 journey_id 가 들어가는 버그가
  있음 — Django 는 이 필드를 신뢰하면 안 되고, `person_uid` 필드만
  기준으로 써야 한다. B가 `global_person_id = person_uid` 로 고칠 예정.

**"등록(확인)된 인물"인지 여부(`Person.confirmed`)는 메인 서버 데이터가
아니라 이 대시보드에서만 관리하는 로컬 판단이다** — 새 `person_uid` 는
항상 미등록(`confirmed=False`)으로 시작하고, 관리자가 Django admin 에서
직접 확인 체크한다. 카메라 테두리 색(초록/빨강)·알림음은 이 로컬 값
기준.

---

## 2-1. Final Identity Review — temporary vs canonical (B 확정, 2026-08-11)

한 여정(journey)의 신원이 확정되기까지 메인 서버 내부에서 여러 단계의
후보값을 거친다. **웹은 이 중 `canonical_person_uid` 만 "이 사람이
누구다"라는 최종 결론으로 취급한다** — 나머지는 전부 참고/진행상황용:

| 필드 | 의미 | 웹에서 최종 ID로 써도 되나 |
|---|---|---|
| `temporary_person_uid` | Re-ID 진행 중 임시로 붙는 값 | ❌ 안 됨 |
| `candidate_person_uid` | 노드 하나가 제안한 후보 | ❌ 안 됨 |
| `final_candidate_person_uid` | 여러 노드 종합한 최종 후보 | ❌ 안 됨 |
| `canonical_person_uid` | Final Review 확정 결과 | ✅ 이것만 |

`final_review_result` 는 셋 중 하나:
- **`NEW`** — 신규 인물로 확정. `canonical_person_uid` 가 새 `person_uid`.
- **`REVISIT`** — 기존 인물 재방문으로 확정. `canonical_person_uid` 가
  기존 `person_uid`(예: 실제 사례에서 `temporary_person_uid=P000072` 로
  들어왔다가 `canonical_person_uid=P000006` 으로 확정됨 — 이 경우 웹은
  P000072 가 아니라 P000006 을 그 사람의 ID로 보여준다).
- **`MANUAL_REVIEW_REQUIRED`** — 아직 확정 안 됨. `canonical_person_uid`
  가 비어있다 — 이 상태인 동안 웹은 이 여정에 어떤 `Person` 도 연결하지
  않고 "검토 필요" 목록에 별도로 보여주기만 한다(§5 참고, Django
  `Journey.person` 이 계속 null).

이 판단(신규/재방문/검토필요) 로직은 전부 메인 서버 담당이고, 웹은
그 결과값만 그대로 표시한다 — Django 쪽에 별도 Re-ID 로직 없음.

**Local Track ID 는 Person ID 가 아니다** (B 명시): 예를 들어
`D_local_track_id=13` 과 `person_uid=P000006` 은 완전히 다른 값이고,
Local Track ID 는 노드 하나의 카메라 안에서만 의미 있는 번호다. 웹의
사람 식별 키는 **항상 `person_uid`(정확히는 확정된 경우
`canonical_person_uid`)**.

---

## 3. MQTT 토픽 (메인 서버 내부용 — Django 는 이제 이걸 안 본다, 참고용)

- `cctv/events/a/entry` — A 입장
- `cctv/responses/a/entry` — 메인 서버 → A (person_uid/journey_id 배정 응답)
- `cctv/events/b/passage` — B 재식별 통과
- `cctv/candidates/b` — 메인 서버 → B (후보 전달)
- `cctv/events/d/arrival` — D 도착
- `cctv/candidates/d` — 메인 서버 → D (후보 전달)
- `cctv/main/journey/completed` — 여정 완료

B PASSAGE 실제 페이로드(참고, Django 가 직접 받진 않음):

```json
{
  "schema_version": 1,
  "event": "PASSAGE",
  "journey_id": "J000002",
  "person_uid": "P000002",
  "current_node": "B",
  "route": ["A", "B"],
  "next_nodes": ["D"],
  "entry_timestamp": "2026-08-10T15:42:45+09:00",
  "b_passage_timestamp": "2026-08-10T15:42:50+09:00",
  "b_local_track_id": 3,
  "gallery_count": 3,
  "gallery": "[512-d embeddings...]",
  "similarity": 0.770680,
  "verification_status": "AUTO_MATCHED"
}
```

D ARRIVAL 실제 페이로드(정상, 참고용):

```json
{
  "event": "ARRIVAL",
  "journey_id": "J000002",
  "person_uid": "P000002",
  "global_person_id": "P000002",
  "node_id": "D",
  "current_node": "D",
  "route": ["A", "B", "D"],
  "entry_timestamp": "2026-08-10T15:42:45+09:00",
  "passage_timestamp": "2026-08-10T15:42:50+09:00",
  "d_arrival_timestamp": "2026-08-10T15:42:53+09:00",
  "total_duration_seconds": 8.0,
  "d_local_track_id": 2,
  "best_similarity": 0.760702,
  "combined_score": 0.757277
}
```

---

## 4. API 스펙 (B 확정) — Django 는 이 4개를 오늘 붙인다

Base URL: `http://10.10.20.33:8080` · GET 전용 · 인증 없음(교육장 LAN MVP,
외부망 배포 시 추가 예정)

### `GET /api/status`
```json
{"server": "ok", "mqtt_connected": true, "last_event_at": "2026-08-11T09:30:21+09:00"}
```

### `GET /api/persons`
```json
{
  "persons": [
    {
      "person_uid": "P000002",
      "visit_count": 2,
      "first_seen": "2026-08-10T15:42:45+09:00",
      "last_seen": "2026-08-10T15:43:33+09:00",
      "status": "COMPLETED"
    }
  ]
}
```

### `GET /api/events?since=<ISO timestamp>`
```json
{
  "events": [
    {"at": "2026-08-10T15:42:50+09:00", "person_uid": "P000002",
     "journey_id": "J000002", "node": "B", "kind": "PASSAGE"},
    {"at": "2026-08-10T15:42:53+09:00", "person_uid": "P000002",
     "journey_id": "J000002", "node": "D", "kind": "ARRIVAL"}
  ]
}
```
`kind`: `ENTRY`/`PASSAGE`/`ARRIVAL` (대문자). 나중에 "의심맨" 기능이
붙으면 `"kind": "SUSPICIOUS"`, `"identity_status": "KNOWN"` 같은 형태로
같은 엔드포인트에 섞여 올 예정 — Django 쪽 파서가 모르는 kind 는 무시
하도록 짜두면 된다.

### `GET /api/stats`
```json
{"persons_total": 4, "visits_total": 7, "active_journeys": 1,
 "completed_journeys": 6, "suspicious_total": 0}
```

### `GET /api/journeys?limit=<n>` — ★ 신규 요청 (§2-1 화면용, 확인 필요)

"현재/최근 이동 목록"·"사람 상세"·"검토 필요" 세 화면 전부 이 엔드포인트
하나로 채우도록 설계했다. `since` 커서 없이 매번 최근 N건을 통째로
다시 준다 — 검토 대기(`MANUAL_REVIEW_REQUIRED`) 상태였던 여정이 나중에
`REVISIT`/`NEW` 로 바뀌는 걸 놓치지 않으려면(하나의 `journey_id` 가
시간이 지나 값이 바뀌는 케이스), since 로 앞부분을 건너뛰는 방식은
위험하다고 판단했다. Django 쪽은 `journey_id` 를 유니크 키로
`update_or_create` 해서 재수신을 그냥 덮어쓴다.

```json
{
  "journeys": [
    {
      "journey_id": "J000104",
      "temporary_person_uid": "P000072",
      "initial_decision": "IDENTITY_PENDING",
      "candidate_person_uid": "P000006",
      "final_candidate_person_uid": "P000006",
      "canonical_person_uid": "P000006",
      "final_review_result": "REVISIT",
      "final_scores": 0.850,
      "person_status": "COMPLETED",
      "route": "A -> C -> D",
      "entry_at": "2026-08-11T10:00:00+09:00",
      "d_exit_at": "2026-08-11T10:00:18+09:00",
      "journey_elapsed_seconds": 18.141,
      "visit_count": 12
    }
  ]
}
```

- `MANUAL_REVIEW_REQUIRED` 인 동안은 `canonical_person_uid` 를 빈 문자열
  또는 필드 자체를 생략해서 보내면 된다 — Django 는 이 값이 없으면
  `Person` 을 아예 만들지 않는다(§2-1).
- `final_scores` 는 예시처럼 숫자 하나든, 후보별 점수 dict 든 상관없다
  (Django 는 그대로 JSON 으로 저장만 하고 그대로 화면에 보여준다).
- `limit` 기본값 100 정도로 가정하고 폴링 중 — 다르게 쓰고 싶으면 알려
  주면 맞춘다.
- 위 필드명은 B가 보낸 "웹 연동 기준" 메시지의 필드명을 그대로 썼다.
  실제 API가 이 이름과 다르게 나가면(예: `d_exit_at` 대신 `exit_at`)
  꼭 알려달라 — Django 쪽 `tracking/main_api_ingest.py::ingest_journey()`
  하나만 고치면 되니 빠르게 맞출 수 있다.

---

## 5. Django(C) 쪽 구현 현황 — 2026-08-11 작업분

**완료:**

| 파일 | 내용 |
|---|---|
| `tracking/models.py` | `RuntimeConfig.main_server_host`/`main_server_port`(기본 `10.10.20.33`/`8080`, admin에서 편집 가능) 추가. `Person.visit_count` 추가. `Person.external_id` 는 이제 `person_uid` 저장용으로 의미 변경(스키마는 그대로) |
| `tracking/main_api_ingest.py` (신규) | `/api/events` 이벤트 1건 → `Person`/`Tracklet`/`Event` 매핑. `ENTRY`/`PASSAGE`/`ARRIVAL` 전부 우리 쪽 "진입" 이벤트로 취급(카메라별 알림 트리거용). `/api/persons` → `visit_count` 동기화 |
| `main_server_worker.py` (신규, `mqtt_worker.py` 대체) | 1초마다 `/api/events?since=` 폴링, 30틱마다 `/api/persons` 동기화. `RuntimeConfig.detection_enabled` 끄면 폴링 자체를 쉼("감지 on/off" 스위치가 이제 이 폴링을 제어). 메인 서버 API 가 아직 없어도 안 죽고 계속 재시도(연결 실패를 정상 상태로 처리) |
| `tracking/views.py` | `/api/state/` 응답 필드명 `mqtt_connected`→`main_connected`, `jetson_entries_total`→`main_events_total` 로 변경 |
| `tracking/templates/tracking/dashboard.html` | 위 필드명 변경 반영, "감지 상태" 배지가 이제 메인 서버 API 연결 여부를 표시 |
| `tracking/models.py` (신규) | `Journey` 모델 추가 — §2-1 의 Final Identity Review 필드(`temporary_person_uid`/`candidate_person_uid`/`final_candidate_person_uid`/`canonical_person_uid`/`final_review_result`/`final_scores`) + 여정 정보(`route`/`entry_at`/`d_exit_at`/`journey_elapsed_seconds`/`visit_count`/`person_status`) 전부 저장. `MANUAL_REVIEW_REQUIRED` 인 동안은 `Journey.person` 이 null |
| `tracking/main_api_ingest.py` | `ingest_journey()` 추가 — §4-2 스펙의 응답 1건을 `Journey`(+확정되면 `Person`)로 적재 |
| `main_server_worker.py` | `/api/journeys?limit=` 폴링 추가(§4-2). 아직 없는 엔드포인트라 실패해도 기존 `/api/events` 연결 상태(`main_connected`)에는 영향 안 주게 따로 감쌈 |
| `tracking/views.py`, `urls.py` | "현재/최근 이동 목록"(`/journeys/`) · "사람 상세"(`/persons/<person_uid>/`) 조회 전용 화면 추가. 둘 다 로그인 필요 |
| `tracking/templates/tracking/journeys.html`, `person_detail.html` (신규) | 위 화면 — 검토 필요(`MANUAL_REVIEW_REQUIRED`) 목록은 상단에 별도 강조, 확정된 여정만 canonical `person_uid` 로 인물 상세 링크가 걸림(`temporary_person_uid` 는 링크 안 걸림) |
| `tracking/admin.py` | `Journey` admin 등록 — 전부 읽기 전용(add/delete 불가, 필드 전부 readonly). MERGE_EXISTING/CONFIRM_NEW 액션 버튼은 다음 단계(§4-2, 아직 REST API 미확정) |

**확인함**: `main_server_worker.py`를 실제로 띄워서 `10.10.20.33:8080`이
아직 응답 없는 상태에서 `main_connected: false` 로 정상 폴백되는 것,
크래시 안 하는 것 확인 완료. `ingest_journey()` 도 이 문서의 Live/Manual
Review 예시 두 건을 그대로 넣어서 REVISIT 은 canonical `P000006` 으로
정상 연결, MANUAL_REVIEW_REQUIRED 는 `Person` 미생성으로 정상 동작하는
것까지 로컬에서 확인 완료(테스트 데이터는 확인 후 삭제함).

**대기 중**: B 가 `/api/status` 를 `0.0.0.0:8080` 으로 띄우면 —
`http://10.10.20.33:8080/api/status` 가 이 PC(10.10.20.26)에서 열리는지
확인 후 바로 실제 데이터로 테스트 시작.

**대기 중 (2)**: `/api/journeys` 는 아직 B가 안 만들었을 수 있어서
§4-2 스펙을 "제안"으로 보낸다 — 필드명/모양 확인해서 알려주면 그대로
붙는다. 지금은 이 엔드포인트가 없어도(404/타임아웃) `main_server_worker.py`
가 죽지 않고 계속 재시도만 한다.

**아직 안 한 것 (필요시 추가 논의)**:
- `/api/stats` 소비 — 지금은 로컬 DB(메인 서버 이벤트로 채워진 Person/Event)
  기준으로 자체 계산 중이라 당장 안 급함
- Manual Review 화면의 실제 액션(`MERGE_EXISTING`/`CONFIRM_NEW`) — B 계획대로
  조회 전용 먼저 붙였고, 이 버튼용 REST API(POST 엔드포인트) 는 다음 단계에서 논의
- `SUSPICIOUS` kind 처리 — 그 기능 붙을 때 같이

---

## 6. 우리(Django 대시보드) 쪽 접속 정보

- **이 PC의 사내망 IP**: `10.10.20.26`
- **대시보드 서버 포트**: `8000` (daphne)
- **요청 주기**: 1초 폴링 (부하 크면 `since` 로 조정 가능)
- **인증**: 지금 없음, 필요하면 헤더로 맞춤

---

## 7. 남은 질문 (급하지 않음)

1. `/api/persons`의 `status` 필드가 가질 수 있는 값 전부 (`COMPLETED` 외에
   뭐가 더 있는지 — 카메라 테두리 상태 표시에 참고하면 좋음)
2. API 서버 재시작/배포 시 Django 쪽에 미리 알려줄 방법이 있으면 좋음
   (없어도 폴링이 알아서 재연결하긴 함)

---

## 8. 신규 요청 (2026-08-14): 실시간 감지 이벤트 API — §4의 `/api/events` 부활 필요

**해결됨(2026-08-14 오후)**: B가 임시 포트 8081에서 `/api/events?since=`
를 살려줬다 — `event_id`/`at`/`node`/`kind`/`person_uid`/
`canonical_person_uid`/`identity_status` 필드로 확인, `since`/
`next_since` 커서 방식. Django `main_server_worker.py`/`main_api_ingest.py`
를 이 스트림 기준으로 다시 짜서 연동 완료, J000061/J000062 실제
이벤트로 검증까지 끝냈다. 아래는 이 작업 중 새로 발견한 **다음 문제**
(§9)다.

**배경**: 대시보드에 카메라별 알림음(A="등록완료"/"등록실패", B·C·D="미등록자
감지")을 붙였는데, C가 실제로 카메라 앞을 왔다갔다하며 테스트해보니 —
카메라 화면(Jetson 자체 오버레이)에는 "ANOMALY: STRANGER" 박스가 그 순간
바로 뜨는데, 대시보드 소리는 그보다 한참 늦게 울리거나 아예 안 울리는
경우가 있었다. 원인을 계속 추적해보니 구조적인 문제였다:

- 지금 Django 는 `/api/journeys` (§4의 `journeys` 엔드포인트)만 폴링해서
  소리를 트리거한다 — 이건 **"이 사람이 카메라 구역을 통과 완료했다"는
  다 처리된 결과**만 준다(`nodes`/`captures` 배열). 화면의 STRANGER 박스가
  뜨는 순간(=1차 탐지)과, 그게 `journeys` 응답에 실제로 반영되는 순간
  사이에 시간차가 있고, 카메라에 따라 그 시간차가 꽤 크다.
- 실측해보니 카메라 C 감지가 `route`/`nodes` 에는 전혀 안 잡히고
  `captures` 배열에만 남아있는 경우가 있어서(Django 쪽에서 이것도 훑어서
  보정함, 2026-08-14), 최종적으로는 잡히긴 하는데 그마저도 폴링 주기(1초)
  + Journey 재조회 타이밍에 따라 딜레이가 생긴다.
- 카메라 B는 이번 세션 내내(오늘+어제 전체) `/api/journeys` 응답에
  **단 한 번도** 통과 기록이 안 잡혔다 — `nodes`에도 `captures`에도 전혀
  없음. B 카메라나 그쪽 MQTT 연동 자체를 한번 확인해줬으면 한다.

**요청**: §4에 이미 B가 확정했던 `GET /api/events?since=<ISO timestamp>`
가 사실 지금 딱 필요한 모양이었다(`kind: ENTRY/PASSAGE/ARRIVAL`, 나중에
`SUSPICIOUS` 도 추가 예정이라고 그때 이미 적어뒀던 것 — §4 참고). 근데
실제로는 이 엔드포인트가 한 번도 뜬 적이 없어서(계속 404) Django 가
`/api/journeys` 로 대체해서 쓰고 있었다. 이제 와서 보니 소리 알림 같은
"즉시성"이 중요한 기능은 `journeys`(가공된 결과)가 아니라 원래 계획했던
`events`(원시 감지 신호) 쪽이 맞는 방향이었다 — 이 엔드포인트를 다시
살려줄 수 있는지 확인 부탁한다.

- 최소로 필요한 정보: `at`(시각), `node`(카메라, A/B/C/D), `person_uid`
  또는 최소한 "이 사람이 확실히 식별됐는지 아닌지"를 구분할 수 있는 값
  하나. §4 예시의 `kind`(ENTRY/PASSAGE/ARRIVAL/SUSPICIOUS)면 충분하고,
  화면의 STRANGER 박스가 뜨는 시점과 최대한 가깝게(=Re-ID 확정 전이라도)
  쏴주면 된다 — Django 는 "이게 확정 신원인지"는 이미 `journeys` 로 따로
  받고 있으니, 여기 `events` 는 "지금 이 카메라에 뭔가 감지됐다"는 신호만
  최대한 빨리 주면 된다.
- 필드명/모양은 §4 예시 그대로도 되고, 다르게 나가도 상관없다 — Django
  쪽 `main_server_worker.py`/`main_api_ingest.py` 하나만 다시 손보면 되니
  확정되면 바로 맞춘다.

---

## 9. 신규 요청 (2026-08-14): 카메라 A를 안 거치고 B/C/D에 단독으로 나타난
   사람도 감지되게 해달라

**배경**: `/api/events` 연동 직후 C가 카메라 D 앞에서 직접 테스트했다 —
카메라 화면(Jetson 오버레이)엔 STRANGER 박스가 바로 떴는데, `/api/journeys`
도 `/api/events`도 그 시점 전후로 **거의 1시간 동안 아무 것도 안 받았다**
(가장 최근 journey가 1시간 전 것 그대로). 확인해보니 이 테스트는 카메라
A를 먼저 안 거치고 D 앞에만 선 경우였다.

**추정 원인**: 지금까지 실측한 걸로는 이 시스템의 "방문(journey)"이 전부
카메라 A(입장)에서 시작되고, B(재식별)/C/D(도착)는 그 안에서 재확인되는
구조다(카메라 이름 자체가 그 역할을 말해준다). A를 거치지 않고 B/C/D 에
단독으로 나타나면 애초에 `journey_id`/`person_uid`가 배정될 계기가 없어서,
Jetson B/C/D 노드가 그 사람을 화면에는 표시해도(STRANGER 박스) Main
쪽으로는 아예 아무 것도 안 보내는 것으로 보인다.

**요청**: A를 거치지 않고 B/C/D에 갑자기 등장한 사람도 최소한 "미등록자
감지됐다"는 신호는 `/api/events`로 받을 수 있게 해달라 — 신원(canonical
_person_uid)까지는 없어도 된다(어차피 이 경우는 미등록자로 처리할 거라
신원이 필요 없다). 최소로 필요한 건 `at`(시각)/`node`(카메라)뿐이고,
`journey_id`/`person_uid`/`canonical_person_uid`는 없어도(null이어도)
괜찮다 — Django 쪽은 이미 `journey_id`가 없어도 죽지 않게(§main_api_ingest
.ingest_event_item, `Journey.objects.filter(journey_id=None)` 는 그냥
매칭 없음으로 처리) 짜여 있다. 다만 지금 코드는 `canonical_person_uid`가
없으면 이벤트 자체를 버리게 돼 있어서, 이 요청이 실제로 오면 Django 쪽도
"신원 없이 카메라만 알려주는 이벤트"를 받아들이도록 추가로 고쳐야 한다 —
B가 이런 신호를 줄 수 있는지부터 확인 부탁한다. 가능하다면 `kind`에
`"STRANGER"`나 `"UNLINKED"` 같은 값을 새로 얹어서 구분해주면 좋겠다(§4
에서 B가 예전에 언급했던 `SUSPICIOUS` kind와 같은 맥락).
