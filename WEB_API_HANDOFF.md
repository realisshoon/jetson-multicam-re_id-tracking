# CCTV Main READ-ONLY REST API 인수인계

## Base URL

팀원 LAN에서 사용하는 Base URL:

```text
http://10.10.20.33:8080
```

Main SQLite DB가 Source of Truth이며 이 API는 GET 전용이다. Django/Web은
`data/main_server.db`를 직접 열지 않는다.

## Endpoint

| Method | Path | 용도 |
|---|---|---|
| GET | `/api/health` | API 및 DB 연결 상태 |
| GET | `/api/dashboard/summary` | Dashboard 집계 |
| GET | `/api/journeys` | 최근 Journey 목록 |
| GET | `/api/journeys/{journey_id}` | Journey 상세, Identity, Timing, Node, Capture metadata |
| GET | `/api/events?since=<ISO-8601>` | ENTRY/PASSAGE/ARRIVAL 증분 이벤트 |
| GET | `/api/persons` | canonical Person 중심 목록 |
| GET | `/api/persons/{person_uid}` | Person 및 최근 Journey |
| GET | `/api/reviews` | Review 목록 |
| GET | `/api/reviews/{journey_id}` | Journey Review 상세 및 score metadata |

POST, PUT, PATCH, DELETE endpoint는 없다.

### Django 연동 시 주의

Django의 Journey 및 증분 이벤트 ingest URL은 다음과 같이 사용한다.

```text
목록: GET /api/journeys?limit=...
상세: GET /api/journeys/{journey_id}
Review: GET /api/reviews 또는 /api/reviews/{journey_id}
Events: GET /api/events?since=2026-08-14T13%3A47%3A00%2B09%3A00
```

`since`는 timezone offset이 포함된 ISO-8601 timestamp가 필수이며 결과는
`at`, `event_id` 오름차순이다. 다음 poll에서는 `next_since`를 사용한다.
Web 저장소는 `event_id`에 unique constraint를 두어 재시도도 idempotent하게
처리한다.

이 event feed는 Main이 수신해 중앙 Journey에 적재한 `ENTRY`, `PASSAGE`,
`ARRIVAL`만 제공한다. 현재 Jetson 로컬에서 STRANGER 박스가 처음 생기는 순간의
`DETECTION`/`SUSPICIOUS` 신호는 Main에 전달되지 않으므로 이 API나
`/api/journeys`로 확정 전 실시간 알림을 만들 수 없다. 그 기능은 별도의 MQTT
detection event 계약과 Main ingest가 선행되어야 한다.

## Query parameter

### `/api/journeys`

- `limit`: 기본 50, 1~200
- `offset`: 기본 0
- `status`: `WAITING_B_OR_C`, `WAITING_D`, `COMPLETED`, `EXPIRED`
- `person_uid`: `Pxxxxxx`
- `final_review_result`: `REVISIT`, `NEW`, `MANUAL_REVIEW_REQUIRED`

목록 항목에도 `route`, `nodes`, `captures`, `arrival_at`, `completed_at`,
`completion_duration_seconds`가 포함된다. route의 A/B/C/D 노드는 node visit
telemetry가 누락됐더라도 중앙 ENTRY/PASSAGE/ARRIVAL event와 capture를 근거로
일관되게 직렬화된다.

### `/api/events`

- `since`: 필수, timezone-aware ISO-8601, 해당 시각보다 이후 이벤트만 반환
- `limit`: 기본 50, 1~200
- `offset`: 기본 0
- 반환 kind: `ENTRY`, `PASSAGE`, `ARRIVAL`
- 반환 node: `A`, `B`, `C`, `D`

최소 event 예:

```json
{
  "event_id": 201,
  "at": "2026-08-14T13:47:48.172+09:00",
  "journey_id": "J000062",
  "node": "D",
  "kind": "ARRIVAL",
  "person_uid": "P000049",
  "canonical_person_uid": "P000049",
  "identity_status": "NEW"
}
```

`person_uid`는 호환 alias이며 canonical이 확정된 경우에만 동일 값을 갖는다.
`IDENTITY_PENDING` 또는 `MANUAL_REVIEW_REQUIRED`이면 두 UID 모두 `null`이다.
`temporary_person_uid`와 candidate UID는 event의 canonical ID로 노출하지 않는다.

### `/api/persons`

- `limit`: 기본 50, 1~200
- `offset`: 기본 0
- `include_merged`: 기본 `false`; `true`이면 merged alias 포함

### `/api/persons/{person_uid}`

- `journey_limit`: 기본 20, 1~100

### `/api/reviews`

- `limit`: 기본 50, 1~200
- `offset`: 기본 0
- `status`: `PENDING`, `RESOLVED`

## `/api/journeys` 실제 목록 응답 예

2026-08-11 Live DB의 `GET /api/journeys?limit=5` 응답 구조:

```json
{
  "items": [
    {
      "journey_id": "J000107",
      "person_uid": "P000006",
      "person_status": "RETURNING",
      "visit_count": 15,
      "journey_status": "EXPIRED",
      "route": ["A"],
      "entry_at": "2026-08-11T15:24:27+09:00",
      "d_exit_at": null,
      "journey_elapsed_seconds": null,
      "initial_decision": null,
      "final_review_result": null
    },
    {
      "journey_id": "J000104",
      "person_uid": "P000006",
      "person_status": "RETURNING",
      "visit_count": 15,
      "journey_status": "COMPLETED",
      "route": ["A", "C", "D"],
      "entry_at": "2026-08-11T15:21:43+09:00",
      "d_exit_at": "2026-08-11T15:22:01.141+09:00",
      "journey_elapsed_seconds": 18.141,
      "initial_decision": "IDENTITY_PENDING",
      "final_review_result": "REVISIT"
    },
    {
      "journey_id": "J000103",
      "person_uid": "P000071",
      "person_status": "REVIEW_REQUIRED",
      "visit_count": 1,
      "journey_status": "COMPLETED",
      "route": ["A", "C", "D"],
      "entry_at": "2026-08-11T15:20:27+09:00",
      "d_exit_at": "2026-08-11T15:20:40.880+09:00",
      "journey_elapsed_seconds": 13.88,
      "initial_decision": "IDENTITY_PENDING",
      "final_review_result": "MANUAL_REVIEW_REQUIRED"
    }
  ],
  "limit": 5,
  "offset": 0
}
```

목록의 `person_uid`는 확정된 canonical Person ID의 호환 alias다. REVISIT/확정
NEW에서는 canonical UID이며, `MANUAL_REVIEW_REQUIRED` 또는
`IDENTITY_PENDING`에서는 `null`이다. 후보와 tracking UID는 별도 필드로만
표시한다.

## 팀원 C 필드 mapping

| 팀원 C 필드 | 목록 API | 상세/Review API | Adapter 규칙 |
|---|---|---|---|
| `journey_id` | `journey_id` | `journey_id` | 직접 사용 |
| `person_uid` | `person_uid` | `person.person_uid` | 현재 표시용 ID |
| `temporary_person_uid` | 없음 | `identity.temporary_person_uid` | 상세 조회 |
| `candidate_person_uid` | 없음 | `identity.initial_candidate_person_uid` | 상세 조회; Review top-level에서는 `candidate_person_uid` |
| `final_candidate_person_uid` | 없음 | `identity.final_candidate_person_uid` | 상세 조회, canonical로 자동 확정 금지 |
| `canonical_person_uid` | `canonical_person_uid` | `identity.canonical_person_uid` | REVISIT/확정 NEW에서 최종 ID |
| `final_review_result` | `final_review_result` | `identity.final_result` | 이름이 다름 |
| `final_scores` | 없음 | `identity.final_scores` | 상세 또는 Review 상세 조회 |
| `route` | `route` | `route` | 직접 사용 |
| `entry_at` | `entry_at` | `entry_at` 또는 `timing.a_start` | Journey ENTRY LINE timestamp |
| `arrival_at` | `arrival_at` | `arrival_at` 또는 `timing.arrival_at` | D ARRIVAL 승인 시각 |
| `completed_at` | `completed_at` | `completed_at` 또는 `timing.completed_at` | Journey 완료 시각 |
| `completion_duration_seconds` | `completion_duration_seconds` | `timing.completion_duration_seconds` | ENTRY부터 완료까지 |
| `d_exit_at` | `d_exit_at` | `timing.d_exit` | D track exit 시각; 완료 시각과 다름 |
| `journey_elapsed_seconds` | `journey_elapsed_seconds` | `timing.elapsed_seconds` | 이름이 다름 |
| `visit_count` | `visit_count` | `person.visit_count` | canonical/current Person 기준 |
| `person_status` | `person_status` | `person_status` | Journey Identity 상태 |
| `journey_status` | `journey_status` | `journey_status` | 직접 사용 |

권장 Django adapter 흐름:

1. 목록/Dashboard는 `/api/journeys` 응답을 그대로 ingest한다.
2. Identity 상세가 필요한 Journey만 `/api/journeys/{journey_id}`를 조회한다.
3. Review 관리 화면은 `/api/reviews`와 `/api/reviews/{journey_id}`를 사용한다.
4. Django에서 `final_candidate_person_uid`를 canonical UID로 추론하지 않는다.

## Identity 규칙

### REVISIT

- 최종 표시 Person: `identity.canonical_person_uid`
- `identity.temporary_person_uid`는 증거/감사용으로 보존
- 목록의 `person_uid`도 canonical UID

### MANUAL_REVIEW_REQUIRED

- `identity.canonical_person_uid`는 `null`
- `identity.final_candidate_person_uid`는 후보일 뿐 확정 Person이 아님
- 최종 표시 Person은 목록 `person_uid` 또는 상세 `person.person_uid`
- UI에 Review 필요 상태를 표시

### NEW

- 확정된 `identity.canonical_person_uid` 또는 상세 `person.person_uid` 사용
- final review 전 `IDENTITY_PENDING`을 NEW로 선확정하지 않음

## J000103 실제 예: Manual Review

```json
{
  "journey_id": "J000103",
  "person": {
    "person_uid": "P000071",
    "status": "REVIEW_REQUIRED",
    "visit_count": 1
  },
  "person_status": "REVIEW_REQUIRED",
  "journey_status": "COMPLETED",
  "route": ["A", "C", "D"],
  "entry_at": "2026-08-11T15:20:27+09:00",
  "timing": {
    "d_exit": "2026-08-11T15:20:40.880+09:00",
    "elapsed_seconds": 13.88
  },
  "identity": {
    "initial_decision": "IDENTITY_PENDING",
    "temporary_person_uid": "P000071",
    "initial_candidate_person_uid": "P000045",
    "final_result": "MANUAL_REVIEW_REQUIRED",
    "final_candidate_person_uid": "P000002",
    "canonical_person_uid": null,
    "final_score": 0.8502318183581035,
    "final_margin": 0.1673468728860219
  }
}
```

`P000002`는 후보이며 최종 Person으로 표시하지 않는다.

## J000104 실제 예: REVISIT

```json
{
  "journey_id": "J000104",
  "person": {
    "person_uid": "P000006",
    "status": "ACTIVE",
    "visit_count": 15
  },
  "person_status": "RETURNING",
  "journey_status": "COMPLETED",
  "route": ["A", "C", "D"],
  "entry_at": "2026-08-11T15:21:43+09:00",
  "timing": {
    "d_exit": "2026-08-11T15:22:01.141+09:00",
    "elapsed_seconds": 18.141
  },
  "identity": {
    "initial_decision": "IDENTITY_PENDING",
    "temporary_person_uid": "P000072",
    "initial_candidate_person_uid": "P000006",
    "final_result": "REVISIT",
    "final_candidate_person_uid": "P000006",
    "canonical_person_uid": "P000006",
    "final_score": 0.7978605687618257,
    "final_margin": 0.07161880830923728
  }
}
```

웹의 최종 Person ID는 `P000006`이다.

## P000006 실제 Person 예

```json
{
  "person_uid": "P000006",
  "status": "ACTIVE",
  "visit_count": 15,
  "created_at": "2026-08-10T16:17:48+09:00",
  "last_seen_at": "2026-08-11T15:24:27+09:00",
  "merged_into_person_uid": null,
  "journeys": [
    {
      "journey_id": "J000106",
      "journey_status": "COMPLETED",
      "route": ["A", "C", "D"],
      "entry_at": "2026-08-11T15:23:26+09:00",
      "d_exit_at": "2026-08-11T15:24:09.768+09:00",
      "elapsed_seconds": 43.768
    }
  ]
}
```

## Score와 Capture

- `identity.initial_scores`, `identity.final_scores`는 BODY/FACE score metadata이다.
- embedding vector는 어떤 기본 API에도 포함되지 않는다.
- `captures[].capture_path`는 Jetson의 원본 경로 metadata이며 Windows 이미지 URL이 아니다.
- 기존 `captures`는 호환성을 위해 flat list 형태를 그대로 유지한다.
- Journey 상세의 `capture_groups.A.body`와 `capture_groups.A.face`는 Camera A가
  ENTRY payload로 보낸 순서대로 최대 3장씩 제공한다.
- grouped item은 `rank`, `quality`, `url`만 포함한다. embedding은 포함하지 않는다.
- BODY URL은 Camera A의 `/captures/body/...`, FACE URL은
  `/captures/face/...`를 사용한다. 허용된 Camera A capture root 밖의 경로는
  URL로 변환하지 않고 `url: null`과 `validation_warnings`를 반환한다.

```json
{
  "captures": [
    {
      "capture_id": 209,
      "node_id": "A",
      "capture_path": "/home/aidl/work/pj/outputs/captures/A/20260811/.../body_1.jpg"
    }
  ],
  "capture_groups": {
    "A": {
      "body": [
        {
          "rank": 1,
          "quality": 0.9336,
          "url": "http://10.10.20.56:8000/captures/body/20260811/.../body_1.jpg"
        }
      ],
      "face": []
    }
  }
}
```

## HTTP error

| 상황 | HTTP | 예 |
|---|---:|---|
| Journey 없음 | 404 | `{"error":"journey_not_found","journey_id":"J999999"}` |
| Person 없음 | 404 | `{"error":"person_not_found","person_uid":"P999999"}` |
| Review 없음 | 404 | `{"error":"review_not_found","journey_id":"J999999"}` |
| 잘못된 query | 400 | `{"error":"invalid_query",...}` |
| 허용되지 않은 method | 405 | `{"error":"method_not_allowed",...}` |
| DB 접근 불가 | 503 | `{"error":"database_unavailable"}` |

HTTP response에는 Python traceback을 포함하지 않는다.

## 팀원 C PC 테스트

```bash
curl http://10.10.20.33:8080/api/health
curl "http://10.10.20.33:8080/api/journeys?limit=5"
curl http://10.10.20.33:8080/api/journeys/J000104
```
