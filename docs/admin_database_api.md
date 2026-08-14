# Main Database Admin API

Web은 SQLite 파일을 열지 않는다. 공개 REST API 프로세스는 인증과 HTTP
proxy만 담당하고, `127.0.0.1`에만 bind된 Main 제어 서버가 DB 상태 조회,
WAL checkpoint, 백업, ingestion 일시 정지, DB 교체와 복구를 수행한다.

## 설정과 재시작

Main과 API 프로세스에 동일한 `MAIN_ADMIN_TOKEN`을 설정한다. 실제 `.env`는
커밋하지 않는다.

```powershell
$env:MAIN_ADMIN_TOKEN='<generate-a-long-random-secret>'
$env:MAIN_ADMIN_CONTROL_PORT='8091'
$env:MAIN_ADMIN_CONTROL_URL='http://127.0.0.1:8091'
$env:MAIN_ADMIN_BACKUP_ROOT='D:\working\jetson-multicam-re_id-tracking\data\backups\admin'
$env:MAIN_ADMIN_CONFIRMATION_TTL_SECONDS='300'
```

- `MAIN_ADMIN_TOKEN`만 필수다. 미설정 시 공개 관리자 API는
  `503 ADMIN_API_DISABLED`이며 일반 API와 MQTT는 그대로 동작한다.
- 나머지 변수는 위 값이 기본값이다.
- 환경변수는 시작할 때 읽으므로 Main과 API를 모두 재시작해야 한다.
- 토큰은 로그와 응답에 기록되지 않는다.
- Main 내부 제어 서버의 bind 주소는 보안을 위해 `127.0.0.1`로 고정된다.

모든 공개 요청은 다음 헤더를 사용한다.

```http
Authorization: Bearer <MAIN_ADMIN_TOKEN>
```

## GET /api/admin/database/status

응답 `200`:

```json
{
  "database_status": "READY",
  "schema_version": 62,
  "integrity_check": "ok",
  "person_count": 0,
  "journey_count": 0,
  "gallery_count": 0,
  "permanent_gallery_count": 0,
  "journey_gallery_count": 0,
  "capture_count": 0,
  "active_journey_count": 0,
  "last_backup_at": null,
  "reset_allowed": true,
  "blocking_reason": null
}
```

`gallery_count`는 영구 `person_embeddings`와 임시 `journey_gallery`의 합이다.
두 세부 개수도 함께 반환한다. 진행 중 Journey, DB 작업 또는 integrity 오류가
있으면 `database_status=BLOCKED`, `reset_allowed=false`가 된다.

## POST /api/admin/database/backup

본문은 없거나 `{}`다. Main이 ingestion을 잠시 멈추고 진행 중 handler를
기다린 다음 WAL checkpoint와 SQLite online backup을 수행한다.

응답 `200`:

```json
{
  "backup_id": "DBBACKUP-20260813-150000-a91f",
  "status": "COMPLETED",
  "created_at": "2026-08-13T15:00:00+09:00",
  "integrity_check": "ok",
  "database_bytes": 212992
}
```

## POST /api/admin/database/reset/preview

본문은 없거나 `{}`다. DB는 변경되지 않는다. confirmation은 기본 5분 동안
유효하고 한 번만 사용할 수 있다.

응답 `200`:

```json
{
  "person_count": 285,
  "journey_count": 488,
  "gallery_count": 1420,
  "permanent_gallery_count": 120,
  "journey_gallery_count": 1300,
  "capture_count": 936,
  "active_journey_count": 0,
  "can_reset": true,
  "blocking_reason": null,
  "confirmation_id": "reset_20260813_a91f",
  "expires_at": "2026-08-13T15:10:00+09:00"
}
```

preview 뒤 count가 바뀌면 execute는 `409 DATABASE_CHANGED_SINCE_PREVIEW`를
반환하므로 Web은 preview를 다시 받아야 한다.

## POST /api/admin/database/reset/execute

요청:

```json
{
  "confirmation_id": "reset_20260813_a91f",
  "confirmation_text": "전체 데이터 초기화",
  "capture_policy": "ARCHIVE",
  "force": false
}
```

응답 `202`:

```json
{
  "accepted": true,
  "job_id": "DBRESET-20260813-001",
  "status": "PREPARING"
}
```

`force`는 향후 정책 확장을 위한 예약 필드다. 현재는 `true`여도
`WAITING_B_OR_C` 또는 `WAITING_D` Journey가 한 건이라도 있으면 안전상
`409`다. 현재 지원하는 capture 정책은 `ARCHIVE`뿐이다.
Main Capture Cache는 reset 백업의 `retired_live/captures`로 이동하고 새 빈
cache 디렉터리를 만든다. Jetson 원본, 모델, 엔진, 설정과 임계값은 건드리지
않는다.

## GET /api/admin/database/jobs/{job_id}

응답 `200`:

```json
{
  "job_id": "DBRESET-20260813-001",
  "status": "COMPLETED",
  "created_at": "2026-08-13T15:00:00+09:00",
  "updated_at": "2026-08-13T15:00:02+09:00",
  "completed_at": "2026-08-13T15:00:02+09:00",
  "backup_id": "DBRESET-20260813-001-20260813-150000-a91f",
  "integrity_check": "ok",
  "error": null,
  "history": [
    {"status": "PREPARING", "at": "2026-08-13T15:00:00+09:00"},
    {"status": "PAUSING_INGESTION", "at": "2026-08-13T15:00:00+09:00"},
    {"status": "BACKING_UP", "at": "2026-08-13T15:00:00+09:00"},
    {"status": "RESETTING", "at": "2026-08-13T15:00:01+09:00"},
    {"status": "REOPENING", "at": "2026-08-13T15:00:01+09:00"},
    {"status": "VERIFYING", "at": "2026-08-13T15:00:02+09:00"},
    {"status": "COMPLETED", "at": "2026-08-13T15:00:02+09:00"}
  ]
}
```

상태 순서는 `PREPARING`, `PAUSING_INGESTION`, `BACKING_UP`, `RESETTING`,
`REOPENING`, `VERIFYING`, `COMPLETED`이며 오류 시 `FAILED`로 끝난다.

## 오류 계약

모든 오류는 JSON이다.

```json
{"error":"ADMIN_AUTH_REQUIRED"}
```

| HTTP | error | 의미 |
|---:|---|---|
| 400 | `INVALID_JSON`, `INVALID_CAPTURE_POLICY`, `INVALID_FORCE_VALUE` | 요청 형식 오류 |
| 401 | `ADMIN_AUTH_REQUIRED` | Bearer 헤더 없음/형식 오류 |
| 403 | `ADMIN_FORBIDDEN` | 토큰 불일치 |
| 404 | `DATABASE_JOB_NOT_FOUND` | job_id 없음 |
| 409 | `ACTIVE_JOURNEYS_EXIST` | 진행 중 Journey 존재 |
| 409 | `INVALID_OR_EXPIRED_CONFIRMATION` | confirmation 없음·사용됨·만료됨 |
| 409 | `CONFIRMATION_TEXT_MISMATCH` | 확인 문구 불일치 |
| 409 | `DATABASE_CHANGED_SINCE_PREVIEW` | preview 이후 DB count 변경 |
| 409 | `DATABASE_JOB_IN_PROGRESS` | reset 중복 실행 |
| 409 | `DATABASE_BACKUP_IN_PROGRESS` | backup과 reset 충돌 |
| 503 | `ADMIN_API_DISABLED` | Main/API 토큰 미설정 |
| 503 | `MAIN_ADMIN_CONTROL_UNAVAILABLE` | Main 내부 제어 서버 미기동 |
| 503 | `DATABASE_UNAVAILABLE` | DB 접근 불가 |

## curl 예시

```bash
curl -H "Authorization: Bearer $MAIN_ADMIN_TOKEN" http://10.10.20.33:8080/api/admin/database/status
curl -X POST -H "Authorization: Bearer $MAIN_ADMIN_TOKEN" -H "Content-Type: application/json" -d '{}' http://10.10.20.33:8080/api/admin/database/backup
curl -X POST -H "Authorization: Bearer $MAIN_ADMIN_TOKEN" -H "Content-Type: application/json" -d '{}' http://10.10.20.33:8080/api/admin/database/reset/preview
curl -X POST -H "Authorization: Bearer $MAIN_ADMIN_TOKEN" -H "Content-Type: application/json" -d '{"confirmation_id":"reset_20260813_a91f","confirmation_text":"전체 데이터 초기화","capture_policy":"ARCHIVE","force":false}' http://10.10.20.33:8080/api/admin/database/reset/execute
curl -H "Authorization: Bearer $MAIN_ADMIN_TOKEN" http://10.10.20.33:8080/api/admin/database/jobs/DBRESET-20260813-001
```

## 2026-08-13 실제 curl smoke 결과

운영 DB가 아닌 별도 임시 DB에서 공개 API `127.0.0.1:18080`과 Main 내부
제어 API `127.0.0.1:18091`를 실행해 위 curl을 그대로 검증했다. 사용한
토큰은 테스트 전용이며 아래 결과에 노출되지 않는다.

```text
GET status (Bearer 없음)
HTTP/1.1 401 Unauthorized
{"error":"ADMIN_AUTH_REQUIRED"}

GET status (불일치 Bearer)
HTTP/1.1 403 Forbidden
{"error":"ADMIN_FORBIDDEN"}

GET status
{"database_status":"READY","schema_version":62,"integrity_check":"ok","person_count":0,"journey_count":0,"gallery_count":0,"permanent_gallery_count":0,"journey_gallery_count":0,"capture_count":0,"active_journey_count":0,"last_backup_at":null,"reset_allowed":true,"blocking_reason":null}

POST backup
{"backup_id":"DBBACKUP-20260813-145710-4d65","status":"COMPLETED","created_at":"2026-08-13T14:57:10+09:00","integrity_check":"ok","database_bytes":212992}

POST reset/preview
{"person_count":0,"journey_count":0,"gallery_count":0,"permanent_gallery_count":0,"journey_gallery_count":0,"capture_count":0,"active_journey_count":0,"can_reset":true,"blocking_reason":null,"confirmation_id":"reset_20260813_1a89","expires_at":"2026-08-13T15:02:10+09:00"}

POST reset/execute
{"accepted":true,"job_id":"DBRESET-20260813-001","status":"PREPARING"}

GET jobs/DBRESET-20260813-001
{"job_id":"DBRESET-20260813-001","status":"COMPLETED","backup_id":"DBRESET-20260813-001-20260813-145710-7057","integrity_check":"ok","error":null}
```

실제 job 응답의 `history`에서도 `PREPARING → PAUSING_INGESTION →
BACKING_UP → RESETTING → REOPENING → VERIFYING → COMPLETED` 전 단계가
순서대로 확인됐다.
