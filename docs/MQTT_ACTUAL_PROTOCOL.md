# MQTT 실제 프로토콜 조사 결과

이 문서는 2026-08-06 현재 원격 Git 브랜치의 실제 코드와 제공된 실행 로그를
구분해 기록한다. Windows 수집기는 확인되지 않은 B/D payload 구조를 추정하지
않고 `cctv/#` 원문을 먼저 보존한다.

## 코드에서 확인된 계약

`origin/main`의 `src/nodes/node_a.py`, `src/nodes/node_b.py`,
`src/network/mqtt_client.py`에서 확인된 실제 topic은 다음 하나다.

### `cctv/entry`

Node A가 QoS 1로 publish하고 Node B가 subscribe한다.

| 필드 | 실제 코드의 값/형식 |
|---|---|
| `timestamp` | ISO-8601 문자열 |
| `node_id` | `"A"` |
| `event` | `"ENTRY"` |
| `local_track_id` | 정수 |
| `global_person_id` | Node A가 만든 문자열 ID |
| `next_nodes` | `['B', 'C']` |
| `reid_model` | 문자열 |
| `embedding_dim` | 정수, 현재 생성 코드는 512 |
| `embedding` | 실수 배열 |

내부 canonical Journey에서는 `global_person_id`를 `journey_id`로 사용한다.
Windows 서버는 새 Journey ID를 만들지 않는다.

## 코드에서 아직 확인되지 않은 계약

- 제공된 실행 로그로 `cctv/passage/b` topic의 존재는 확인됐지만 payload 필드와
  타입은 저장소 어디에도 구현돼 있지 않다.
- 현재 Node B 코드는 `cctv/entry`를 받아 후보를 저장할 뿐 passage를 publish하지
  않는다.
- Node D 구현과 A → B → D completion publish 계약은 원격 브랜치들에 없다.

따라서 이 topic들을 필드 추정으로 파싱하지 않는다. `mqtt_capture.py` 또는 Journey
서버로 실제 메시지를 수집하면 `raw_mqtt_messages`에 원문과 topic, 수신 시각,
중복 판별 키가 남는다. 캡처된 계약을 확인한 뒤 외부-topic adapter를 추가한다.

## 내부 canonical envelope

향후 노드가 명시적으로 `schema_version`과 `journey_id`를 보내는 경우에만 내부
canonical adapter를 사용할 수 있다. 공통 필수 필드는 다음과 같다.

- `schema_version`, `event_type`, `journey_id`, `source_node`, `timestamp`, `status`
- 선택: `message_id`, `target_node`, `local_track_id`, `route`, `similarity`, `quality`
- Gallery: `sample_index`, `embedding_dim`, `embedding`, `gallery_count`, `gallery_nodes`
- Completion: `best_similarity`, `top2_mean`, `combined_score`, `total_duration_sec`,
  `previous_node`, `previous_to_destination_sec`

이는 Windows 내부 저장 계약이며, 실제 B/D 외부 계약이라고 간주해서는 안 된다.
QoS 1 재전송은 `message_id`가 있으면 그 값, 없으면 안정적인 메시지 필드 hash로
중복 제거한다.

## 실행

```powershell
.\.venv310\Scripts\python.exe -m src.server.mqtt_capture
.\.venv310\Scripts\python.exe -m src.server.journey_sqlite_server
.\.venv310\Scripts\python.exe -m src.server.inspect_journey_db --journey-id <ID>
```

기본 DB는 `data/central_tracking.db`이며 DB, CSV, 로그 파일은 Git에서 제외된다.
