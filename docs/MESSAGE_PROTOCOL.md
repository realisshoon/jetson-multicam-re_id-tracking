# Windows 중앙 서버 MQTT 메시지 프로토콜

이 문서는 Jetson 카메라 노드와 Windows 중앙 서버 사이의 실제 Re-ID 후보
라우팅 규격을 정의한다. 기존 `test_value` 기반 MQTT 왕복 MVP는 변경 없이 별도
용도로 유지한다.

## Topic

| 방향 | Topic | 설명 |
| --- | --- | --- |
| Jetson → Windows | `nodes/{node_id}/data` | Node 이벤트와 Re-ID 결과 |
| Windows → Jetson | `server/{node_id}/result` | 특정 Node로 보낼 후보 또는 결과 |
| Windows → 전체 | `server/all/command` | 전체 Node 제어 명령 |

Windows 서버는 `nodes/+/data`를 구독한다. Topic의 `{node_id}`와 payload의
`node_id`가 다르면 메시지를 거부한다.

## 카메라 경로

```text
A ──→ B ──→ D
 └──→ C ──→ D
```

라우팅 테이블은 다음과 같다.

```python
ROUTES = {
    "A": ["B", "C"],
    "B": ["D"],
    "C": ["D"],
    "D": [],
}
```

A의 후보는 B와 C로만 전송하고 D로 직접 보내지 않는다. B 또는 C에서 등록된
후보의 `match_result`가 오면 D로 전송한다. 동일 후보를 D로 중복 전송하지
않는다. D의 결과를 받으면 후보 상태를 `COMPLETED`로 바꾸고 더 전송하지 않는다.

## 지원 메시지

### `entry_candidate`

A가 새 진입자의 embedding을 Windows로 전송한다.

```json
{
  "message_type": "entry_candidate",
  "node_id": "A",
  "local_id": 3,
  "global_id": "G000001",
  "timestamp": "2026-08-06T11:00:00+09:00",
  "embedding_dim": 512,
  "embedding": [0.01]
}
```

실제 `embedding` 배열에는 유한한 숫자가 정확히 512개 있어야 한다. `NaN`,
`Infinity`, boolean 및 문자열은 허용하지 않는다. `global_id`는 `G`와 6자리
이상의 숫자로 구성한다.

### `reid_candidate`

Windows가 A에서 받은 embedding을 B, C 또는 D로 전달한다.

```json
{
  "message_type": "reid_candidate",
  "source_node": "A",
  "target_node": "B",
  "global_id": "G000001",
  "timestamp": "2026-08-06T11:00:00+09:00",
  "embedding_dim": 512,
  "embedding": [0.01]
}
```

이 메시지는 Windows → Jetson 방향 전용이다.

### `match_result`

B, C 또는 D가 비교 결과를 Windows로 전송한다.

```json
{
  "message_type": "match_result",
  "node_id": "B",
  "local_id": 7,
  "global_id": "G000001",
  "timestamp": "2026-08-06T11:00:15+09:00",
  "similarity": 0.842,
  "status": "matched"
}
```

`similarity`는 유한한 숫자이며 `0.0`부터 `1.0` 사이여야 한다. 서버에 등록되지
않았거나 해당 Node로 전송되지 않은 `global_id` 결과는 거부한다.

### `unknown`, `heartbeat`, `timeout`

- `unknown`: 매칭할 수 없는 이상 이벤트를 저장한다.
- `heartbeat`: Node의 마지막 상태와 timestamp를 갱신한다. `status`가 없으면
  `online`으로 저장한다.
- `timeout`: 등록된 후보를 `TIMED_OUT`으로 바꾸고 timeout 이벤트를 저장한다.

모든 수신 메시지는 `message_type`, `node_id`, ISO-8601 `timestamp`가 필요하다.
잘못된 JSON이나 필드가 들어와도 서버는 종료하지 않고 `[REJECTED]` 로그를 남긴다.

## 저장 계층

중앙 서버는 `EventRepository` 인터페이스만 사용한다. 현재 개발 및 테스트에서는
`MemoryEventRepository`가 ENTRY, MATCH, UNKNOWN, TIMEOUT, NODE_STATUS 이벤트를
수신 순서대로 메모리에 보관한다. Django가 병합되면 동일 인터페이스의
`DjangoEventRepository` 또는 팀원 C의 저장 서비스를 주입한다. 중앙 서버는
SQLite 테이블을 직접 만들지 않는다.

## 실행

```powershell
python -m src.server.central_server --config configs/mqtt_config.yaml
```

코드 수준 검증:

```powershell
python -m unittest discover -s tests -v
python -m compileall -q src tests
```
