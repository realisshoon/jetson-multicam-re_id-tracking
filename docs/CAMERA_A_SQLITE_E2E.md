# Camera A → Windows SQLite End-to-End

## Source contract

이 계약은 별도 JSON 샘플을 추측해 만든 것이 아니라 `origin/main`의
`src/nodes/node_a.py`와 `src/network/mqtt_client.py`를 기준으로 작성했다.

Camera A는 YOLO와 ByteTrack으로 `local_track_id`를 얻고 ENTRY LINE 통과 시
TensorRT Re-ID 엔진으로 512차원 embedding을 추출한다. Jetson A가 CSV의 기존
번호를 확인해 `G000001` 형식의 `global_person_id`를 생성하고 ENTRY 이벤트를
CSV에 기록한 다음 NumPy embedding을 Python list로 변환해 MQTT로 전송한다.

- Topic: `cctv/entry`
- Encoding: UTF-8 JSON object
- QoS: 1
- 기본 Broker: `localhost:1883`
- 영상 프레임: 전송하지 않음

```json
{
  "timestamp": "2026-08-06T11:00:00+09:00",
  "node_id": "A",
  "event": "ENTRY",
  "local_track_id": 1,
  "global_person_id": "G000001",
  "next_nodes": ["B", "C"],
  "reid_model": "osnet_x0_25",
  "embedding_dim": 512,
  "embedding": [0.01]
}
```

실제 `embedding`에는 유한한 숫자가 정확히 512개 있어야 한다.

## Global ID policy for this MVP

이번 MVP의 Global ID 발급 주체는 Jetson A이고 Windows는 수신한
`global_person_id`를 변경하지 않고 SQLite에 저장한다. Windows에서 새 ID를
발급하거나 `local_track_id`를 Global ID로 해석하지 않는다.

현재 정책에는 다음 한계가 있다.

- 여러 Camera A 인스턴스가 동시에 실행되면 Global ID가 충돌할 수 있다.
- Jetson CSV가 삭제되면 번호가 초기화될 수 있다.
- 장기적으로 Windows 중앙 서버 또는 DB가 ID 발급 주체가 되어야 한다.
- 현재 payload에는 `message_id`가 없다. 팀 계약 후 추가하면 요청 추적과
  idempotency를 더 명확하게 만들 수 있다.

테스트용 `camera_a_roundtrip_server.py`의 메모리 ID 카운터는 이 실제 저장
서버에서 사용하지 않는다.

## Validation and duplicate handling

Windows는 timestamp, Node/Event 값, Local/Global ID, B/C 대상, Re-ID 모델과
512개 finite embedding을 검증한다. 알 수 없는 추가 필드는 raw payload에
보존하지만 필수 필드가 없거나 값이 잘못되면 저장하지 않는다.

QoS 1 중복 수신을 막기 위해 다음 필드를 canonical JSON으로 직렬화하고
SHA-256 `event_key`를 만든다.

1. `timestamp`
2. `node_id`
3. `event`
4. `local_track_id`
5. `global_person_id`

`tracking_events.event_key`에는 UNIQUE 제약이 있다. 같은 payload가 다시 오면
서버는 종료되지 않고 `[SQLITE DUPLICATE IGNORED]`를 출력한다.

## SQLite schema and transaction

기본 DB는 Git에서 제외되는 `data/central_tracking.db`다. Python 표준 라이브러리
`sqlite3`만 사용하며 연결마다 `PRAGMA foreign_keys = ON`을 적용한다.

- `persons`: Jetson이 발급한 Global ID와 첫 ENTRY/Re-ID 정보를 저장한다.
- `tracking_events`: event key, ENTRY 메타데이터, embedding과 raw payload를
  저장한다.

한 Transaction에서 기존 Person을 유지하거나 새로 INSERT한 다음 tracking event를
INSERT한다. 중복이면 INSERT하지 않고, 오류가 발생하면 전체 Transaction을
rollback한다. embedding과 payload는 `allow_nan=False` JSON TEXT로 저장된다.

## MQTT configuration

버전 관리 예제에는 loopback과 Topic만 둔다.

```yaml
broker:
  host: 127.0.0.1
  port: 1883
  keepalive: 60

topics:
  camera_a_entry: cctv/entry
```

Windows 로컬 서버는 `127.0.0.1`, Jetson A는 Git에서 제외되는
`configs/mqtt_config.yaml`에 확인된 Windows LAN IPv4를 사용한다. 실제 IP나
인증정보를 Commit하지 않는다.

`node_a.py`에는 `--config` 인자와 Broker host/port/keepalive/Topic 주입만
추가했다. YOLO, ByteTrack, ENTRY LINE, TensorRT, Crop, 스트리밍과 모델 경로
정책은 변경하지 않았다.

## Windows local E2E

Python 3.10 환경을 활성화한다.

```powershell
.\.venv310\Scripts\Activate.ps1
```

SQLite 수신 서버:

```powershell
python -m src.server.camera_a_sqlite_server `
  --config configs/mqtt_config.yaml `
  --db data/central_tracking.db
```

별도 PowerShell에서 main-compatible payload를 두 번 보내 중복 방지를 확인한다.

```powershell
python -m src.nodes.camera_a_main_payload_test `
  --config configs/mqtt_config.yaml `
  --global-id G900001 `
  --repeat 2
```

DB 조회:

```powershell
python -m src.server.inspect_tracking_db `
  --db data/central_tracking.db `
  --global-id G900001
```

조회 출력은 Person과 Event 메타데이터, event key 앞 12자 및 무결성 개수만
표시한다. 512개 embedding 전체는 Console에 출력하지 않는다.

## Jetson Camera A execution

가짜 payload와 TCP 연결을 먼저 검증한 뒤 Jetson에서 실행한다.

```bash
python -m src.nodes.node_a --config configs/mqtt_config.yaml
```

실제 사람이 START SIDE에서 ENTRY LINE을 지나 ENTRY SIDE로 이동했을 때 Jetson
로그의 Local ID, embedding dimension과 Global ID가 Windows 로그 및 SQLite 조회
결과와 같은지 확인한다. generic PyPI Torch, OpenCV, TensorRT로 JetPack 패키지를
교체하지 않는다.
