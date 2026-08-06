# Camera A MQTT 왕복 검증

이 단계는 실제 카메라나 Re-ID 파이프라인을 연결하기 전에 가짜 Jetson A
프로세스로 다음 흐름을 검증한다.

```text
가짜 Jetson A
  └─ nodes/A/data: entry_candidate
       └─ Windows Mosquitto
            └─ Camera A 왕복 서버 검증 및 임시 global_id 발급
                 └─ server/A/result: entry_ack
                      └─ 가짜 Jetson A가 동일 message_id 응답 확인
```

응답을 받는 주체는 물리적인 카메라가 아니라 Jetson A에서 실행되는 Python
프로세스다. 이 시험에서는 영상, YOLO, ByteTrack, TensorRT를 사용하지 않는다.

## Windows 준비

Windows 중앙 서버의 로컬 MQTT 설정은 같은 PC의 Mosquitto에 연결한다.

```yaml
broker:
  host: 127.0.0.1
  port: 1883
  keepalive: 60
```

Windows LAN IPv4는 PowerShell에서 다음 명령으로 확인할 수 있다.

```powershell
Get-NetIPAddress -AddressFamily IPv4 |
  Where-Object { $_.IPAddress -notlike '127.*' }
```

또는 `ipconfig`에서 실제 Jetson과 같은 LAN에 연결된 Ethernet/Wi-Fi 어댑터의
IPv4 주소를 확인한다. 실제 주소를 Git에 추적되는 설정이나 문서에 기록하지
않는다.

Jetson에서 접속하려면 Mosquitto가 LAN 인터페이스에서 TCP 1883을 수신해야 하며
Windows 방화벽 인바운드 규칙도 필요하다. 관리자 PowerShell에서 개발망 범위를
검토한 뒤 TCP 1883 허용 규칙을 추가한다. 공개 네트워크에서는 익명 listener를
사용하지 말고 계정과 TLS를 구성한다.

## 로컬 왕복 시험

Python 3.10 환경을 활성화한다.

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv310\Scripts\Activate.ps1
```

첫 번째 PowerShell에서 Windows 왕복 서버를 실행한다.

```powershell
python -m src.server.camera_a_roundtrip_server `
  --config configs/mqtt_config.yaml
```

두 번째 PowerShell에서 가짜 Jetson A를 실행한다.

```powershell
python -m src.nodes.camera_a_roundtrip_test `
  --config configs/mqtt_config.yaml
```

가짜 A는 UUID `message_id`, 512차원 테스트 embedding과 메타데이터를
`nodes/A/data`로 보낸다. 서버는 메시지를 검증하고 프로세스 메모리에서
`G000001`부터 증가하는 `global_id`를 발급한 뒤 `server/A/result`로
`entry_ack`를 보낸다. 서버를 재시작하면 이 임시 카운터도 초기화된다.

## 실제 Jetson A 설정

- Windows 서버 설정: `broker.host=127.0.0.1`
- Jetson A 설정: `broker.host=<Windows LAN IPv4>`
- Jetson A Publish: `nodes/A/data`
- Jetson A Subscribe: `server/A/result`

실제 영상 프레임은 MQTT로 보내지 않는다. MQTT payload에는 embedding, local ID,
timestamp, message ID와 이벤트 메타데이터만 포함한다. 실제 Camera A 통합
전에는 반드시 가짜 클라이언트로 Broker와 방화벽을 포함한 왕복을 검증한다.
통합 후에도 매 프레임이 아니라 사람 진입 이벤트가 확정된 시점에만 Publish한다.

## 실제 Camera A 통합용 Adapter 제안

기존 `node_a.py`는 이 단계에서 변경하지 않는다. 팀원 A와 메시지 규격을 합의한
후 별도 브랜치에서 다음 경계를 구현한다.

```python
class CameraAEventPublisher:
    def publish_entry(
        self,
        local_id: int,
        timestamp: str,
        embedding: list[float],
    ) -> str:
        """entry_candidate를 전송하고 생성한 message_id를 반환한다."""


class CameraAServerResultHandler:
    def handle_entry_ack(self, payload: dict) -> None:
        """pending message_id와 응답을 매칭하고 global_id를 track에 연결한다."""
```

`CameraAEventPublisher`는 각 요청의 `message_id`와 local track을 pending 상태로
보관한다. `CameraAServerResultHandler`는 동일한 `message_id`인지 확인하고,
`accepted=true`일 때만 응답 `global_id`를 해당 local track에 연결한다. 거부,
timeout, 중복 응답의 재시도·정리 정책도 실제 통합 전에 함께 합의해야 한다.
