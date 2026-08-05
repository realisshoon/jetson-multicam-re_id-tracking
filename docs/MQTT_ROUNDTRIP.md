# Jetson-Windows MQTT 왕복 통신

이 기능은 Re-ID/ByteTrack 파이프라인과 독립된 테스트 JSON으로 여러 Jetson과
Windows 중앙 서버 사이의 양방향 통신을 검증한다. 실제 Re-ID embedding은 아직
연결하지 않는다.

## 1. 설정

예제 파일을 Git에서 제외되는 실제 설정 파일로 복사한다.

Windows PowerShell:

```powershell
Copy-Item configs/mqtt_config.example.yaml configs/mqtt_config.yaml
```

Jetson:

```bash
cp configs/mqtt_config.example.yaml configs/mqtt_config.yaml
```

`configs/mqtt_config.yaml`의 `broker.host`를 Mosquitto가 실행되는 Windows PC의
IP로 바꾼다. Node ID는 `node.id`에서 `A`, `B`, `D` 등으로 바꾸거나 실행할 때
`--node-id D`로 덮어쓸 수 있다. 실제 설정 파일은 `.gitignore`에 포함되어 있다.

Broker 인증을 사용할 때는 비밀번호를 YAML에 쓰지 않는다. 예제의
`username_env`, `password_env` 주석을 해제하고 지정한 환경 변수에 인증 정보를
넣는다.

## 2. Topic 구조

| 방향 | Topic | 용도 |
| --- | --- | --- |
| Jetson → Windows | `nodes/{node_id}/data` | Node별 테스트 데이터 |
| Windows → Jetson | `server/{node_id}/result` | 해당 Node의 처리 결과 |
| Windows → 전체 | `server/all/command` | 모든 Node가 받는 명령 |

Windows 서버는 `nodes/+/data`를 구독하고, 메시지의 `node_id`와 Topic의 Node가
일치하는지 확인한다. 잘못된 JSON, 필수 필드 누락, 잘못된 값은 응답 없이
`[REJECTED]` 또는 오류 로그를 남긴다.

## 3. Mosquitto Broker 실행

Windows에 Mosquitto를 설치하고 Broker PC에서 개발용 예제 설정으로 실행한다.
이미 Mosquitto Windows 서비스가 1883 포트를 사용 중이면 새 Broker를 중복
실행하지 않는다.

```powershell
& 'C:\Program Files\Mosquitto\mosquitto.exe' `
  -c configs\mosquitto.example.conf -v
```

이 예제는 Jetson이 접속할 수 있도록 모든 인터페이스에서 익명 접속을 받으므로
신뢰할 수 있는 개발용 사설망에서만 사용한다. Windows 방화벽의 TCP 1883
인바운드 허용도 필요할 수 있다. 운영 또는 공개 네트워크에서는 익명 접속을
비활성화하고 계정과 TLS를 사용한다.

## 4. Windows 중앙 서버 실행

프로젝트 루트에서 가상환경을 활성화하고 다음을 실행한다.

```powershell
python -m src.server.mqtt_roundtrip_server --config configs/mqtt_config.yaml
```

서버는 숫자 `test_value`를 2배로 만든 테스트 결과를 송신한다.

## 5. Jetson Node 실행

동일한 설정 파일을 준비한 뒤 프로젝트 루트에서 한 번 전송하고 응답을
기다린다.

```bash
python3 -m src.nodes.mqtt_roundtrip_node \
  --config configs/mqtt_config.yaml --node-id A --local-id 1 --value 100
```

반복 시험은 `--interval`을 사용한다.

```bash
python3 -m src.nodes.mqtt_roundtrip_node \
  --config configs/mqtt_config.yaml --node-id D --interval 5
```

## 6. 테스트 순서와 예상 출력

1. Windows에서 Mosquitto Broker를 실행한다.
2. Windows 중앙 서버를 실행한다.
3. Jetson에서 Node 시험 프로그램을 실행한다.
4. Windows에서 `[RECEIVED]`, `[RESPONSE]`을 확인한다.
5. Jetson에서 `[PUBLISH]`, `[RESULT]`를 확인한다.

`--value 100`을 보냈다면 응답의 `processed_value`는 `200`이다. Broker가 다른
네트워크나 PC로 이동하면 각 장비의 `configs/mqtt_config.yaml`에서
`broker.host`만 새 Broker IP 또는 DNS 이름으로 바꾼다.

Broker 없이 코드 수준 검사를 실행하려면 다음을 사용한다.

```bash
python -m unittest tests.test_mqtt_roundtrip -v
python -m compileall -q src/network src/server src/nodes/mqtt_roundtrip_node.py
```
