# jetson-multicam-re_id-tracking
Multi-camera person tracking with YOLO, ByteTrack, OSNet Re-ID, and Jetson Orin Nano.

Jetson-Windows MQTT JSON 왕복 통신 시험은
[docs/MQTT_ROUNDTRIP.md](docs/MQTT_ROUNDTRIP.md)를 참고하세요.

## Main PC live stack

Main PC의 Mosquitto Broker, Main Server, API Server는 저장소 루트에서 하나의
PowerShell 명령으로 시작·확인·종료할 수 있다. 스크립트는 Camera A/B/C/D를
실행하지 않으며 DB 초기화도 수행하지 않는다.

최초 한 번 관리자 토큰을 Windows User 환경변수로 등록하고 새 PowerShell을
연다. 토큰 값은 로그나 상태 출력에 표시되지 않는다.

```powershell
[Environment]::SetEnvironmentVariable(
    'CCTV_TEST_ADMIN_TOKEN',
    '<long-random-secret>',
    [EnvironmentVariableTarget]::User
)
```

전체 서버 시작:

```powershell
.\scripts\start_live_stack.ps1
```

기본 endpoint는 Broker `10.10.20.33:1883`, Main 관리자 제어
`127.0.0.1:8091`, API `10.10.20.33:8080`이다. 다른 Main PC 주소를 사용할
때는 명시적으로 전달한다.

```powershell
.\scripts\start_live_stack.ps1 `
    -BrokerAddress 10.10.20.33 `
    -ApiAddress 10.10.20.33
```

시작 스크립트는 Broker → Main → API 순서로 실행하고 포트, API health,
관리자 DB `integrity_check=ok`까지 확인한다. readiness 후 지정 endpoint의
실제 TCP `OwningProcess`를 조회하므로, 실행 래퍼가 자식 Python을 생성해도
래퍼가 아닌 실제 listener PID가 기록된다. 프로세스별 PID, executable path,
command line, UTC creation time과 endpoint는 `data/run/*.pid.json`,
stdout/stderr는 `data/logs`에 저장한다. 이미 전체 메타데이터와 endpoint owner가
일치하는 관리 대상 프로세스는 재사용하고, 포트가 추적되지 않은 다른
프로세스에 의해 사용 중이면 중복 실행하지 않고 실패한다.

상태 확인:

```powershell
.\scripts\status_live_stack.ps1
```

PID 생존 여부, 1883/8091/8080 포트, `/api/health`, 관리자 DB status와 각
서비스의 최근 stderr 10줄을 출력한다.

안전 종료:

```powershell
.\scripts\stop_live_stack.ps1
```

PID, executable, command line, UTC creation time, endpoint owner가 모두
일치하는 API, Main, Broker만 이 순서로 종료한다. Windows process creation
time 정밀도 차이는 2초 이내에서만 허용한다. Broker 검증은 정확히
`10.10.20.33:1883` listener를 대상으로 하므로 별도 실행 중인
`127.0.0.1:1883` Mosquitto는 상태에 unmanaged로 표시만 하고 종료하지 않는다.
다른 Python/Mosquitto 프로세스도 일괄 종료하지 않는다.

시작 실패 시 출력된 서비스명과 다음 로그를 확인한다.

```text
data/logs/broker_*.stderr.log
data/logs/main_*.stderr.log
data/logs/api_*.stderr.log
```
