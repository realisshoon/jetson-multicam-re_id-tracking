# 대시보드 서버 제어 명령어 정리

작성: 2026-08-12. 이 PC(사내망 IP `10.10.20.26`)에서 Django 대시보드
서버(daphne)와 메인 서버 연동 워커(`main_server_worker.py`)를 켜고 끄고
확인하는 방법 전부.

---

## 0. 알아둘 것

- 서버는 정식 Windows 서비스가 아니라 **임시로 띄운 프로세스**다 — PC를
  끄거나 재부팅하면, 또는 어떤 이유로 프로세스가 죽으면(2026-08-11 밤
  실제로 한 번 죽었었음, §5 참고) 다시 켜줘야 한다.
- 아래 모든 명령어는 **이 PC 전용**이다. 다른 컴퓨터에서는 §4(SSH)로만
  제어할 수 있다.

---

## 1. 이 PC에서 직접 제어 — `server.ps1`

파일 위치: `D:\20260728\jetson-multicam-re_id-tracking\server.ps1`

```powershell
powershell -ExecutionPolicy Bypass -File "D:\20260728\jetson-multicam-re_id-tracking\server.ps1" <명령어>
```

| 명령어 | 하는 일 |
|---|---|
| `status` | 켜져 있는지 + 접속 주소(로컬/LAN) 확인 |
| `start` | 꺼져있는 것만 켜기 (이미 켜져 있으면 안 건드림) |
| `stop` | daphne + worker 둘 다 끄기 |
| `restart` | 끄고 다시 켜기 — 서버가 이상하게 안 될 때 기본으로 쓰면 됨 |
| `logs` | 최근 로그 확인 (worker 에러 로그가 비어있으면 정상) |
| `open` | 이 PC 기본 브라우저로 대시보드 바로 열기 |

---

## 2. 짧은 단축 명령어 (이 PC의 새 PowerShell 창에서만)

`server.ps1`을 매번 전체 경로로 안 쳐도 되게, PowerShell 프로필에
함수를 등록해뒀다. **새로 여는** PowerShell 창부터 자동 적용된다
(지금 열려있는 창엔 안 먹힘 — `. $PROFILE` 로 새로고침하거나 새 창 열기).

| 명령어 | server.ps1 몇 번째와 동일 |
|---|---|
| `webstatus` | `status` |
| `webstart` | `start` |
| `webstop` | `stop` |
| `webrestart` | `restart` |
| `weblogs` | `logs` |
| `weburl` | `open` |

등록된 파일 (둘 다 동일 내용, PowerShell 버전별로 프로필 위치가 달라서
두 군데에 다 넣어둠):
- `C:\Users\kccistc\Documents\PowerShell\Microsoft.PowerShell_profile.ps1` (PowerShell 7)
- `C:\Users\kccistc\Documents\WindowsPowerShell\Microsoft.PowerShell_profile.ps1` (Windows PowerShell 5.1)

추가로 적용한 설정: Windows PowerShell 5.1의 실행 정책을 사용자 계정
범위로 `RemoteSigned`로 변경함(`Set-ExecutionPolicy -Scope CurrentUser
-ExecutionPolicy RemoteSigned`) — 로컬에서 만든 스크립트가 정책에
막히지 않고 실행되게 하기 위함. 관리자 권한 불필요, 이 계정에만 적용.

---

## 3. 웹사이트 접속 (제어와는 별개 — 그냥 URL)

서버가 켜져 있으면 아무 기기에서나(모바일 포함) 같은 Wi-Fi/LAN에서
브라우저 주소창에 입력:

```
http://10.10.20.26:8000/
```

Termux 등 모바일 터미널 앱에서 브라우저를 바로 띄우고 싶으면:
```
termux-open-url http://10.10.20.26:8000/
```
(Termux:API 패키지 별도 설치 필요)

---

## 4. 다른 컴퓨터에서 원격으로 켜고/끄기 — SSH

### 4-1. 이 PC에서 최초 1회 설정 (관리자 권한 PowerShell 필요)

**⚠ 아직 실행 안 함 — 관리자 권한이 있는 사람이 이 PC에서 직접 실행해야
하는 단계.** (일반 권한으로는 설치 불가라 Claude 가 대신 못 함.)

시작 메뉴 → PowerShell 우클릭 → "관리자 권한으로 실행" 후:

```powershell
# OpenSSH 서버 설치
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0

# 서비스 시작 + 자동시작 등록
Start-Service sshd
Set-Service -Name sshd -StartupType Automatic

# 방화벽 22번 포트 열기(보통 자동 생성되지만 확인 차원)
if (-not (Get-NetFirewallRule -Name sshd -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -Name sshd -DisplayName "OpenSSH Server (sshd)" `
        -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22
}

Get-Service sshd   # Running 으로 뜨면 완료
```

### 4-2. 다른 컴퓨터에서 켜고/끄기

같은 Wi-Fi/LAN에 있는 아무 컴퓨터(Mac/Linux/Windows 다 가능)에서:

```
ssh kccistc@10.10.20.26 "powershell -ExecutionPolicy Bypass -File D:\20260728\jetson-multicam-re_id-tracking\server.ps1 start"
ssh kccistc@10.10.20.26 "powershell -ExecutionPolicy Bypass -File D:\20260728\jetson-multicam-re_id-tracking\server.ps1 stop"
ssh kccistc@10.10.20.26 "powershell -ExecutionPolicy Bypass -File D:\20260728\jetson-multicam-re_id-tracking\server.ps1 restart"
ssh kccistc@10.10.20.26 "powershell -ExecutionPolicy Bypass -File D:\20260728\jetson-multicam-re_id-tracking\server.ps1 status"
```

- 처음 연결하면 지문(fingerprint) 확인 창이 뜬다 — `yes` 입력.
- 그 다음엔 이 PC의 **Windows 로그인 비밀번호**를 물어본다.
- `webstart` 같은 짧은 단축 명령어는 SSH 원격 실행에선 안 먹힌다(SSH가
  기본적으로 cmd.exe로 실행하고, PowerShell 프로필을 안 불러오기
  때문) — 그래서 위처럼 `server.ps1` 전체 경로를 직접 불러야 한다.
- 매번 비밀번호 입력이 번거로우면 SSH 키 등록으로 비밀번호 없이 접속
  가능 — 필요하면 별도로 설정.

---

## 5. 참고 — 2026-08-11 밤 실제 장애 원인

`main_server_worker.py`가 상태를 저장할 때 Windows에서 파일을
"원자적 교체"(`os.replace`)하는 과정이 다른 프로세스(백신 실시간
검사 등)의 순간적인 파일 잠금과 겹치면 `PermissionError`가 나는데,
이걸 못 잡고 있어서 워커 전체가 죽었었다. 2026-08-12 오전에 고침:
- `tracking/bus.py` — 파일 교체 실패 시 최대 5번 짧게 재시도
- `main_server_worker.py` — 메인 루프를 통째로 try/except로 감싸서
  예상 못한 오류가 나도 로그만 남기고 계속 돌게 함

그래도 daphne/worker 자체가 프로세스로만 떠 있는 구조라, PC 재부팅이나
수동 종료 시엔 여전히 §1~§2 명령어로 다시 켜야 한다.
