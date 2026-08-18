# 대시보드 서버(daphne) + 메인 서버 연동 워커(main_server_worker.py) 관리 스크립트.
# Claude 없이 직접 켜고/끄고/상태 확인할 수 있게 정리한 것.
#
# 사용법 (이 파일이 있는 폴더에서, 또는 PowerShell 아무 위치에서 전체 경로로):
#   .\server.ps1 start     서버 켜기 (이미 켜져 있으면 건드리지 않음)
#   .\server.ps1 stop      서버 끄기
#   .\server.ps1 restart   끄고 다시 켜기
#   .\server.ps1 status    지금 켜져 있는지 + 접속 주소 확인
#   .\server.ps1 logs      최근 로그(에러 위주) 보기
#   .\server.ps1 open      이 PC 기본 브라우저로 대시보드 바로 열기
#
# 참고: 이 스크립트는 이 Windows PC에서 서버를 켜고 끄는 용도다. 모바일 등
# 다른 기기에서 "웹에 접속"하는 건 이거랑 무관하고, 그냥 브라우저에서
# 아래 LAN 주소를 열면 된다(같은 Wi-Fi 필요) — 명령어랄 게 따로 없다.
#
# "실행할 수 없음/스크립트 차단" 오류가 뜨면 이렇게 실행:
#   powershell -ExecutionPolicy Bypass -File "D:\20260728\jetson-multicam-re_id-tracking\server.ps1" start

param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop", "restart", "status", "logs", "open")]
    [string]$Action = "status"
)

$Root   = "D:\20260728\jetson-multicam-re_id-tracking"
$Web    = "$Root\web"
$Venv   = "$Root\.venv\Scripts"
$LanUrl = "http://10.10.20.26:8000/"   # 이 PC의 사내망 IP — 모바일/다른 PC 접속용

function Get-DaphneProc { Get-CimInstance Win32_Process -Filter "Name='daphne.exe'" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match 'jetson-multicam' } }
function Get-WorkerProc { Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match 'main_server_worker' } }

function Show-Status {
    $d = Get-DaphneProc
    $w = Get-WorkerProc
    if ($d) { Write-Host "daphne (대시보드) : 실행 중 (PID $($d.ProcessId -join ', '))" -ForegroundColor Green }
    else    { Write-Host "daphne (대시보드) : 꺼짐" -ForegroundColor Red }
    if ($w) { Write-Host "worker (메인 연동) : 실행 중 (PID $($w.ProcessId -join ', '))" -ForegroundColor Green }
    else    { Write-Host "worker (메인 연동) : 꺼짐" -ForegroundColor Red }
    try {
        Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction Stop | Out-Null
        Write-Host "포트 8000          : 대기 중" -ForegroundColor Green
        Write-Host "  이 PC에서         : http://localhost:8000/"
        Write-Host "  모바일/다른 PC서 : $LanUrl (같은 Wi-Fi 필요)"
    } catch {
        Write-Host "포트 8000          : 안 열림" -ForegroundColor Red
    }
}

function Open-Dashboard {
    Start-Process $LanUrl
}

function Stop-Servers {
    $procs = @(Get-DaphneProc) + @(Get-WorkerProc)
    if (-not $procs) { Write-Host "이미 다 꺼져 있음"; return }
    $procs | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 800
    Write-Host "중지 완료"
}

function Start-Servers {
    if (Get-DaphneProc) {
        Write-Host "daphne 이미 실행 중 — 건드리지 않음"
    } else {
        Start-Process -FilePath "$Venv\daphne.exe" `
            -ArgumentList "-b", "0.0.0.0", "-p", "8000", "config.asgi:application" `
            -RedirectStandardOutput "$Web\daphne.log" -RedirectStandardError "$Web\daphne.log.err" `
            -WorkingDirectory $Web -WindowStyle Hidden
        Write-Host "daphne 시작함"
    }
    if (Get-WorkerProc) {
        Write-Host "worker 이미 실행 중 — 건드리지 않음"
    } else {
        Start-Process -FilePath "$Venv\python.exe" `
            -ArgumentList "-u", "main_server_worker.py" `
            -RedirectStandardOutput "$Web\main_server_worker.log" -RedirectStandardError "$Web\main_server_worker.log.err" `
            -WorkingDirectory $Web -WindowStyle Hidden
        Write-Host "worker 시작함"
    }
    Start-Sleep -Seconds 3
    Show-Status
}

function Show-Logs {
    Write-Host "── daphne 에러 로그(최근 10줄) ──────────────────" -ForegroundColor Cyan
    Get-Content "$Web\daphne.log.err" -Tail 10 -ErrorAction SilentlyContinue
    Write-Host ""
    Write-Host "── worker 로그(최근 10줄) ───────────────────────" -ForegroundColor Cyan
    Get-Content "$Web\main_server_worker.log" -Tail 10 -ErrorAction SilentlyContinue
    Write-Host ""
    Write-Host "── worker 에러 로그(최근 10줄, 비어있으면 정상) ──" -ForegroundColor Cyan
    Get-Content "$Web\main_server_worker.log.err" -Tail 10 -ErrorAction SilentlyContinue
}

switch ($Action) {
    "start"   { Start-Servers }
    "stop"    { Stop-Servers }
    "restart" { Stop-Servers; Start-Servers }
    "status"  { Show-Status }
    "logs"    { Show-Logs }
    "open"    { Open-Dashboard }
}
