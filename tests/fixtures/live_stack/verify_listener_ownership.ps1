param(
    [Parameter(Mandatory)][string]$ProjectRoot,
    [Parameter(Mandatory)][string]$PythonPath,
    [Parameter(Mandatory)][string]$ListenerPath,
    [Parameter(Mandatory)][string]$WrapperPath,
    [Parameter(Mandatory)][string]$TempRoot,
    [Parameter(Mandatory)][int]$ChildPort,
    [Parameter(Mandatory)][int]$DualPort,
    [Parameter(Mandatory)][int]$UnusedApiPort
)
$ErrorActionPreference = 'Stop'
. (Join-Path $ProjectRoot 'scripts\live_stack_common.ps1')
$RunRoot = Join-Path $TempRoot 'data\run'
New-Item -ItemType Directory -Path $RunRoot -Force | Out-Null
$Started = [Collections.Generic.List[Diagnostics.Process]]::new()

function Start-Wrapper([string]$Address, [int]$Port) {
    $Out = Join-Path $TempRoot "wrapper_${Address}_$Port.out.log"
    $Err = Join-Path $TempRoot "wrapper_${Address}_$Port.err.log"
    $Process = Start-Process -FilePath 'powershell.exe' -ArgumentList @(
        '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $WrapperPath,
        '-PythonPath', $PythonPath,
        '-ListenerPath', $ListenerPath,
        '-Address', $Address,
        '-Port', [string]$Port
    ) -WindowStyle Hidden -RedirectStandardOutput $Out `
        -RedirectStandardError $Err -PassThru
    $Started.Add($Process) | Out-Null
    $Owner = Wait-LiveEndpointOwner $Address $Port 15
    if ($null -eq $Owner) {
        $ErrorText = if (Test-Path $Err) { Get-Content $Err -Raw } else { '' }
        throw "fixture listener failed: ${Address}:$Port wrapper_exited=$($Process.HasExited) stderr=$ErrorText"
    }
    return [pscustomobject]@{ wrapper=$Process; owner=$Owner }
}

try {
    # Wrapper PID must differ from the child Python socket owner.
    $ChildFixture = Start-Wrapper '127.0.0.1' $ChildPort
    if ($ChildFixture.wrapper.Id -eq $ChildFixture.owner.owning_process) {
        throw 'wrapper PID unexpectedly owns the listener'
    }
    $MainRecord = New-LiveProcessRecord 'main' '127.0.0.1' $ChildPort $null $null
    # Simulate the precision/rounding difference seen between Win32_Process and
    # other Windows process APIs. The 1-second delta must remain within policy.
    $MainRecord.creation_time_utc = ConvertTo-LiveUtcTimestamp (
        ([datetime]::Parse($MainRecord.creation_time_utc)).AddSeconds(1)
    )
    $MainRecord | ConvertTo-Json -Depth 5 |
        Set-Content (Join-Path $RunRoot 'main.pid.json') -Encoding utf8
    & (Join-Path $ProjectRoot 'scripts\stop_live_stack.ps1') `
        -ProjectRoot $TempRoot -BrokerAddress '127.0.0.2' `
        -BrokerPort ($DualPort + 1) -ApiAddress '127.0.0.2' `
        -ApiPort $UnusedApiPort -AdminPort $ChildPort -WaitSeconds 5 | Out-Null
    $ChildStopped = $null -eq (Get-LiveEndpointOwner '127.0.0.1' $ChildPort)

    # Two listeners share the same port on distinct addresses. The LAN-target
    # broker record/stop must not touch the loopback listener.
    $Loopback = Start-Wrapper '127.0.0.1' $DualPort
    $Lan = Start-Wrapper '127.0.0.2' $DualPort
    $BrokerRecord = New-LiveProcessRecord 'broker' '127.0.0.2' $DualPort $null $null
    $BrokerRecord | ConvertTo-Json -Depth 5 |
        Set-Content (Join-Path $RunRoot 'broker.pid.json') -Encoding utf8
    & (Join-Path $ProjectRoot 'scripts\stop_live_stack.ps1') `
        -ProjectRoot $TempRoot -BrokerAddress '127.0.0.2' `
        -BrokerPort $DualPort -ApiAddress '127.0.0.2' `
        -ApiPort $UnusedApiPort -AdminPort ($ChildPort + 1) -WaitSeconds 5 | Out-Null
    $LanStopped = $null -eq (Get-LiveEndpointOwner '127.0.0.2' $DualPort)
    $LoopbackOwner = Get-LiveEndpointOwner '127.0.0.1' $DualPort
    $LoopbackAlive = $null -ne $LoopbackOwner

    [pscustomobject]@{
        wrapper_pid = $ChildFixture.wrapper.Id
        listener_pid = $MainRecord.pid
        wrapper_child_differ = $ChildFixture.wrapper.Id -ne [int]$MainRecord.pid
        executable_recorded = -not [string]::IsNullOrWhiteSpace($MainRecord.executable_path)
        command_line_recorded = -not [string]::IsNullOrWhiteSpace($MainRecord.command_line)
        creation_time_utc = $MainRecord.creation_time_utc
        creation_time_adjustment_seconds = 1
        child_listener_stopped = $ChildStopped
        lan_listener_stopped = $LanStopped
        loopback_listener_alive = $LoopbackAlive
        loopback_pid = if ($LoopbackAlive) { $LoopbackOwner.owning_process } else { $null }
        lan_pid = $BrokerRecord.pid
    } | ConvertTo-Json -Compress
} finally {
    foreach ($Address in @('127.0.0.1','127.0.0.2')) {
        foreach ($Port in @($ChildPort,$DualPort)) {
            $Owner = Get-LiveEndpointOwner $Address $Port
            if ($null -ne $Owner) {
                Stop-Process -Id $Owner.owning_process -Force -ErrorAction SilentlyContinue
            }
        }
    }
    foreach ($Process in $Started) {
        if (-not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        }
    }
}
