[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$BrokerAddress = '10.10.20.33', [int]$BrokerPort = 1883,
    [string]$ApiAddress = '10.10.20.33', [int]$ApiPort = 8080,
    [int]$AdminPort = 8091
)
$ErrorActionPreference = 'Stop'
$ProjectRoot = [IO.Path]::GetFullPath($ProjectRoot)
. (Join-Path $PSScriptRoot 'live_stack_common.ps1')
$RunRoot = Join-Path $ProjectRoot 'data\run'
$LogRoot = Join-Path $ProjectRoot 'data\logs'
$AdminToken = [Environment]::GetEnvironmentVariable(
    'CCTV_TEST_ADMIN_TOKEN', [EnvironmentVariableTarget]::User
)

function Get-RecordPath { param([string]$Name) Join-Path $RunRoot "$Name.pid.json" }
function Read-Record {
    param([string]$Name)
    $Path = Get-RecordPath $Name
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}
function Get-ServiceStatus {
    param([string]$Name, [string]$Address, [int]$Port)
    $Record = Read-Record $Name
    if ($null -eq $Record) {
        return [pscustomobject]@{Service=$Name;Pid=$null;State='NOT_RECORDED';Detail='PID_FILE_ABSENT'}
    }
    if (
        [string]$Record.endpoint_address -ne $Address -or
        [int]$Record.endpoint_port -ne $Port
    ) {
        return [pscustomobject]@{
            Service=$Name;Pid=$Record.pid;State='MISMATCH';Detail='ENDPOINT_RECORD_MISMATCH'
        }
    }
    $Check = Test-LiveProcessRecord $Record
    [pscustomobject]@{
        Service=$Name
        Pid=$Record.pid
        State=$(if ($Check.valid) {'RUNNING'} else {'MISMATCH'})
        Detail=$Check.reason
    }
}
function Get-RecentErrors {
    param([string]$Name)
    $Record = Read-Record $Name
    $Path = if ($null -ne $Record -and $Record.stderr) { [string]$Record.stderr } else {
        $Latest = Get-ChildItem -LiteralPath $LogRoot -Filter "$Name`_*.stderr.log" `
            -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
        if ($null -ne $Latest) { $Latest.FullName } else { $null }
    }
    Write-Output "[$Name] $Path"
    if ($null -eq $Path -or -not (Test-Path -LiteralPath $Path)) {
        Write-Output '(no stderr log)'
    } else {
        $Lines = @(Get-Content -LiteralPath $Path -Tail 10)
        if ($Lines.Count) { $Lines } else { Write-Output '(stderr log is empty)' }
    }
}

$Processes = @(
    Get-ServiceStatus 'broker' $BrokerAddress $BrokerPort
    Get-ServiceStatus 'main' '127.0.0.1' $AdminPort
    Get-ServiceStatus 'api' $ApiAddress $ApiPort
)
$Endpoints = @(
    [pscustomobject]@{Service='Broker LAN';Endpoint="${BrokerAddress}:$BrokerPort";Owner=(Get-LiveEndpointOwner $BrokerAddress $BrokerPort).owning_process},
    [pscustomobject]@{Service='Broker loopback (unmanaged)';Endpoint="127.0.0.1:$BrokerPort";Owner=(Get-LiveEndpointOwner '127.0.0.1' $BrokerPort).owning_process},
    [pscustomobject]@{Service='Main';Endpoint="127.0.0.1:$AdminPort";Owner=(Get-LiveEndpointOwner '127.0.0.1' $AdminPort).owning_process},
    [pscustomobject]@{Service='API';Endpoint="${ApiAddress}:$ApiPort";Owner=(Get-LiveEndpointOwner $ApiAddress $ApiPort).owning_process}
)
try {
    $Health = Invoke-RestMethod "http://${ApiAddress}:$ApiPort/api/health" -TimeoutSec 3
} catch { $Health = [pscustomobject]@{error='API_HEALTH_UNAVAILABLE';detail=$_.Exception.Message} }
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    $DbStatus = [pscustomobject]@{error='CCTV_TEST_ADMIN_TOKEN_NOT_SET'}
} else {
    try {
        $DbStatus = Invoke-RestMethod `
            "http://${ApiAddress}:$ApiPort/api/admin/database/status" `
            -Headers @{Authorization="Bearer $AdminToken"} -TimeoutSec 5
    } catch {
        $DbStatus = [pscustomobject]@{error='ADMIN_DATABASE_STATUS_UNAVAILABLE';detail=$_.Exception.Message}
    }
}
Write-Output 'Processes / verified PID records'
$Processes | Format-Table -AutoSize
Write-Output 'Exact endpoint owners'
$Endpoints | Format-Table -AutoSize
Write-Output 'API health'
$Health | ConvertTo-Json -Depth 5
Write-Output 'Admin database status'
$DbStatus | ConvertTo-Json -Depth 5
Write-Output 'Recent stderr (last 10 lines per service)'
foreach ($Name in @('broker','main','api')) { Get-RecentErrors $Name }
$Overall = if (
    ($Processes.State -notcontains 'MISMATCH') -and
    ($Processes.State -notcontains 'NOT_RECORDED') -and
    $Health.status -eq 'ok' -and $DbStatus.integrity_check -eq 'ok'
) {'READY'} else {'NOT_READY'}
Write-Output "Overall: $Overall"
