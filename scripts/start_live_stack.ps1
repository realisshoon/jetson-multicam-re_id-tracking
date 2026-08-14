[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$BrokerAddress = '10.10.20.33',
    [int]$BrokerPort = 1883,
    [string]$ApiAddress = '10.10.20.33',
    [int]$ApiPort = 8080,
    [int]$AdminPort = 8091,
    [int]$ReadinessTimeoutSeconds = 30,
    [string]$PythonPath,
    [string]$MosquittoPath = 'C:\Program Files\mosquitto\mosquitto.exe',
    [string]$BrokerConfigPath
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = [IO.Path]::GetFullPath($ProjectRoot)
. (Join-Path $PSScriptRoot 'live_stack_common.ps1')

if ([string]::IsNullOrWhiteSpace($PythonPath)) {
    $PythonPath = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
}
if ([string]::IsNullOrWhiteSpace($BrokerConfigPath)) {
    $BrokerConfigPath = Join-Path $ProjectRoot 'configs\mosquitto.main-server.conf'
}
$Database = Join-Path $ProjectRoot 'data\main_server.db'
$LogRoot = Join-Path $ProjectRoot 'data\logs'
$RunRoot = Join-Path $ProjectRoot 'data\run'
$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'

foreach ($RequiredPath in @($PythonPath, $MosquittoPath, $BrokerConfigPath, $Database)) {
    if (-not (Test-Path -LiteralPath $RequiredPath)) {
        throw "Required live-stack path not found: $RequiredPath"
    }
}
$AdminToken = [Environment]::GetEnvironmentVariable(
    'CCTV_TEST_ADMIN_TOKEN', [EnvironmentVariableTarget]::User
)
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    throw 'User environment variable CCTV_TEST_ADMIN_TOKEN is not set.'
}
New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null
New-Item -ItemType Directory -Path $RunRoot -Force | Out-Null

function Get-RecordPath {
    param([string]$Name)
    Join-Path $RunRoot "$Name.pid.json"
}
function Read-Record {
    param([string]$Name)
    $Path = Get-RecordPath $Name
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}
function Save-Record {
    param([string]$Name, $Record)
    $Record | ConvertTo-Json -Depth 5 |
        Set-Content -LiteralPath (Get-RecordPath $Name) -Encoding utf8
}
function Wait-ApiHealth {
    param([string]$Uri, [int]$TimeoutSeconds)
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $Value = Invoke-RestMethod -Uri $Uri -TimeoutSec 2
            if ($Value.status -eq 'ok' -and $Value.database -eq 'ok') { return $Value }
        } catch { }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $Deadline)
    $null
}
function Wait-DatabaseStatus {
    param([string]$Uri, [string]$Token, [int]$TimeoutSeconds)
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $Value = Invoke-RestMethod -Uri $Uri -TimeoutSec 3 `
                -Headers @{ Authorization="Bearer $Token" }
            if ($Value.integrity_check -eq 'ok') { return $Value }
        } catch { }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $Deadline)
    $null
}
function Start-And-CaptureListener {
    param(
        [string]$Name, [string]$Address, [int]$Port,
        [string]$FilePath, [object[]]$ArgumentList
    )
    $ExistingRecord = Read-Record $Name
    if ($null -ne $ExistingRecord) {
        $EndpointMatches = (
            [string]$ExistingRecord.endpoint_address -eq $Address -and
            [int]$ExistingRecord.endpoint_port -eq $Port
        )
        if ($EndpointMatches) {
            $Check = Test-LiveProcessRecord $ExistingRecord
            if ($Check.valid) { return $ExistingRecord }
        }
    }
    $ExistingOwner = Get-LiveEndpointOwner -Address $Address -Port $Port
    if ($null -ne $ExistingOwner) {
        throw "$Name endpoint is owned by an untracked/mismatched process: ${Address}:$Port PID=$($ExistingOwner.owning_process)"
    }
    $Out = Join-Path $LogRoot "$Name`_$Timestamp.stdout.log"
    $Err = Join-Path $LogRoot "$Name`_$Timestamp.stderr.log"
    $Launcher = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList `
        -WorkingDirectory $ProjectRoot -WindowStyle Hidden `
        -RedirectStandardOutput $Out -RedirectStandardError $Err -PassThru
    $script:Launchers.Add($Launcher) | Out-Null
    $Owner = Wait-LiveEndpointOwner -Address $Address -Port $Port `
        -TimeoutSeconds $ReadinessTimeoutSeconds
    if ($null -eq $Owner) {
        throw "$Name listener readiness failed: ${Address}:$Port; launcher PID=$($Launcher.Id)"
    }
    # The listener can be a child created by a Python/PowerShell wrapper. Always
    # persist the socket OwningProcess, never Start-Process.Id.
    $Record = New-LiveProcessRecord -Name $Name -Address $Address -Port $Port `
        -Stdout $Out -Stderr $Err
    Save-Record $Name $Record
    $script:StartedNames.Add($Name) | Out-Null
    return $Record
}
function Stop-StartedListeners {
    for ($Index = $StartedNames.Count - 1; $Index -ge 0; $Index--) {
        $Name = $StartedNames[$Index]
        $Record = Read-Record $Name
        if ($null -eq $Record) { continue }
        $Check = Test-LiveProcessRecord $Record
        if ($Check.valid) {
            Stop-Process -Id ([int]$Record.pid) -Force -ErrorAction SilentlyContinue
            Remove-Item -LiteralPath (Get-RecordPath $Name) -ErrorAction SilentlyContinue
        }
    }
    foreach ($Launcher in $Launchers) {
        if (-not $Launcher.HasExited) {
            Stop-Process -Id $Launcher.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

$env:CCTV_MAIN_DB = $Database
$env:CCTV_MQTT_CONFIG = Join-Path $ProjectRoot 'configs\mqtt.yaml'
$env:CCTV_IDENTITY_CONFIG = Join-Path $ProjectRoot 'configs\identity.yaml'
$env:CCTV_CAPTURE_CACHE_CONFIG = Join-Path $ProjectRoot 'configs\capture_cache.yaml'
$env:CCTV_JOURNEY_VALIDATION_CONFIG = Join-Path $ProjectRoot 'configs\journey_validation.yaml'
$env:MAIN_ADMIN_TOKEN = $AdminToken
$env:MAIN_ADMIN_CONTROL_PORT = [string]$AdminPort
$env:MAIN_ADMIN_CONTROL_URL = "http://127.0.0.1:$AdminPort"
$env:MAIN_ADMIN_BACKUP_ROOT = Join-Path $ProjectRoot 'data\backups\admin'
$env:MAIN_ADMIN_CONFIRMATION_TTL_SECONDS = '300'

$Launchers = [Collections.Generic.List[Diagnostics.Process]]::new()
$StartedNames = [Collections.Generic.List[string]]::new()
$Records = @{}
$CurrentService = 'Broker'
try {
    $Records.broker = Start-And-CaptureListener 'broker' $BrokerAddress $BrokerPort `
        $MosquittoPath @('-c', $BrokerConfigPath, '-v')
    $CurrentService = 'Main'
    $Records.main = Start-And-CaptureListener 'main' '127.0.0.1' $AdminPort `
        $PythonPath @('-m', 'cctv_main.main_server')
    $CurrentService = 'API'
    $Records.api = Start-And-CaptureListener 'api' $ApiAddress $ApiPort `
        $PythonPath @(
            '-m', 'cctv_main.api_server', '--host', $ApiAddress,
            '--port', [string]$ApiPort, '--db', $Database, '--cors-origin', '*'
        )
    $Health = Wait-ApiHealth "http://${ApiAddress}:$ApiPort/api/health" `
        $ReadinessTimeoutSeconds
    if ($null -eq $Health) { throw 'API /api/health readiness failed.' }
    $CurrentService = 'DB'
    $DbStatus = Wait-DatabaseStatus `
        "http://${ApiAddress}:$ApiPort/api/admin/database/status" `
        $AdminToken $ReadinessTimeoutSeconds
    if ($null -eq $DbStatus) { throw 'DB integrity readiness failed.' }

    Write-Output ('{0,-7} {1,-6} {2} PID={3}' -f 'Broker','READY',"${BrokerAddress}:$BrokerPort",$Records.broker.pid)
    Write-Output ('{0,-7} {1,-6} {2} PID={3}' -f 'Main','READY',"127.0.0.1:$AdminPort",$Records.main.pid)
    Write-Output ('{0,-7} {1,-6} {2} PID={3}' -f 'API','READY',"${ApiAddress}:$ApiPort",$Records.api.pid)
    Write-Output ('{0,-7} {1,-6} {2}' -f 'DB','READY',"integrity_check=$($DbStatus.integrity_check)")
} catch {
    Write-Output "Live stack FAILED at $CurrentService`: $($_.Exception.Message)"
    foreach ($Name in @('broker','main','api')) {
        $Record = $Records[$Name]
        if ($null -eq $Record) { $Record = Read-Record $Name }
        if ($null -ne $Record) {
            Write-Output "$Name stdout: $($Record.stdout)"
            Write-Output "$Name stderr: $($Record.stderr)"
        }
    }
    Stop-StartedListeners
    throw
}
