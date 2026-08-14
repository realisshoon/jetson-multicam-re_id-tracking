[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$BrokerAddress = '10.10.20.33', [int]$BrokerPort = 1883,
    [string]$ApiAddress = '10.10.20.33', [int]$ApiPort = 8080,
    [int]$AdminPort = 8091, [int]$WaitSeconds = 10
)
$ErrorActionPreference = 'Stop'
$ProjectRoot = [IO.Path]::GetFullPath($ProjectRoot)
. (Join-Path $PSScriptRoot 'live_stack_common.ps1')
$RunRoot = Join-Path $ProjectRoot 'data\run'

function Get-RecordPath { param([string]$Name) Join-Path $RunRoot "$Name.pid.json" }
$Results = @()
foreach ($Name in @('api','main','broker')) {
    $Path = Get-RecordPath $Name
    if (-not (Test-Path -LiteralPath $Path)) {
        $Results += [pscustomobject]@{Service=$Name;Pid=$null;Result='PID_FILE_NOT_FOUND'}
        continue
    }
    $Record = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    $ExpectedEndpoint = switch ($Name) {
        'api' { @($ApiAddress, $ApiPort) }
        'main' { @('127.0.0.1', $AdminPort) }
        'broker' { @($BrokerAddress, $BrokerPort) }
    }
    if (
        [string]$Record.endpoint_address -ne [string]$ExpectedEndpoint[0] -or
        [int]$Record.endpoint_port -ne [int]$ExpectedEndpoint[1]
    ) {
        $Results += [pscustomobject]@{Service=$Name;Pid=$Record.pid;Result='ENDPOINT_RECORD_MISMATCH'}
        continue
    }
    $Check = Test-LiveProcessRecord $Record
    if (-not $Check.valid) {
        $Results += [pscustomobject]@{Service=$Name;Pid=$Record.pid;Result=$Check.reason}
        continue
    }
    Stop-Process -Id ([int]$Record.pid)
    $Deadline = (Get-Date).AddSeconds($WaitSeconds)
    do {
        Start-Sleep -Milliseconds 100
        $Owner = Get-LiveEndpointOwner -Address $Record.endpoint_address `
            -Port ([int]$Record.endpoint_port)
    } while ($null -ne $Owner -and (Get-Date) -lt $Deadline)
    if ($null -ne $Owner -and [int]$Owner.owning_process -eq [int]$Record.pid) {
        Stop-Process -Id ([int]$Record.pid) -Force
        $Result = 'FORCE_STOPPED_AFTER_TIMEOUT'
    } else { $Result = 'STOPPED' }
    Remove-Item -LiteralPath $Path
    $Results += [pscustomobject]@{Service=$Name;Pid=$Record.pid;Result=$Result}
}
$Results | Format-Table -AutoSize
Write-Output 'Endpoint owners after stop'
@(
    [pscustomobject]@{Service='Broker LAN';Endpoint="${BrokerAddress}:$BrokerPort";Owner=(Get-LiveEndpointOwner $BrokerAddress $BrokerPort).owning_process},
    [pscustomobject]@{Service='Main';Endpoint="127.0.0.1:$AdminPort";Owner=(Get-LiveEndpointOwner '127.0.0.1' $AdminPort).owning_process},
    [pscustomobject]@{Service='API';Endpoint="${ApiAddress}:$ApiPort";Owner=(Get-LiveEndpointOwner $ApiAddress $ApiPort).owning_process}
) | Format-Table -AutoSize
