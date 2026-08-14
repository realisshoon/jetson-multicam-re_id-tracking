$LiveStackCreationTimeToleranceSeconds = 2.0

function ConvertTo-LiveUtcTimestamp {
    param([Parameter(Mandatory)]$Value)
    return ([datetime]$Value).ToUniversalTime().ToString(
        'yyyy-MM-ddTHH:mm:ss.fffffffZ',
        [Globalization.CultureInfo]::InvariantCulture
    )
}

function Normalize-LiveCommandLine {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return $null }
    return (($Value -replace '\s+', ' ').Trim())
}

function Normalize-LiveExecutablePath {
    param([AllowNull()][string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $null }
    try { return [IO.Path]::GetFullPath($Value) } catch { return $Value.Trim() }
}

function Get-LiveEndpointOwner {
    param(
        [Parameter(Mandatory)][string]$Address,
        [Parameter(Mandatory)][int]$Port
    )
    $Connections = @(
        Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Where-Object { [string]$_.LocalAddress -eq $Address }
    )
    if ($Connections.Count -gt 1) {
        throw "Multiple listeners found for ${Address}:$Port"
    }
    if ($Connections.Count -eq 0) { return $null }
    return [pscustomobject]@{
        address = $Address
        port = $Port
        owning_process = [int]$Connections[0].OwningProcess
    }
}

function Get-LiveProcessMetadata {
    param([Parameter(Mandatory)][int]$ProcessId)
    $Cim = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" `
        -ErrorAction SilentlyContinue
    if ($null -eq $Cim) { return $null }
    $Process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    $ExecutablePath = Normalize-LiveExecutablePath $Cim.ExecutablePath
    if ($null -eq $ExecutablePath -and $null -ne $Process) {
        $ExecutablePath = Normalize-LiveExecutablePath $Process.Path
    }
    return [pscustomobject]@{
        pid = $ProcessId
        executable_path = $ExecutablePath
        command_line = Normalize-LiveCommandLine $Cim.CommandLine
        creation_time_utc = ConvertTo-LiveUtcTimestamp $Cim.CreationDate
    }
}

function New-LiveProcessRecord {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$Address,
        [Parameter(Mandatory)][int]$Port,
        [AllowNull()][string]$Stdout,
        [AllowNull()][string]$Stderr
    )
    $Owner = Get-LiveEndpointOwner -Address $Address -Port $Port
    if ($null -eq $Owner) {
        throw "Listener owner not found: ${Address}:$Port"
    }
    $Metadata = Get-LiveProcessMetadata -ProcessId $Owner.owning_process
    if ($null -eq $Metadata) {
        throw "Listener process metadata unavailable: PID=$($Owner.owning_process)"
    }
    return [ordered]@{
        name = $Name
        pid = $Metadata.pid
        executable_path = $Metadata.executable_path
        command_line = $Metadata.command_line
        creation_time_utc = $Metadata.creation_time_utc
        endpoint_address = $Address
        endpoint_port = $Port
        stdout = $Stdout
        stderr = $Stderr
        recorded_at_utc = ConvertTo-LiveUtcTimestamp (Get-Date)
    }
}

function Test-LiveProcessRecord {
    param([Parameter(Mandatory)]$Record)
    if ($null -eq $Record -or $null -eq $Record.pid) {
        return [pscustomobject]@{ valid=$false; reason='INVALID_PID_RECORD' }
    }
    if (
        [string]::IsNullOrWhiteSpace([string]$Record.endpoint_address) -or
        $null -eq $Record.endpoint_port -or
        [string]::IsNullOrWhiteSpace([string]$Record.executable_path) -or
        [string]::IsNullOrWhiteSpace([string]$Record.command_line) -or
        [string]::IsNullOrWhiteSpace([string]$Record.creation_time_utc)
    ) {
        return [pscustomobject]@{ valid=$false; reason='PID_RECORD_SCHEMA_MISMATCH' }
    }
    $Owner = Get-LiveEndpointOwner `
        -Address ([string]$Record.endpoint_address) `
        -Port ([int]$Record.endpoint_port)
    if ($null -eq $Owner) {
        return [pscustomobject]@{ valid=$false; reason='ENDPOINT_NOT_LISTENING' }
    }
    if ([int]$Owner.owning_process -ne [int]$Record.pid) {
        return [pscustomobject]@{
            valid=$false
            reason='ENDPOINT_OWNER_MISMATCH'
            actual_pid=[int]$Owner.owning_process
        }
    }
    $Metadata = Get-LiveProcessMetadata -ProcessId ([int]$Record.pid)
    if ($null -eq $Metadata) {
        return [pscustomobject]@{ valid=$false; reason='PROCESS_NOT_FOUND' }
    }
    $ExpectedExecutable = Normalize-LiveExecutablePath $Record.executable_path
    if (-not [string]::Equals(
        $ExpectedExecutable,
        $Metadata.executable_path,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        return [pscustomobject]@{ valid=$false; reason='EXECUTABLE_MISMATCH' }
    }
    $ExpectedCommand = Normalize-LiveCommandLine $Record.command_line
    if (-not [string]::Equals(
        $ExpectedCommand,
        $Metadata.command_line,
        [StringComparison]::Ordinal
    )) {
        return [pscustomobject]@{ valid=$false; reason='COMMAND_LINE_MISMATCH' }
    }
    try {
        $ExpectedCreation = [datetime]::Parse(
            [string]$Record.creation_time_utc,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal -bor
            [Globalization.DateTimeStyles]::AdjustToUniversal
        )
        $ActualCreation = [datetime]::Parse(
            [string]$Metadata.creation_time_utc,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal -bor
            [Globalization.DateTimeStyles]::AdjustToUniversal
        )
    } catch {
        return [pscustomobject]@{ valid=$false; reason='CREATION_TIME_INVALID' }
    }
    $Delta = [Math]::Abs(($ActualCreation - $ExpectedCreation).TotalSeconds)
    if ($Delta -gt $LiveStackCreationTimeToleranceSeconds) {
        return [pscustomobject]@{
            valid=$false
            reason='CREATION_TIME_MISMATCH'
            delta_seconds=$Delta
        }
    }
    return [pscustomobject]@{
        valid=$true
        reason='MATCHED'
        pid=$Metadata.pid
        creation_time_delta_seconds=$Delta
    }
}

function Wait-LiveEndpointOwner {
    param(
        [Parameter(Mandatory)][string]$Address,
        [Parameter(Mandatory)][int]$Port,
        [Parameter(Mandatory)][int]$TimeoutSeconds
    )
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $Owner = Get-LiveEndpointOwner -Address $Address -Port $Port
        if ($null -ne $Owner) { return $Owner }
        Start-Sleep -Milliseconds 200
    } while ((Get-Date) -lt $Deadline)
    return $null
}
