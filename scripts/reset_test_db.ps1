[CmdletBinding()]
param(
    [string]$BaseUrl = "http://127.0.0.1:8080"
)

$ErrorActionPreference = 'Stop'

# Validate MAIN_ADMIN_TOKEN from environment
$AdminToken = $env:MAIN_ADMIN_TOKEN
if ([string]::IsNullOrWhiteSpace($AdminToken)) {
    Write-Host "[RESET ABORTED]" -ForegroundColor Red
    Write-Host "MAIN_ADMIN_TOKEN is not set" -ForegroundColor Red
    exit 1
}

function Invoke-AdminApiRequest {
    param(
        [Parameter(Mandatory=$true)][string]$Method,
        [Parameter(Mandatory=$true)][string]$Endpoint,
        [Parameter(Mandatory=$false)][object]$Body = $null,
        [int]$TimeoutSec = 30
    )

    $Uri = "$($BaseUrl.TrimEnd('/'))$Endpoint"
    $Headers = @{
        'Authorization' = "Bearer $AdminToken"
    }

    $Params = @{
        Method     = $Method
        Uri        = $Uri
        Headers    = $Headers
        TimeoutSec = $TimeoutSec
    }

    if ($null -ne $Body) {
        $JsonBody = $Body | ConvertTo-Json -Compress -Depth 10
        $Params['Body'] = [System.Text.Encoding]::UTF8.GetBytes($JsonBody)
        $Params['ContentType'] = 'application/json; charset=utf-8'
    }

    try {
        return (Invoke-RestMethod @Params)
    }
    catch {
        $StatusCode = $null
        $ResponseBody = $null

        if ($_.Exception -and $_.Exception.Response) {
            $Resp = $_.Exception.Response
            if ($Resp.PSObject.Properties['StatusCode']) {
                $StatusCode = [int]$Resp.StatusCode
            }
            if ($Resp.PSObject.Methods['GetResponseStream']) {
                try {
                    $Stream = $Resp.GetResponseStream()
                    if ($Stream) {
                        $Reader = [System.IO.StreamReader]::new($Stream, [System.Text.Encoding]::UTF8)
                        $ResponseBody = $Reader.ReadToEnd()
                        $Reader.Dispose()
                        $Stream.Dispose()
                    }
                } catch {}
            }
        }

        if (-not $ResponseBody -and $_.ErrorDetails -and $_.ErrorDetails.Message) {
            $ResponseBody = $_.ErrorDetails.Message
        }

        $Parsed = $null
        if ($ResponseBody) {
            try {
                $Parsed = $ResponseBody | ConvertFrom-Json
            } catch {}
        }

        $ErrorTag = if ($Parsed -and $Parsed.error) {
            $Parsed.error
        } elseif ($StatusCode) {
            "HTTP_$StatusCode"
        } else {
            "REQUEST_FAILED"
        }

        $Formatted = if ($StatusCode) { "$StatusCode $ErrorTag" } else { $ErrorTag }
        if ($Parsed -and $Parsed.detail) {
            $Formatted += " ($($Parsed.detail))"
        } elseif ($Parsed -and $Parsed.blocking_reason) {
            $Formatted += " ($($Parsed.blocking_reason))"
        }

        throw [System.InvalidOperationException]::new($Formatted)
    }
}

try {
    # Step 1 - Status
    try {
        $Status = Invoke-AdminApiRequest -Method "GET" -Endpoint "/api/admin/database/status"
    } catch {
        Write-Host "[RESET ABORTED]" -ForegroundColor Red
        Write-Host "Failed to connect to Admin API: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

    $IntegrityOk = ($null -ne $Status.integrity_check -and $Status.integrity_check.ToString().ToLower() -eq 'ok')
    $ActiveJourneys = if ($null -ne $Status.active_journey_count) { [int]$Status.active_journey_count } else { 0 }
    $ResetAllowed = ($null -ne $Status.reset_allowed -and [bool]$Status.reset_allowed -eq $true)
    $BlockingReason = $Status.blocking_reason

    if (-not $IntegrityOk -or $ActiveJourneys -gt 0 -or -not $ResetAllowed) {
        Write-Host "[RESET BLOCKED]" -ForegroundColor Red
        Write-Host ("Active Journey : {0}" -f $ActiveJourneys) -ForegroundColor Yellow
        Write-Host ("Reason         : {0}" -f $(if ($BlockingReason) { $BlockingReason } else { "INTEGRITY_OR_ACTIVE_JOURNEYS" })) -ForegroundColor Red
        exit 1
    }

    Write-Host "[1/5] Database status OK"

    # Step 2 - Backup
    try {
        $Backup = Invoke-AdminApiRequest -Method "POST" -Endpoint "/api/admin/database/backup" -Body @{}
    } catch {
        Write-Host "[RESET ABORTED]" -ForegroundColor Red
        Write-Host "Backup request failed: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

    $BackupId = $Backup.backup_id
    $BackupStatus = $Backup.status
    $BackupIntegrity = $Backup.integrity_check

    if ([string]::IsNullOrWhiteSpace($BackupId) -or $BackupStatus -ne "COMPLETED" -or $BackupIntegrity.ToString().ToLower() -ne "ok") {
        Write-Host "[RESET ABORTED]" -ForegroundColor Red
        Write-Host "Backup verification failed: Status=$BackupStatus, Integrity=$BackupIntegrity, ID=$BackupId" -ForegroundColor Red
        exit 1
    }

    Write-Host "[2/5] Backup completed"
    Write-Host "Backup ID: $BackupId"

    # Step 3 - Reset Preview
    try {
        $Preview = Invoke-AdminApiRequest -Method "POST" -Endpoint "/api/admin/database/reset/preview" -Body @{}
    } catch {
        Write-Host "[RESET ABORTED]" -ForegroundColor Red
        Write-Host "Reset preview failed: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

    if (-not $Preview.can_reset -or [string]::IsNullOrWhiteSpace($Preview.confirmation_id)) {
        Write-Host "[RESET BLOCKED]" -ForegroundColor Red
        Write-Host ("Active Journey : {0}" -f $Preview.active_journey_count) -ForegroundColor Yellow
        Write-Host ("Reason         : {0}" -f $(if ($Preview.blocking_reason) { $Preview.blocking_reason } else { "CANNOT_RESET" })) -ForegroundColor Red
        exit 1
    }

    $ConfirmationId = $Preview.confirmation_id
    Write-Host "[3/5] Reset preview ready"
    Write-Host ("Person          : {0}" -f $Preview.person_count)
    Write-Host ("Journey         : {0}" -f $Preview.journey_count)
    Write-Host ("Gallery         : {0}" -f $Preview.gallery_count)
    Write-Host ("Capture         : {0}" -f $Preview.capture_count)

    # Step 4 - Reset Execute
    $ExecuteBody = @{
        confirmation_id   = $ConfirmationId
        confirmation_text = "전체 데이터 초기화"
        capture_policy    = "ARCHIVE"
        force             = $false
    }

    try {
        $ExecuteResp = Invoke-AdminApiRequest -Method "POST" -Endpoint "/api/admin/database/reset/execute" -Body $ExecuteBody
    } catch {
        Write-Host "[RESET ABORTED]" -ForegroundColor Red
        Write-Host "Reset execute failed: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

    if (-not $ExecuteResp.accepted -or [string]::IsNullOrWhiteSpace($ExecuteResp.job_id)) {
        Write-Host "[RESET ABORTED]" -ForegroundColor Red
        Write-Host "Reset was not accepted by server." -ForegroundColor Red
        exit 1
    }

    $JobId = $ExecuteResp.job_id
    Write-Host "[4/5] Reset execute accepted"
    Write-Host "Job ID: $JobId"

    # Step 5 - Job Polling
    Write-Host "[5/5] Reset job polling..."
    $MaxWaitSeconds = 60
    $PollIntervalSeconds = 1
    $Elapsed = 0
    $FinalJob = $null
    $LastPrintedStatus = ""

    while ($Elapsed -lt $MaxWaitSeconds) {
        try {
            $Job = Invoke-AdminApiRequest -Method "GET" -Endpoint "/api/admin/database/jobs/$JobId"
        } catch {
            Write-Host "[RESET ABORTED]" -ForegroundColor Red
            Write-Host "Failed to query job status: $($_.Exception.Message)" -ForegroundColor Red
            exit 1
        }

        $CurrentStatus = $Job.status
        if ($CurrentStatus -ne $LastPrintedStatus) {
            Write-Host "Status: $CurrentStatus"
            $LastPrintedStatus = $CurrentStatus
        }

        if ($CurrentStatus -eq "COMPLETED") {
            $FinalJob = $Job
            break
        }
        elseif ($CurrentStatus -eq "FAILED") {
            Write-Host "[RESET FAILED]" -ForegroundColor Red
            Write-Host "Error   : $($Job.error)" -ForegroundColor Red
            Write-Host "History : $($Job.history | ConvertTo-Json -Compress)" -ForegroundColor Yellow
            exit 1
        }

        Start-Sleep -Seconds $PollIntervalSeconds
        $Elapsed += $PollIntervalSeconds
    }

    if ($null -eq $FinalJob) {
        Write-Host "[RESET TIMEOUT]" -ForegroundColor Red
        Write-Host "Job $JobId did not complete within $MaxWaitSeconds seconds." -ForegroundColor Red
        exit 1
    }

    # Step 6 - Post Reset Verification
    try {
        $PostStatus = Invoke-AdminApiRequest -Method "GET" -Endpoint "/api/admin/database/status"
    } catch {
        Write-Host "[RESET VERIFICATION FAILED]" -ForegroundColor Red
        Write-Host "Failed to fetch post-reset status: $($_.Exception.Message)" -ForegroundColor Red
        exit 1
    }

    $IsClean = (
        ([int]$PostStatus.person_count -eq 0) -and
        ([int]$PostStatus.journey_count -eq 0) -and
        ([int]$PostStatus.gallery_count -eq 0) -and
        ([int]$PostStatus.permanent_gallery_count -eq 0) -and
        ([int]$PostStatus.journey_gallery_count -eq 0) -and
        ([int]$PostStatus.capture_count -eq 0) -and
        ([int]$PostStatus.active_journey_count -eq 0) -and
        ($null -ne $PostStatus.integrity_check -and $PostStatus.integrity_check.ToString().ToLower() -eq "ok") -and
        ($PostStatus.database_status -eq "READY")
    )

    if ($IsClean) {
        Write-Host ""
        Write-Host "========================================"
        Write-Host "        CLEAN NEW TEST READY"
        Write-Host "========================================"
        Write-Host ""
        Write-Host ("Backup ID       : {0}" -f $BackupId)
        Write-Host ("Reset Job       : {0}" -f $JobId)
        Write-Host ("Integrity       : {0}" -f $PostStatus.integrity_check)
        Write-Host ""
        Write-Host ("Person          : {0}" -f $PostStatus.person_count)
        Write-Host ("Journey         : {0}" -f $PostStatus.journey_count)
        Write-Host ("Gallery         : {0}" -f $PostStatus.gallery_count)
        Write-Host ("Permanent       : {0}" -f $PostStatus.permanent_gallery_count)
        Write-Host ("Journey Gallery : {0}" -f $PostStatus.journey_gallery_count)
        Write-Host ("Capture         : {0}" -f $PostStatus.capture_count)
        Write-Host ("Active Journey  : {0}" -f $PostStatus.active_journey_count)
        Write-Host ""
        Write-Host "READY FOR NEW   : YES"
        Write-Host "========================================"
        exit 0
    } else {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "     RESET VERIFICATION FAILED" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        Write-Host ("Database Status : {0}" -f $PostStatus.database_status) -ForegroundColor Yellow
        Write-Host ("Integrity       : {0}" -f $PostStatus.integrity_check) -ForegroundColor Yellow
        Write-Host ("Person          : {0}" -f $PostStatus.person_count) -ForegroundColor Yellow
        Write-Host ("Journey         : {0}" -f $PostStatus.journey_count) -ForegroundColor Yellow
        Write-Host ("Gallery         : {0}" -f $PostStatus.gallery_count) -ForegroundColor Yellow
        Write-Host ("Permanent       : {0}" -f $PostStatus.permanent_gallery_count) -ForegroundColor Yellow
        Write-Host ("Journey Gallery : {0}" -f $PostStatus.journey_gallery_count) -ForegroundColor Yellow
        Write-Host ("Capture         : {0}" -f $PostStatus.capture_count) -ForegroundColor Yellow
        Write-Host ("Active Journey  : {0}" -f $PostStatus.active_journey_count) -ForegroundColor Yellow
        Write-Host ""
        Write-Host "READY FOR NEW   : NO" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "[UNEXPECTED ERROR]" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}
