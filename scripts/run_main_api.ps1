param(
    [string]$HostAddress = "0.0.0.0",
    [int]$Port = 8080,
    [string]$DatabasePath = "data/main_server.db",
    [string]$CorsOrigin = "*"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Main virtualenv Python not found: $Python"
}

$ResolvedDatabase = if ([System.IO.Path]::IsPathRooted($DatabasePath)) {
    $DatabasePath
} else {
    Join-Path $ProjectRoot $DatabasePath
}

if (-not (Test-Path -LiteralPath $ResolvedDatabase)) {
    throw "Main database not found: $ResolvedDatabase"
}

Set-Location -LiteralPath $ProjectRoot
& $Python -m cctv_main.api_server `
    --host $HostAddress `
    --port $Port `
    --db $ResolvedDatabase `
    --cors-origin $CorsOrigin
