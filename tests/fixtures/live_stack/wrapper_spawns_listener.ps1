param(
    [Parameter(Mandatory)][string]$PythonPath,
    [Parameter(Mandatory)][string]$ListenerPath,
    [Parameter(Mandatory)][string]$Address,
    [Parameter(Mandatory)][int]$Port
)
$ErrorActionPreference = 'Stop'
$Child = Start-Process -FilePath $PythonPath -ArgumentList @(
    $ListenerPath, '--host', $Address, '--port', [string]$Port
) -WindowStyle Hidden -PassThru
$Child.WaitForExit()
exit $Child.ExitCode
