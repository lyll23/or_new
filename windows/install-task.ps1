[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)][string]$DataRoot,
    [Parameter(Mandatory = $true)][string]$PythonPath,
    [string]$Repository = 'lyll23/or_new',
    [string]$TaskName = 'OpenRouter research data sync'
)
$ErrorActionPreference = 'Stop'
$resolvedDataRoot = [IO.Path]::GetFullPath($DataRoot)
$resolvedPython = [IO.Path]::GetFullPath($PythonPath)
$syncScript = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot 'sync.ps1'))
if (-not (Test-Path -LiteralPath $resolvedPython -PathType Leaf)) { throw 'Python executable not found.' }
if ($Repository -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw 'Invalid repository name.' }
foreach ($value in @($resolvedDataRoot, $resolvedPython, $syncScript, $Repository)) {
    if ($value.Contains('"') -or $value.Contains("`r") -or $value.Contains("`n")) { throw 'Paths may not contain quotes or newlines.' }
}
$pythonVersion = & $resolvedPython -c 'import sys; print(str(sys.version_info.major)+chr(46)+str(sys.version_info.minor))'
if ($LASTEXITCODE -ne 0 -or [version]$pythonVersion -lt [version]'3.11') { throw 'Python 3.11 or newer is required.' }
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$taskArguments = '-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}" -DataRoot "{1}" -PythonPath "{2}" -Repository "{3}"' -f $syncScript, $resolvedDataRoot, $resolvedPython, $Repository
$powershellExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$action = New-ScheduledTaskAction -Execute $powershellExe -Argument $taskArguments -WorkingDirectory (Split-Path -Parent $PSScriptRoot)
$logon = New-ScheduledTaskTrigger -AtLogOn -User $identity
# Omitting RepetitionDuration makes the hourly trigger repeat indefinitely.
$hourly = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) -RepetitionInterval (New-TimeSpan -Hours 1)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 3)
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
$task = New-ScheduledTask -Action $action -Trigger @($logon, $hourly) -Settings $settings -Principal $principal -Description 'Read public monthly GitHub Releases without a token; verify and retain originals locally, then retry incremental ingestion.'
if ($PSCmdlet.ShouldProcess($TaskName, 'Register current-user login and hourly data sync task')) {
    Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
    Write-Output ('Installed task: ' + $TaskName)
    Write-Output ('Local permanent storage: ' + $resolvedDataRoot)
}
