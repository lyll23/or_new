[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$DataRoot,
    [Parameter(Mandatory = $true)][string]$PythonPath,
    [string]$Repository = 'lyll23/or_new'
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$resolvedDataRoot = [IO.Path]::GetFullPath($DataRoot)
$resolvedPython = [IO.Path]::GetFullPath($PythonPath)
if (-not (Test-Path -LiteralPath $resolvedPython -PathType Leaf)) { throw 'Python executable not found.' }
if ($Repository -notmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$') { throw 'Invalid repository name.' }
$env:PYTHONPATH = Join-Path $repoRoot 'src'
$env:PYTHONUTF8 = '1'
# No token is requested, read, or persisted by this task.
$logDir = Join-Path $resolvedDataRoot '同步记录\计划任务日志'
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$logFile = Join-Path $logDir ((Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N') + '.log')
try {
    & $resolvedPython -X utf8 -m or_pipeline.sync --repository $Repository --root $resolvedDataRoot *> $logFile
    $syncExitCode = $LASTEXITCODE
    exit $syncExitCode
} catch {
    $_ | Out-File -LiteralPath $logFile -Append -Encoding utf8
    exit 1
}
