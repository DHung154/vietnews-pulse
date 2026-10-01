param(
  [string]$PythonExecutable = "python",
  [int]$TimeoutSeconds = 540
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $ProjectRoot
$LogRoot = Join-Path $ProjectRoot "runtime\data_collection_v2\logs"
New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null
$LogPath = Join-Path $LogRoot ("collector_{0}.log" -f (Get-Date -Format "yyyyMMdd"))
$started = Get-Date
Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value ("START local={0} pid={1} python_arg={2} cwd={3}" -f $started.ToString("o"), $PID, $PythonExecutable, (Get-Location).Path)

try {
  $Python = (Get-Command $PythonExecutable -ErrorAction Stop).Source
  Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value ("PYTHON resolved={0}" -f $Python)
  & $Python "scripts\collector_runner.py" --timeout-seconds $TimeoutSeconds |
    ForEach-Object { Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value $_ }
  $exitCode = $LASTEXITCODE
} catch {
  Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value ("ERROR type={0} message={1}" -f $_.Exception.GetType().FullName, $_.Exception.Message)
  $exitCode = 1
}
$finished = Get-Date
Add-Content -LiteralPath $LogPath -Encoding UTF8 -Value ("END local={0} exit_code={1} duration_seconds={2}" -f $finished.ToString("o"), $exitCode, [math]::Round(($finished - $started).TotalSeconds, 3))
exit $exitCode
