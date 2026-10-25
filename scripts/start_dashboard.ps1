param(
    [Parameter(Mandatory = $true)][string]$ServingRoot,
    [Parameter(Mandatory = $true)][string]$BulletinPath,
    [string]$PythonExecutable = 'python',
    [int]$Port = 8502
)
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$servingPath = (Resolve-Path -LiteralPath $ServingRoot).Path
$bulletinFile = (Resolve-Path -LiteralPath $BulletinPath).Path
$pointer = Get-Content -LiteralPath (Join-Path $servingPath 'current.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$bulletin = Get-Content -LiteralPath $bulletinFile -Raw -Encoding UTF8 | ConvertFrom-Json
$databasePath = Join-Path $servingPath $pointer.db_path
if (-not (Test-Path -LiteralPath $databasePath -PathType Leaf)) { throw 'Missing serving SQLite' }
if ($bulletin.snapshot_id -ne $pointer.snapshot_id -or $bulletin.model_version -ne $pointer.model_version) {
    throw 'Bulletin does not match serving snapshot/model'
}
$listeners = [System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
if ($listeners.Port -contains $Port) {
    throw "Port $Port is already owned by another process; no process was stopped"
}
$pythonPath = (Get-Command $PythonExecutable -ErrorAction Stop).Source
$logRoot = Join-Path $projectRoot ('runtime/dashboard_launch_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ') + '_' + [Guid]::NewGuid().ToString('N').Substring(0,8))
New-Item -ItemType Directory -Path $logRoot -ErrorAction Stop | Out-Null
$previousServing = $env:NEWS_SERVING_ROOT
$previousBulletin = $env:NEWS_EVENT_BULLETIN_PATH
try {
    $env:NEWS_SERVING_ROOT = $servingPath
    $env:NEWS_EVENT_BULLETIN_PATH = $bulletinFile
    $arguments = @('-m','streamlit','run','dashboard/app.py','--server.headless','true','--server.address','127.0.0.1','--server.port',"$Port",'--browser.gatherUsageStats','false')
    $process = Start-Process -FilePath $pythonPath -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $logRoot 'stdout.log') -RedirectStandardError (Join-Path $logRoot 'stderr.log')
    Write-Output "PID=$($process.Id) URL=http://localhost:$Port LOG=$logRoot SNAPSHOT=$($pointer.snapshot_id)"
} finally {
    $env:NEWS_SERVING_ROOT = $previousServing
    $env:NEWS_EVENT_BULLETIN_PATH = $previousBulletin
}
