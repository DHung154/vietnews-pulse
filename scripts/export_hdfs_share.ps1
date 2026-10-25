param(
    [Parameter(Mandatory=$true)][string]$HdfsPath,
    [Parameter(Mandatory=$true)][string]$Name
)
$ErrorActionPreference = 'Stop'
if ($Name -notmatch '^[a-zA-Z0-9_-]+$') { throw 'Name must contain only letters, numbers, hyphen or underscore' }
if ($HdfsPath -notmatch '^/[a-zA-Z0-9_./-]+$' -or $HdfsPath.Contains('..')) { throw 'Invalid HDFS path' }
Set-Location (Split-Path $PSScriptRoot -Parent)
$destination = Join-Path (Get-Location) "runtime/shared_downloads/$Name.tar.gz"
if (Test-Path -LiteralPath $destination) { throw 'Snapshot already exists; use a new name' }
New-Item -ItemType Directory -Force -Path 'runtime/shared_downloads' | Out-Null
$stage = '/tmp/share_' + [guid]::NewGuid().ToString('N')
docker compose exec -T namenode mkdir -p $stage
if ($LASTEXITCODE -ne 0) { throw 'Cannot create container staging directory' }
docker compose exec -T namenode hdfs dfs -get $HdfsPath "$stage/data"
if ($LASTEXITCODE -ne 0) { throw 'HDFS export failed' }
docker compose exec -T namenode tar -czf "$stage/snapshot.tar.gz" -C $stage data
if ($LASTEXITCODE -ne 0) { throw 'Archive failed' }
$container = docker compose ps -q namenode
docker cp "${container}:$stage/snapshot.tar.gz" "$destination.partial"
if ($LASTEXITCODE -ne 0) { throw 'Archive download failed' }
Move-Item -LiteralPath "$destination.partial" -Destination $destination
$checksum = (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant()
@{hdfs_path=$HdfsPath; archive="$Name.tar.gz"; sha256=$checksum; exported_at=(Get-Date).ToUniversalTime().ToString('o'); sentiment='model predictions, not human gold'} | ConvertTo-Json | Set-Content -Encoding UTF8 "$destination.manifest.json"
Write-Output "Exported $destination"
# Container staging is retained for diagnosis; no production HDFS files are changed.
