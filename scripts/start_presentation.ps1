param([int]$Port = 8504)
$ErrorActionPreference='Stop'
$projectRoot=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if($Port -lt 1024 -or $Port -gt 65535){throw 'Invalid demo port'}
Set-Location -LiteralPath $projectRoot
$dockerConfig=Join-Path $projectRoot 'runtime/docker-config'
# Start existing containers only: preserve the verified VPN ports and all volumes.
$dockerProbe=Start-Process -FilePath (Get-Command docker).Source -ArgumentList @('--config',$dockerConfig,'info','--format','{{.ServerVersion}}') -WindowStyle Hidden -PassThru
if(-not $dockerProbe.WaitForExit(15000)){ $dockerProbe.Kill(); throw 'Docker info timeout. Open Docker Desktop and try again.' }
if($dockerProbe.ExitCode -ne 0){throw 'Docker unavailable. Open Docker Desktop, wait for Engine, then run again.'}
$containers=@('bigdata-namenode-1','bigdata-datanode-1','bigdata-datanode2-1','bigdata-datanode3-1','vietnews-lab-kafka-1','vietnews-lab-airflow-1')
foreach($containerName in $containers){
    & docker --config $dockerConfig inspect $containerName --format '{{.Name}}' | Out-Null
    if($LASTEXITCODE -ne 0){throw "Missing $containerName; follow README setup, no new volume was created"}
}
& docker --config $dockerConfig start @containers
if($LASTEXITCODE -ne 0){throw 'Service startup failed; existing data retained'}
$listeners=[System.Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners()
if($listeners.Port -contains $Port){
    Write-Output "Port $Port already in use. No process stopped. Verify http://localhost:$Port manually."
    return
}
$pythonPath=(Get-Command python).Source
$logRoot=Join-Path $projectRoot ('runtime/presentation_web_'+[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')+'_'+[guid]::NewGuid().ToString('N').Substring(0,8))
New-Item -ItemType Directory -Path $logRoot | Out-Null
$argsList=@('-m','streamlit','run','dashboard/demo.py','--server.headless','true','--server.address','127.0.0.1','--server.port',"$Port",'--browser.gatherUsageStats','false')
$process=Start-Process -FilePath $pythonPath -ArgumentList $argsList -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $logRoot 'stdout.log') -RedirectStandardError (Join-Path $logRoot 'stderr.log')
Write-Output "Demo PID=$($process.Id) URL=http://localhost:$Port LOG=$logRoot"
Write-Output 'Wait for HDFS/Kafka/Airflow healthy BEFORE the timed defence. Click Start once in the page to open the real terminal.'
