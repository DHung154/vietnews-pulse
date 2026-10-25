"""Capture stalled-stage evidence, then SIGTERM only the identified acceptance child."""
import json
import subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'evidence/completion_full_20261016_v1/dedup_stall.json'
assert not out.exists()
code=r'''
import json,os,signal,urllib.request
from pathlib import Path
url='http://localhost:4040/api/v1/applications'
apps=json.load(urllib.request.urlopen(url,timeout=5))
stages=json.load(urllib.request.urlopen(url+'/'+apps[0]['id']+'/stages?status=active',timeout=5))
assert any(s['numActiveTasks']==0 and s['numCompleteTasks']<s['numTasks'] for s in stages), 'stage not stalled as expected'
targets=[]
for folder in Path('/proc').iterdir():
    if folder.name.isdigit():
        try: args=(folder/'cmdline').read_bytes().decode().split(chr(0))
        except (OSError,UnicodeDecodeError): continue
        if '/app/scripts/full_snapshot_pipeline.py' in args and '--step' in args and args[args.index('--step')+1]=='dedup' and args[args.index('--run-id')+1]=='full_459915cb6b992d7246f6':
            targets.append((int(folder.name),args))
assert len(targets)==1, targets
pid,args=targets[0]
print(json.dumps(dict(applications=apps,active_stages=stages,owned_pid=pid,owned_command=args,action='SIGTERM acceptance dedup attempt1; preserve all artifacts, Airflow retry expected')),flush=True)
os.kill(pid,signal.SIGTERM)
'''
command=['docker','--config','runtime/docker-config','exec','vietnews-lab-airflow-1','python','-c',code]
result=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,timeout=30)
with out.open('x',encoding='utf-8') as handle:
    json.dump(dict(command=command,exit_code=result.returncode,stdout=result.stdout,stderr=result.stderr),handle,indent=2)
print(result.returncode,result.stdout,result.stderr)
raise SystemExit(result.returncode)
