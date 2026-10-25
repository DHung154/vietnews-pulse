"""Enable hourly DAG and retain three real full-snapshot runs; never fake states."""
import hashlib
import json
import subprocess
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'evidence/completion_full_20261016_v1'
out.mkdir(parents=True,exist_ok=False)
base=['docker','--config',str(ROOT/'runtime/docker-config')]
cli=base+['exec','vietnews-lab-airflow-1','airflow']
commands=[]


def command(name,args,timeout=30):
    run=subprocess.run(args,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    with (out/(name+'.log')).open('x',encoding='utf-8') as handle:
        handle.write(run.stdout+run.stderr)
    commands.append(dict(name=name,command=args,exit_code=run.returncode))
    with (out/'commands.json').open('w',encoding='utf-8') as handle:
        json.dump(commands,handle,indent=2)
    return run


def arrays(text):
    decoder=json.JSONDecoder()
    for index,char in enumerate(text):
        if char=='[':
            try:
                value,_=decoder.raw_decode(text[index:])
                if isinstance(value,list) and all(isinstance(row,dict) and 'dag_id' in row for row in value):
                    return value
            except ValueError:
                pass
    raise ValueError('No DAG run JSON array found')


unpause=command('unpause',cli+['dags','unpause','vietnews_full_snapshot'])
if unpause.returncode:
    raise SystemExit(unpause.returncode)
for i in (1,2):
    run=command('trigger_'+str(i),cli+['dags','trigger','-r',f'completion_full_20261016_{i:02d}','vietnews_full_snapshot'])
    if run.returncode:
        raise SystemExit(run.returncode)

deadline=time.monotonic()+2400
delivered=set()
poll=0
while time.monotonic()<deadline:
    poll+=1
    response=command(f'runs_{poll:03d}',cli+['dags','list-runs','-d','vietnews_full_snapshot','--output','json'])
    if response.returncode:
        raise SystemExit(response.returncode)
    rows=arrays(response.stdout)
    print('poll',poll,[(r['run_id'],r['state']) for r in rows],flush=True)
    candidates=[r for r in rows if r['run_id'].startswith(('completion_full_20261016_','scheduled__'))]
    for row in candidates:
        run_id=row['run_id']
        if row['state'] not in {'success','failed'} or run_id in delivered:
            continue
        tag=hashlib.sha256(run_id.encode()).hexdigest()[:20]
        states=command('tasks_'+tag,cli+['tasks','states-for-dag-run','vietnews_full_snapshot',run_id,'--output','json'])
        local=ROOT/'runtime/nine_steps'/('full_'+tag)
        if local.is_dir():
            for path in local.glob('*.json'):
                target=out/('full_'+tag)
                target.mkdir(exist_ok=True)
                (target/path.name).write_bytes(path.read_bytes())
        dest=out/('task_logs_'+tag)
        command('copy_logs_'+tag,base+['cp',f'vietnews-lab-airflow-1:/opt/airflow/logs/dag_id=vietnews_full_snapshot/run_id={run_id}',str(dest)])
        delivered.add(run_id)
    successful=[r for r in candidates if r['state']=='success']
    if len(successful)>=3:
        summary=dict(status='ok',runs=successful,scheduled_runs=sum(r['run_id'].startswith('scheduled__') for r in successful),production_publish_performed=False)
        with (out/'summary.json').open('x',encoding='utf-8') as handle:
            json.dump(summary,handle,indent=2)
        print(json.dumps(summary),flush=True)
        break
    if any(r['state']=='failed' for r in candidates):
        raise SystemExit('DAG failed; outputs/logs preserved, do not clear tasks to overwrite')
    time.sleep(15)
else:
    raise SystemExit('Timed out: schedule/runs not proved; no services stopped or evidence removed')
