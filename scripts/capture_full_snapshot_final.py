"""Read-only post-restart evidence for the full-snapshot acceptance run."""
import json
import argparse
import subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output',type=Path,default=ROOT/'evidence/completion_final_20261016_v2/services')
out=parser.parse_args().output.resolve()
if ROOT/'evidence' not in out.parents:
    parser.error('output must be below evidence')
out.mkdir(parents=True,exist_ok=False)
docker=['docker','--config',str(ROOT/'runtime/docker-config')]
airflow=docker+['exec','vietnews-lab-airflow-1']
topic=json.loads((ROOT/'evidence/completion_full_20261016_v1/full_2a5e58de5cb434258ff9/kafka.json').read_text())['topic']
commands={
    'health':airflow+['python','-c',"import json,urllib.request; print(json.dumps(json.load(urllib.request.urlopen('http://localhost:8080/health',timeout=5))))"],
    'runs':airflow+['airflow','dags','list-runs','-d','vietnews_full_snapshot','--output','json'],
    'schedule':airflow+['python','-c',"import json; from airflow.models import DagModel; d=DagModel.get_current('vietnews_full_snapshot'); print(json.dumps(dict(dag_id=d.dag_id,is_paused=d.is_paused,timezone=str(d.timezone),schedule=str(d.timetable_description),next_dagrun=str(d.next_dagrun))))"],
    'import_errors':airflow+['airflow','dags','list-import-errors','--output','json'],
    'kafka_topic':docker+['exec','vietnews-lab-kafka-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--describe','--topic',topic],
    'kafka_offsets':docker+['exec','vietnews-lab-kafka-1','/opt/kafka/bin/kafka-get-offsets.sh','--bootstrap-server','localhost:29092','--topic',topic],
    'http_dashboard':['curl.exe','--max-time','8','--fail','http://localhost:8502/_stcore/health'],
}
results=[]
for name,command in commands.items():
    result=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=50)
    with (out/(name+'.log')).open('x',encoding='utf-8') as handle:
        handle.write(result.stdout+result.stderr)
    results.append(dict(name=name,command=command,exit_code=result.returncode))
    print(name,result.returncode,flush=True)
with (out/'commands.json').open('x',encoding='utf-8') as handle:
    json.dump(results,handle,indent=2)
raise SystemExit(0 if all(r['exit_code']==0 for r in results) else 1)
