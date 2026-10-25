"""Read-only HDFS, Kafka and Airflow evidence, with bounded CLI calls."""
import json
import subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'evidence/completion_20261016_v1/services'
out.mkdir(parents=True,exist_ok=False)
docker=['docker','--config',str(ROOT/'runtime/docker-config')]
hdfs=docker+['exec','bigdata-namenode-1','hdfs']
target='/news/shared/rss_live_v2/snapshot_20261008T090238Z_81c6a670'
kafka=docker+['exec','vietnews-lab-kafka-1']
airflow=docker+['exec','vietnews-lab-airflow-1','airflow']
commands={
    'containers':docker+['ps','--format','{{.Names}} {{.Status}}'],
    'hdfs_ls':hdfs+['dfs','-ls',target],
    'hdfs_du':hdfs+['dfs','-du','-h',target],
    'hdfs_count':hdfs+['dfs','-count',target],
    'hdfs_fsck':hdfs+['fsck',target,'-files','-blocks','-locations'],
    'kafka_topic':kafka+['/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--describe','--topic','vietnews-lab-airflow_c1b767d12036570e'],
    'kafka_offsets':kafka+['/opt/kafka/bin/kafka-get-offsets.sh','--bootstrap-server','localhost:29092','--topic','vietnews-lab-airflow_c1b767d12036570e'],
    'airflow_import_errors':airflow+['dags','list-import-errors','--output','json'],
    'airflow_runs':airflow+['dags','list-runs','-d','vietnews_nine_steps','--output','json'],
}
results=[]
for name,command in commands.items():
    try:
        result=subprocess.run(command,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=40)
        code,log=result.returncode,result.stdout+result.stderr
    except subprocess.TimeoutExpired:
        code,log=None,'Timed out after 40 seconds; no service stopped.'
    with (out/(name+'.log')).open('x',encoding='utf-8') as handle:
        handle.write(log)
    results.append(dict(name=name,command=command,exit_code=code))
    print(name,code,flush=True)
with (out/'commands.json').open('x',encoding='utf-8') as handle:
    json.dump(results,handle,indent=2)
raise SystemExit(0 if all(r['exit_code']==0 for r in results) else 1)
