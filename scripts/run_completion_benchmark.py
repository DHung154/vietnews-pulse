"""Run isolated local/standalone measurements and retain exact command logs."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
out = ROOT/'evidence/completion_20261016_v1/benchmark'
out.mkdir(parents=True, exist_ok=False)
base = ['docker','--config',str(ROOT/'runtime/docker-config')]
commands = []
for label, master in [('local','local[*]'),('standalone','spark://spark-master:7077')]:
    name = 'vietnews-bench-20261016-' + label
    command = base + ['run','--rm','--name',name,'--hostname',name,'--network','bigdata_default',
        '--cpus','1','--memory','2g','--entrypoint','python','--env','HADOOP_USER_NAME=hadoop',
        '--env','JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=1',
        '--env',f'PYSPARK_SUBMIT_ARGS=--driver-memory 768m --conf spark.driver.host={name} --conf spark.driver.bindAddress=0.0.0.0 pyspark-shell',
        '--mount',f'type=bind,source={ROOT},target=/app', 'bigdata-spark-dev','/app/scripts/benchmark_snapshot.py',
        '--master', master, '--input','hdfs://namenode:8020/news/shared/rss_live_v2/snapshot_20261008T090238Z_81c6a670/clean',
        '--output','/app/runtime/completion_benchmark_20261016_'+label+'_v1', '--run-id','completion_20261016_'+label+'_v1']
    with (out/(label+'.log')).open('x',encoding='utf-8') as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            code = process.wait(timeout=600)
        except subprocess.TimeoutExpired:
            # Only our named benchmark container is stopped; no shared services touched.
            subprocess.run(base+['stop','--time','10',name],cwd=ROOT,timeout=30,check=False)
            process.wait(timeout=30)
            code = 124
    commands.append(dict(label=label, command=command,exit_code=code))
    with (out/'commands.json').open('w',encoding='utf-8') as handle:
        json.dump(commands,handle,indent=2)
    print(label, 'exit', code, flush=True)
raise SystemExit(0 if all(r['exit_code']==0 for r in commands) else 1)
