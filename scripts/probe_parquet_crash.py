"""Kill only the owned fixture writer after HDFS staging exists, keep partials."""
import hashlib
import json
import subprocess
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'evidence/fault_parquet_20261016_v1'
out.mkdir(parents=True,exist_ok=False)
work=ROOT/'runtime/fault_parquet_20261016_v1'
work.mkdir(exist_ok=False)
base=['docker','--config',str(ROOT/'runtime/docker-config')]
source=ROOT/'runtime/dashboard_refresh_20261008T084631Z_r2/serving'
pointer=source/'current.json'
db=source/json.loads(pointer.read_text())['db_path']
before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (pointer,db)}
commands=[]
def call(label,args,timeout=40):
    result=subprocess.run(args,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    with (out/(label+'.log')).open('x',encoding='utf-8') as handle:
        handle.write(result.stdout+result.stderr)
    commands.append(dict(label=label,command=args,exit_code=result.returncode))
    return result
common=['--network','bigdata_default','--cpus','1','--memory','1g','--entrypoint','python','--env','HADOOP_USER_NAME=hadoop','--env','PYSPARK_SUBMIT_ARGS=--driver-memory 512m pyspark-shell','--mount',f'type=bind,source={ROOT},target=/app,readonly','--mount',f'type=bind,source={work},target=/app/runtime/fault_parquet_20261016_v1','bigdata-spark-dev','/app/scripts/fault_parquet_writer.py']
name='vietnews-owned-fault-parquet-20261016'
start=call('start',base+['run','-d','--name',name]+common+['--attempt','1','--slow'])
assert start.returncode==0
target='/news/fault_tests/fault_parquet_20261016_v1/attempt_1'
found=False
for i in range(35):
    result=call(f'staging_{i:02d}',base+['exec','bigdata-namenode-1','hdfs','dfs','-ls','-R',target])
    if '_temporary' in result.stdout and '.parquet' in result.stdout:
        found=True
        break
    time.sleep(1)
if not found:
    raise SystemExit('Could not confirm write staging; no crash proof claimed')
assert call('kill_owned_writer',base+['kill','--signal','KILL',name]).returncode==0
state=call('writer_state',base+['inspect',name,'--format','{{json .State}}'])
call('writer_log',base+['logs',name])
success=call('success_marker_after_crash',base+['exec','bigdata-namenode-1','hdfs','dfs','-test','-e',target+'/_SUCCESS'])
assert success.returncode==1, 'crashed write unexpectedly committed'
call('preserved_partial',base+['exec','bigdata-namenode-1','hdfs','dfs','-ls','-R',target])
retry=call('retry_new_attempt',base+['run','--rm','--name','vietnews-owned-fault-retry-20261016']+common+['--attempt','2'],180)
assert retry.returncode==0
after={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (pointer,db)}
assert before==after
result=dict(status='ok',data_type='SYNTHETIC',staging_observed=True,crashed_container_state=json.loads(state.stdout),crashed_success_marker=False,retry=json.loads((work/'attempt_2.json').read_text()),production_hashes_unchanged=True,production_hashes=after,limitations=['Batch Parquet crash and new-attempt recovery, not streaming checkpoint recovery.', 'No production publisher was run; this is not proof of all crash-during-publish paths.', 'Partial HDFS attempt1 and stopped fixture container retained.'])
with (out/'result.json').open('x',encoding='utf-8') as handle:
    json.dump(result,handle,indent=2)
with (out/'commands.json').open('x',encoding='utf-8') as handle:
    json.dump(commands,handle,indent=2)
print(json.dumps(result),flush=True)
