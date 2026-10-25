"""Manual isolated lab. No production scheduler, socket mount or secret defaults."""
import hashlib
import subprocess
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

STEPS=('prepare','upload','read','clean','sql','cluster','save','kafka','verify')


def execute_step(step, **context):
    run_id='airflow_'+hashlib.sha256(context['dag_run'].run_id.encode()).hexdigest()[:16]
    command=[sys.executable,'/app/scripts/nine_steps.py','--step',step,'--run-id',run_id]
    if step=='prepare':
        conf=context['dag_run'].conf or {}
        for key in ('source_manifest','coverage_manifest','as_of'):
            if not isinstance(conf.get(key),str) or not conf[key]:
                raise ValueError('Manual DAG requires conf.'+key)
            command.extend(['--'+key.replace('_','-'),conf[key]])
    with subprocess.Popen(command,cwd='/app',stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True) as process:
        for line in process.stdout:
            print(line.rstrip(),flush=True)
        code=process.wait()
        if code:
            raise subprocess.CalledProcessError(code,command)


with DAG('vietnews_nine_steps',start_date=datetime(2026,10,8),schedule=None,catchup=False,
    max_active_runs=1,default_args={'retries':0,'execution_timeout':timedelta(minutes=30)},
    tags=['vietnews','manual','experimental']) as dag:
    previous=None
    for step in STEPS:
        task=PythonOperator(task_id=step,python_callable=execute_step,op_kwargs={'step':step})
        if previous is not None:
            previous >> task
        previous=task
