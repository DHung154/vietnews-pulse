"""Hourly frozen full-snapshot replay; isolated from dashboard and one-poll lab."""
import hashlib
import subprocess
import sys
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

STEPS=('prepare','kafka','read','dedup','sql','cluster','score','serve','verify')


def execute(step, **context):
    run_id='full_'+hashlib.sha256(context['dag_run'].run_id.encode()).hexdigest()[:20]
    command=[sys.executable,'/app/scripts/full_snapshot_pipeline.py','--step',step,'--run-id',run_id,
             '--attempt',str(context['ti'].try_number),'--config','/app/config/full_snapshot.json']
    with subprocess.Popen(command,cwd='/app',stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True) as process:
        for line in process.stdout:
            print(line.rstrip(),flush=True)
        code=process.wait()
        if code:
            raise subprocess.CalledProcessError(code,command)


with DAG('vietnews_full_snapshot',start_date=pendulum.datetime(2026,10,16,tz='Asia/Ho_Chi_Minh'),
         schedule='0 * * * *',catchup=False,max_active_runs=1,
         default_args={'retries':1,'retry_delay':timedelta(minutes=1),'execution_timeout':timedelta(minutes=30)},
         tags=['vietnews','full-snapshot','frozen-replay']) as dag:
    previous=None
    for step in STEPS:
        task=PythonOperator(task_id=step,python_callable=execute,op_kwargs={'step':step})
        if previous is not None:
            previous >> task
        previous=task
