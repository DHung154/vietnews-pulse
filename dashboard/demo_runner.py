"""Fixed manual Airflow trigger; no arbitrary paths or shell commands from UI."""
import hashlib
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from src.news_pipeline.writer_lock import single_writer
from src.news_pipeline.rss import atomic_write_json

ROOT=Path(__file__).resolve().parents[1]
CONTROL=ROOT/'runtime/demo_control'
SOURCE=ROOT/'evidence/d02/collect_manifest_20261008T084057Z_3ef803fd.json'
COVERAGE=ROOT/'evidence/dashboard_refresh_20261008T084631Z_r2/coverage.json'


def cli(*args):
    argv=['docker','--config',str(ROOT/'runtime/docker-config'),'compose','-f',
        'compose.nine-steps.yaml','exec','-T','airflow','airflow',*args]
    result=subprocess.run(argv,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=20)
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout)[-2500:])
    return result.stdout


def json_output(text):
    # Airflow may prepend log lines before the JSON list.
    for index,char in enumerate(text):
        if char=='[':
            try:
                value=json.loads(text[index:])
                if isinstance(value,list):
                    return value
            except ValueError:
                pass
    raise ValueError('Airflow did not return a JSON list')


def current():
    file=CONTROL/'current.json'
    return json.loads(file.read_text(encoding='utf-8')) if file.exists() else None


def start():
    with single_writer(CONTROL/'trigger.lock'):
        active=json_output(cli('dags','list-runs','-d','vietnews_nine_steps','--output','json'))
        if any(row['state'] in ('running','queued') for row in active):
            raise RuntimeError('Pipeline đang có lượt chạy/queued; không tạo lượt chồng.')
        if not SOURCE.is_file() or not COVERAGE.is_file():
            raise FileNotFoundError('Thiếu input RSS/coverage đã chọn cho demo')
        run='ui_demo_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8]
        scope='airflow_'+hashlib.sha256(run.encode()).hexdigest()[:16]
        conf={'source_manifest':'/app/'+SOURCE.relative_to(ROOT).as_posix(),
              'coverage_manifest':'/app/'+COVERAGE.relative_to(ROOT).as_posix(),
              'as_of':'2026-10-08T08:55:08.792652+00:00'}
        info=dict(run_id=run,scope=scope,started_at=datetime.now(timezone.utc).isoformat(),
            conf=conf,input_type='Replay một poll RSS thật ngày08/10; không crawl mới')
        # Persist identity before external mutation: a timeout must not hide an accepted run.
        atomic_write_json(CONTROL/'current.json',info)
        cli('dags','unpause','vietnews_nine_steps')
        cli('dags','trigger','--run-id',run,'--conf',json.dumps(conf),'vietnews_nine_steps')
        return info


def states(info):
    run=info['run_id']
    if not run.startswith('ui_demo_') or not all(c.isalnum() or c=='_' for c in run):
        raise ValueError('Invalid demo run ID')
    return json_output(cli('tasks','states-for-dag-run','vietnews_nine_steps',run,'--output','json'))


def work_path(info):
    scope='airflow_'+hashlib.sha256(info['run_id'].encode()).hexdigest()[:16]
    return ROOT/'runtime/nine_steps'/scope
