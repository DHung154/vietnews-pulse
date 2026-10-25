"""Package completed rehearsal outputs without publishing data or changing services."""
import hashlib
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
out=ROOT/'evidence/demo_presentation_20261021_v1/rehearsals'
out.mkdir(parents=True,exist_ok=False)
rows=[]
for work in sorted((ROOT/'runtime/presentation').glob('talk_20261021*')):
    state=json.loads((work/'state.json').read_text(encoding='utf-8'))
    if state['status'] not in ('success','partial','failed'):
        raise ValueError('Rehearsal still running')
    target=out/work.name;target.mkdir()
    for file in work.iterdir():
        if file.suffix in {'.json','.log'} and file.is_file():shutil.copy2(file,target/file.name)
    etl=json.loads((work/'etl.json').read_text(encoding='utf-8')) if (work/'etl.json').exists() else None
    rows.append(dict(session_id=work.name,status=state['status'],terminal_attached=state.get('terminal_attached'),
        duration_seconds=round((datetime.fromisoformat(state['completed_at'])-datetime.fromisoformat(state['started_at'])).total_seconds(),3),
        command_seconds={k:s.get('elapsed_seconds') for k,s in state['steps'].items()},
        raw=etl['raw_count'] if etl else None,clean=etl['clean_count'] if etl else None,
        duplicates=etl['duplicate_removed'] if etl else None,source_unchanged=etl['source_unchanged'] if etl else None,
        sql_sources=etl['sql_sources'] if etl else None,production_publish_performed=etl['production_publish_performed'] if etl else None))
assert len(rows)==3 and all(r['status']=='success' and (r['raw'],r['clean'],r['duplicates'])==(809,778,31) for r in rows)
root=ROOT/'runtime/dashboard_refresh_20261008T084631Z_r2/serving'
pointer=root/'current.json';p=json.loads(pointer.read_text(encoding='utf-8'));db=root/p['db_path']
pointer_hash=hashlib.sha256(pointer.read_bytes()).hexdigest();database_hash=hashlib.sha256(db.read_bytes()).hexdigest()
assert pointer_hash=='e93bd98a64c5e0b099507f8414f3d81d5fefe9effd08c2885e565dd9182fd50c'
assert database_hash=='ccc0cf24126a35f3654a9bfd5ab3edc74ce4eb7a0f2f41554526afe3204a672a'
with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as connection:
    integrity=connection.execute('PRAGMA integrity_check').fetchone()[0]
    count,unique=connection.execute("SELECT COUNT(*),COUNT(DISTINCT article_id) FROM articles WHERE dataset_id='rss_live_v2'").fetchone()
    model=connection.execute('SELECT model_version FROM snapshot_meta WHERE snapshot_id=?',(p['snapshot_id'],)).fetchone()[0]
    assert integrity=='ok' and count==unique==13388 and model==p['model_version']
summary=dict(status='ok',rehearsals=rows,production=dict(pointer_sha256=pointer_hash,database_sha256=database_hash,unchanged=True,
    count=count,unique=unique,integrity=integrity,model_version=model),
    limitations=['Three warm rehearsals are not a latency SLA.', 'Live ETL reprocesses one archived real RSS poll, not a new network crawl or full corpus.',
                 'Kafka/Airflow checks are read-only commands, not new ingest/full DAG runs.', 'Visual review has no measured FPS trace.'])
with (out/'summary.json').open('x',encoding='utf-8') as handle:json.dump(summary,handle,ensure_ascii=False,indent=2)
print(json.dumps(summary,ensure_ascii=True))
