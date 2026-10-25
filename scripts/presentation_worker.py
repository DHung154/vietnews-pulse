"""Bounded background defence checks; every displayed line comes from a real call."""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from dashboard.presentation import session_path,PRODUCTION,FULL
from src.news_pipeline.rss import atomic_write_json
from src.news_pipeline.writer_lock import single_writer


def main(session_id):
    work=session_path(session_id)
    state=json.loads((work/'state.json').read_text(encoding='utf-8'))
    if state.get('status')!='queued':raise ValueError('Session already started')
    state.update(status='running',pid=os.getpid(),terminal_attached=bool(sys.stdout and sys.stdout.isatty()))
    docker=['docker','--config',str(ROOT/'runtime/docker-config')]
    topic=json.loads((ROOT/'runtime/nine_steps'/FULL/'kafka.json').read_text())['topic']
    commands=[
        ('health',docker+['exec','bigdata-namenode-1','hdfs','dfsadmin','-report'],15),
        ('etl',docker+['run','--rm','--name','vn-'+session_id,'--network','bigdata_default','--cpus','2','--memory','2g',
            '--entrypoint','python','--env','HADOOP_USER_NAME=hadoop','--env','PYSPARK_SUBMIT_ARGS=--driver-memory 768m pyspark-shell',
            '--mount','type=bind,source='+str(ROOT)+',target=/app','bigdata-spark-dev',
            '/app/scripts/presentation_etl.py','--session-id',session_id],100),
        ('kafka',docker+['exec','vietnews-lab-kafka-1','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092','--describe','--topic',topic],12),
        ('airflow',docker+['exec','vietnews-lab-airflow-1','airflow','dags','list-runs','-d','vietnews_full_snapshot','--output','json'],12),
        ('sqlite',[sys.executable,'-c',"import sqlite3,json,pathlib; root=pathlib.Path("+repr(str(PRODUCTION))+");p=json.loads((root/'current.json').read_text());c=sqlite3.connect((root/p['db_path']).as_uri()+'?mode=ro',uri=True);integrity=c.execute('PRAGMA integrity_check').fetchone()[0];rows=c.execute('SELECT dataset_id,COUNT(*),COUNT(DISTINCT article_id) FROM articles GROUP BY dataset_id').fetchall();assert integrity=='ok' and rows and all(n==u and n>0 for _,n,u in rows);assert c.execute('SELECT model_version FROM snapshot_meta WHERE snapshot_id=?',(p['snapshot_id'],)).fetchone()[0]==p['model_version'];print('integrity:',integrity);print('dataset/count/unique:',rows);print('model:',p['model_version']);c.close()"],8),
    ]
    def save():atomic_write_json(work/'state.json',state)
    def log(text):
        print(text,flush=True)
        with (work/'terminal.log').open('a',encoding='utf-8') as handle:handle.write(text+'\n')
    with single_writer(work/'worker.lock'):
        save()
        log('VIETNEWS PULSE | DEMO DO AN | REAL COMMANDS / REAL OUTPUT')
        log('Session: '+session_id+' | Archived RSS poll -> live ETL; production unchanged')
        for key,argv,timeout in commands:
            step=state['steps'][key]
            step.update(state='running',started_at=datetime.now(timezone.utc).isoformat(),command=subprocess.list2cmdline(argv))
            save();log('\n$ '+subprocess.list2cmdline(argv))
            start=time.monotonic()
            logpath=work/(key+'.log')
            try:
                # Output goes straight to disk, independent of browser rerenders.
                with logpath.open('x',encoding='utf-8') as file:
                    proc=subprocess.Popen(argv,cwd=ROOT,stdout=file,stderr=subprocess.STDOUT,env=dict(os.environ,PYTHONUTF8='1'),creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
                    offset=0
                    while proc.poll() is None:
                        with logpath.open(encoding='utf-8',errors='replace') as reader:
                            reader.seek(offset);chunk=reader.read();offset=reader.tell()
                        if chunk:
                            log(chunk.rstrip())
                            step['tail']=logpath.read_text(encoding='utf-8',errors='replace')[-9000:]
                            save()
                        if time.monotonic()-start>timeout:
                            if key=='etl':
                                # Only our uniquely named container; never shared services.
                                stopped=subprocess.run(docker+['stop','--time','3','vn-'+session_id],capture_output=True,text=True,timeout=10)
                                log('Owned container stop exit='+str(stopped.returncode))
                            proc.kill();proc.wait(timeout=5)
                            raise subprocess.TimeoutExpired(argv,timeout)
                        time.sleep(.35)
                    with logpath.open(encoding='utf-8',errors='replace') as reader:
                        reader.seek(offset);chunk=reader.read()
                    if chunk:log(chunk.rstrip())
                step.update(exit_code=proc.returncode,state='success' if proc.returncode==0 else 'failed')
                if key=='etl' and proc.returncode==0:
                    result=json.loads((work/'etl.json').read_text(encoding='utf-8'))
                    if result['session_id']!=session_id or result['status']!='ok':raise ValueError('ETL output mismatch')
                    step['result']=result
            except (OSError,ValueError,subprocess.TimeoutExpired) as exc:
                step.update(state='failed',exit_code=None,error=str(exc));log('ERROR: '+str(exc))
            step.update(completed_at=datetime.now(timezone.utc).isoformat(),elapsed_seconds=round(time.monotonic()-start,2))
            step['tail']=logpath.read_text(encoding='utf-8',errors='replace')[-9000:] if logpath.exists() else step.get('error','')
            log('EXIT '+str(step.get('exit_code'))+' | '+key+' | '+str(step['elapsed_seconds'])+'s')
            save()
        state.update(status='success' if all(s['state']=='success' for s in state['steps'].values()) else 'partial',completed_at=datetime.now(timezone.utc).isoformat())
        save();log('\nSESSION '+state['status'].upper()+' | logs retained at '+str(work))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session-id',required=True)
    parser.add_argument('--keep-open',action='store_true')
    args=parser.parse_args()
    if args.keep_open and os.name=='nt':
        # Streamlit may inherit redirected handles. Explicitly attach this new console.
        try:
            sys.stdout=open('CONOUT$','w',encoding='utf-8',buffering=1)
            sys.stderr=sys.stdout
            sys.stdin=open('CONIN$','r',encoding='utf-8')
            import ctypes
            ctypes.windll.kernel32.SetConsoleTitleW('VietNews Pulse | Demo do an | Lenh va log that')
        except OSError:
            pass  # File logs still work if the host cannot expose a console.
    try:main(args.session_id)
    except Exception as exc:
        target=session_path(args.session_id)/'state.json'
        if target.exists():
            data=json.loads(target.read_text(encoding='utf-8'));data.update(status='failed',error=str(exc));atomic_write_json(target,data)
        print('FAILED:',exc,flush=True)
    if args.keep_open:
        print('\nNhan Enter de dong terminal (ket qua da luu). ',flush=True)
        try:
            if os.name=='nt':
                import msvcrt
                while msvcrt.getwch() not in ('\r','\n'):
                    time.sleep(.05)
            else:input()
        except (EOFError,KeyboardInterrupt):pass
