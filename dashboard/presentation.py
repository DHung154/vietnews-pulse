"""Short defence session: persistent browser scene + isolated, real work in a child."""
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from src.news_pipeline.rss import atomic_write_json
from src.news_pipeline.writer_lock import single_writer

ROOT=Path(__file__).resolve().parents[1]
CONTROL=ROOT/'runtime/presentation'
LAB='airflow_181ed4cba01a3cab'
FULL='full_2a5e58de5cb434258ff9'
PRODUCTION=ROOT/'runtime/dashboard_refresh_20261008T084631Z_r2/serving'
LIVE_STEPS=('health','etl','kafka','airflow','sqlite')


def session_path(session_id):
    if not re.fullmatch(r'talk_[0-9]{8}T[0-9]{6}Z_[a-f0-9]{8}',session_id):
        raise ValueError('Invalid presentation session ID')
    return CONTROL/session_id


def current():
    pointer=CONTROL/'current.json'
    if not pointer.exists():
        return None
    info=json.loads(pointer.read_text(encoding='utf-8'))
    work=session_path(info['session_id'])
    status=work/'state.json'
    state=json.loads(status.read_text(encoding='utf-8')) if status.exists() else info
    pid=state.get('pid') or info.get('pid')
    if state.get('status') in ('queued','running') and pid and not _pid_running(pid):
        return dict(state,status='interrupted')
    return state


def _pid_running(pid):
    if os.name=='nt':
        import ctypes
        from ctypes import wintypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
        kernel.OpenProcess.restype=wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)]
        kernel.CloseHandle.argtypes=[wintypes.HANDLE]
        handle=kernel.OpenProcess(0x1000,False,int(pid))
        if not handle:return ctypes.get_last_error()!=87
        try:
            code=wintypes.DWORD()
            return not kernel.GetExitCodeProcess(handle,ctypes.byref(code)) or code.value==259
        finally:kernel.CloseHandle(handle)
    try:os.kill(int(pid),0)
    except ProcessLookupError:return False
    except PermissionError:return True
    return True


def open_terminal(session_id):
    work=session_path(session_id)
    if not (work/'state.json').is_file():raise ValueError('Không có phiên demo để mở terminal.')
    with single_writer(CONTROL/'terminal.lock'):
        path=work/'terminal.json'
        previous=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        if previous.get('pid') and _pid_running(previous['pid']):return previous
        argv=[sys.executable,'-u',str(ROOT/'scripts/presentation_worker.py'),
              '--session-id',session_id,'--follow-only','--keep-open']
        options=dict(cwd=ROOT,env=dict(os.environ,PYTHONUTF8='1'))
        if os.name=='nt':options['creationflags']=subprocess.CREATE_NEW_CONSOLE
        process=subprocess.Popen(argv,**options)
        result=dict(session_id=session_id,pid=process.pid)
        atomic_write_json(path,result)
        return result


def start(visible_terminal=True):
    CONTROL.mkdir(parents=True,exist_ok=True)
    with single_writer(CONTROL/'launch.lock'):
        active=current()
        if active and active.get('status') in ('queued','running'):
            raise RuntimeError('Có phiên kiểm tra chưa kết thúc; xem log thay vì chạy chồng.')
        if active and active.get('status')=='interrupted':
            atomic_write_json(session_path(active['session_id'])/'state.json',dict(active,status='failed',error='Worker của phiên trước đã dừng.'))
        session_id='talk_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:8]
        work=session_path(session_id)
        work.mkdir(exist_ok=False)
        info=dict(session_id=session_id,status='queued',started_at=datetime.now(timezone.utc).isoformat(),
                  terminal_requested=bool(visible_terminal),steps={key:dict(state='queued') for key in LIVE_STEPS})
        atomic_write_json(work/'state.json',info)
        atomic_write_json(CONTROL/'current.json',dict(session_id=session_id))
        argv=[sys.executable,'-u',str(ROOT/'scripts/presentation_worker.py'),'--session-id',session_id]
        options=dict(cwd=ROOT,env=dict(os.environ,PYTHONUTF8='1'),stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        if os.name=='nt':options['creationflags']=subprocess.CREATE_NO_WINDOW
        try:
            process=subprocess.Popen(argv,**options)
        except OSError as exc:
            info.update(status='failed',error=str(exc))
            atomic_write_json(work/'state.json',info)
            raise
        atomic_write_json(CONTROL/'current.json',dict(session_id=session_id,pid=process.pid))
        if visible_terminal:open_terminal(session_id)
        return dict(info,pid=process.pid)


def archived_evidence():
    """Missing evidence stays unavailable, never a guessed KPI."""
    lab=ROOT/'runtime/nine_steps'/LAB
    full=ROOT/'runtime/nine_steps'/FULL
    clean=json.loads((lab/'clean.json').read_text(encoding='utf-8'))
    verify=json.loads((full/'verify.json').read_text(encoding='utf-8'))
    cluster=json.loads((full/'cluster.json').read_text(encoding='utf-8'))
    kafka=json.loads((full/'kafka.json').read_text(encoding='utf-8'))
    for row in (clean,verify,cluster,kafka):
        if row.get('status')!='ok':raise ValueError('Archived evidence is not successful')
    if verify['article_count']!=verify['unique_keys'] or clean['raw_count']!=clean['clean_count']+clean['duplicate_removed']+clean['invalid_count']:
        raise ValueError('Archived counts do not reconcile')
    if verify.get('run_id')!=FULL or clean.get('run_id')!=LAB:
        raise ValueError('Archived run identity mismatch')
    stages=[]
    for name in ('prepare','kafka','read','dedup','sql','cluster','score','serve','verify'):
        row=json.loads((full/(name+'.json')).read_text(encoding='utf-8'))
        if row.get('run_id')!=FULL or row.get('status')!='ok':
            raise ValueError('Incomplete full-run evidence')
        stages.append(dict(name=name,completed_at=row['completed_at'],status=row['status']))
        if name=='serve':coverage=row['serving']['coverage_status']
    with sqlite3.connect((lab/'serving'/('lab_'+LAB+'.sqlite')).resolve().as_uri()+'?mode=ro',uri=True) as connection:
        samples=[dict(zip(('title','source','category','sentiment'),r)) for r in connection.execute('SELECT title,source,category,sentiment_label FROM articles ORDER BY article_id LIMIT 3')]
    return dict(full_count=verify['article_count'],full_run=FULL,full_date=verify['completed_at'],model=verify['model_version'],
        poll=dict(raw=clean['raw_count'],clean=clean['clean_count'],duplicates=clean['duplicate_removed'],date=clean['completed_at'],run=LAB),
        stages=stages,silhouette=cluster['silhouette'],clusters=cluster['sizes'],topic=kafka['topic'],
        samples=samples,coverage=coverage,scope='Kết quả đã lưu; không phải task đang chạy')


def payload():
    result=dict(archive=None,archive_error=None,live=None,live_error=None)
    try:result['archive']=archived_evidence()
    except (OSError,ValueError,KeyError,sqlite3.Error) as exc:result['archive_error']=str(exc)
    try:result['live']=current()
    except (OSError,ValueError,KeyError) as exc:result['live_error']=str(exc)
    return result


def panel(st):
    import streamlit.components.v1 as components
    component=components.declare_component('vietnews_defence',path=str(ROOT/'dashboard/presentation_ui'))
    # Only local JSON reads on each tick; Docker never blocks Streamlit's render thread.
    @st.experimental_fragment(run_every='1s')
    def scene():
        data=payload()
        action=component(data=data,key='defence_scene',default=None)
        if isinstance(action,dict) and action.get('nonce')!=st.session_state.get('talk_action_nonce'):
            st.session_state['talk_action_nonce']=action.get('nonce')
            if action.get('action')=='start':
                try:start(visible_terminal=action.get('terminal') is True)
                except (OSError,ValueError,RuntimeError) as exc:st.error(str(exc))
            elif action.get('action')=='terminal':
                try:open_terminal(action.get('session_id',''))
                except (OSError,ValueError,RuntimeError) as exc:st.error(str(exc))
    scene()
