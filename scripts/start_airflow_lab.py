"""Preserve stale webserver PID metadata before starting the lab supervisor."""
import os
from datetime import datetime, timezone
from pathlib import Path


def archive_stale_pid(pid_file,proc_root=Path('/proc')):
    if not pid_file.exists():
        return None
    if pid_file.is_symlink() or pid_file.name!='airflow-webserver.pid':
        raise ValueError('unexpected PID path')
    value=pid_file.read_text().strip()
    if not value.isdigit() or int(value)<=0:
        raise ValueError('invalid PID contents; inspect before changing')
    command_file=proc_root/value/'cmdline'
    command=command_file.read_bytes().replace(b'\0',b' ').decode(errors='replace') if command_file.exists() else ''
    if 'gunicorn' in command.lower() or 'webserver' in command.lower():
        raise RuntimeError('refuse moving PID file for a live webserver')
    archive=pid_file.with_name(pid_file.name+'.stale_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    if archive.exists():
        raise FileExistsError(archive)
    pid_file.rename(archive)
    return archive


if __name__=='__main__':
    for directory in ('/app/src','/app/scripts','/app/airflow'):
        list(Path(directory).iterdir())
    archive=archive_stale_pid(Path('/opt/airflow/airflow-webserver.pid'))
    if archive:
        print('Preserved stale webserver PID:',archive,flush=True)
    os.execvp('airflow',['airflow','standalone'])
