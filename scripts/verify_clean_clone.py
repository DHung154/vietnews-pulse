"""Install and test the public clean clone in an isolated Windows venv."""
import json
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
clone=ROOT/'runtime/clean_clone_20261016_v1'
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output',type=Path,default=ROOT/'evidence/clean_clone_20261016_v1')
parser.add_argument('--resume-installed',action='store_true')
args=parser.parse_args()
out=args.output.resolve()
if ROOT/'evidence' not in out.parents:
    parser.error('output must be below evidence')
out.mkdir(parents=True,exist_ok=False)
assert (clone/'.git').is_dir()
python=clone/'.venv/Scripts/python.exe'
env=dict(os.environ,PYTHONUTF8='1',PIP_CONFIG_FILE=os.devnull)
env.pop('PIP_EXTRA_INDEX_URL',None)
results=[]


def command(name,args,timeout=600):
    with (out/(name+'.log')).open('x',encoding='utf-8') as log:
        process=subprocess.Popen(args,cwd=clone,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            code=process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill();process.wait();code=124
    results.append(dict(name=name,command=args,exit_code=code))
    with (out/'commands.json').open('w',encoding='utf-8') as handle:
        json.dump(results,handle,indent=2)
    print(name,code,flush=True)
    if code:
        raise SystemExit(code)


pip=[str(python),'-m','pip','--isolated','--disable-pip-version-check']
if not args.resume_installed:
    command('create_venv',[sys.executable,'-m','venv',str(clone/'.venv')],120)
    command('install_locked',pip+['install','--no-input','--index-url','https://pypi.org/simple','-r','requirements.lock.txt','-r','requirements.dashboard.lock.txt'])
    command('install_project',pip+['install','--no-input','--index-url','https://pypi.org/simple','-e','.','--no-deps'])
else:
    assert python.is_file(), 'venv not installed'
command('pip_check',pip+['check'],60)
command('tests',[str(python),'-B','-m','unittest','discover','-s','tests','-q'],180)
command('compile',[str(python),'-m','compileall','-q','src','scripts','tests','dashboard'],60)
command('git_status',['git','status','--short'],30)
sha=subprocess.run(['git','rev-parse','HEAD'],cwd=clone,capture_output=True,text=True,check=True).stdout.strip()
dirty=subprocess.run(['git','status','--short'],cwd=clone,capture_output=True,text=True,check=True).stdout
summary=dict(status='passed_with_local_patch' if dirty.strip() else 'ok',public_commit=sha,local_patch=dirty,readme_matches_workspace=(clone/'README.md').read_bytes()==(ROOT/'README.md').read_bytes(),limitations=['A local patch is not proof the public remote includes that fix.', 'No production corpus/model/annotations bundled; full HDFS pipeline requires prepared artifacts.', 'Top-level pins are installed; transitive dependencies may not be fully locked.'])
with (out/'summary.json').open('x',encoding='utf-8') as handle:
    json.dump(summary,handle,indent=2)
print(json.dumps(summary),flush=True)
