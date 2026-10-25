"""Record reproducible offline checks without changing production artifacts."""
import json
import os
import subprocess
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, default=ROOT / 'evidence/completion_step1_20261015_v1')
args = parser.parse_args()
out = args.output.resolve()
if ROOT / 'evidence' not in out.parents:
    parser.error('output must be a new directory below evidence')
out.mkdir(parents=True, exist_ok=False)
temp = ROOT / 'runtime/completion_step1_20261015_temp'
temp.mkdir(exist_ok=True)
env = dict(os.environ, TEMP=str(temp), TMP=str(temp), PYTHONUTF8='1')
commands = [
    [sys.executable, '-B', '-m', 'unittest', 'discover', '-s', 'tests', '-q'],
    [sys.executable, '-m', 'compileall', '-q', 'src', 'scripts', 'tests', 'dashboard', 'airflow'],
    ['docker', '--config', 'runtime/docker-config', 'info', '--format', '{{.ServerVersion}}'],
]
results = []
for i, command in enumerate(commands):
    try:
        run = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=120 if i == 0 else 20)
        text = run.stdout + run.stderr
        code = run.returncode
    except subprocess.TimeoutExpired as exc:
        code, text = None, f'Timeout: {exc}'
    with (out / f'command_{i}.log').open('x', encoding='utf-8') as handle:
        handle.write(text)
    results.append(dict(command=command, exit_code=code, log=str(out / f'command_{i}.log')))
with (out/'checks.json').open('x',encoding='utf-8') as handle:
    json.dump(results,handle,ensure_ascii=False,indent=2)
print(json.dumps(results,ensure_ascii=True))
raise SystemExit(0 if all(r['exit_code'] == 0 for r in results) else 1)
