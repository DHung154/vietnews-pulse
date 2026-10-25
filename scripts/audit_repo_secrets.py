"""Redacted candidate scan of Git source/history; never emits credential values."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PATTERNS={
    'github_token':re.compile(r'(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})'),
    'openai_key':re.compile(r'sk-(?:proj-)?[A-Za-z0-9_-]{35,}'),
    'aws_access_key':re.compile(r'(?:AKIA|ASIA)[A-Z0-9]{16}'),
    'private_key':re.compile(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    'literal_credential':re.compile(r'''(?i)(?:password|api_key|access_token|secret_key)\s*[=:]\s*["']([^"'\r\n]{4,})["']'''),
}


def git(*args):
    return subprocess.run(['git',*args],cwd=ROOT,capture_output=True,check=True).stdout


def matches(text, location):
    findings=[]
    for number,line in enumerate(text.splitlines(),1):
        for kind,pattern in PATTERNS.items():
            for match in pattern.finditer(line):
                value=match.group(1) if kind=='literal_credential' else match.group()
                if kind=='literal_credential' and any(x in value.lower() for x in ('example','placeholder','changeme','${','fixture','test_')):
                    continue
                findings.append(dict(location=location,line=number,kind=kind,value_sha256=hashlib.sha256(value.encode()).hexdigest(),status='candidate_requires_review'))
    return findings


def run(output):
    if output.exists():
        raise FileExistsError('use new output version')
    findings=[]
    skipped=[]
    files=set(git('ls-files','-z','--cached','--others','--exclude-standard').decode('utf-8').split('\0'))-{''}
    files.update(str(p.relative_to(ROOT)) for p in ROOT.glob('.env*') if p.is_file())
    scanned=0
    def scan(data,location):
        nonlocal scanned
        if len(data)>2*1024*1024 or b'\x00' in data:
            skipped.append(location)
            return
        scanned+=1
        findings.extend(matches(data.decode('utf-8',errors='replace'),location))
    for name in sorted(files):
        path=ROOT/name
        if path.is_file() and not path.is_symlink():
            scan(path.read_bytes(),'working:'+name)
    objects=[]
    for line in git('rev-list','--objects','--all').decode('utf-8').splitlines():
        oid,_,name=line.partition(' ')
        objects.append((oid,name))
    history_blobs=0
    for oid,name in objects:
        if git('cat-file','-t',oid).strip()!=b'blob':
            continue
        history_blobs+=1
        if int(git('cat-file','-s',oid))>2*1024*1024:
            skipped.append('git:'+oid+':'+name)
        else:
            scan(git('cat-file','blob',oid),'git:'+oid+':'+name)
    result=dict(status='review_required' if findings else 'no_pattern_matches',working_files=len(files),history_blobs=history_blobs,texts_scanned=scanned,findings=findings,skipped_binary_or_large=skipped,
       limitations=['Pattern scan is not proof that no secret exists.', 'Ignored runtime/corpus/models/volumes and external accounts not scanned; only root .env files explicitly included.', 'No values emitted; no history rewritten, credentials rotated or files deleted.'])
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('x',encoding='utf-8') as handle:
        json.dump(result,handle,indent=2)
    print(json.dumps(dict(status=result['status'],working_files=len(files),history_blobs=history_blobs,candidates=len(findings),output=str(output))))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    run(parser.parse_args().output)
