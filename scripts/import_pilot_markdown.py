"""Validate a completed pilot Markdown; import one self-reported annotator only."""
import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from scripts.validate_labeling import _read_metadata, _file_sha256

LABELS={'positive','neutral','negative','mixed','needs_context','invalid'}


def parse(text, metadata):
    sections=re.split(r'^## Mẫu \d+\s*$',text,flags=re.M)[1:]
    rows=[]
    errors=[]
    seen=set()
    for section in sections:
        def field(name,code=False):
            value=re.search(r'^- \*\*'+re.escape(name)+r':\*\*[ \t]*([^\n]*)',section,re.M)
            result=value.group(1).strip() if value else ''
            return result.strip('`') if code else result
        key=field('annotation_id',True)
        sha=field('input_sha256',True)
        body=re.search(r'```text\n(.*?)\n```',section,re.S)
        content=body.group(1) if body else None
        label,evidence,reason=field('Nhãn'),field('Bằng chứng'),field('Lý do')
        issues=[]
        if key in seen or key not in metadata:
            issues.append('duplicate_or_unknown_id')
        else:
            source=metadata[key]
            if content!=source['input_text'] or sha!=source['input_sha256'] or hashlib.sha256((content or '').encode()).hexdigest()!=sha:
                issues.append('input_text_or_hash_mismatch')
        seen.add(key)
        if label not in LABELS or not reason:
            issues.append('missing_label_or_reason')
        if evidence and (content is None or evidence not in content):
            issues.append('evidence_not_exact_span')
        if label in {'positive','negative'} and not evidence:
            issues.append('missing_evidence')
        errors.extend(dict(annotation_id=key,error=issue) for issue in issues)
        rows.append(dict(annotation_id=key,input_sha256=sha,input_text=content,label=label,evidence_span=evidence,reason=reason))
    if seen!=set(metadata) or len(rows)!=len(metadata):
        errors.append(dict(error='packet_ids_or_count_mismatch',expected=len(metadata),actual=len(rows)))
    return rows,errors


def run(markdown,metadata_path,output,annotator):
    if output.exists():
        raise FileExistsError('use a new output version')
    if not annotator.strip():
        raise ValueError('annotator is required')
    metadata=_read_metadata(metadata_path)
    if any(row.get('split')!='pilot' for row in metadata.values()):
        raise ValueError('pilot only; cannot import a main test through this command')
    text=markdown.read_text(encoding='utf-8-sig').replace('\r\n','\n')
    rows,errors=parse(text,metadata)
    output.mkdir(parents=True,exist_ok=False)
    manifest=dict(status='rejected' if errors else 'imported_single_annotator_pilot',row_count=len(rows),errors=errors,label_counts=dict(Counter(r['label'] for r in rows)),markdown_sha256=_file_sha256(markdown),metadata_sha256=_file_sha256(metadata_path),annotator=annotator,label_origin='human_self_reported',verification='user_attestation_not_independently_verified',human_gold=False,gold_ready=False,metrics=None,received_at=datetime.now(timezone.utc).isoformat(),annotated_at=None,
        source_preface=text.split('## Mẫu 001')[0],limitations=['One annotator, not two independent packets.', 'Pilot has prior weak-training exposure; not a final independent test.', 'Original source preface retained, including any provenance discrepancy; no history rewritten.', 'Event grouping/adjudication/frozen gold still required.'])
    with (output/'manifest.json').open('x',encoding='utf-8') as handle:
        json.dump(manifest,handle,ensure_ascii=False,indent=2)
    if not errors:
        with (output/'reviewed_labels.jsonl').open('x',encoding='utf-8') as handle:
            for row in rows:
                row.update(annotator=annotator,label_origin='human_self_reported',human_verified=False,human_gold=False,split='pilot',annotated_at=None)
                handle.write(json.dumps(row,ensure_ascii=False)+'\n')
    print(json.dumps({k:v for k,v in manifest.items() if k not in {'source_preface','errors'}},ensure_ascii=True))
    return 1 if errors else 0


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for field in ('markdown','metadata','output'):
        parser.add_argument('--'+field,type=Path,required=True)
    parser.add_argument('--annotator',required=True)
    args=parser.parse_args()
    raise SystemExit(run(args.markdown,args.metadata,args.output,args.annotator))
