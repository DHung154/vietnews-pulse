"""Copy legacy RSS and reimport historical into a NEW scope, preserving old bytes."""
import hashlib
import json
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from news_pipeline.history import import_jsonl
from news_pipeline.rss import atomic_write_json


def checksum(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    old = ROOT / 'runtime/m02/raw'
    target = ROOT / 'runtime/data_collection_v2/raw'
    evidence = ROOT / 'evidence/d00_20260917/migration.json'
    if target.exists() or evidence.exists():
        raise FileExistsError('D0 migration destination/evidence already exists; use a reviewed new version')
    inventory = []
    for path in sorted(old.rglob('*.jsonl')):
        datasets = {}
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                if line.strip():
                    dataset = json.loads(line).get('dataset_id')
                    datasets[dataset] = datasets.get(dataset, 0) + 1
        inventory.append({'path':str(path), 'sha256_before':checksum(path),'bytes':path.stat().st_size,'datasets':datasets})
    atomic_write_json(evidence.with_name('raw_before.json'), {'files':inventory})
    (target / 'rss').mkdir(parents=True)
    for item in inventory:
        path = Path(item['path'])
        if set(item['datasets']) <= {'rss_live'}:
            dest = target / 'rss' / 'legacy_m02' / path.relative_to(old)
            dest.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(path,dest)
            item['copy_path'] = str(dest)
            item['copy_sha256'] = checksum(dest)
            assert item['copy_sha256'] == item['sha256_before']
        else:
            assert set(item['datasets']) == {'historical_kaggle_v1'}, item
            item['policy'] = 'retained in place; v2 reimport from original with explicit body mapping'
    original = ROOT / 'data/incoming/kaggle_vietnamese_online_news/news_dataset.jsonl'
    original_hash = checksum(original)
    result = import_jsonl(original, ROOT / 'config/history_mapping.kaggle_v2.json', target / 'historical/historical_kaggle_v2/news.jsonl', 'historical_kaggle_v2')
    checks = {'rows':0,'description_nonnull':0,'body_nonempty':0,'published_nonnull':0}
    with Path(result['raw_path']).open(encoding='utf-8') as handle:
        for line in handle:
            row=json.loads(line)
            checks['rows'] += 1
            checks['description_nonnull'] += row['description_raw'] is not None
            checks['body_nonempty'] += bool(row['body_raw'])
            checks['published_nonnull'] += row['published_at_raw'] is not None
    for item in inventory:
        item['sha256_after']=checksum(Path(item['path']))
        assert item['sha256_after']==item['sha256_before']
    assert checksum(original)==original_hash
    assert checks['rows']==184539 and checks['description_nonnull']==checks['published_nonnull']==0
    atomic_write_json(evidence, {'status':'ok','legacy_files':inventory,'original_sha256':original_hash,'historical_v2':result,'v2_checks':checks,'rss_copied_observations':sum(sum(i['datasets'].values()) for i in inventory if 'copy_path' in i),'canonical_count':None,'note':'Copy/reimport only; no canonical snapshot, no alias/category migration, old raw/checkpoints untouched.'})
    print(json.dumps({'status':'ok','evidence':str(evidence),'v2_checks':checks}))


if __name__ == '__main__':
    main()
