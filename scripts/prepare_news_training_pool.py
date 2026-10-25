"""Sample a bounded, unlabeled pool from a read-only serving snapshot."""
import argparse
import hashlib
import json
import random
import sqlite3
from collections import Counter
from pathlib import Path

from scripts.create_labeling_pool import _write_packet
from scripts.validate_labeling import _read_metadata, _file_sha256


def prepare(database, excluded_metadata, output, size=400, seed=20261015):
    if size <= 0:
        raise ValueError('size must be positive')
    if output.exists():
        raise FileExistsError('Choose a new version; output already exists')
    excluded = _read_metadata(excluded_metadata)
    article_ids = {r['article_id'] for r in excluded.values()}
    text_hashes = {r['input_sha256'] for r in excluded.values()}
    before = _file_sha256(database)
    rng = random.Random(seed)
    pool, seen, eligible, excluded_count, duplicates = [], set(), 0, 0, 0
    connection = sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        cursor = connection.execute('SELECT article_id, source, category, published_at, clean_text, title, description FROM articles WHERE dataset_id=? ORDER BY article_id', ('rss_live_v2',))
        input_count = 0
        for row in cursor:
            input_count += 1
            text = row['clean_text'] or ''
            if not text.strip():
                raise ValueError('Missing model input; do not silently reconstruct it')
            digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
            if row['article_id'] in article_ids or digest in text_hashes:
                excluded_count += 1
                continue
            if digest in seen:
                duplicates += 1
                continue
            seen.add(digest)
            eligible += 1
            item = dict(row)
            item.update(annotation_id=hashlib.sha256(f'news_pool|{seed}|{row["article_id"]}'.encode()).hexdigest(), input_text=text, input_sha256=digest, content_group_id=digest, event_group_id=None, dataset_id='rss_live_v2', split='unassigned', label='', label_origin='unlabeled', human_verified=False, guideline_version='news_sentiment_labeling_guide_v0.1', sample_seed=seed)
            del item['clean_text']
            if len(pool) < size:
                pool.append(item)
            else:
                index = rng.randrange(eligible)
                if index < size:
                    pool[index] = item
    finally:
        connection.close()
    if len(pool) != size:
        raise ValueError(f'Only {len(pool)} eligible samples; requested {size}')
    if _file_sha256(database) != before:
        raise ValueError('Snapshot changed while sampling')
    output.mkdir(parents=True, exist_ok=False)
    pool.sort(key=lambda r: r['annotation_id'])
    for row in pool:
        row['selection_probability'] = size / eligible
    with (output / 'metadata.jsonl').open('x', encoding='utf-8', newline='\n') as handle:
        for row in pool:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + '\n')
    _write_packet(output / 'annotator_a.csv', pool)
    shuffled = list(pool)
    rng.shuffle(shuffled)
    _write_packet(output / 'annotator_b.csv', shuffled)
    manifest = dict(status='awaiting_labels_and_event_review', input_database=str(database.resolve()), database_sha256=before, excluded_metadata_sha256=_file_sha256(excluded_metadata), input_count=input_count, excluded_pilot_articles=excluded_count, duplicate_texts_removed=duplicates, eligible_count=eligible, selected_count=size, seed=seed, labels_filled=0, human_gold=False, metrics=None, split='unassigned', source_counts=dict(Counter(r['source'] for r in pool)), limitations=['Uniform sampling after pilot/article/exact-text exclusion; not stratified.', 'Events shared with pilot may remain; human event review required before split.', 'No labels inferred from deployed model; not a training dataset yet.'])
    manifest['metadata_sha256'] = _file_sha256(output / 'metadata.jsonl')
    with (output / 'manifest.json').open('x', encoding='utf-8') as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--exclude-metadata', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--size', type=int, default=400)
    parser.add_argument('--seed', type=int, default=20261015)
    args = parser.parse_args()
    print(json.dumps(prepare(args.database, args.exclude_metadata, args.output, args.size, args.seed), ensure_ascii=True))
