"""Streaming D0 audit. SQLite holds duplicate keys; no corpus is collected in RAM."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from news_pipeline.normalization import canonicalize_url, clean_text


def profile(path, output, legacy=False):
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    db = sqlite3.connect(output / "duplicate_keys.sqlite")
    db.execute("CREATE TABLE keys(kind TEXT, key TEXT, n INTEGER, PRIMARY KEY(kind,key))")
    counts, sources, hosts, topics, sentinels, pairs = (Counter() for _ in range(6))
    lengths = defaultdict(Counter)
    digest = hashlib.sha256()
    crawl_range = []
    def key(kind, value):
        if value:
            value = hashlib.sha256(value.encode()).hexdigest()
            db.execute("INSERT INTO keys VALUES (?,?,1) ON CONFLICT(kind,key) DO UPDATE SET n=n+1", (kind, value))
    with path.open("rb") as handle:
        for line_number, line in enumerate(handle, 1):
            digest.update(line)
            counts['physical_lines'] += 1
            if not line.strip():
                counts['blank_lines'] += 1
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError('not an object')
            except (ValueError, UnicodeError):
                counts['invalid_json'] += 1
                continue
            counts['records'] += 1
            title, body, url, topic = (record.get(k) for k in (('title_raw', 'description_raw', 'link_raw', 'category_raw') if legacy else ('title', 'content', 'url', 'topic')))
            source = str(record.get('source'))
            sources[source] += 1
            topic_key = json.dumps(topic, ensure_ascii=False, sort_keys=True)
            topics[topic_key] += 1
            if topic is None:
                sentinels['json_null_or_missing'] += 1
            elif isinstance(topic, str):
                if not topic.strip(): sentinels['empty_string'] += 1
                elif topic.strip().casefold() in {'none','null','unknown','nan','n/a'}: sentinels[topic] += 1
            try:
                parts = urlsplit(url or '')
                host = parts.hostname or '<missing>'
                if parts.scheme not in {'http','https'} or not parts.hostname:
                    counts['invalid_url'] += 1
            except ValueError:
                host = '<invalid>'
                counts['invalid_url'] += 1
            hosts[host] += 1
            pairs[(source,host)] += 1
            for name, value in [('title',title),('body',body)]:
                value = value if isinstance(value,str) else ''
                lengths[name][len(value)] += 1
                if not value: counts['missing_'+name+'_raw_empty'] += 1
                elif not value.strip(): counts['missing_'+name+'_whitespace_only'] += 1
                if not value.strip(): counts['missing_'+name] += 1
            for name in ('published_at','crawled_at'):
                value = record.get('published_at_raw' if legacy and name=='published_at' else name)
                if not value or not str(value).strip(): counts['missing_'+name] += 1
            if legacy:
                counts['dataset:'+str(record.get('dataset_id'))] += 1
                counts['body_in_description_nonempty'] += bool(body)
            else:
                crawl = record.get('crawled_at')
                if crawl:
                    try:
                        stamp = datetime.fromisoformat(crawl).isoformat()
                        crawl_range = [min(crawl_range[0], stamp), max(crawl_range[1], stamp)] if crawl_range else [stamp, stamp]
                    except ValueError: counts['unparsed_crawled_at'] += 1
                normalized = clean_text((title or '')+' '+(body or ''))
                if normalized and not any('\u00c0' <= c <= '\u024f' or '\u1e00' <= c <= '\u1eff' for c in normalized):
                    counts['no_vietnamese_diacritic_suspect'] += 1
                key('normalized_title_body', normalized)
                key('nonempty_body_exact', body)
            key('record_exact', json.dumps(record,ensure_ascii=False,sort_keys=True))
            key('url_exact', url)
            canonical_url = canonicalize_url(url)
            if canonical_url: key('source_canonical_url', source+'|'+canonical_url)
            if counts['records'] % 10000 == 0:
                db.commit()
                print(f'{path.name}: {counts["records"]}', flush=True)
    db.commit()
    duplicates = {kind: {'unique_keys': unique, 'rows_with_key': rows, 'duplicate_excess': rows-unique, 'duplicate_groups': groups} for kind,unique,rows,groups in db.execute('SELECT kind,count(*),sum(n),sum(n>1) FROM keys GROUP BY kind')}
    db.close()
    distributions = {}
    for name, histogram in lengths.items():
        ordered = sorted(histogram.items())
        n = sum(histogram.values())
        def quantile(q):
            total=0
            for length, frequency in ordered:
                total += frequency
                if total >= max(1, int(n*q+0.999999)): return length
        distributions[name] = {'unit':'Unicode codepoints before normalization','min':ordered[0][0], 'max':ordered[-1][0], 'mean':sum(k*v for k,v in ordered)/n, 'p50':quantile(.5),'p90':quantile(.9),'p99':quantile(.99),'histogram':dict(ordered)}
    result = {'input':str(path.resolve()),'bytes':path.stat().st_size,'sha256':digest.hexdigest(),'checked_at':datetime.now(timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-started,'counts':dict(counts),'source_raw':dict(sources.most_common()),'url_hosts':dict(hosts.most_common()),'source_alias_host_counts':[{'source_raw':s,'host':h,'count':n} for (s,h),n in pairs.most_common()],'topic_raw_frequencies':dict(topics.most_common()),'topic_sentinels':dict(sentinels),'lengths':distributions,'duplicates':duplicates,'source_crawled_at_range':crawl_range,'plan_expected_records':184539,'plan_record_residual':counts['records']-184539,'canonical_count':None,'limitations':['Exact key counts are not canonical counts; no near-dedup or source alias migration performed.','Diacritic absence is only a language suspicion heuristic, not language classification.','Source aliases are inventory of raw source -> observed URL host, not verified publisher identities.']}
    (output/'profile.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'output':str(output),'counts':dict(counts),'sha256':digest.hexdigest(),'duplicates':duplicates}))


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--legacy',action='store_true')
    args=parser.parse_args()
    profile(args.input,args.output,args.legacy)
