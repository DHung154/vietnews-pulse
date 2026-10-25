"""Offline snapshot insights and explicit-window coverage; never publishes."""
import argparse
import csv
import hashlib
import html
import json
import sqlite3
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from news_pipeline.coverage import profile_manifests, _slot
from news_pipeline.rss import load_sources


def bars(rows, title):
    maximum = max((row['count'] for row in rows),default=1)
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="950" height="{100+len(rows)*40}" viewBox="0 0 950 {100+len(rows)*40}"><rect width="100%" height="100%" fill="white"/><g font-family="Arial" font-size="16" fill="#102d45">',f'<text x="20" y="30">{html.escape(title)}</text>']
    for i,row in enumerate(rows):
        y=65+i*40
        parts.append(f'<text x="20" y="{y+18}">{html.escape(str(row["name"]))}</text><rect x="200" y="{y}" width="{600*row["count"]/maximum}" height="26" fill="#087f8c"/><text x="{210+600*row["count"]/maximum}" y="{y+18}">{row["count"]}</text>')
    return ''.join(parts)+ '</g></svg>'


def run(database, output, end_local):
    end = datetime.fromisoformat(end_local).date()
    if end >= datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).date():
        raise ValueError('end_local must be a completed Vietnam day')
    if output.exists():
        raise FileExistsError('use a new output version')
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    connection = sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True)
    try:
        def query(sql):
            cursor = connection.execute(sql,('rss_live_v2',))
            return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor]
        source = query('SELECT source AS name,COUNT(*) AS count FROM articles WHERE dataset_id=? GROUP BY source ORDER BY count DESC')
        categories = query('SELECT category AS name,COUNT(*) AS count FROM articles WHERE dataset_id=? GROUP BY category ORDER BY count DESC')
        daily = query('SELECT event_date AS day,COUNT(*) AS count FROM articles WHERE dataset_id=? GROUP BY event_date ORDER BY event_date')
        total = query('SELECT COUNT(*) AS total,COUNT(DISTINCT article_id) AS unique_keys,SUM(published_at IS NULL) AS missing_published FROM articles WHERE dataset_id=?')[0]
    finally:
        connection.close()
    assert before == hashlib.sha256(database.read_bytes()).hexdigest()
    assert sum(r['count'] for r in source)==sum(r['count'] for r in categories)==sum(r['count'] for r in daily)==total['total']==total['unique_keys']
    paths=sorted((ROOT/'evidence/d02').glob('*.json'))
    manifests=[json.loads(p.read_text(encoding='utf-8')) for p in paths]
    sources=load_sources(ROOT/'config/rss_sources.yaml')
    coverage=profile_manifests(manifests,sources,14,0.95,end_local)
    output.mkdir(parents=True,exist_ok=False)
    def save(name,value):
        with (output/name).open('x',encoding='utf-8') as handle:
            json.dump(value,handle,ensure_ascii=False,indent=2)
    save('coverage.json',coverage)
    # Slot union, not run count: latest copies and retries cannot fill missing slots.
    attempts={}
    for manifest in manifests:
        for row in manifest.get('results',[]):
            feed=row.get('feed_id')
            stamp=row.get('last_attempt_at') or manifest.get('timestamp_utc')
            if feed in sources and stamp:
                slot=_slot(stamp,int(sources[feed].get('poll_seconds') or 600))
                attempts.setdefault((feed,slot),set()).add(str(row.get('status_code')))
    with (output/'missing_or_failed_slots.csv').open('x',encoding='utf-8',newline='') as handle:
        writer=csv.writer(handle)
        writer.writerow(['feed_id','slot_start_local','state','observed_http_codes'])
        for feed,config in sorted(sources.items()):
            interval=int(config.get('poll_seconds') or 600)
            zone=ZoneInfo(config.get('timezone','Asia/Ho_Chi_Minh'))
            start=datetime.combine(end-timedelta(days=13),time.min,zone)
            stop=datetime.combine(end+timedelta(days=1),time.min,zone)
            for stamp in range(int(start.timestamp()),int(stop.timestamp()),interval):
                codes=attempts.get((feed,stamp//interval),set())
                if not codes.intersection({'200','304'}):
                    writer.writerow([feed,datetime.fromtimestamp(stamp,zone).isoformat(),'missing' if not codes else 'attempt_failed','|'.join(sorted(codes))])
    facts=dict(snapshot_id='snapshot_20261008T090238Z_81c6a670',dataset_id='rss_live_v2',database_sha256=before,**total,sources=source,categories=categories,daily=daily,limitations=['Publication dates are not collector coverage.', 'Counts describe observed articles, not publisher-wide production or causal effects.', 'Missing slots do not identify root cause without scheduler/system logs.'])
    save('insights.json',facts)
    for name,rows,title in [('sources',source,'So bai theo nguon - snapshot 08/10/2026'),('categories',categories,'So bai theo chuyen muc - snapshot 08/10/2026')]:
        with (output/(name+'.svg')).open('x',encoding='utf-8') as handle:
            handle.write(bars(rows,title))
    save('inputs.json',dict(database=str(database.resolve()),end_local=end_local,manifests=[dict(path=str(p.relative_to(ROOT)),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in paths]))
    print(json.dumps(dict(total=total,coverage_status=coverage['coverage_status'],window=[coverage['window_start_local'],coverage['window_end_local']],min_success_rate=coverage['min_success_rate_observed'],output=str(output)),ensure_ascii=True))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--end-local',required=True)
    args=parser.parse_args()
    run(args.database,args.output,args.end_local)
