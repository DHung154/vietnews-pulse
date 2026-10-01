"""Bounded D1 discovery/smoke, reusing the collector HTTP and RSS parser."""
import argparse
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import sys
import time
from urllib.parse import urljoin, urlsplit
from urllib.error import HTTPError
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from news_pipeline.rss import fetch_feed, parse_feed, parse_published_at, atomic_write_json

LISTINGS={'vnexpress':'https://vnexpress.net/rss','tuoitre':'https://tuoitre.vn/rss.htm','thanhnien':'https://thanhnien.vn/rss.html'}


class Links(HTMLParser):
    def __init__(self):
        super().__init__(); self.links=[]; self.current=None
    def handle_starttag(self, tag, attrs):
        if tag=='a': self.current={'href':dict(attrs).get('href',''),'label':''}
    def handle_data(self,data):
        if self.current is not None: self.current['label']+=data
    def handle_endtag(self,tag):
        if tag=='a' and self.current is not None:
            self.links.append(self.current); self.current=None


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--candidates',type=Path)
    parser.add_argument('--discovery',type=Path)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    responses=args.output/'responses'; responses.mkdir()
    results=[]
    entries=json.loads(args.candidates.read_text(encoding='utf-8')) if args.candidates else [{'source_id':s,'url':u} for s,u in LISTINGS.items()]
    discovery=json.loads(args.discovery.read_text(encoding='utf-8')) if args.discovery else None
    for entry in entries:
        name=entry.get('feed_id',entry['source_id'])
        url=entry.get('feed_url',entry.get('url'))
        item={**entry,'checked_at':datetime.now(timezone.utc).isoformat(),'timeout_seconds':20,'retries':1,'http_status':None,'content_type':None,'xml_parse':None}
        started=time.monotonic()
        try:
            if args.candidates:
                listing=next(r for r in discovery['results'] if r['source_id']==entry['source_id'])
                assert entry['official_listing_url']==listing['url']
                assert url in {link['url'] for link in listing['rss_links']}, 'candidate must be an observed listing href'
            response=fetch_feed(url,{},timeout=20,retries=1)
            payload=response['payload']
            path=responses/(name+('.xml' if args.candidates else '.html'))
            path.write_bytes(payload)
            item.update({'http_status':response['status_code'],'content_type':response.get('content_type'),'final_url':response.get('final_url'),'payload_ref':str(path),'sha256':hashlib.sha256(payload).hexdigest(),'bytes':len(payload)})
            if args.candidates:
                root=ET.fromstring(payload)
                item['xml_parse']='ok'
                items=root.findall('./channel/item')
                observations,errors=parse_feed(payload,entry,'d1_verify_'+name)
                dates=[e.findtext('pubDate') for e in items]
                parsed=[parse_published_at(d,entry['timezone']) for d in dates]
                valid=[d for d in parsed if d]
                item.update({'entry_count':len(items),'observations':len(observations),'parse_errors':errors,'sample_dates_raw':dates[:3],'sample_dates_utc':[d.isoformat() if d else None for d in parsed[:3]],'parsed_dates':len(valid),'min_published_at':min(valid).isoformat() if valid else None,'max_published_at':max(valid).isoformat() if valid else None,'sample_item_categories':[e.findtext('category') for e in items[:3]]})
                item['newest_age_hours']=(datetime.now(timezone.utc)-max(valid)).total_seconds()/3600 if valid else None
                item['status']='verified' if observations and not errors and valid and -24 <= item['newest_age_hours'] <= 72 else 'needs_review'
            else:
                reader=Links(); reader.feed(payload.decode('utf-8',errors='replace'))
                links=[]
                for link in reader.links:
                    href=urljoin(url,link['href'])
                    if urlsplit(href).path.endswith('.rss'):
                        links.append({'url':href,'label':' '.join(link['label'].split())})
                item.update({'rss_links':links,'status':'ok' if links else 'no_rss_links'})
        except Exception as exc:
            item.update({'status':'failed','error_type':type(exc).__name__,'error':str(exc)})
            if isinstance(exc, ET.ParseError): item['xml_parse']='failed'
            if isinstance(exc,HTTPError):
                body=exc.read(); path=responses/(name+'.error')
                path.write_bytes(body)
                item.update({'http_status':exc.code,'content_type':exc.headers.get('Content-Type'),'payload_ref':str(path),'sha256':hashlib.sha256(body).hexdigest()})
        item['elapsed_seconds']=time.monotonic()-started
        results.append(item)
        atomic_write_json(args.output/'manifest.json',{'results':results,'complete':len(results)==len(entries)})
        print(json.dumps({k:item.get(k) for k in ('source_id','feed_id','http_status','content_type','status','entry_count','error')},ensure_ascii=True),flush=True)
        # One-shot pilot etiquette only; durable scheduler/backoff/jitter belongs to D2.
        if len(results)<len(entries): time.sleep(5)
    return 0 if all(r['status'] in {'ok','verified'} for r in results) else 1


if __name__=='__main__':
    raise SystemExit(main())
