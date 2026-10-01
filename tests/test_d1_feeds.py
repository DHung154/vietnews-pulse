import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import collect_rss
from src.news_pipeline.normalization import normalize_record
from src.news_pipeline.rss import load_sources


class FeedTest(unittest.TestCase):
    def test_two_feeds_one_source_keep_independent_validators_and_article_identity(self):
        payload=b'<rss><channel><item><title>Shared article</title><link>https://example.test/a</link></item></channel></rss>'
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            config=root/'config.json'; state=root/'state.json'; raw=root/'raw'; evidence=root/'manifest.json'; lock=root/'collector.lock'
            entries=[{'feed_id':f'x_{name}','source_id':'x','feed_url':f'https://example.test/{name}.rss','timezone':'Asia/Ho_Chi_Minh','canonical_category':'business','feed_category_raw':'Business'} for name in ('a','b')]
            config.write_text(json.dumps(entries))
            self.assertEqual(set(load_sources(config)),{'x_a','x_b'})
            args=['collect','--config',str(config),'--raw-root',str(raw),'--state',str(state),'--lock',str(lock),'--evidence',str(evidence),'--source','x','--request-gap','0']
            calls=[]
            def fetch(url,previous,**kwargs):
                calls.append((url,dict(previous)))
                return {'status_code':200,'payload':payload,'etag':url,'last_modified':url+'-date'}
            with patch('sys.argv',args), patch.object(collect_rss,'fetch_feed',side_effect=fetch), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(collect_rss.main(),0)
            saved=json.loads(state.read_text())
            self.assertEqual(set(saved),{'x_a','x_b'})
            self.assertEqual(len(calls),2)
            for entry in entries:
                self.assertEqual(saved[entry['feed_id']]['etag'],entry['feed_url'])
            first_files=list(raw.rglob('*.jsonl'))
            self.assertEqual(len(first_files),2)
            first_run_new = {key: saved[key]['last_new_article_at'] for key in saved}
            same_content = lambda url, previous, **kwargs: {'status_code':200,'payload':payload,'etag':url,'last_modified':url+'-date'}
            with patch('sys.argv',args), patch.object(collect_rss,'fetch_feed',side_effect=same_content), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(collect_rss.main(),0)
            unchanged_state=json.loads(state.read_text())
            self.assertEqual({unchanged_state[key]['last_new_article_at'] for key in unchanged_state}, set(first_run_new.values()))
            self.assertEqual({json.loads(evidence.read_text())['results'][i]['new_article_count'] for i in range(2)}, {0})
            self.assertEqual(len(list(evidence.parent.glob('manifest_*.json'))), 2)
            files=list(raw.rglob('*.jsonl'))
            self.assertEqual(len(files),4)
            before={p:p.read_bytes() for p in files}
            rows=[json.loads(p.read_text()) for p in first_files]
            self.assertEqual({r['feed_id'] for r in rows},{'x_a','x_b'})
            self.assertEqual(len({r['observation_id'] for r in rows}),2)
            normalized=[normalize_record(r) for r in rows]
            self.assertEqual(len({(r['dataset_id'],r['article_id']) for r in normalized}),1)
            self.assertEqual({r['category_basis'] for r in normalized},{'feed_registry'})
            def unchanged(url, previous, **kwargs):
                self.assertEqual(previous['etag'],url)
                self.assertEqual(previous['last_modified'],url+'-date')
                return {'status_code':304}
            with patch('sys.argv',args), patch.object(collect_rss,'fetch_feed',side_effect=unchanged), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(collect_rss.main(),0)
            self.assertEqual(before,{p:p.read_bytes() for p in raw.rglob('*.jsonl')})
            def broken(url, previous, **kwargs):
                if url.endswith('/a.rss'): raise TimeoutError('fixture timeout')
                return {'status_code':200,'payload':b'<html>not a feed</html>','etag':'must-not-commit'}
            with patch('sys.argv',args), patch.object(collect_rss,'fetch_feed',side_effect=broken), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(collect_rss.main(),1)
            failed=json.loads(state.read_text())
            self.assertEqual({r['status'] for r in json.loads(evidence.read_text())['results']},{'failed'})
            for key in saved:
                self.assertEqual(failed[key]['etag'],saved[key]['etag'])
                self.assertIsNotNone(failed[key]['last_success_at'])
            self.assertEqual(before,{p:p.read_bytes() for p in raw.rglob('*.jsonl')})

    def test_duplicate_feed_id_and_legacy_state_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); config=root/'config.json'; state=root/'state.json'
            entry={'feed_id':'x_general','source_id':'x','feed_url':'https://example.test/home.rss'}
            config.write_text(json.dumps([entry,entry]))
            with self.assertRaises(ValueError): load_sources(config)
            config.write_text(json.dumps([entry])); state.write_text('{"x":{"etag":"legacy"}}')
            before=state.read_bytes()
            args=['collect','--config',str(config),'--raw-root',str(root/'rss'),'--state',str(state),'--lock',str(root/'collector.lock'),'--evidence',str(root/'manifest.json')]
            with patch('sys.argv',args), patch.object(collect_rss,'fetch_feed') as fetch, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error: collect_rss.main()
                self.assertEqual(error.exception.code,2)
                fetch.assert_not_called()
            self.assertEqual(before,state.read_bytes())
