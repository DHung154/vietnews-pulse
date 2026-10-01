import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.news_pipeline.history import import_jsonl
from src.news_pipeline.normalization import normalize_record
from src.news_pipeline.stream import read_rss_stream, rss_input_scope, LIVE_DATASETS


class ScopeTest(unittest.TestCase):
    def test_parent_selects_rss_and_blocks_nonlive_records_without_touching_raw(self):
        # Execute the reader construction against a small recording file source;
        # actual Spark integration is a separate, opt-in test.
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            rss=root/'rss'; rss.mkdir()
            history=root/'historical'; history.mkdir()
            old=history/'old.jsonl'; old.write_text('{"dataset_id":"historical_test"}\n')
            rows=[{'dataset_id':d} for d in ('rss_live','rss_live_v2','historical_test','demo_replay',None)]
            live=rss/'live.jsonl'; live.write_text(''.join(json.dumps(r)+'\n' for r in rows))
            before={p:p.read_bytes() for p in (old,live)}
            class Reader:
                def option(self,*args): return self
                def schema(self,*args): return self
                def json(self,path):
                    self.paths=list(Path(path).rglob('*.jsonl'))
                    self.rows=[json.loads(line) for p in self.paths for line in p.read_text().splitlines()]
                    return self
                def filter(self, allowed): return [r for r in self.rows if r['dataset_id'] in allowed]
            class Session:
                readStream=Reader()
            with patch('src.news_pipeline.stream.F.col') as column:
                column.return_value.isin.side_effect=lambda *values: values
                output=read_rss_stream(Session(),root,2)
            self.assertEqual([r['dataset_id'] for r in output],list(LIVE_DATASETS))
            self.assertEqual(Session.readStream.paths,[live])
            self.assertEqual(before,{p:p.read_bytes() for p in before})
            with self.assertRaises(ValueError): rss_input_scope(history)

    def test_body_is_not_model_description_and_dates_stay_null(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); original=root/'original.jsonl'; mapping=root/'mapping.json'
            original.write_text(json.dumps({'title':'Headline','url':'https://example.test/a','content':'Long body','crawled_at':'2022-07-01'})+'\n')
            mapping.write_text(json.dumps({'title':'title','link':'url','body_text':'content','source_crawled_at':'crawled_at'}))
            before=original.read_bytes()
            output=root/'v2.jsonl'
            import_jsonl(original,mapping,output,'historical_test')
            row=json.loads(output.read_text())
            clean=normalize_record(row)
            self.assertEqual(clean['body_text'],'Long body')
            self.assertIsNone(clean['description'])
            self.assertIsNone(clean['published_at'])
            self.assertIsNone(clean['event_time'])
            self.assertEqual((clean['model_input_type'],clean['model_input_text']),('title_only','Headline'))
            self.assertEqual(row['source_crawled_at_raw'],'2022-07-01')
            with self.assertRaises(FileExistsError): import_jsonl(original,mapping,output,'historical_test')
            self.assertEqual(original.read_bytes(),before)
