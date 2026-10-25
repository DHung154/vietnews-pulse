import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from scripts.prepare_news_training_pool import prepare


class TrainingPoolTest(unittest.TestCase):
    def test_sampling_exclusion_hashes_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / 'source.sqlite'
            with sqlite3.connect(db) as con:
                con.execute('CREATE TABLE articles(article_id,source,category,published_at,clean_text,title,description,dataset_id)')
                for i in range(8):
                    con.execute('INSERT INTO articles VALUES (?,?,?,?,?,?,?,?)', (str(i),'fixture','test',None,f'Text {i}',f'Title {i}','Description','rss_live_v2'))
            con.close()
            metadata = root / 'pilot.jsonl'
            metadata.write_text(json.dumps(dict(annotation_id='pilot',article_id='0',input_text='Text 0',input_sha256=hashlib.sha256(b'Text 0').hexdigest()))+'\n',encoding='utf-8')
            before = db.read_bytes()
            out = root / 'v1'
            result = prepare(db, metadata, out, 4)
            self.assertEqual(result['eligible_count'],7)
            self.assertEqual(result['labels_filled'],0)
            self.assertEqual(db.read_bytes(),before)
            rows = [json.loads(x) for x in (out/'metadata.jsonl').read_text(encoding='utf-8').splitlines()]
            self.assertEqual(len(rows),4)
            self.assertNotIn('0',{r['article_id'] for r in rows})
            self.assertTrue(all(r['split']=='unassigned' and r['label_origin']=='unlabeled' for r in rows))
            with self.assertRaises(FileExistsError):
                prepare(db,metadata,out,4)
            out2 = root/'v2'
            prepare(db,metadata,out2,4)
            self.assertEqual((out/'metadata.jsonl').read_bytes(),(out2/'metadata.jsonl').read_bytes())
            with self.assertRaises(ValueError):
                prepare(db,metadata,root/'too_large',20)
            self.assertFalse((root/'too_large').exists())
