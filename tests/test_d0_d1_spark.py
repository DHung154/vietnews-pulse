"""Run with NEWS_SPARK_TESTS=1 in the existing Linux Spark runtime."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from src.news_pipeline.stream import run_available


@unittest.skipUnless(os.environ.get('NEWS_SPARK_TESTS') == '1', 'requires usable Linux Spark runtime; Docker currently unavailable')
class SparkScopeTest(unittest.TestCase):
    def test_real_stream_excludes_sibling_and_contaminating_historical(self):
        from pyspark.sql import SparkSession
        spark=SparkSession.builder.master('local[2]').appName('d0-scope-regression').getOrCreate()
        try:
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp); raw=root/'raw'; rss=raw/'rss'; rss.mkdir(parents=True)
                history=raw/'historical'; history.mkdir()
                row={'dataset_id':'rss_live','source':'fixture','title_raw':'Title','link_raw':'https://example.test/a','crawled_at':'2026-09-16T00:00:00+00:00','observation_id':'live'}
                old=dict(row,dataset_id='historical_test',observation_id='old')
                (rss/'live.jsonl').write_text(json.dumps(row)+'\n'+json.dumps(old)+'\n')
                (history/'old.jsonl').write_text(json.dumps(old)+'\n')
                before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in raw.rglob('*.jsonl')}
                result=run_available(spark,raw,root/'sink',root/'checkpoint',root/'manifest.json',2)
                self.assertEqual(result['output_rows_total'],1)
                self.assertEqual(spark.read.parquet(str(root/'sink')).first().dataset_id,'rss_live')
                self.assertEqual(before,{p:hashlib.sha256(p.read_bytes()).hexdigest() for p in before})
                # Exercise existing Spark dedup without changing M3 semantics.
                from src.news_pipeline.contracts import RAW_SCHEMA
                from src.news_pipeline.etl import normalize_news, deduplicate_news
                observations = [dict(row, feed_id='source_a', observation_id='a'), dict(row, feed_id='source_b', observation_id='b')]
                clean, duplicate = deduplicate_news(normalize_news(spark.createDataFrame(observations, RAW_SCHEMA)))
                self.assertEqual((clean.count(), duplicate.count()), (1, 1))
                older_category = dict(row, title_raw='Old version', feed_id='category_feed', observation_id='category_old', category_raw=None, feed_canonical_category='business', crawled_at='2026-09-16T00:00:00+00:00')
                newer_general = dict(row, title_raw='Newest version', feed_id='general_feed', observation_id='category_new', feed_canonical_category='other', crawled_at='2026-09-16T01:00:00+00:00')
                preferred, _ = deduplicate_news(normalize_news(spark.createDataFrame([older_category, newer_general], RAW_SCHEMA)))
                selected = preferred.select('title', 'category').first()
                self.assertEqual((selected['title'], selected['category']), ('Newest version', 'business'))
        finally:
            spark.stop()
