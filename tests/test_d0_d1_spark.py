"""Run with NEWS_SPARK_TESTS=1 in the existing Linux Spark runtime."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from src.news_pipeline.stream import run_available


@unittest.skipUnless(os.environ.get('NEWS_SPARK_TESTS') == '1', 'Linux Spark integration is opt-in: set NEWS_SPARK_TESTS=1 in the container')
class SparkScopeTest(unittest.TestCase):
    def test_local_serving_cache_is_not_resolved_against_hdfs_default(self):
        from pyspark.sql import SparkSession, functions as F
        from src.news_pipeline.serving import export_snapshot
        from src.news_pipeline.analytics import (volume_6h, sentiment_daily, sentiment_summary,
            category_trends, keyword_document_frequency, completed_window_bounds)
        from src.news_pipeline.etl import normalize_news, deduplicate_news
        from src.news_pipeline.contracts import RAW_SCHEMA
        from scripts.score_analytics import enrich_news
        from datetime import datetime
        spark = SparkSession.builder.master('local[2]').appName('serving-local-cache').getOrCreate()
        try:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp); gold = root / 'gold'
                raw = {'dataset_id':'rss_live_v2','source':'fixture','feed_id':'fixture',
                    'title_raw':'Tin kiểm thử','link_raw':'https://example.test/a',
                    'crawled_at':'2026-10-04T00:00:00+00:00','observation_id':'fixture'}
                clean, _ = deduplicate_news(normalize_news(spark.createDataFrame([raw], RAW_SCHEMA)))
                enriched = enrich_news(clean.withColumn('snapshot_id', F.lit('fixture-snapshot')), None, 'unavailable')
                bounds = completed_window_bounds(datetime.fromisoformat('2026-10-04T13:00:00+00:00'))
                frames = {'volume_6h':volume_6h(enriched,bounds[0],bounds[2]),
                    'sentiment_daily':sentiment_daily(enriched), 'sentiment_summary':sentiment_summary(enriched),
                    'category_trends':category_trends(enriched,*bounds), 'keywords_window':keyword_document_frequency(enriched)}
                for name, frame in frames.items():
                    frame.write.parquet((gold / name).resolve().as_uri())
                manifest = {'status':'ok','gold_path':str(gold),'gold_counts':{name:frame.count() for name,frame in frames.items()},
                    'enriched_count':1,'input_snapshot_ids':['fixture-snapshot'],'model_versions':['unavailable'],
                    'window_bounds':dict(zip(('previous_start','current_start','window_end'), bounds))}
                score = root / 'score.json'; score.write_text(json.dumps(manifest), encoding='utf-8')
                # A bare local path would incorrectly become hdfs://.../tmp/... here.
                spark._jsc.hadoopConfiguration().set('fs.defaultFS', 'hdfs://namenode:8020')
                result = export_snapshot(enriched,gold,root/'serving','fixture-snapshot','unavailable',root/'evidence.json',score_manifest_path=score)
                self.assertEqual((result['article_count'], result['integrity_check']), (1,'ok'))
                self.assertEqual(enriched.filter(F.col('sentiment_label').isNotNull()).count(), 0)
        finally:
            spark._jsc.hadoopConfiguration().set('fs.defaultFS', 'file:///')
            spark.stop()

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
