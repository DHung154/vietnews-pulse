"""Safety checks independent of Docker plus opt-in real Spark SQL checks."""
import ast
import os
import tempfile
from pathlib import Path
import unittest
from scripts.nine_steps import validate_run_id


class NineStepSafetyTest(unittest.TestCase):
    def test_run_id_rejects_path_and_shell_injection(self):
        for value in ('../raw','/news','a/b','a\\b','x;rm -rf','', 'x'*81):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_run_id(value)
        self.assertEqual(validate_run_id('airflow_deadbeef_1'),'airflow_deadbeef_1')

    def test_dag_is_manual_and_has_nine_stages(self):
        # Parses without importing Airflow into the existing host environment.
        path=Path(__file__).resolve().parents[1]/'airflow/dags/vietnews_nine_steps.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        stages=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='STEPS' for t in n.targets))
        self.assertEqual(len(stages),9)
        call=next(n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='DAG')
        kwargs={item.arg: ast.literal_eval(item.value) for item in call.keywords if item.arg in ('schedule','catchup','max_active_runs')}
        self.assertEqual(kwargs,dict(schedule=None,catchup=False,max_active_runs=1))


@unittest.skipUnless(os.environ.get('NEWS_SPARK_TESTS')=='1','Linux Spark integration opt-in')
class NineStepSparkTest(unittest.TestCase):
    def test_materialized_snapshot_keeps_latest_and_quarantine(self):
        from pyspark.sql import SparkSession
        from src.news_pipeline.contracts import RAW_SCHEMA
        from scripts.nine_steps import build_lab_snapshot
        spark=SparkSession.builder.master('local[2]').appName('nine-canonical-regression').getOrCreate()
        try:
            old=dict(dataset_id='rss_live_v2',source='fixture',title_raw='Old title',
                link_raw='https://example.test/a',crawled_at='2026-10-08T00:00:00Z',
                observation_id='old',feed_canonical_category='business')
            new=dict(old,title_raw='Newest title',observation_id='new',
                crawled_at='2026-10-08T01:00:00Z',feed_canonical_category='other')
            invalid=dict(old,title_raw='',link_raw='',observation_id='invalid')
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)
                result=build_lab_snapshot(spark.createDataFrame([old,new,invalid],RAW_SCHEMA),
                    str(root/'clean'),str(root/'quarantine'),root/'manifest.json','fixture')
                self.assertEqual((result['raw_count'],result['clean_count'],result['duplicate_removed'],result['invalid_count']),(3,1,1,1))
                row=spark.read.parquet(str(root/'clean')).first()
                self.assertEqual((row.title,row.category),('Newest title','business'))
                self.assertEqual(spark.read.parquet(str(root/'quarantine')).count(),2)
        finally:
            spark.stop()

    def test_sql_matches_dataframe_with_empty_and_zero_baseline(self):
        from pyspark.sql import SparkSession
        from src.news_pipeline.lab_analytics import sql_tables
        from src.news_pipeline.analytics import category_trends
        spark=SparkSession.builder.master('local[2]').appName('nine-sql-regression').config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate()
        try:
            rows=[('rss_live_v2','s','same','2026-10-07T19:00:00Z','2026-10-08'),
                  ('rss_live_v2','s','same','2026-10-08T01:00:00Z','2026-10-08'),
                  ('rss_live_v2','s','new','2026-10-08T01:00:00Z','2026-10-08'),
                  ('rss_live_v2','s','empty',None,None),
                  ('historical_fixture','s','new','2026-10-08T01:00:00Z','2026-10-08')]
            frame=spark.createDataFrame(rows,'dataset_id string,source string,category string,event_time string,event_date string')
            bounds=('2026-10-07T17:00:00Z','2026-10-07T23:00:00Z','2026-10-08T05:00:00Z')
            actual=sql_tables(frame,'rss_live_v2',bounds)
            reference=category_trends(frame.filter("dataset_id='rss_live_v2'"),*bounds)
            self.assertEqual(actual['category_trends_sql'].select(reference.columns).exceptAll(reference).count(),0)
            self.assertEqual(reference.exceptAll(actual['category_trends_sql'].select(reference.columns)).count(),0)
            self.assertEqual(sum(r['count'] for r in actual['daily_volume_sql'].collect()),3)
            statuses={r['category']:r['status'] for r in actual['category_trends_sql'].collect()}
            self.assertEqual(statuses,dict(same='ok',new='new',empty='no_activity'))
        finally:
            spark.stop()
