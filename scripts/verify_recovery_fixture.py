"""Isolated cluster recovery probes. Every input in this script is SYNTHETIC."""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'scripts'))
from news_pipeline.stream import run_available
from news_pipeline.snapshot import build_snapshot
from news_pipeline.writer_lock import single_writer
from news_pipeline.rss import atomic_write_json
from news_pipeline.serving import export_snapshot
from news_pipeline.analytics import (completed_window_bounds, volume_6h, sentiment_daily,
    sentiment_summary, category_trends, keyword_document_frequency)
from score_analytics import enrich_news


def run(output: Path):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    output = output.resolve()
    if ROOT / 'runtime' not in output.parents or not output.name.startswith('recovery_fixture_'):
        raise ValueError('output must be a new runtime/recovery_fixture_* directory')
    output.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.master('spark://spark-master:7077')
        .appName('vietnews-synthetic-recovery').config('spark.sql.session.timeZone','UTC')
        .config('spark.sql.shuffle.partitions','2').config('spark.hadoop.fs.defaultFS','hdfs://namenode:8020')
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    jpath = spark._jvm.org.apache.hadoop.fs.Path
    remote = 'hdfs://namenode:8020/news/integration/' + output.name
    fs = jpath(remote).getFileSystem(spark._jsc.hadoopConfiguration())
    phases = []
    try:
        if fs.exists(jpath(remote)):
            raise FileExistsError(remote)
        fs.mkdirs(jpath(remote + '/raw/rss'))
        def row(index, published='2026-10-04T05:00:00+00:00'):
            return {'schema_version':'1','dataset_id':'rss_live_v2','source':'fixture','feed_id':'fixture',
                'observation_id':f'fixture-{index}','title_raw':f'FIXTURE kiểm thử bài {index}',
                'description_raw':'Dữ liệu tổng hợp kiểm thử, không phải tin thật.',
                'link_raw':f'https://example.test/fixture/{index}','published_at_raw':published,
                'crawled_at':'2026-10-04T13:00:00+00:00'}
        for name, rows, expected in [('base',[row(0)],1),
                ('plus10',[row(i) for i in range(1,11)],11),
                ('replay',[row(i) for i in range(1,11)],11),
                ('late',[row(11,'2026-10-01T23:00:00+00:00')],12)]:
            local = output / (name + '.jsonl')
            with local.open('x', encoding='utf-8') as handle:
                for item in rows:
                    handle.write(json.dumps(item, ensure_ascii=False) + '\n')
            fs.copyFromLocalFile(False,False,jpath(str(local)),jpath(remote+'/raw/rss/'+local.name))
            progress = run_available(spark,remote+'/raw/rss',remote+'/observations',remote+'/checkpoint',output/(name+'_stream.json'),100)
            snapshot = build_snapshot(spark.read.parquet(remote+'/observations'),
                remote+'/'+name+'/clean',remote+'/'+name+'/quarantine',output/(name+'_snapshot.json'),name,True)
            assert snapshot['clean_count'] == expected, (name,snapshot)
            phases.append({'phase':name,'canonical_count':expected,'observations':progress['output_rows_total'],
                'raw_count':snapshot['raw_count'],'duplicate_removed':snapshot['duplicate_removed']})
        clean = spark.read.parquet(snapshot['clean_path'])
        late = clean.filter(F.col('canonical_url')=='https://example.test/fixture/11').select('event_date').first()[0]
        assert late == '2026-10-02', late
        enriched = enrich_news(clean,None,'unavailable')
        bounds = completed_window_bounds(datetime.fromisoformat('2026-10-04T13:00:00+00:00'))
        frames = {'volume_6h':volume_6h(enriched,bounds[0],bounds[2]), 'sentiment_daily':sentiment_daily(enriched),
            'sentiment_summary':sentiment_summary(enriched),'category_trends':category_trends(enriched,*bounds),
            'keywords_window':keyword_document_frequency(enriched)}
        gold = output/'gold_cache'; gold.mkdir(); counts={}
        for name,frame in frames.items():
            target=remote+'/gold/'+name
            frame.write.mode('errorifexists').parquet(target)
            counts[name]=spark.read.parquet(target).count()
            fs.copyToLocalFile(False,jpath(target),jpath(str(gold/name)),True)
        score={'status':'ok','gold_path':str(gold),'gold_counts':counts,'enriched_count':12,
            'input_snapshot_ids':['late'],'model_versions':['unavailable'],
            'window_bounds':dict(zip(('previous_start','current_start','window_end'),bounds))}
        score_path=output/'score.json'; atomic_write_json(score_path,score)
        serving=output/'serving'; lock=serving/'.snapshot_writer.lock'
        with single_writer(lock):
            export_snapshot(enriched,gold,serving,'late','unavailable',output/'serving.json',score_manifest_path=score_path)
        pointer=(serving/'current.json').read_bytes()
        bad=output/'bad_score.json'; atomic_write_json(bad,dict(score,enriched_count=13,input_snapshot_ids=['failed']))
        try:
            with single_writer(lock):
                export_snapshot(enriched.withColumn('snapshot_id',F.lit('failed')),gold,serving,'failed','unavailable',
                    output/'failed_export.json',score_manifest_path=bad)
        except ValueError as exc:
            failure=str(exc)
        else:
            raise AssertionError('invalid count was published')
        assert (serving/'current.json').read_bytes()==pointer
        code="from pathlib import Path; from news_pipeline.writer_lock import single_writer; import sys;\ntry:\n with single_writer(Path(sys.argv[1])): pass\nexcept RuntimeError: sys.exit(8)"
        with single_writer(lock):
            concurrent=subprocess.run([sys.executable,'-c',code,str(lock)],cwd=ROOT,timeout=10)
        assert concurrent.returncode==8
        code="from pathlib import Path; from news_pipeline.writer_lock import single_writer; import os,sys;\nwith single_writer(Path(sys.argv[1])): os._exit(9)"
        crashed=subprocess.run([sys.executable,'-c',code,str(lock)],cwd=ROOT,timeout=10)
        assert crashed.returncode==9
        with single_writer(lock):
            assert (serving/'current.json').read_bytes()==pointer
        result={'status':'ok','data_type':'synthetic fixture ONLY; not live news or human labels',
            'phases':phases,'late_event_date_vietnam':late,'failed_publish_preserves_pointer':True,
            'failure':failure,'concurrent_writer_exit':concurrent.returncode,'crashed_writer_exit':crashed.returncode,
            'crash_releases_lock':True,'hdfs_root':remote,'spark_master':spark.sparkContext.master,
            'completed_at':datetime.now(timezone.utc).isoformat(),
            'limitations':['Publisher crash was before publication; this is not kill-during-Parquet-write evidence.',
                'rss_live_v2 is used only inside this isolated synthetic test namespace; never copied into production.']}
        atomic_write_json(output/'result.json',result)
        print(json.dumps(result,ensure_ascii=True))
    finally:
        spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--output',type=Path,required=True)
    run(parser.parse_args().output)
