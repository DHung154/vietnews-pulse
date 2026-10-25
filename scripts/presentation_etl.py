"""One Spark session: real archived RSS HDFS -> core ETL -> SQL -> new Parquet."""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'src'))
from dashboard.presentation import session_path,LAB
from news_pipeline.rss import atomic_write_json


def run(session_id):
    from pyspark.sql import SparkSession,functions as F
    from news_pipeline.contracts import RAW_SCHEMA
    from scripts.nine_steps import build_lab_snapshot
    work=session_path(session_id)
    if not work.is_dir():raise ValueError('Session must be created by launcher')
    output=work/'etl.json'
    if output.exists():raise FileExistsError('Use a new session')
    uri='hdfs://namenode:8020/news/presentations/'+session_id
    source='hdfs://namenode:8020/news/lab/'+LAB+'/raw/rss'
    started=time.monotonic()
    spark=(SparkSession.builder.master('local[2]').appName('VietNews Defence '+session_id)
        .config('spark.sql.shuffle.partitions','2').config('spark.sql.session.timeZone','UTC')
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        def inventory():
            path=spark._jvm.org.apache.hadoop.fs.Path(source)
            fs=path.getFileSystem(spark._jsc.hadoopConfiguration())
            iterator=fs.listFiles(path,True);items=[]
            while iterator.hasNext():
                f=iterator.next();items.append((str(f.getPath()),f.getLen(),str(fs.getFileChecksum(f.getPath()))))
            return sorted(items)
        before=inventory()
        print('LIVE: spark.read.schema(RAW_SCHEMA).json('+source+')',flush=True)
        raw=spark.read.schema(RAW_SCHEMA).json(source)
        raw_count=raw.count()
        print('RAW observations =',raw_count,flush=True)
        if raw.filter(~F.col('dataset_id').eqNullSafe('rss_live_v2')).count():raise ValueError('Wrong dataset')
        print('LIVE: normalize_news -> validate -> deduplicate_news -> write Parquet (new scope)',flush=True)
        result=build_lab_snapshot(raw,uri+'/clean',uri+'/quarantine',work/'canonical.json',session_id)
        if not result['reconciled']:raise ValueError('ETL reconciliation failed')
        print(json.dumps(result,ensure_ascii=True),flush=True)
        clean=spark.read.parquet(uri+'/clean')
        clean.createOrReplaceTempView('demo_news')
        sql='SELECT source, COUNT(*) AS articles FROM demo_news GROUP BY source ORDER BY articles DESC'
        print('LIVE SPARK SQL:',sql,flush=True)
        rows=[r.asDict() for r in spark.sql(sql).collect()]
        spark.sql(sql).show(truncate=False)
        unique=clean.select('dataset_id','article_id').distinct().count()
        if unique!=result['clean_count'] or sum(r['articles'] for r in rows)!=unique:
            raise ValueError('SQL/count/unique mismatch')
        if before!=inventory():raise ValueError('Source HDFS changed')
        samples=[r.asDict() for r in clean.select('title','source','category').orderBy('article_id').limit(3).collect()]
        result.update(status='ok',session_id=session_id,sql=sql,sql_sources=rows,unique_keys=unique,
            samples=samples,source_hdfs=source,output_hdfs=uri,source_unchanged=True,
            elapsed_seconds=round(time.monotonic()-started,3),completed_at=datetime.now(timezone.utc).isoformat(),
            scope='Live ETL of one archived RSS poll, not a new crawl or full corpus',production_publish_performed=False)
        atomic_write_json(output,result)
        print('VERIFIED: counts/SQL/unique keys/HDFS source checksum; no production publish',flush=True)
    finally:spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session-id',required=True)
    run(parser.parse_args().session_id)
