"""Publish a frozen dashboard corpus to a new HDFS sharing scope, never overwrite."""
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
SNAPSHOT='snapshot_20261008T090238Z_81c6a670'
TARGET='hdfs://namenode:8020/news/shared/rss_live_v2/'+SNAPSHOT
WORK=ROOT/'runtime/team_full_20261014'
SERVING=ROOT/'runtime/dashboard_refresh_20261008T084631Z_r2/serving'
JOB=ROOT/'runtime/dashboard_refresh_20261008T084631Z_r2/jobs/refresh_20261008T085806Z_f5f2cc24'
COMPARE=('article_id','title','description','source','category','canonical_url',
         'published_at','event_time','event_date','sentiment_label','model_version')


def main():
    from pyspark.sql import SparkSession,functions as F,types as T
    pointer=json.loads((SERVING/'current.json').read_text())
    if pointer['snapshot_id']!=SNAPSHOT:
        raise ValueError('Dashboard snapshot changed; review new source before publishing')
    db=SERVING/pointer['db_path']
    before=hashlib.sha256(db.read_bytes()).hexdigest()
    reference=WORK/'reference.jsonl'
    with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as con:
        count,unique=con.execute("SELECT COUNT(*),COUNT(DISTINCT article_id) FROM articles WHERE dataset_id='rss_live_v2'").fetchone()
        if count!=unique or count!=13388:
            raise ValueError('Unexpected SQLite corpus count/key')
        with reference.open('x',encoding='utf-8') as out:
            for row in con.execute('SELECT '+','.join(COMPARE)+" FROM articles WHERE dataset_id='rss_live_v2'"):
                out.write(json.dumps(dict(zip(COMPARE,row)),ensure_ascii=False)+'\n')
    spark=(SparkSession.builder.master('local[2]').appName('publish-team-full')
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
        .config('spark.hadoop.dfs.replication','3')
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        path=spark._jvm.org.apache.hadoop.fs.Path
        fs=path(TARGET).getFileSystem(spark._jsc.hadoopConfiguration())
        if fs.exists(path(TARGET)):
            raise FileExistsError('HDFS sharing scope already exists; no overwrite')
        clean=spark.read.parquet(str(JOB/'snapshots'/SNAPSHOT/'clean')).filter(F.col('dataset_id')=='rss_live_v2')
        source=spark.read.parquet(str(JOB/'score/score_7ccc5c33/enriched')).filter(F.col('dataset_id')=='rss_live_v2')
        # No model feature vectors are needed for team analysis/sharing.
        columns=clean.columns+['sentiment_label','prob_negative','prob_neutral','prob_positive',
            'predicted_class_probability','model_version','text_preprocess_version']
        enriched=source.select(*columns).cache()
        schema=T.StructType([T.StructField(c,T.StringType(),True) for c in COMPARE])
        expected=spark.read.schema(schema).json(str(reference))
        def compare(frame):
            actual=frame.select(*COMPARE).withColumn('_actual',F.lit(1)).alias('p')
            ref=expected.withColumn('_reference',F.lit(1)).alias('r')
            joined=actual.join(ref,F.col('p.article_id')==F.col('r.article_id'),'full')
            condition=F.col('p._actual').isNull()|F.col('r._reference').isNull()
            for name in COMPARE:
                condition=condition|~F.col('p.'+name).eqNullSafe(F.col('r.'+name))
            return joined.filter(condition).count()
        if enriched.count()!=count or enriched.select('article_id').distinct().count()!=count or compare(enriched):
            raise ValueError('Source Parquet differs from dashboard')
        if clean.count()!=count or clean.select('article_id').distinct().count()!=count:
            raise ValueError('Clean count mismatch')
        clean_ids=clean.select('article_id').withColumn('_clean',F.lit(1)).alias('c')
        enriched_ids=enriched.select('article_id').withColumn('_enriched',F.lit(1)).alias('e')
        if clean_ids.join(enriched_ids,'article_id','full').filter(F.col('_clean').isNull()|F.col('_enriched').isNull()).count():
            raise ValueError('Clean/enriched article keys differ')
        clean.write.mode('errorifexists').parquet(TARGET+'/clean')
        enriched.write.mode('errorifexists').parquet(TARGET+'/enriched')
        reread=spark.read.parquet(TARGET+'/enriched')
        mismatch=compare(reread)
        if mismatch or reread.count()!=count or spark.read.parquet(TARGET+'/clean').count()!=count:
            raise ValueError('HDFS read-back mismatch')
        after=hashlib.sha256(db.read_bytes()).hexdigest()
        if before!=after:
            raise ValueError('Dashboard database changed')
        result=dict(status='ok',snapshot_id=SNAPSHOT,dataset_id='rss_live_v2',article_count=count,
            clean_count=count,unique_keys=count,sqlite_field_mismatches=mismatch,
            compared_fields=COMPARE,sqlite_sha256=after,model_version=pointer['model_version'],
            source_counts=[r.asDict() for r in reread.groupBy('source').count().collect()],
            hdfs_root=TARGET,live_datanodes=len(fs.getDataNodeStats()),
            note='Frozen dashboard snapshot08/10, not latest crawl; sentiment predictions are not human gold')
        with (WORK/'publish.json').open('x',encoding='utf-8') as out:
            json.dump(result,out,ensure_ascii=False,indent=2)
        fs.copyFromLocalFile(False,False,path(str(WORK/'publish.json')),path(TARGET+'/manifest.json'))
        print(json.dumps(result,ensure_ascii=True))
    finally:
        spark.stop()


if __name__=='__main__':
    main()
