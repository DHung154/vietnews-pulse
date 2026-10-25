"""Versioned Kafka/HDFS replay of an existing canonical snapshot, never raw crawl."""
import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
STEPS=('prepare','kafka','read','dedup','sql','cluster','score','serve','verify')


def validate_config(config):
    if config.get('mode')!='frozen_canonical_snapshot_replay':
        raise ValueError('input is canonical replay, not new raw RSS')
    if not str(config.get('input_hdfs','')).startswith('hdfs://namenode:8020/news/shared/rss_live_v2/'):
        raise ValueError('input outside shared read-only snapshot scope')
    if not isinstance(config.get('expected_count'),int) or config['expected_count']<=0:
        raise ValueError('positive expected_count required')
    if not re.fullmatch(r'[A-Za-z0-9_-]+',config.get('model_version','')):
        raise ValueError('invalid model version')
    stamp=datetime.fromisoformat(config['as_of'].replace('Z','+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('as_of needs timezone')
    return config


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args):
    from news_pipeline.writer_lock import single_writer
    from news_pipeline.rss import atomic_write_json
    if not re.fullmatch(r'full_[a-f0-9]{20}',args.run_id) or args.attempt<1:
        raise ValueError('invalid run/attempt')
    config=validate_config(json.loads(args.config.read_text(encoding='utf-8')))
    coverage=Path(config['coverage_manifest']).resolve()
    if ROOT/'evidence' not in coverage.parents:
        raise ValueError('coverage must be below evidence')
    cp=json.loads(coverage.read_text(encoding='utf-8'))
    if cp.get('window_end_basis')!='explicit_end_local':
        raise ValueError('coverage needs explicit end_local')
    fingerprint=dict(config_sha256=sha(args.config),coverage_sha256=sha(coverage))
    work=ROOT/'runtime/nine_steps'/args.run_id
    with single_writer(ROOT/'runtime/nine_steps/.lab_writer.lock'):
        work.mkdir(parents=True,exist_ok=True)
        plan_path=work/'plan.json'
        if plan_path.exists():
            plan=json.loads(plan_path.read_text())
            if any(plan.get(k)!=v for k,v in fingerprint.items()):
                raise ValueError('input config/coverage changed during run')
        else:
            if args.step!='prepare':
                raise ValueError('prepare must run first')
            plan=dict(config,**fingerprint,run_id=args.run_id)
        target=work/(args.step+'.json')
        if target.exists():
            result=json.loads(target.read_text())
            if result.get('status')!='ok' or result.get('run_id')!=args.run_id:
                raise ValueError('invalid completed-stage evidence')
            return result
        return execute(args,work,plan,plan_path,target,atomic_write_json)


def execute(args,work,plan,plan_path,target,save):
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import StructType
    from news_pipeline.etl import deduplicate_news
    from news_pipeline.lab_analytics import sql_tables,cluster_news
    from news_pipeline.analytics import completed_window_bounds,category_trends,volume_6h,sentiment_daily,sentiment_summary,keyword_document_frequency
    from news_pipeline.serving import export_snapshot
    from score_analytics import enrich_news
    jars=sorted(Path('/home/airflow/.ivy2/jars').glob('*.jar'))
    spark=(SparkSession.builder.master('local[2]').appName('vietnews-full-'+args.step)
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
        .config('spark.jars',','.join(str(p) for p in jars))
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').config('spark.hadoop.dfs.replication','3').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    remote='hdfs://namenode:8020/news/full_runs/'+args.run_id+'/'+args.step+'/attempt_'+str(args.attempt)
    path=spark._jvm.org.apache.hadoop.fs.Path
    fs=path(remote).getFileSystem(spark._jsc.hadoopConfiguration())
    bounds=completed_window_bounds(datetime.fromisoformat(plan['as_of']))
    result=dict(status='running',run_id=args.run_id,step=args.step,attempt=args.attempt,mode=plan['mode'],expected_count=plan['expected_count'],master=spark.sparkContext.master,production_publish_performed=False)
    def previous(step):
        data=json.loads((work/(step+'.json')).read_text())
        if data.get('status')!='ok':
            raise ValueError('previous stage not successful')
        return data
    def inventory():
        entries=fs.listFiles(path(plan['input_hdfs']),True)
        values=[]
        while entries.hasNext():
            item=entries.next()
            values.append(dict(path=str(item.getPath()),bytes=item.getLen(),checksum=str(fs.getFileChecksum(item.getPath()))))
        return sorted(values,key=lambda r:r['path'])
    def clean():
        return spark.read.parquet(previous('dedup')['output_hdfs'])
    try:
        if fs.exists(path(remote)):
            raise FileExistsError('attempt output exists; preserve it and use next retry/run')
        if args.step=='prepare':
            source=spark.read.parquet(plan['input_hdfs'])
            count=source.count()
            if source.filter(~F.col('dataset_id').eqNullSafe('rss_live_v2')).count() or source.select('article_id').distinct().count()!=count or count!=plan['expected_count']:
                raise ValueError('input count/keys/scope mismatch')
            model=ROOT/'runtime/m04/models'/plan['model_version']
            if not model.is_dir():
                raise FileNotFoundError(model)
            plan.update(input_inventory=inventory(),schema_json=source.schema.json(),model_files={str(p.relative_to(model)):sha(p) for p in sorted(model.rglob('*')) if p.is_file()})
            if not plan_path.exists():
                save(plan_path,plan)
            result.update(article_count=count,input_snapshot=plan['snapshot_id'],coverage_status=json.loads(Path(plan['coverage_manifest']).read_text())['coverage_status'])
        elif args.step=='kafka':
            if inventory()!=plan['input_inventory']:
                raise ValueError('source changed before producer')
            topic='vietnews-'+args.run_id+'-a'+str(args.attempt)
            source=spark.read.parquet(plan['input_hdfs'])
            source.select(F.col('article_id').alias('key'),F.to_json(F.struct(*[F.col(c) for c in source.columns])).alias('value')).write.format('kafka').option('kafka.bootstrap.servers','vietnews-kafka:29092').option('topic',topic).option('kafka.acks','all').save()
            stream=spark.readStream.format('kafka').option('kafka.bootstrap.servers','vietnews-kafka:29092').option('subscribe',topic).option('startingOffsets','earliest').option('failOnDataLoss','true').load()
            query=stream.select('topic','partition','offset',F.col('value').cast('string').alias('payload_json')).writeStream.format('parquet').option('path',remote+'/observations').option('checkpointLocation',remote+'/checkpoint').trigger(availableNow=True).start()
            query.awaitTermination()
            total=spark.read.parquet(remote+'/observations').count()
            if total<plan['expected_count']:
                raise ValueError('Kafka/HDFS lost rows')
            result.update(topic=topic,observation_count=total,output_hdfs=remote+'/observations',checkpoint=remote+'/checkpoint',query_id=str(query.id),progress=list(query.recentProgress),semantics='At-least-once producer; next stage deduplicates canonical keys')
        elif args.step=='read':
            observed=spark.read.parquet(previous('kafka')['output_hdfs'])
            schema=StructType.fromJson(json.loads(plan['schema_json']))
            parsed=observed.select(F.from_json('payload_json',schema,{'mode':'FAILFAST'}).alias('row')).select('row.*')
            total=parsed.count()
            if parsed.filter(F.col('article_id').isNull()|~F.col('dataset_id').eqNullSafe('rss_live_v2')).count():
                raise ValueError('invalid Kafka payload')
            parsed.write.mode('errorifexists').parquet(remote+'/parsed')
            result.update(observation_count=total,output_hdfs=remote+'/parsed')
        elif args.step=='dedup':
            source=spark.read.parquet(previous('read')['output_hdfs'])
            canonical,duplicates=deduplicate_news(source)
            canonical=canonical.withColumn('snapshot_id',F.lit(args.run_id)).cache()
            count=canonical.count()
            if count!=plan['expected_count']:
                raise ValueError('canonical count mismatch')
            reference=spark.read.parquet(plan['input_hdfs'])
            cols=[c for c in reference.columns if c!='snapshot_id']
            # Keys are unique on both sides; a full join avoids the stalled cached EXCEPT plan.
            from functools import reduce
            pairs=canonical.withColumn('_actual_present',F.lit(True)).alias('actual').join(reference.withColumn('_reference_present',F.lit(True)).alias('reference'),['dataset_id','article_id'],'full')
            conditions=[~F.col('actual.'+c).eqNullSafe(F.col('reference.'+c)) for c in cols if c not in {'dataset_id','article_id'}]
            mismatch=pairs.filter(F.col('_actual_present').isNull()|F.col('_reference_present').isNull()|reduce(lambda a,b:a|b,conditions,F.lit(False))).count()
            if mismatch:
                raise ValueError('canonical differs from frozen source')
            canonical.write.mode('errorifexists').parquet(remote+'/clean')
            result.update(article_count=count,duplicate_count=duplicates.count(),reference_mismatches=mismatch,output_hdfs=remote+'/clean')
            canonical.unpersist()
        elif args.step=='sql':
            frames=sql_tables(clean(),'rss_live_v2',bounds)
            reference=category_trends(clean(),*bounds)
            actual=frames['category_trends_sql'].select(reference.columns)
            mismatch=actual.exceptAll(reference).count()+reference.exceptAll(actual).count()
            if mismatch:
                raise ValueError('SQL/DataFrame formula differs')
            counts={}
            for name,frame in frames.items():
                frame.write.mode('errorifexists').parquet(remote+'/'+name)
                counts[name]=frame.count()
            result.update(table_counts=counts,reference_mismatches=mismatch,output_hdfs=remote)
        elif args.step=='cluster':
            result.update(cluster_news(clean(),'rss_live_v2',remote+'/cluster',k=8,vocab_size=512,pca_dim=16))
            result['output_hdfs']=remote+'/cluster'
        elif args.step=='score':
            model=ROOT/'runtime/m04/models'/plan['model_version']
            if plan['model_files']!={str(p.relative_to(model)):sha(p) for p in sorted(model.rglob('*')) if p.is_file()}:
                raise ValueError('model artifact changed')
            fs.mkdirs(path(remote+'/model'))
            fs.copyFromLocalFile(False,False,path(str(model)),path(remote+'/model/'+model.name))
            enriched=enrich_news(clean(),remote+'/model/'+model.name,model.name)
            enriched.write.mode('errorifexists').parquet(remote+'/enriched')
            result.update(output_hdfs=remote+'/enriched',article_count=enriched.count(),model_version=model.name)
        elif args.step=='serve':
            from news_pipeline.writer_lock import single_writer
            enriched=spark.read.parquet(previous('score')['output_hdfs']).cache()
            frames=dict(volume_6h=volume_6h(enriched,bounds[0],bounds[2]),sentiment_daily=sentiment_daily(enriched),sentiment_summary=sentiment_summary(enriched),category_trends=category_trends(enriched,*bounds),keywords_window=keyword_document_frequency(enriched))
            local=work/'serve'/('attempt_'+str(args.attempt))
            gold=local/'gold'
            gold.mkdir(parents=True,exist_ok=False)
            counts={}
            for name,frame in frames.items():
                uri=remote+'/gold/'+name
                frame.write.mode('errorifexists').parquet(uri)
                counts[name]=frame.count()
                fs.copyToLocalFile(False,path(uri),path(str(gold/name)),True)
            score=dict(status='ok',gold_path=str(gold),gold_counts=counts,enriched_count=enriched.count(),input_snapshot_ids=[args.run_id],model_versions=[plan['model_version']],window_bounds=dict(zip(('previous_start','current_start','window_end'),bounds)))
            save(local/'score.json',score)
            with single_writer(local/'serving/.snapshot_writer.lock'):
                serving=export_snapshot(enriched,gold,local/'serving',args.run_id,plan['model_version'],local/'serving.json',None,local/'score.json',ROOT/'config/rss_sources.yaml',Path(plan['coverage_manifest']))
            enriched.unpersist()
            result.update(serving=serving,serving_root=str(local/'serving'),article_count=score['enriched_count'])
        elif args.step=='verify':
            root=Path(previous('serve')['serving_root'])
            pointer=json.loads((root/'current.json').read_text())
            connection=sqlite3.connect((root/pointer['db_path']).as_uri()+'?mode=ro',uri=True)
            try:
                assert connection.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
                count,unique=connection.execute("SELECT COUNT(*),COUNT(DISTINCT article_id) FROM articles WHERE dataset_id='rss_live_v2'").fetchone()
                assert count==unique==plan['expected_count']
            finally:
                connection.close()
            assert inventory()==plan['input_inventory']
            result.update(article_count=count,unique_keys=unique,input_unchanged=True,model_version=pointer['model_version'],limitations=['Frozen canonical snapshot replay, not raw collection or live freshness.', 'One-host local[2] Airflow jobs; standalone benchmark is separate.', 'Sentiment experimental; no independent human gold accuracy claim.'])
        result.update(status='ok',completed_at=datetime.now(timezone.utc).isoformat())
        save(target,result)
        print(json.dumps(result,ensure_ascii=True),flush=True)
        return result
    except Exception as exc:
        save(work/(args.step+'_failed_attempt_'+str(args.attempt)+'_'+datetime.now(timezone.utc).strftime('%H%M%S%f')+'.json'),dict(status='failed',error=str(exc),error_type=type(exc).__name__))
        raise
    finally:
        spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--step',choices=STEPS,required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--attempt',type=int,default=1)
    parser.add_argument('--config',type=Path,required=True)
    run(parser.parse_args())
