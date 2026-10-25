"""Isolated, manual news adaptation of the nine-step teaching lab."""
from __future__ import annotations
import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'scripts'))
STEPS = ('prepare', 'upload', 'read', 'clean', 'sql', 'cluster', 'save', 'kafka', 'verify')


def validate_run_id(value):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', value):
        raise ValueError('run_id must be a safe, new version name of at most 80 characters')
    return value


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def inventory(manifest):
    from verify_hdfs_pipeline import input_path
    manifest = manifest.resolve()
    if ROOT / 'evidence/d02' not in manifest.parents or not manifest.name.startswith('collect_manifest_'):
        raise ValueError('use an immutable collector manifest below evidence/d02')
    data = json.loads(manifest.read_text(encoding='utf-8'))
    files = []
    seen = set()
    for row in data.get('results', []):
        if not row.get('raw_path'):
            continue
        path = input_path(row['raw_path'])
        if ROOT / 'runtime/data_collection_v2/raw/rss' not in path.parents or path.is_symlink():
            raise ValueError('raw path is outside immutable RSS scope')
        if path in seen:
            raise ValueError('duplicate raw path in manifest')
        seen.add(path)
        count = sum(1 for line in path.open(encoding='utf-8') if line.strip())
        if digest(path) != row['sha256'] or count != row['records']:
            raise ValueError('immutable raw hash/count mismatch')
        files.append(dict(path=str(path), sha256=row['sha256'], rows=count))
    if not files:
        raise ValueError('manifest contains no committed raw files')
    return dict(source_manifest=str(manifest), source_sha256=digest(manifest), files=files)


def build_lab_snapshot(raw, *args):
    from news_pipeline.etl import normalize_news
    from news_pipeline.snapshot import build_snapshot
    # Materialize once on executors before branching into clean/quarantine.
    normalized = normalize_news(raw).repartition(2).cache()
    try:
        normalized.count()
        return build_snapshot(normalized, *args, input_is_normalized=True)
    finally:
        normalized.unpersist()


def _run(args):
    from pyspark.sql import SparkSession, functions as F
    from news_pipeline.rss import atomic_write_json
    from news_pipeline.contracts import RAW_SCHEMA
    from news_pipeline.lab_analytics import sql_tables, cluster_news
    from news_pipeline.analytics import (completed_window_bounds, category_trends, volume_6h,
        sentiment_daily, sentiment_summary, keyword_document_frequency)
    from news_pipeline.serving import export_snapshot
    from news_pipeline.writer_lock import single_writer
    from score_analytics import enrich_news

    work = ROOT / 'runtime/nine_steps' / validate_run_id(args.run_id)
    remote = 'hdfs://namenode:8020/news/lab/' + args.run_id
    if args.step == 'prepare':
        plan = inventory(args.source_manifest)
        as_of = datetime.fromisoformat(args.as_of.replace('Z', '+00:00'))
        if as_of.tzinfo is None:
            raise ValueError('as_of requires timezone')
        coverage = args.coverage_manifest.resolve()
        if ROOT / 'evidence' not in coverage.parents:
            raise ValueError('coverage manifest must be below evidence')
        cp = json.loads(coverage.read_text(encoding='utf-8'))
        if cp.get('window_end_basis') != 'explicit_end_local':
            raise ValueError('coverage must use explicit end_local')
        plan.update(run_id=args.run_id, hdfs_root=remote, dataset_id='rss_live_v2',
            as_of=as_of.isoformat(), model_version='tfidf_lr_weak_v3_20260917',
            coverage_manifest=str(coverage), coverage_sha256=digest(coverage),
            coverage_status=cp['coverage_status'], source_type='real RSS, one immutable poll')
        work.mkdir(parents=True, exist_ok=False)
        atomic_write_json(work / 'plan.json', plan)
    else:
        plan = json.loads((work / 'plan.json').read_text(encoding='utf-8'))
    target = work / (args.step + '.json')
    if target.exists():
        raise FileExistsError('completed stage evidence exists; use a new run for a full rerun')
    if digest(Path(plan['source_manifest'])) != plan['source_sha256'] or digest(Path(plan['coverage_manifest'])) != plan['coverage_sha256']:
        raise ValueError('input provenance changed since prepare')
    jars = sorted(Path('/home/airflow/.ivy2/jars').glob('*.jar'))
    spark = (SparkSession.builder.master('local[2]').appName('vietnews-lab-' + args.step)
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
        .config('spark.jars', ','.join(str(p) for p in jars))
        .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
        .config('spark.hadoop.dfs.replication', '3').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    jpath = spark._jvm.org.apache.hadoop.fs.Path
    fs = jpath(remote).getFileSystem(spark._jsc.hadoopConfiguration())
    bounds = completed_window_bounds(datetime.fromisoformat(plan['as_of']))
    result = dict(step=args.step, status='ok', run_id=args.run_id, dataset_id=plan['dataset_id'],
                  spark_master=spark.sparkContext.master, data_type=plan['source_type'])
    try:
        if args.step == 'prepare':
            if fs.exists(jpath(remote)):
                raise FileExistsError('HDFS run path already exists')
            assert fs.mkdirs(jpath(remote + '/raw/rss'))
            result['live_datanodes'] = len(fs.getDataNodeStats())
        elif args.step == 'upload':
            for row in plan['files']:
                path = Path(row['path'])
                if digest(path) != row['sha256']:
                    raise ValueError('raw input changed')
                fs.copyFromLocalFile(False, False, jpath(str(path)), jpath(remote + '/raw/rss/' + path.name))
            result['input_rows'] = sum(r['rows'] for r in plan['files'])
            result['raw_files'] = len(plan['files'])
        elif args.step == 'read':
            raw = spark.read.schema(RAW_SCHEMA).json(remote + '/raw/rss')
            result['input_rows'] = raw.count()
            assert result['input_rows'] == sum(r['rows'] for r in plan['files'])
            result['excluded_other_datasets'] = raw.filter(~F.col('dataset_id').eqNullSafe('rss_live_v2')).count()
        elif args.step == 'clean':
            raw = spark.read.schema(RAW_SCHEMA).json(remote + '/raw/rss').filter(F.col('dataset_id') == 'rss_live_v2')
            result.update(build_lab_snapshot(raw, remote + '/clean', remote + '/quarantine',
                work / 'canonical_manifest.json', 'lab_' + args.run_id))
        elif args.step == 'sql':
            clean = spark.read.parquet(remote + '/clean')
            frames = sql_tables(clean, 'rss_live_v2', bounds)
            reference = category_trends(clean, *bounds)
            cols = reference.columns
            mismatch = frames['category_trends_sql'].select(cols).exceptAll(reference.select(cols)).count() + reference.select(cols).exceptAll(frames['category_trends_sql'].select(cols)).count()
            assert mismatch == 0, 'SQL changed the core trend formula'
            for name, frame in frames.items():
                frame.write.mode('errorifexists').parquet(remote + '/sql/' + name)
            result.update(table_counts={name: frame.count() for name, frame in frames.items()}, reference_mismatches=mismatch, window_bounds=bounds)
        elif args.step == 'cluster':
            result.update(cluster_news(spark.read.parquet(remote + '/clean'), 'rss_live_v2', remote + '/clustering'))
        elif args.step == 'save':
            clean = spark.read.parquet(remote + '/clean')
            model = ROOT / 'runtime/m04/models' / plan['model_version']
            fs.mkdirs(jpath(remote + '/model'))
            fs.copyFromLocalFile(False, False, jpath(str(model)), jpath(remote + '/model/' + model.name))
            enriched = enrich_news(clean, remote + '/model/' + model.name, model.name).cache()
            enriched.write.mode('errorifexists').parquet(remote + '/enriched')
            frames = dict(volume_6h=volume_6h(enriched, bounds[0], bounds[2]), sentiment_daily=sentiment_daily(enriched),
                sentiment_summary=sentiment_summary(enriched), category_trends=category_trends(enriched,*bounds),
                keywords_window=keyword_document_frequency(enriched))
            local_gold = work / 'gold_cache'
            local_gold.mkdir()
            counts = {}
            for name, frame in frames.items():
                uri = remote + '/gold/' + name
                frame.write.mode('errorifexists').parquet(uri)
                counts[name] = spark.read.parquet(uri).count()
                fs.copyToLocalFile(False, jpath(uri), jpath(str(local_gold / name)), True)
            score = dict(status='ok', gold_path=str(local_gold), gold_counts=counts, enriched_count=enriched.count(),
                input_snapshot_ids=['lab_' + args.run_id], model_versions=[model.name],
                window_bounds=dict(zip(('previous_start','current_start','window_end'),bounds)))
            atomic_write_json(work / 'score.json', score)
            with single_writer(work / 'serving/.snapshot_writer.lock'):
                result['serving'] = export_snapshot(enriched,local_gold,work/'serving','lab_'+args.run_id,
                    model.name,work/'serving_manifest.json',Path(plan['source_manifest']),work/'score.json',
                    ROOT/'config/rss_sources.yaml',Path(plan['coverage_manifest']))
            enriched.unpersist()
        elif args.step == 'kafka':
            topic = 'vietnews-lab-' + args.run_id
            broker = 'vietnews-kafka:29092'
            raw = spark.read.schema(RAW_SCHEMA).json(remote + '/raw/rss').filter(F.col('dataset_id') == 'rss_live_v2')
            records = raw.select(F.col('observation_id').alias('key'), F.to_json(F.struct(*[F.col(f.name) for f in RAW_SCHEMA])).alias('value'))
            def produce():
                records.write.format('kafka').option('kafka.bootstrap.servers',broker).option('topic',topic).option('kafka.acks','all').save()
            def consume(label):
                stream = spark.readStream.format('kafka').option('kafka.bootstrap.servers',broker).option('subscribe',topic).option('startingOffsets','earliest').option('failOnDataLoss','true').load()
                query = stream.select('topic','partition','offset','timestamp',F.col('value').cast('string').alias('raw_json')).writeStream.format('parquet').option('path',remote+'/kafka_observations').option('checkpointLocation',remote+'/kafka_checkpoint').trigger(availableNow=True).start()
                query.awaitTermination()
                total = spark.read.parquet(remote+'/kafka_observations').count()
                atomic_write_json(work/(label+'.json'),dict(query_id=query.id,run_id=query.runId,recent_progress=list(query.recentProgress),output_rows_total=total))
                parsed = spark.read.parquet(remote+'/kafka_observations').select(F.from_json('raw_json',RAW_SCHEMA,{'mode':'FAILFAST'}).alias('payload')).select('payload.*')
                manifest = build_lab_snapshot(parsed.filter(F.col('dataset_id')=='rss_live_v2'),remote+'/'+label+'/clean',remote+'/'+label+'/quarantine',work/(label+'_canonical.json'),'lab_'+args.run_id)
                return str(query.id),total,manifest['clean_count']
            expected = json.loads((work/'canonical_manifest.json').read_text())['clean_count']
            produce(); first = consume('kafka_first')
            produce(); replay = consume('kafka_replay')
            restarted = consume('kafka_restart')
            assert first[2] == replay[2] == restarted[2] == expected
            assert first[0] == replay[0] == restarted[0]
            assert replay[1] >= first[1] + raw.count() and restarted[1] == replay[1]
            result.update(topic=topic,first=first,replay=replay,restart=restarted,
                semantics='Kafka producer at-least-once; canonical article dedup, not exactly-once publishing')
        elif args.step == 'verify':
            pointer = json.loads((work/'serving/current.json').read_text())
            db = work/'serving'/pointer['db_path']
            with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as con:
                assert con.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
                total,unique = con.execute("SELECT COUNT(*),COUNT(DISTINCT article_id) FROM articles WHERE dataset_id='rss_live_v2'").fetchone()
                assert total == unique == json.loads((work/'canonical_manifest.json').read_text())['clean_count']
            for filename in STEPS[:-1]:
                if not (work/(filename+'.json')).is_file():
                    raise ValueError('missing stage evidence: '+filename)
            assert all(digest(Path(row['path']))==row['sha256'] for row in plan['files'])
            result.update(article_count=total,raw_unchanged=True,production_publish_performed=False,
                limitations=['One RSS poll, local[2] Spark; no full-corpus cluster benchmark.',
                            'Sentiment is experimental; clustering is not proven event grouping.',
                            'No broker/NameNode process restart or crash-during-write tested by this DAG.'])
        result['completed_at'] = datetime.now(timezone.utc).isoformat()
        atomic_write_json(target,result)
        print(json.dumps(result,ensure_ascii=True))
        return result
    except Exception as exc:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        atomic_write_json(work/(args.step+'_failed_'+stamp+'.json'),
            dict(status='failed',step=args.step,error_type=type(exc).__name__,error=str(exc)))
        raise
    finally:
        spark.stop()


def run(args):
    from news_pipeline.writer_lock import single_writer
    validate_run_id(args.run_id)
    with single_writer(ROOT / 'runtime/nine_steps/.lab_writer.lock'):
        return _run(args)


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--step',choices=STEPS,required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--source-manifest',type=Path)
    parser.add_argument('--coverage-manifest',type=Path)
    parser.add_argument('--as-of')
    args=parser.parse_args()
    if args.step=='prepare' and not all((args.source_manifest,args.coverage_manifest,args.as_of)):
        parser.error('prepare requires source-manifest, coverage-manifest and explicit as-of')
    run(args)
