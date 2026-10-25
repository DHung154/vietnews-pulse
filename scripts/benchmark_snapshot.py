"""Read-only input benchmark of HDFS canonical aggregation, not ETL or ML."""
import argparse
import hashlib
import json
import re
import socket
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path


def executor_host(rows):
    return iter([socket.gethostname()]) if next(rows, None) is not None else iter(())


def run(args):
    from pyspark.sql import SparkSession, functions as F
    if not re.fullmatch(r'[a-zA-Z0-9_-]+', args.run_id) or args.repeats < 3:
        raise ValueError('safe new run_id and at least 3 repetitions required')
    output = args.output.resolve()
    if Path('/app/runtime') not in output.parents:
        raise ValueError('output must be a new directory below /app/runtime')
    output.mkdir(parents=True, exist_ok=False)
    spark = (SparkSession.builder.master(args.master).appName('vietnews-snapshot-benchmark')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
        .config('spark.default.parallelism', '2').config('spark.cores.max', '1')
        .config('spark.executor.cores', '1').config('spark.executor.memory', '512m')
        .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    remote = 'hdfs://namenode:8020/news/evaluation/' + args.run_id
    path = spark._jvm.org.apache.hadoop.fs.Path
    fs = path(args.input).getFileSystem(spark._jsc.hadoopConfiguration())
    def inventory():
        files = fs.listFiles(path(args.input), True)
        result = []
        while files.hasNext():
            item = files.next()
            result.append(dict(path=str(item.getPath()), bytes=item.getLen(), checksum=str(fs.getFileChecksum(item.getPath()))))
        return sorted(result, key=lambda r: r['path'])
    result = dict(status='running', master=spark.sparkContext.master, input=args.input, output_hdfs=remote,
                  workload='HDFS canonical read, source/category aggregation, Parquet write/readback',
                  repeats=args.repeats, warmup_runs=1, cache='No Spark cache; OS/HDFS caches uncontrolled; warmup excluded',
                  parameters=dict(shuffle_partitions=2, max_executor_cores=1, executor_memory='512m'),
                  scope='One physical Windows host; Docker processes, not a multi-machine benchmark')
    try:
        if fs.exists(path(remote)):
            raise FileExistsError('HDFS output exists; choose new run_id')
        before = inventory()
        frame = spark.read.parquet(args.input).filter(F.col('dataset_id') == 'rss_live_v2')
        count = frame.count()
        unique = frame.select('article_id').distinct().count()
        if count != unique or count != args.expected_count:
            raise ValueError(f'Input mismatch: count={count}, unique={unique}')
        result.update(article_count=count, unique_keys=unique, input_inventory=before,
                      executor_hosts=sorted(set(frame.rdd.mapPartitions(executor_host).collect())))
        reference = None
        trials = []
        for index in range(args.repeats + 1):
            started = time.perf_counter()
            current = spark.read.parquet(args.input).filter(F.col('dataset_id') == 'rss_live_v2')
            aggregates = current.groupBy('source', 'category').agg(F.count('*').alias('article_count'), F.sum(F.length('clean_text')).alias('text_characters'))
            target = remote + f'/trial_{index}'
            aggregates.write.mode('errorifexists').parquet(target)
            observed = [r.asDict() for r in spark.read.parquet(target).orderBy('source','category').collect()]
            elapsed = time.perf_counter() - started
            assert len(observed) <= 1000, 'Aggregation unexpectedly large'
            assert sum(r['article_count'] for r in observed) == count
            checksum = hashlib.sha256(json.dumps(observed, sort_keys=True).encode()).hexdigest()
            if reference is None:
                reference = checksum
            assert checksum == reference, 'Aggregate changed between repetitions'
            trials.append(dict(trial=index, warmup=index == 0, elapsed_seconds=elapsed, aggregate_sha256=checksum, output=target))
            (output / f'trial_{index}.json').write_text(json.dumps(trials[-1],indent=2),encoding='utf-8')
        assert before == inventory(), 'Input HDFS files changed'
        result.update(status='ok', trials=trials, median_seconds=statistics.median(t['elapsed_seconds'] for t in trials if not t['warmup']), aggregate_sha256=reference, input_unchanged=True, completed_at=datetime.now(timezone.utc).isoformat())
        return result
    except Exception as exc:
        result.update(status='failed', error_type=type(exc).__name__, error=str(exc))
        raise
    finally:
        (output/'result.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--master', required=True, choices=('local[*]', 'spark://spark-master:7077'))
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--expected-count', type=int, default=13388)
    print(json.dumps(run(parser.parse_args())))
