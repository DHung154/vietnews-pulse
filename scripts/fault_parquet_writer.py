"""Synthetic writer used only in a named isolated crash probe."""
import argparse
import json
import time
from pathlib import Path

def run(args):
    from pyspark.sql import SparkSession,functions as F
    from pyspark.sql.types import LongType
    if args.attempt not in (1,2):
        raise ValueError('fixture attempts 1/2 only')
    output=Path('/app/runtime/fault_parquet_20261016_v1')
    output.mkdir(exist_ok=True)
    remote=f'hdfs://namenode:8020/news/fault_tests/fault_parquet_20261016_v1/attempt_{args.attempt}'
    spark=(SparkSession.builder.master('local[1]').appName('SYNTHETIC-parquet-crash')
           .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        path=spark._jvm.org.apache.hadoop.fs.Path
        fs=path(remote).getFileSystem(spark._jsc.hadoopConfiguration())
        if fs.exists(path(remote)):
            raise FileExistsError('fixture output already exists')
        frame=spark.range(1000,numPartitions=1).withColumn('fixture',F.lit('SYNTHETIC - NOT NEWS'))
        if args.slow:
            def delay(value):
                time.sleep(0.1)
                return value
            frame=frame.withColumn('delayed_id',F.udf(delay,LongType())('id'))
        else:
            frame=frame.withColumn('delayed_id',F.col('id'))
        frame.write.mode('errorifexists').parquet(remote)
        read=spark.read.parquet(remote)
        count=read.count()
        assert count==read.select('id').distinct().count()==1000
        result=dict(status='ok',data_type='synthetic fixture',count=count,output_hdfs=remote,success_marker=fs.exists(path(remote+'/_SUCCESS')))
        with (output/f'attempt_{args.attempt}.json').open('x',encoding='utf-8') as handle:
            json.dump(result,handle,indent=2)
    finally:
        spark.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--attempt',type=int,required=True)
    parser.add_argument('--slow',action='store_true')
    run(parser.parse_args())
