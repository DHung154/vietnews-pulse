"""Isolated TF-IDF/PCA K-Means sweep; unsupervised quality, not sentiment."""
import json
import sys
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from news_pipeline.lab_analytics import cluster_news
from pyspark.sql import SparkSession, functions as F

out=ROOT/'runtime/completion_cluster_20261016_v1'
out.mkdir(parents=True,exist_ok=False)
spark=(SparkSession.builder.master('local[*]').appName('vietnews-cluster-sweep')
       .config('spark.sql.shuffle.partitions','2').config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
remote='hdfs://namenode:8020/news/evaluation/completion_cluster_20261016_v1'
path=spark._jvm.org.apache.hadoop.fs.Path
fs=path(remote).getFileSystem(spark._jsc.hadoopConfiguration())
results=[]
try:
    if fs.exists(path(remote)):
        raise FileExistsError(remote)
    frame=spark.read.parquet('hdfs://namenode:8020/news/shared/rss_live_v2/snapshot_20261008T090238Z_81c6a670/clean').filter(F.col('dataset_id')=='rss_live_v2').cache()
    assert frame.count()==13388
    for dim in (None,16):
        for k in (2,5,8):
            start=time.perf_counter()
            name=f'tfidf512_pca{dim or 0}_k{k}'
            result=cluster_news(frame,'rss_live_v2',remote+'/'+name,k=k,seed=42,vocab_size=512,pca_dim=dim)
            result.update(name=name,elapsed_seconds=time.perf_counter()-start,output_hdfs=remote+'/'+name)
            results.append(result)
            with (out/(name+'.json')).open('x',encoding='utf-8') as handle:
                json.dump(result,handle,ensure_ascii=False,indent=2)
            print(name,result['silhouette'],flush=True)
    valid=[r for r in results if r['silhouette'] is not None]
    with (out/'summary.json').open('x',encoding='utf-8') as handle:
        json.dump(dict(status='ok',input_count=13388,results=results,highest_silhouette_config=max(valid,key=lambda r:r['silhouette'])['name'] if valid else None,
             limitations=['One snapshot unsupervised fit, no held-out semantic labels.', 'Silhouette is squared-Euclidean on each feature space; not proof of event clusters.', 'PCA top words intentionally omitted: coordinates are not vocabulary indices.', 'No model deployed or sentiment labels changed.']),handle,ensure_ascii=False,indent=2)
finally:
    spark.stop()
