"""Read-only example for the member's mapped-host Docker client."""
import argparse
import ipaddress
import json
import socket

DATASETS={
    'full':('/news/shared/rss_live_v2/snapshot_20261008T090238Z_81c6a670/enriched',13388,
            'full rss_live_v2 dashboard snapshot08/10; sentiment is model prediction, not human gold'),
    'lab':('/news/lab/airflow_181ed4cba01a3cab/clean',778,'one real RSS poll, not full dashboard corpus'),
}

def validate_server(value):
    address = ipaddress.IPv4Address(value)
    if address not in ipaddress.IPv4Network('100.64.0.0/10'):
        raise ValueError('Use verified Tailscale IPv4, not LAN/public/localhost')
    return str(address)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', required=True, type=validate_server)
    parser.add_argument('--dataset',choices=DATASETS,default='full')
    args = parser.parse_args()
    for host, port in ((args.server,8020),('datanode',9866),('datanode2',9868),('datanode3',9869)):
        if host != args.server and socket.gethostbyname(host) != args.server:
            raise ValueError('DataNode hostname must resolve to VPN server: '+host)
        with socket.create_connection((host,port),timeout=5):
            pass
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[2]').appName('vietnews-team-read')
        .config('spark.hadoop.dfs.client.use.datanode.hostname','true').getOrCreate())
    try:
        suffix,expected,scope=DATASETS[args.dataset]
        path = 'hdfs://'+args.server+':8020'+suffix
        frame = spark.read.parquet(path)
        count = frame.count()
        unique = frame.select('dataset_id','article_id').distinct().count()
        if count != unique or count != expected:
            raise ValueError('Frozen snapshot count/key mismatch')
        frame.select('title','source','category').show(5,truncate=False)
        print(json.dumps(dict(path=path,article_count=count,unique_keys=unique,
            scope=scope,mode='read-only example')))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
