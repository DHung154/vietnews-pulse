"""Cluster integration: raw JSON and canonical snapshot publication on HDFS."""

from __future__ import annotations

import json
import socket
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from news_pipeline.etl import read_raw
from news_pipeline.snapshot import build_snapshot
from news_pipeline.contracts import RAW_SCHEMA


def executor_host(rows):
    if next(rows, None) is None:
        return iter(())
    return iter([socket.gethostname()])


def main() -> int:
    run_id = "hdfs_core_" + uuid.uuid4().hex[:8]
    raw_path = f"hdfs://namenode:8020/news/remediation/{run_id}/raw"
    clean_path = f"hdfs://namenode:8020/news/remediation/{run_id}/clean"
    quarantine_path = f"hdfs://namenode:8020/news/remediation/{run_id}/quarantine"
    evidence_path = Path("/app/evidence/m03") / f"{run_id}.json"
    spark = SparkSession.builder.appName("news-pipeline-hdfs-core-remediation").master("spark://spark-master:7077").config("spark.sql.session.timeZone", "UTC").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020").config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.dfs.replication", "1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        local_raw = spark.read.schema(RAW_SCHEMA).json("file:///app/runtime/m02/raw")
        input_count = local_raw.count()
        local_raw.write.mode("errorifexists").json(raw_path)
        raw = read_raw(spark, raw_path)
        if raw.count() != input_count:
            raise AssertionError("HDFS raw roundtrip count mismatch")
        manifest = build_snapshot(raw, clean_path, quarantine_path, evidence_path, run_id)
        clean_count = spark.read.parquet(clean_path).count()
        if clean_count != manifest["clean_count"]:
            raise AssertionError("HDFS clean roundtrip count mismatch")
        result = {"status": "ok", "run_id": run_id, "raw_path": raw_path, "clean_path": clean_path, "quarantine_path": quarantine_path, "input_count": input_count, "hdfs_raw_count": raw.count(), "clean_count": clean_count, "reconciled": manifest["reconciled"], "executor_hosts": sorted(set(raw.rdd.mapPartitions(executor_host).collect()))}
        evidence_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
