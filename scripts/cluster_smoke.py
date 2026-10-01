"""Small Spark standalone + HDFS fixture smoke for M1."""

from __future__ import annotations

import argparse
import json
import platform
import socket
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import pyspark
from pyspark.sql import SparkSession

from news_pipeline.dev_smoke import SCHEMA


INPUT = "file:///app/data_sample/news_sample.jsonl"
HDFS_OUTPUT = "hdfs:///tmp/news_pipeline_m1/cluster_smoke/parquet"
EVIDENCE = Path("/app/evidence/m01/cluster_smoke_manifest.json")
EXPECTED_COUNTS = {"dantri": 3, "thanhnien": 3, "tuoitre": 3, "vnexpress": 3}


def executor_host(rows):
    if next(rows, None) is None:
        return iter(())
    return iter([socket.gethostname()])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("write", "verify"), default="write")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--storage", default=HDFS_OUTPUT)
    parser.add_argument("--evidence", type=Path, default=EVIDENCE)
    return parser.parse_args()


def _json(url: str) -> dict:
    with urlopen(url, timeout=3) as response:
        return json.load(response)


def wait_for_services(timeout: int) -> tuple[int, int]:
    deadline = time.monotonic() + timeout
    last_error = "services not ready"
    while time.monotonic() < deadline:
        try:
            namenode = _json(
                "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
            )["beans"][0]
            safe_mode = str(namenode.get("Safemode", "")).lower()
            live_datanodes = int(namenode.get("NumLiveDataNodes", 0))
            spark = _json("http://spark-master:8080/json/")
            workers = [worker for worker in spark.get("workers", []) if worker.get("state") == "ALIVE"]
            if safe_mode in ("", "off") and live_datanodes >= 1 and workers:
                return live_datanodes, len(workers)
            last_error = f"safe_mode={safe_mode!r}, live_datanodes={live_datanodes}, workers={len(workers)}"
        except Exception as exc:  # service startup is expected to be transient
            last_error = str(exc)
        time.sleep(2)
    raise TimeoutError(f"cluster services did not become usable: {last_error}")


def runtime_metadata(spark: SparkSession) -> dict[str, str | None]:
    system = spark.sparkContext._jvm.java.lang.System
    return {
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "java_version": system.getProperty("java.version"),
        "java_runtime_version": system.getProperty("java.runtime.version"),
    }


def write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, object]:
    live_datanodes, workers = wait_for_services(args.timeout)
    spark = (
        SparkSession.builder.appName("news-pipeline-m1-cluster-smoke")
        .master("spark://spark-master:7077")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        source_df = spark.read.schema(SCHEMA).json(INPUT)
        input_count = source_df.count()
        if input_count != 12:
            raise AssertionError(f"expected 12 fixture rows, got {input_count}")
        grouped = {row["source"]: row["count"] for row in source_df.groupBy("source").count().collect()}
        if grouped != EXPECTED_COUNTS:
            raise AssertionError(f"source counts {grouped!r} != {EXPECTED_COUNTS!r}")

        if args.mode == "write":
            source_df.write.mode("errorifexists").parquet(args.storage)
        roundtrip = spark.read.parquet(args.storage)
        actual_schema = [(field.name, field.dataType.simpleString()) for field in roundtrip.schema.fields]
        expected_schema = [(field.name, field.dataType.simpleString()) for field in SCHEMA.fields]
        if actual_schema != expected_schema:
            raise AssertionError(f"HDFS Parquet schema {actual_schema!r} != {expected_schema!r}")
        output_count = roundtrip.count()
        if output_count != input_count:
            raise AssertionError(f"input/output mismatch: {input_count} != {output_count}")
        if roundtrip.select(*SCHEMA.fieldNames()).orderBy("article_id").collect() != source_df.orderBy("article_id").collect():
            raise AssertionError("HDFS Parquet read-back content differs from fixture")
        executor_hosts = sorted(set(source_df.rdd.mapPartitions(executor_host).collect()))
        if not executor_hosts:
            raise AssertionError("Spark job did not report an executor hostname")

        result = {
            "status": "ok",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "input": INPUT,
            "storage": args.storage,
            "spark_master": "spark://spark-master:7077",
            "input_count": input_count,
            "output_count": output_count,
            "source_counts": dict(sorted(grouped.items())),
            "schema": [{"name": name, "type": data_type} for name, data_type in actual_schema],
            "hdfs_live_datanodes": live_datanodes,
            "spark_workers": workers,
            "executor_hosts": executor_hosts,
            "runtime": runtime_metadata(spark),
        }
        write_manifest(args.evidence, result)
        return result
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        result = run(args)
    except Exception as exc:
        write_manifest(
            args.evidence,
            {
                "status": "failed",
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "mode": args.mode,
                "storage": args.storage,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
        traceback.print_exc(file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
