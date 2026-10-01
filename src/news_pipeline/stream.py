"""Structured Streaming file-source runner for committed RSS JSONL files."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .contracts import RAW_SCHEMA
from .etl import normalize_news
from .rss import atomic_write_json


LIVE_DATASETS = ("rss_live", "rss_live_v2")


def rss_input_scope(input_path: Path | str, spark=None) -> str:
    """Select only an explicit rss root (or its direct child); fail closed otherwise."""
    value = str(input_path).rstrip("/\\")
    if "://" in value:
        path = spark._jvm.org.apache.hadoop.fs.Path(value)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        candidate = path if path.getName() == "rss" else spark._jvm.org.apache.hadoop.fs.Path(path, "rss")
        if not fs.isDirectory(candidate):
            raise ValueError("stream requires an existing raw/rss scope; mixed legacy roots must be migrated")
        return candidate.toString()
    path = Path(value).resolve()
    candidate = path if path.name == "rss" else path / "rss"
    if not candidate.is_dir():
        raise ValueError("stream requires an existing raw/rss scope; mixed legacy roots must be migrated")
    return str(candidate)


def read_rss_stream(spark, input_path, max_files):
    scope = rss_input_scope(input_path, spark)
    stream = spark.readStream.option("recursiveFileLookup", "true").option("pathGlobFilter", "*.jsonl").option("maxFilesPerTrigger", max_files).schema(RAW_SCHEMA).json(scope)
    # Raw stays immutable for audit. Unknown/historical datasets are blocked before normalization.
    return stream.filter(F.col("dataset_id").isin(*LIVE_DATASETS))


def run_available(spark: SparkSession, input_path: Path | str, output_path: Path | str, checkpoint_path: Path | str, evidence_path: Path, max_files: int) -> dict:
    scope = rss_input_scope(input_path, spark)
    stream = read_rss_stream(spark, scope, max_files)
    observations = normalize_news(stream).filter(F.col("valid"))
    query = observations.writeStream.format("parquet").outputMode("append").option("path", str(output_path)).option("checkpointLocation", str(checkpoint_path)).trigger(availableNow=True).start()
    query.awaitTermination()
    progress_entries = list(query.recentProgress)
    progress = progress_entries[-1] if progress_entries else {}
    input_rows = sum(int(source.get("numInputRows", 0)) for item in progress_entries for source in item.get("sources", []))
    total_rows = spark.read.parquet(str(output_path)).count()
    manifest = {"status": "ok", "query_id": query.id, "run_id": query.runId, "input_path": str(input_path), "resolved_rss_scope": scope, "allowed_dataset_ids": LIVE_DATASETS, "blocked_dataset_policy": "exclude before normalization; retain immutable raw for audit", "output_path": str(output_path), "checkpoint_path": str(checkpoint_path), "max_files_per_trigger": max_files, "progress_batch_count": len(progress_entries), "input_rows_this_run": input_rows, "output_rows_total": total_rows, "last_progress": progress, "schema_logic_version": "rss_clean_v3_scoped_body_contract", "note": "Observation sink is consumed by refresh.py; canonical article dedup remains in snapshot."}
    atomic_write_json(evidence_path, manifest)
    return manifest
