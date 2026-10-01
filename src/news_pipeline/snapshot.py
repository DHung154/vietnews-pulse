"""Single-writer snapshot publication and reconciliation metrics."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .etl import deduplicate_news, normalize_news
from .rss import atomic_write_json


def _is_uri(value: str) -> bool:
    return "://" in value


def _publish_frame(frame: DataFrame, target: Path | str) -> None:
    target_text = str(target)
    if _is_uri(target_text):
        temporary = f"{target_text}.inprogress-{uuid.uuid4().hex}"
        jvm = frame.sparkSession.sparkContext._jvm
        hadoop_path = jvm.org.apache.hadoop.fs.Path(target_text)
        fs = hadoop_path.getFileSystem(frame.sparkSession.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(hadoop_path):
            raise FileExistsError(f"refusing to overwrite snapshot path: {target_text}")
        try:
            frame.write.mode("errorifexists").parquet(temporary)
            if not fs.rename(jvm.org.apache.hadoop.fs.Path(temporary), hadoop_path):
                raise IOError(f"failed to publish {target_text}")
        except Exception:
            fs.delete(jvm.org.apache.hadoop.fs.Path(temporary), True)
            raise
        return
    target = Path(target_text).resolve()
    if target.exists():
        raise FileExistsError(f"refusing to overwrite snapshot path: {target}")
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.inprogress")
    try:
        frame.write.mode("errorifexists").parquet(str(temporary))
        os.replace(temporary, target)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def build_snapshot(raw: DataFrame, clean_path: Path | str, quarantine_path: Path | str, manifest_path: Path, snapshot_id: str, input_is_normalized: bool = False) -> dict[str, Any]:
    normalized = raw if input_is_normalized else normalize_news(raw)
    invalid = normalized.filter(~F.col("valid"))
    valid_before = normalized.filter(F.col("valid"))
    clean, duplicate = deduplicate_news(normalized)
    raw_count = raw.count()
    invalid_count = invalid.count()
    valid_count = valid_before.count()
    duplicate_count = duplicate.count()
    clean_count = clean.count()
    if raw_count != invalid_count + duplicate_count + clean_count:
        raise ValueError("snapshot reconciliation failed")
    clean = clean.withColumn("snapshot_id", F.lit(snapshot_id))
    invalid = invalid.withColumn("snapshot_id", F.lit(snapshot_id))
    duplicate = duplicate.withColumn("snapshot_id", F.lit(snapshot_id))
    _publish_frame(clean, clean_path)
    _publish_frame(invalid.unionByName(duplicate), quarantine_path)
    stats = {
        "source_counts": [row.asDict() for row in clean.groupBy("source").count().orderBy("source").collect()],
        "fallback_time_count": clean.filter(F.col("event_time_basis") == "crawled_fallback").count(),
        "unknown_category_count": clean.filter(F.col("category") == "other").count(),
        "null_event_date_count": clean.filter(F.col("event_date").isNull()).count(),
        "event_date_min": clean.agg(F.min("event_date")).first()[0],
        "event_date_max": clean.agg(F.max("event_date")).first()[0],
    }
    manifest = {
        "status": "ok",
        "snapshot_id": snapshot_id,
        "raw_count": raw_count,
        "invalid_count": invalid_count,
        "valid_before_dedup": valid_count,
        "duplicate_removed": duplicate_count,
        "clean_count": clean_count,
        "reconciled": raw_count == invalid_count + duplicate_count + clean_count,
        "clean_path": str(clean_path),
        "quarantine_path": str(quarantine_path),
        "stats": stats,
    }
    atomic_write_json(manifest_path, manifest)
    return manifest
