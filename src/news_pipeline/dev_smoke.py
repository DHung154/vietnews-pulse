"""Minimal local Spark smoke: explicit JSON schema -> Parquet -> read-back."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


SCHEMA = StructType(
    [
        StructField("dataset_id", StringType(), False),
        StructField("article_id", StringType(), False),
        StructField("source", StringType(), False),
        StructField("category", StringType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), False),
        StructField("published_at", StringType(), False),
        StructField("sentiment_label", StringType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--master", default="local[2]")
    return parser.parse_args()


def _is_within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def validate_paths(args: argparse.Namespace) -> None:
    """Reject unsafe aliases before the failure-manifest writer can run."""
    input_path = args.input.resolve()
    output_path = args.output.resolve()
    in_progress = output_path.with_name(output_path.name + ".inprogress")
    evidence_path = args.evidence.resolve()

    if not input_path.is_file():
        raise ValueError(f"input must be an existing file: {input_path}")
    if output_path == input_path or in_progress == input_path:
        raise ValueError("output/staging path must differ from input")
    if evidence_path == input_path or evidence_path == output_path or evidence_path == in_progress:
        raise ValueError("evidence path must differ from input, output, and staging")
    if _is_within(evidence_path, output_path) or _is_within(evidence_path, in_progress):
        raise ValueError("evidence path must not be inside output or staging")
    if evidence_path.exists() and evidence_path.is_dir():
        raise ValueError(f"evidence must be a file path, not a directory: {evidence_path}")
    if input_path.exists() and evidence_path.exists():
        try:
            if os.path.samefile(input_path, evidence_path):
                raise ValueError("evidence path aliases input")
        except OSError:
            pass

    args.input = input_path
    args.output = output_path
    args.evidence = evidence_path


def runtime_metadata(spark: SparkSession | None = None) -> dict[str, str | None]:
    metadata: dict[str, str | None] = {
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "java_version": None,
        "java_runtime_version": None,
    }
    if spark is not None:
        system = spark.sparkContext._jvm.java.lang.System
        metadata["java_version"] = system.getProperty("java.version")
        metadata["java_runtime_version"] = system.getProperty("java.runtime.version")
    return metadata


def _base_manifest(args: argparse.Namespace, status: str) -> dict[str, object]:
    return {
        "status": status,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "input": str(args.input),
        "output": str(args.output),
        "master": args.master,
        "session_timezone": "UTC",
        "runtime": getattr(args, "runtime", runtime_metadata()),
    }


def run(args: argparse.Namespace) -> dict[str, object]:
    validate_paths(args)
    if args.output.exists():
        raise FileExistsError(f"Refusing to overwrite existing output: {args.output}")
    in_progress = args.output.with_name(args.output.name + ".inprogress")
    if in_progress.exists():
        raise FileExistsError(f"Refusing to reuse incomplete output: {in_progress}")

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("news-pipeline-m1-smoke")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    args.runtime = runtime_metadata(spark)
    try:
        source_df = spark.read.schema(SCHEMA).json(str(args.input))
        input_count = source_df.count()
        if input_count != 12:
            raise AssertionError(f"expected 12 fixture rows, got {input_count}")
        if source_df.filter("sentiment_label IS NULL OR sentiment_label = ''").count():
            raise AssertionError("synthetic labels must be present")

        grouped = {
            row["source"]: row["count"]
            for row in source_df.groupBy("source").count().collect()
        }
        expected = {"dantri": 3, "thanhnien": 3, "tuoitre": 3, "vnexpress": 3}
        if grouped != expected:
            raise AssertionError(f"source counts {grouped!r} != {expected!r}")
        print(json.dumps({"input_count": input_count, "source_counts": dict(sorted(grouped.items()))}, ensure_ascii=False))

        in_progress.parent.mkdir(parents=True, exist_ok=True)
        source_df.write.mode("errorifexists").parquet(str(in_progress))
        roundtrip = spark.read.parquet(str(in_progress))
        expected_schema = [(field.name, field.dataType.simpleString()) for field in SCHEMA.fields]
        actual_schema = [(field.name, field.dataType.simpleString()) for field in roundtrip.schema.fields]
        if actual_schema != expected_schema:
            raise AssertionError(f"Parquet schema {actual_schema!r} != {expected_schema!r}")
        output_count = roundtrip.count()
        if output_count != input_count:
            raise AssertionError(f"input/output mismatch: {input_count} != {output_count}")
        expected_rows = source_df.orderBy("article_id").collect()
        actual_rows = roundtrip.select(*SCHEMA.fieldNames()).orderBy("article_id").collect()
        if actual_rows != expected_rows:
            raise AssertionError("Parquet read-back content differs from input fixture")
        os.replace(in_progress, args.output)

        result = _base_manifest(args, "ok") | {
            "input_count": input_count,
            "output_count": output_count,
            "source_counts": dict(sorted(grouped.items())),
            "schema": [{"name": name, "type": data_type} for name, data_type in actual_schema],
        }
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()
    try:
        validate_paths(args)
    except ValueError as exc:
        print(f"argument validation failed: {exc}", file=sys.stderr)
        return 2
    try:
        result = run(args)
    except Exception as exc:
        failure = _base_manifest(args, "failed") | {
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(
            json.dumps(failure, ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
        traceback.print_exc()
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
