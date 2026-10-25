"""Real RSS batch -> HDFS stream/snapshot/inference/SQL -> isolated SQLite.

Run inside the existing Linux image on the Compose network. Reuses production
transforms, preserves all inputs, and never changes the dashboard's serving root.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from news_pipeline.analytics import (category_trends, completed_window_bounds,
    keyword_document_frequency, sentiment_daily, sentiment_summary, volume_6h)
from news_pipeline.rss import atomic_write_json
from news_pipeline.serving import export_snapshot
from news_pipeline.snapshot import build_snapshot
from news_pipeline.stream import run_available
from news_pipeline.writer_lock import single_writer
from score_analytics import enrich_news


def input_path(value: str) -> Path:
    if PureWindowsPath(value).drive:
        return ROOT / Path(*PureWindowsPath(value).relative_to(PureWindowsPath("E:/BigData")).parts)
    return Path(value).resolve()


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        digest = hashlib.sha256()
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def executor_host(rows):
    return iter([socket.gethostname()]) if next(rows, None) is not None else iter(())


def run(args) -> dict:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    output = args.output.resolve()
    if ROOT / "runtime" not in output.parents or not re.fullmatch(r"[A-Za-z0-9_-]+", output.name):
        raise ValueError("output must be a named, isolated directory below /app/runtime")
    if args.mode == "run":
        output.mkdir(parents=True, exist_ok=False)
    elif not (output / "result.json").is_file():
        raise ValueError("verify requires a previously successful result.json")
    evidence = output / ("restart.json" if args.mode == "verify" else "result.json")
    if evidence.exists():
        raise FileExistsError(evidence)
    spark = (SparkSession.builder.master("spark://spark-master:7077")
        .appName("vietnews-hdfs-integration")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    jpath = spark._jvm.org.apache.hadoop.fs.Path
    remote = "hdfs://namenode:8020/news/integration/" + output.name
    fs = jpath(remote).getFileSystem(spark._jsc.hadoopConfiguration())
    started = time.monotonic()
    try:
        if args.mode == "verify":
            old = json.loads((output / "result.json").read_text(encoding="utf-8"))
            restarted = run_available(spark, remote + "/raw/rss", remote + "/observations",
                remote + "/checkpoint", output / "stream_restart.json", 1000)
            assert restarted["query_id"] == old["query_id"], "checkpoint query ID changed"
            assert restarted["input_rows_this_run"] == 0, "old files reprocessed"
            assert restarted["output_rows_total"] == old["replay_observations"], "sink count changed"
            count = spark.read.parquet(old["hdfs_enriched"]).count()
            assert count == old["canonical_count"], "persisted HDFS count changed"
            result = {"status": "ok", "mode": "verify", "canonical_count": count,
                "stream": restarted, "executor_hosts": sorted(set(spark.read.parquet(old["hdfs_enriched"]).rdd.mapPartitions(executor_host).collect())),
                "note": "Separate process, same HDFS input/sink/checkpoint; no cleanup or reset."}
        else:
            if fs.exists(jpath(remote)):
                raise FileExistsError(remote)
            source = json.loads(args.source_manifest.read_text(encoding="utf-8"))
            files = []
            raw_root = (ROOT / "runtime/data_collection_v2/raw/rss").resolve()
            for item in source["results"]:
                if not item.get("raw_path"):
                    continue
                path = input_path(item["raw_path"])
                if raw_root not in path.parents or not path.is_file() or path.is_symlink():
                    raise ValueError(f"unexpected raw source: {path}")
                digest = sha256(path)
                count = sum(1 for line in path.open(encoding="utf-8") if line.strip())
                if digest != item["sha256"] or count != item["records"]:
                    raise ValueError(f"immutable source hash/count mismatch: {path}")
                files.append({"path": str(path), "sha256": digest, "rows": count})
            if not files:
                raise ValueError("selected immutable manifest contains no raw files")
            atomic_write_json(output / "input_inventory.json", {"source_manifest_sha256": sha256(args.source_manifest), "files": files})
            for item in files:
                target = remote + "/raw/rss/" + Path(item["path"]).name
                fs.mkdirs(jpath(remote + "/raw/rss"))
                fs.copyFromLocalFile(False, False, jpath(item["path"]), jpath(target))
            first = run_available(spark, remote + "/raw/rss", remote + "/observations",
                remote + "/checkpoint", output / "stream_initial.json", 1000)
            base = build_snapshot(spark.read.parquet(remote + "/observations"),
                remote + "/snapshots/base/clean", remote + "/snapshots/base/quarantine",
                output / "base_snapshot.json", "hdfs_base_" + output.name, True)
            for item in files:
                target = remote + "/raw/rss/replay_" + Path(item["path"]).name
                fs.copyFromLocalFile(False, False, jpath(item["path"]), jpath(target))
            replay = run_available(spark, remote + "/raw/rss", remote + "/observations",
                remote + "/checkpoint", output / "stream_replay.json", 1000)
            snapshot_id = "hdfs_" + output.name
            snapshot = build_snapshot(spark.read.parquet(remote + "/observations"),
                remote + "/snapshots/replay/clean", remote + "/snapshots/replay/quarantine",
                output / "replay_snapshot.json", snapshot_id, True)
            assert replay["output_rows_total"] == first["output_rows_total"] * 2
            assert snapshot["clean_count"] == base["clean_count"], "replay changed canonical count"
            clean = spark.read.parquet(snapshot["clean_path"])
            assert clean.filter(~F.col("dataset_id").isin("rss_live", "rss_live_v2")).count() == 0
            model = args.model.resolve()
            model_remote = remote + "/model/" + model.name
            fs.mkdirs(jpath(remote + "/model"))
            fs.copyFromLocalFile(False, False, jpath(str(model)), jpath(model_remote))
            enriched_path = remote + "/enriched"
            enrich_news(clean, model_remote, model.name).write.mode("errorifexists").parquet(enriched_path)
            enriched = spark.read.parquet(enriched_path)
            bounds = completed_window_bounds(datetime.fromisoformat(args.as_of.replace("Z", "+00:00")))
            frames = {"volume_6h": volume_6h(enriched, bounds[0], bounds[2]),
                "sentiment_daily": sentiment_daily(enriched), "sentiment_summary": sentiment_summary(enriched),
                "category_trends": category_trends(enriched, *bounds),
                "keywords_window": keyword_document_frequency(enriched)}
            gold_cache = output / "gold_cache"
            gold_cache.mkdir()
            gold_counts = {}
            for name, frame in frames.items():
                target = remote + "/gold/" + name
                frame.write.mode("errorifexists").parquet(target)
                gold_counts[name] = spark.read.parquet(target).count()
                # Only aggregates are mirrored; serving consumes this verified local cache.
                fs.copyToLocalFile(False, jpath(target), jpath(str(gold_cache / name)), True)
            score = {"status": "ok", "gold_path": str(gold_cache), "gold_hdfs_path": remote + "/gold",
                "gold_counts": gold_counts, "enriched_count": enriched.count(),
                "input_snapshot_ids": [snapshot_id], "model_versions": [model.name],
                "window_bounds": dict(zip(("previous_start", "current_start", "window_end"), bounds))}
            atomic_write_json(output / "score.json", score)
            with single_writer(output / "serving/.snapshot_writer.lock"):
                serving = export_snapshot(enriched, gold_cache, output / "serving", snapshot_id,
                    model.name, output / "serving.json", args.source_manifest, output / "score.json",
                    ROOT / "config/rss_sources.yaml", args.coverage_manifest)
            assert serving["article_count"] == snapshot["clean_count"]
            assert all(sha256(Path(item["path"])) == item["sha256"] for item in files), "original raw changed"
            result = {"status": "ok", "mode": "run", "data_type": "real RSS from one immutable poll; not full corpus",
                "raw_rows": sum(item["rows"] for item in files), "raw_files": len(files),
                "initial_observations": first["output_rows_total"], "replay_observations": replay["output_rows_total"],
                "canonical_count": snapshot["clean_count"], "query_id": first["query_id"],
                "hdfs_root": remote, "hdfs_enriched": enriched_path, "model_version": model.name,
                "serving": serving, "executor_hosts": sorted(set(clean.rdd.mapPartitions(executor_host).collect())),
                "gold_counts": gold_counts, "raw_unchanged": True, "replay_canonical_unchanged": True,
                "limitations": ["One real poll only, not full-corpus cluster acceptance.",
                    "No crash-during-write, late-event or +10-new-article evidence in this run.",
                    "Inference coverage is not model accuracy; no human gold evaluation."]}
        result.update({"spark_master": spark.sparkContext.master,
            "elapsed_seconds": time.monotonic() - started, "completed_at": datetime.now(timezone.utc).isoformat()})
        atomic_write_json(evidence, result)
        print(json.dumps(result, ensure_ascii=True))
        return result
    except Exception as exc:
        atomic_write_json(output / (args.mode + "_failed.json"), {"status": "failed", "error": str(exc), "error_type": type(exc).__name__})
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("run", "verify"), default="run")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--coverage-manifest", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    run(parser.parse_args())
