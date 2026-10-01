"""One-writer batch refresh: raw/observation sink -> snapshot -> score -> serving."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.paths import validate_artifact_paths  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402
from news_pipeline.serving import export_snapshot  # noqa: E402
from news_pipeline.writer_lock import single_writer  # noqa: E402


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default=str(ROOT / "runtime/data_collection_v2/raw/rss"))
    parser.add_argument("--observations", default=str(ROOT / "runtime/data_collection_v2/observations/rss_v2"))
    parser.add_argument("--checkpoint", default=str(ROOT / "runtime/data_collection_v2/checkpoints/rss_clean_v3"))
    parser.add_argument("--runtime-root", type=Path, default=ROOT / "runtime/refresh")
    parser.add_argument("--serving-root", type=Path, default=ROOT / "serving")
    parser.add_argument("--model", type=Path)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--coverage-manifest", type=Path)
    parser.add_argument("--max-files-per-trigger", type=int, default=2)
    parser.add_argument("--as-of", help="explicit UTC analytics anchor for mixed or historical scopes")
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/m07/refresh_manifest.json")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    validate_artifact_paths(inputs=(Path(args.raw), args.source_manifest) + ((args.model,) if args.model else ()) + ((args.coverage_manifest,) if args.coverage_manifest else ()), outputs=(Path(args.observations), Path(args.checkpoint), args.runtime_root, args.serving_root), evidence=args.evidence)
    run_id = "refresh_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    lock_path = args.serving_root / ".snapshot_writer.lock"
    payload = {"status": "failed", "run_id": run_id}
    try:
        with single_writer(lock_path):
            stream_evidence = args.evidence.parent / f"{run_id}_stream.json"
            snapshot_evidence = args.evidence.parent / f"{run_id}_snapshot.json"
            score_evidence = args.evidence.parent / f"{run_id}_score.json"
            serving_evidence = args.evidence.parent / f"{run_id}_serving.json"
            stream = _run([sys.executable, "scripts/stream.py", "--input", args.raw, "--output", args.observations, "--checkpoint", args.checkpoint, "--evidence", str(stream_evidence), "--max-files-per-trigger", str(args.max_files_per_trigger), "--master", args.master])
            if stream.returncode:
                raise RuntimeError(f"stream failed ({stream.returncode}): {stream.stderr[-1000:]}")
            snapshot_root = args.runtime_root / run_id / "snapshots"
            snapshot = _run([sys.executable, "scripts/snapshot.py", "--input", args.observations, "--input-format", "normalized", "--output-root", str(snapshot_root), "--evidence", str(snapshot_evidence), "--master", args.master])
            if snapshot.returncode:
                raise RuntimeError(f"snapshot failed ({snapshot.returncode}): {snapshot.stderr[-1000:]}")
            snapshot_manifest = json.loads(snapshot_evidence.read_text(encoding="utf-8"))
            score_args = [sys.executable, "scripts/score_analytics.py", "--clean", snapshot_manifest["clean_path"], "--output-root", str(args.runtime_root / run_id / "score"), "--evidence", str(score_evidence), "--master", args.master]
            if args.model:
                score_args.extend(["--model", str(args.model)])
            if args.as_of:
                score_args.extend(["--as-of", args.as_of])
            score = _run(score_args)
            if score.returncode:
                raise RuntimeError(f"score failed ({score.returncode}): {score.stderr[-1000:]}")
            score_manifest = json.loads(score_evidence.read_text(encoding="utf-8"))
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master(args.master).appName("news-m07-refresh-serving").config("spark.sql.session.timeZone", "UTC").getOrCreate()
            try:
                export_snapshot(spark.read.parquet(score_manifest["enriched_path"]), Path(score_manifest["gold_path"]), args.serving_root, snapshot_manifest["snapshot_id"], score_manifest["model_version"], serving_evidence, args.source_manifest, score_evidence, ROOT / "config/rss_sources.yaml", args.coverage_manifest)
            finally:
                spark.stop()
            payload = {"status": "ok", "run_id": run_id, "stream_evidence": str(stream_evidence), "snapshot_evidence": str(snapshot_evidence), "score_evidence": str(score_evidence), "serving_evidence": str(serving_evidence), "snapshot_id": snapshot_manifest["snapshot_id"], "output_count": snapshot_manifest["clean_count"], "model_version": score_manifest["model_version"]}
        atomic_write_json(args.evidence, payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        payload.update({"error_type": type(exc).__name__, "error": str(exc)})
        atomic_write_json(args.evidence, payload)
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
