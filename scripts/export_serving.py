from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.serving import export_snapshot  # noqa: E402
from news_pipeline.paths import validate_artifact_paths  # noqa: E402
from news_pipeline.writer_lock import single_writer  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enriched", type=Path, required=True)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--model-version", default="unavailable")
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--source-config", type=Path, default=ROOT / "config/rss_sources.yaml")
    parser.add_argument("--score-manifest", type=Path, required=True)
    parser.add_argument("--coverage-manifest", type=Path)
    parser.add_argument("--output-root", type=Path, default=ROOT / "serving")
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/m06/serving_manifest.json")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    inputs = (args.enriched, args.gold, args.score_manifest, args.source_config, args.source_manifest, args.coverage_manifest) if args.source_manifest else (args.enriched, args.gold, args.score_manifest, args.source_config, args.coverage_manifest)
    validate_artifact_paths(inputs=tuple(path for path in inputs if path), outputs=(args.output_root,), evidence=args.evidence)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master(args.master).appName("news-m06-serving").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        with single_writer(args.output_root / ".snapshot_writer.lock"):
            manifest = export_snapshot(spark.read.parquet(str(args.enriched)), args.gold, args.output_root, args.snapshot_id, args.model_version, args.evidence, args.source_manifest, args.score_manifest, args.source_config, args.coverage_manifest)
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
