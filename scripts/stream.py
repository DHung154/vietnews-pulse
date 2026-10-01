from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.stream import run_available  # noqa: E402
from news_pipeline.paths import validate_artifact_paths  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(ROOT / "runtime/data_collection_v2/raw/rss"))
    parser.add_argument("--output", default=str(ROOT / "runtime/data_collection_v2/observations/rss_v2"))
    parser.add_argument("--checkpoint", default=str(ROOT / "runtime/data_collection_v2/checkpoints/rss_clean_v3"))
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/d00/stream_manifest.json")
    parser.add_argument("--max-files-per-trigger", type=int, default=2)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    if "://" not in args.input and "://" not in args.output and "://" not in args.checkpoint:
        validate_artifact_paths(inputs=(Path(args.input),), outputs=(Path(args.output), Path(args.checkpoint)), evidence=args.evidence)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master(args.master).appName("news-m07-stream").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        manifest = run_available(spark, args.input, args.output, args.checkpoint, args.evidence, args.max_files_per_trigger)
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
