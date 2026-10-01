from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.train import train_fixture  # noqa: E402
from news_pipeline.paths import validate_artifact_paths  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=ROOT / "data_sample/sentiment_train.jsonl")
    parser.add_argument("--model", type=Path, default=ROOT / "runtime/m04/models/tfidf_lr_demo_v1")
    parser.add_argument("--model-version", default="tfidf_lr_demo_v2")
    parser.add_argument("--dataset-id", default="demo_sentiment")
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/m04/train_manifest.json")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--no-save", action="store_true", help="run an in-memory experiment without persisting the Spark model")
    args = parser.parse_args()
    validate_artifact_paths(inputs=(args.input,), outputs=(args.model,), evidence=args.evidence)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master(args.master).appName("news-m04-train").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        metadata = train_fixture(spark, args.input, args.model, args.evidence, args.model_version, args.dataset_id, save_model=not args.no_save)
        print(json.dumps(metadata, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
