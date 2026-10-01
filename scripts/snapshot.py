from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.etl import read_input  # noqa: E402
from news_pipeline.paths import validate_evidence_only  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402
from news_pipeline.snapshot import build_snapshot  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(ROOT / "runtime/data_collection_v2/raw/rss"))
    parser.add_argument("--input-format", choices=("raw", "normalized"), default="raw")
    parser.add_argument("--output-root", default=str(ROOT / "runtime/m03/snapshots"))
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/m03/snapshot_manifest.json")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    if "://" not in args.input and "://" not in args.output_root:
        validate_evidence_only(args.evidence, Path(args.input), Path(args.output_root))
    snapshot_id = "snapshot_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    output_root = args.output_root.rstrip("/\\")
    clean_path = f"{output_root}/{snapshot_id}/clean"
    quarantine_path = f"{output_root}/{snapshot_id}/quarantine"
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master(args.master).appName("news-m03-snapshot").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        manifest = build_snapshot(read_input(spark, args.input, args.input_format), clean_path, quarantine_path, args.evidence, snapshot_id, args.input_format == "normalized")
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        atomic_write_json(args.evidence, {"status": "failed", "error_type": type(exc).__name__, "error": str(exc), "snapshot_id": snapshot_id})
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
