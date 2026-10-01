from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.history import import_jsonl  # noqa: E402
from news_pipeline.paths import validate_artifact_paths  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Import JSONL history only with an explicit mapping")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    try:
        validate_artifact_paths(inputs=(args.input, args.mapping), outputs=(args.output,), evidence=args.evidence)
        result = import_jsonl(args.input, args.mapping, args.output, args.dataset_id)
        manifest = {"status": "ok", "timestamp_utc": datetime.now(timezone.utc).isoformat(), "dataset_id": args.dataset_id, "input": str(args.input.resolve()), "mapping": str(args.mapping.resolve()), **result}
        atomic_write_json(args.evidence, manifest)
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        if isinstance(exc, ValueError) and str(exc).startswith(("evidence path", "input aliases")):
            print(f"argument validation failed: {exc}", file=sys.stderr)
            return 2
        manifest = {"status": "failed", "timestamp_utc": datetime.now(timezone.utc).isoformat(), "dataset_id": args.dataset_id, "input": str(args.input.resolve()), "mapping": str(args.mapping.resolve()), "error_type": type(exc).__name__, "error": str(exc)}
        atomic_write_json(args.evidence, manifest)
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
