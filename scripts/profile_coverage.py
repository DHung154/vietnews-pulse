"""Profile immutable RSS manifests and apply the D4 incomplete-window gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.coverage import profile_manifests  # noqa: E402
from news_pipeline.rss import atomic_write_json, load_sources  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifests", type=Path, required=True, help="directory containing immutable collector manifests")
    parser.add_argument("--config", type=Path, default=ROOT / "config/rss_sources.yaml")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--required-days", type=int, default=14)
    parser.add_argument("--min-success-rate", type=float, default=0.95)
    parser.add_argument("--end-local", help="explicit local as-of date (YYYY-MM-DD); default is latest observed day")
    args = parser.parse_args()
    if not args.manifests.is_dir():
        parser.error("manifests must be an existing directory")
    if args.required_days <= 0 or not 0 <= args.min_success_rate <= 1:
        parser.error("required-days must be positive and min-success-rate must be between 0 and 1")
    manifests = []
    for path in sorted(args.manifests.glob("*.json")):
        if path.resolve() == args.output.resolve():
            continue
        manifests.append(json.loads(path.read_text(encoding="utf-8")))
    result = profile_manifests(manifests, load_sources(args.config), args.required_days, args.min_success_rate, args.end_local)
    result.update({"status": "ok", "manifest_count": len(manifests), "config": str(args.config.resolve()), "input": str(args.manifests.resolve()), "output": str(args.output.resolve())})
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
