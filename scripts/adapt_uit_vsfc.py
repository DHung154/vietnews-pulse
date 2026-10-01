from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.rss import atomic_write_json


LABELS = {0: "negative", 1: "neutral", 2: "positive"}
SPLITS = ("train", "validation", "test")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(f"refusing to overwrite output: {args.output}")
    counts = {}
    label_counts = {}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as output:
        for split in SPLITS:
            paths = {name: args.raw_dir / f"{split}_{name}.txt" for name in ("sentences", "sentiments", "topics")}
            if not all(path.exists() for path in paths.values()):
                raise FileNotFoundError(f"missing {split} files")
            split_count = 0
            sentiments = Counter()
            with paths["sentences"].open(encoding="utf-8") as sentences, paths["sentiments"].open(encoding="utf-8") as sentiment_file, paths["topics"].open(encoding="utf-8") as topic_file:
                for index, values in enumerate(zip(sentences, sentiment_file, topic_file)):
                    sentence, sentiment, topic = (value.rstrip("\r\n") for value in values)
                    sentiment_id = int(sentiment)
                    topic_id = int(topic)
                    if sentiment_id not in LABELS or not 0 <= topic_id <= 3 or not sentence:
                        raise ValueError(f"invalid {split} row {index}")
                    output.write(json.dumps({"row_id": f"uit-vsfc-{split}-{index}", "text": sentence, "label": LABELS[sentiment_id], "split": split, "topic_id": topic_id}, ensure_ascii=False) + "\n")
                    split_count += 1
                    sentiments[LABELS[sentiment_id]] += 1
                if any(handle.readline() for handle in (sentences, sentiment_file, topic_file)):
                    raise ValueError(f"split files have different lengths: {split}")
            counts[split] = split_count
            label_counts[split] = dict(sorted(sentiments.items()))
    manifest = {"status": "ok", "dataset_id": "uit_vsfc", "output": str(args.output.resolve()), "counts": counts, "label_counts": label_counts, "raw_sha256": {path.name: _sha256(path) for path in args.raw_dir.glob("*.txt")}}
    atomic_write_json(args.evidence, manifest)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
