"""Create blind, unlabeled news-sentiment packets from RSS raw observations."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.normalization import normalize_record  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402


def _refuse_existing(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite labeling artifact: {path.resolve()}")


def _write_packet(path: Path, rows: list[dict]) -> None:
    _refuse_existing(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ("annotation_id", "input_text", "label", "status", "evidence_span", "reason", "confidence")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=ROOT / "runtime/data_collection_v2/raw/rss")
    parser.add_argument("--output", type=Path, default=ROOT / "annotations/news_sentiment_gold_v1/pilot_20260917")
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260917)
    args = parser.parse_args()
    if not args.input.exists() or args.sample_size <= 0:
        parser.error("input must exist and sample-size must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    existing = sorted(path.name for path in args.output.iterdir())
    if existing:
        raise FileExistsError(
            f"refusing to overwrite non-empty labeling output {args.output.resolve()}; "
            "choose a new versioned output directory"
        )
    candidates: dict[tuple[str, str], dict] = {}
    input_rows = 0
    for path in sorted(args.input.rglob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                input_rows += 1
                raw = json.loads(line)
                if raw.get("dataset_id") not in {"rss_live", "rss_live_v2"}:
                    continue
                row = normalize_record(raw)
                if not row["valid"] or not row["model_input_text"]:
                    continue
                key = (row["dataset_id"], row["article_id"])
                old = candidates.get(key)
                if old is None or (row.get("last_seen_at") or "") > (old.get("last_seen_at") or ""):
                    candidates[key] = row
    ordered = list(candidates.values())
    random.Random(args.seed).shuffle(ordered)
    selected = ordered[: min(args.sample_size, len(ordered))]
    metadata = []
    packets = []
    probability = len(selected) / len(ordered) if ordered else None
    for row in selected:
        annotation_id = hashlib.sha256(f"news_sentiment_gold_v1|{args.seed}|{row['article_id']}".encode()).hexdigest()
        item = {
            "annotation_id": annotation_id,
            "article_id": row["article_id"],
            "content_group_id": row.get("text_hash"),
            "event_group_id": None,
            "dataset_id": row["dataset_id"],
            "input_type": row["model_input_type"],
            "input_text": row["model_input_text"],
            "input_sha256": hashlib.sha256(row["model_input_text"].encode("utf-8")).hexdigest(),
            "source": row["source"],
            "category": row["category"],
            "published_at": row["published_at"],
            "sample_seed": args.seed,
            "sampling_stratum": f"{row['source']}|{row['category']}|{row.get('event_date') or 'unknown'}",
            "selection_probability": probability,
            "split": "pilot",
            "guideline_version": "news_sentiment_labeling_guide_v0.1",
            "label": "",
            "status": "",
            "evidence_span": "",
            "reason": "",
            "confidence": "",
        }
        metadata.append(item)
        packets.append({key: item[key] for key in ("annotation_id", "input_text", "label", "status", "evidence_span", "reason", "confidence")})
    metadata_path = args.output / "metadata.jsonl"
    _refuse_existing(metadata_path)
    with metadata_path.open("x", encoding="utf-8", newline="\n") as handle:
        for item in metadata:
            handle.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
    _write_packet(args.output / "annotator_a.csv", packets)
    shuffled = list(packets)
    random.Random(args.seed + 1).shuffle(shuffled)
    _write_packet(args.output / "annotator_b.csv", shuffled)
    manifest = {
        "status": "blocked_human_labeling",
        "input": str(args.input.resolve()),
        "output": str(args.output.resolve()),
        "input_rows_seen": input_rows,
        "unique_valid_rss_articles": len(ordered),
        "selected_pilot": len(selected),
        "sample_size_requested": args.sample_size,
        "seed": args.seed,
        "split": "pilot",
        "labels_filled": 0,
        "annotators_required": 2,
        "gold_ready": False,
        "limitations": ["Current RSS raw covers a short existing capture, not the planned 14-day pool.", "Packets are blank; no model or automatic labeling was used.", "content_group_id is a text-hash proxy until D3 event grouping is audited."],
    }
    manifest_path = args.output / "manifest.json"
    _refuse_existing(manifest_path)
    atomic_write_json(manifest_path, manifest)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
