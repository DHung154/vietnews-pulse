"""Prepare a conservative event-group review sheet without editing the packet."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from news_pipeline.rss import atomic_write_json


def _group_id(prefix: str, value: str) -> str:
    return f"{prefix}_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:16]}"


def prepare(metadata_path: Path, output_dir: Path) -> dict:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing to overwrite event-group review output: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [json.loads(line) for line in metadata_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    ids = [row.get("annotation_id") for row in rows]
    if not ids or any(not value for value in ids) or len(set(ids)) != len(ids):
        raise ValueError("metadata annotation_id must be present and unique")
    by_article: dict[str, list[dict]] = {}
    by_content: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("article_id"):
            by_article.setdefault(str(row["article_id"]), []).append(row)
        if row.get("content_group_id"):
            by_content.setdefault(str(row["content_group_id"]), []).append(row)
    review_rows = []
    counts = {"rows": len(rows), "safe_article_candidates": 0, "safe_content_candidates": 0, "pending_manual": 0}
    for row in rows:
        article_rows = by_article.get(str(row.get("article_id")), [])
        content_rows = by_content.get(str(row.get("content_group_id")), [])
        if len(article_rows) > 1:
            suggested = _group_id("article", str(row["article_id"]))
            reason = "same_article_id"
            counts["safe_article_candidates"] += 1
        elif len(content_rows) > 1:
            suggested = _group_id("content", str(row["content_group_id"]))
            reason = "same_content_group_id"
            counts["safe_content_candidates"] += 1
        else:
            suggested = ""
            reason = "unresolved_event_relationship"
            counts["pending_manual"] += 1
        review_rows.append({"annotation_id": row["annotation_id"], "article_id": row.get("article_id", ""), "content_group_id": row.get("content_group_id", ""), "input_text": row.get("input_text", ""), "input_sha256": row.get("input_sha256") or hashlib.sha256(str(row.get("input_text") or "").encode("utf-8")).hexdigest(), "source": row.get("source", ""), "category": row.get("category", ""), "published_at": row.get("published_at", ""), "split": row.get("split", ""), "current_event_group_id": row.get("event_group_id") or "", "suggested_event_group_id": suggested, "event_group_id": "", "suggestion_reason": reason, "review_status": "pending_manual" if not suggested else "candidate_requires_review", "reviewer": "", "review_reason": ""})
    review_path = output_dir / "event_group_review.csv"
    with review_path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(review_rows[0]))
        writer.writeheader()
        writer.writerows(review_rows)
    manifest = {"status": "blocked_manual_event_grouping", "input": str(metadata_path.resolve()), "output": str(output_dir.resolve()), "input_sha256": hashlib.sha256(metadata_path.read_bytes()).hexdigest(), "counts": counts, "review_csv": str(review_path.resolve()), "limitations": ["Only exact article/content identity creates a conservative candidate; no semantic event is auto-inferred.", "Every row still requires human review before event_group_id is written to a new metadata version.", "This tool does not create sentiment labels."]}
    atomic_write_json(output_dir / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.metadata, args.output)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
