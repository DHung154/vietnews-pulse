"""Create a new metadata version from a human-reviewed event-group sheet."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from news_pipeline.rss import atomic_write_json


def apply_groups(metadata_path: Path, review_path: Path, output_path: Path) -> dict:
    if output_path.exists():
        raise FileExistsError(f"refusing to overwrite metadata version: {output_path}")
    rows = [json.loads(line) for line in metadata_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    with review_path.open(encoding="utf-8", newline="") as handle:
        review_rows = list(csv.DictReader(handle))
    manifest_path = review_path.with_name("manifest.json")
    if not manifest_path.exists():
        raise ValueError("review manifest.json is required")
    metadata_sha256 = hashlib.sha256(metadata_path.read_bytes()).hexdigest()
    prepared = json.loads(manifest_path.read_text(encoding="utf-8"))
    if prepared.get("input_sha256") != metadata_sha256:
        raise ValueError("review sheet was prepared from a different metadata version")
    metadata_ids = [row.get("annotation_id") for row in rows]
    review_ids = [row.get("annotation_id") for row in review_rows]
    if len(set(review_ids)) != len(review_ids) or set(metadata_ids) != set(review_ids):
        raise ValueError("review IDs must exactly match metadata IDs once")
    groups = {}
    for row in review_rows:
        group = (row.get("event_group_id") or "").strip()
        if not group or (row.get("review_status") or "").strip() != "reviewed":
            raise ValueError(f"event group is not reviewed for {row.get('annotation_id')}")
        if not (row.get("reviewer") or "").strip() or not (row.get("review_reason") or "").strip():
            raise ValueError(f"reviewer and review_reason are required for {row.get('annotation_id')}")
        groups[row["annotation_id"]] = group
    metadata_by_id = {row["annotation_id"]: row for row in rows}
    for row in review_rows:
        metadata_row = metadata_by_id[row["annotation_id"]]
        expected_text = str(metadata_row.get("input_text") or "")
        expected_hash = metadata_row.get("input_sha256") or hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
        if expected_hash != hashlib.sha256(expected_text.encode("utf-8")).hexdigest():
            raise ValueError(f"metadata input hash is internally inconsistent for {row['annotation_id']}")
        if row.get("input_text") != expected_text or row.get("input_sha256") != expected_hash:
            raise ValueError(f"review content/hash mismatch for {row['annotation_id']}")
    seen_groups: dict[str, set[str]] = {}
    for row in rows:
        seen_groups.setdefault(groups[row["annotation_id"]], set()).add(str(row.get("split") or ""))
    if any(len(splits) > 1 for splits in seen_groups.values()):
        raise ValueError("one event group crosses splits")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(output_path.name + ".inprogress")
    with temp_path.open("x", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            row = dict(row)
            row["event_group_id"] = groups[row["annotation_id"]]
            row["event_group_source"] = "human_reviewed"
            review = next(item for item in review_rows if item["annotation_id"] == row["annotation_id"])
            row["event_group_reviewer"] = review["reviewer"].strip()
            row["event_group_review_reason"] = review["review_reason"].strip()
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    os.replace(temp_path, output_path)
    manifest = {"status": "ok", "metadata_input": str(metadata_path.resolve()), "review_input": str(review_path.resolve()), "review_manifest": str(manifest_path.resolve()), "output": str(output_path.resolve()), "metadata_sha256": metadata_sha256, "review_sha256": hashlib.sha256(review_path.read_bytes()).hexdigest(), "row_count": len(rows), "event_group_count": len(seen_groups), "reviewer_count": len({row["reviewer"].strip() for row in review_rows}), "human_review_required": True}
    atomic_write_json(output_path.with_suffix(output_path.suffix + ".manifest.json"), manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--review", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = apply_groups(args.metadata, args.review, args.output)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
