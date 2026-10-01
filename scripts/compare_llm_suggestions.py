"""Compare two independent LLM suggestion packets without creating gold labels."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.rss import atomic_write_json  # noqa: E402


def _read(path: Path) -> tuple[dict[str, dict], str]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_id = {row["annotation_id"]: row for row in rows}
    if len(by_id) != len(rows):
        raise ValueError(f"duplicate annotation_id: {path}")
    return by_id, hashlib.sha256(path.read_bytes()).hexdigest()


def _partition(by_id: dict[str, dict]) -> tuple[dict[str, frozenset[str]], set[tuple[str, str]]]:
    groups: dict[str, set[str]] = {}
    unresolved = {None, "", "unresolved"}
    for key, row in by_id.items():
        group = row.get("suggested_event_group_id")
        if group not in unresolved:
            groups.setdefault(group, set()).add(key)
    members = {key: frozenset(group) for group in groups.values() for key in group}
    pairs = {tuple(sorted((left, right))) for group in groups.values() for left in group for right in group if left < right}
    return members, pairs


def compare(first_path: Path, second_path: Path, metadata_path: Path, output_dir: Path) -> dict:
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite comparison output: {output_dir}")
    first, first_hash = _read(first_path)
    second, second_hash = _read(second_path)
    metadata = {json.loads(line)["annotation_id"]: json.loads(line) for line in metadata_path.read_text(encoding="utf-8").splitlines() if line.strip()}
    if set(first) != set(second) or set(first) != set(metadata):
        raise ValueError("both suggestion packets must match metadata IDs")
    first_members, first_pairs = _partition(first)
    second_members, second_pairs = _partition(second)
    resolved_ids = set(first_members) & set(second_members)
    resolved_first_pairs = {pair for pair in first_pairs if set(pair) <= resolved_ids}
    resolved_second_pairs = {pair for pair in second_pairs if set(pair) <= resolved_ids}
    rows = []
    review_reasons = Counter()
    for key in sorted(metadata):
        a, b, source = first[key], second[key], metadata[key]
        if a.get("input_sha256") != source.get("input_sha256") or b.get("input_sha256") != source.get("input_sha256"):
            raise ValueError(f"input hash mismatch: {key}")
        sentiment_agreement = a.get("suggested_sentiment") == b.get("suggested_sentiment")
        group_agreement = key in resolved_ids and first_members[key] == second_members[key]
        reasons = []
        if not sentiment_agreement:
            reasons.append("sentiment_disagreement")
        if key in resolved_ids and not group_agreement:
            reasons.append("event_group_membership_disagreement")
        if a.get("suggested_sentiment") in {"mixed", "needs_context", "invalid"} or b.get("suggested_sentiment") in {"mixed", "needs_context", "invalid"}:
            reasons.append("non_simple_sentiment")
        if key not in resolved_ids:
            reasons.append("unresolved_event_group")
        if a.get("review_priority") == "high" or b.get("review_priority") == "high":
            reasons.append("high_priority")
        for reason in reasons:
            review_reasons[reason] += 1
        rows.append({"annotation_id": key, "input_sha256": source.get("input_sha256"), "input_text": source.get("input_text") or "", "sentiment_v1": a.get("suggested_sentiment"), "sentiment_v2": b.get("suggested_sentiment"), "sentiment_agreement": sentiment_agreement, "event_group_v1": a.get("suggested_event_group_id"), "event_group_v2": b.get("suggested_event_group_id"), "event_group_agreement": group_agreement, "review_priority": "high" if "high_priority" in reasons else "normal", "needs_review": bool(reasons), "review_reason": ";".join(reasons), "human_sentiment": "", "human_event_group_id": "", "human_reviewer": "", "human_review_reason": ""})
    output_dir.mkdir(parents=True)
    comparison_path = output_dir / "llm_comparison.csv"
    with comparison_path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)
    manifest = {"status": "ok", "human_gold": False, "first_suggestions": str(first_path.resolve()), "second_suggestions": str(second_path.resolve()), "metadata": str(metadata_path.resolve()), "first_suggestions_sha256": first_hash, "second_suggestions_sha256": second_hash, "row_count": len(rows), "sentiment_agreement_count": sum(row["sentiment_agreement"] for row in rows), "resolved_in_both_count": len(resolved_ids), "event_group_partition_agreement_count": sum(row["event_group_agreement"] for row in rows), "v1_co_membership_count": len(resolved_first_pairs), "v2_co_membership_count": len(resolved_second_pairs), "co_membership_intersection_count": len(resolved_first_pairs & resolved_second_pairs), "co_membership_symmetric_difference_count": len(resolved_first_pairs ^ resolved_second_pairs), "needs_review_count": sum(row["needs_review"] for row in rows), "review_reason_counts": dict(sorted(review_reasons.items())), "output": str(comparison_path.resolve()), "limitations": ["Comparison measures LLM-pass agreement only; it does not establish human correctness.", "Event groups remain proposals within the 100-row pool.", "Unresolved rows are excluded from co-membership agreement."]}
    atomic_write_json(output_dir / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--first", type=Path, required=True)
    parser.add_argument("--second", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(compare(args.first, args.second, args.metadata, args.output_dir), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
