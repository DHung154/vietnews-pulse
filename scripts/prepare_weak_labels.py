"""Build a small, agreement-only weak-label train set from two LLM passes."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.rss import atomic_write_json  # noqa: E402


LABELS = {"negative", "neutral", "positive"}
UNRESOLVED = {None, "", "unresolved"}


def _read(path: Path) -> tuple[list[dict], str]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return rows, hashlib.sha256(path.read_bytes()).hexdigest()


def build(first_path: Path, second_path: Path, metadata_path: Path, output_dir: Path) -> dict:
    if output_dir.exists():
        raise FileExistsError(f"refusing to overwrite weak-label output: {output_dir}")
    first, first_hash = _read(first_path)
    second, second_hash = _read(second_path)
    metadata, metadata_hash = _read(metadata_path)
    meta = {row["annotation_id"]: row for row in metadata}
    left = {row["annotation_id"]: row for row in first}
    right = {row["annotation_id"]: row for row in second}
    if set(left) != set(right) or set(left) != set(meta):
        raise ValueError("LLM passes and metadata must contain the same annotation IDs")
    excluded = Counter()
    accepted_by_id: dict[str, dict] = {}
    for key in sorted(meta):
        a, b, source = left[key], right[key], meta[key]
        if a.get("input_sha256") != source.get("input_sha256") or b.get("input_sha256") != source.get("input_sha256"):
            raise ValueError(f"input hash mismatch: {key}")
        if a.get("suggested_sentiment") != b.get("suggested_sentiment"):
            excluded["sentiment_disagreement"] += 1
            continue
        if a.get("suggested_sentiment") not in LABELS:
            excluded["non_trainable_label"] += 1
            continue
        if a.get("suggested_event_group_id") in UNRESOLVED or b.get("suggested_event_group_id") in UNRESOLVED:
            excluded["unresolved_event_group"] += 1
            continue
        accepted_by_id[key] = {"row_id": key, "text": source.get("input_text") or "", "label": a["suggested_sentiment"], "event_group_status": "resolved_llm", "label_origin": "llm_agreement", "human_verified": False}
    if not accepted_by_id:
        raise ValueError("no agreement-only rows available for weak training")

    parent = {key: key for key in accepted_by_id}

    def find(key: str) -> str:
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(left: str, right: str) -> None:
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    identity_pairs = set()
    for field in ("article_id", "content_group_id"):
        buckets: dict[str, list[str]] = {}
        for key in accepted_by_id:
            value = meta[key].get(field)
            if value:
                buckets.setdefault(str(value), []).append(key)
        for members in buckets.values():
            for index, member_left in enumerate(members):
                for member_right in members[index + 1:]:
                    identity_pairs.add(tuple(sorted((member_left, member_right))))
    for left_key, right_key in identity_pairs:
        union(left_key, right_key)

    event_pairs = set()
    for packet in (left, right):
        buckets: dict[str, list[str]] = {}
        for key in accepted_by_id:
            buckets.setdefault(str(packet[key].get("suggested_event_group_id")), []).append(key)
        for members in buckets.values():
            for index, member_left in enumerate(members):
                for member_right in members[index + 1:]:
                    event_pairs.add(tuple(sorted((member_left, member_right))))
    for left_key, right_key in event_pairs:
        union(left_key, right_key)

    components: dict[str, list[str]] = {}
    for key in accepted_by_id:
        components.setdefault(find(key), []).append(key)
    component_ids = {}
    for members in components.values():
        members.sort()
        component_id = "event_component_" + hashlib.sha256("|".join(members).encode("utf-8")).hexdigest()[:16]
        for key in members:
            component_ids[key] = component_id
    groups = {component_id: [] for component_id in set(component_ids.values())}
    for key, row in accepted_by_id.items():
        row["split_group_id"] = component_ids[key]
        groups[component_ids[key]].append(row)
    ordered_groups = sorted(groups)
    split_by_group = {group: ("train" if index % 20 < 14 else "validation" if index % 20 < 17 else "test") for index, group in enumerate(ordered_groups)}
    accepted = list(accepted_by_id.values())
    for row in accepted:
        row["split"] = split_by_group[row["split_group_id"]]
    output_dir.mkdir(parents=True)
    train_path = output_dir / "weak_train.jsonl"
    with train_path.open("x", encoding="utf-8") as handle:
        for row in accepted:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    manifest = {
        "status": "ok",
        "label_origin": "llm_agreement_weak",
        "human_gold": False,
        "first_suggestions": str(first_path.resolve()),
        "second_suggestions": str(second_path.resolve()),
        "metadata": str(metadata_path.resolve()),
        "first_suggestions_sha256": first_hash,
        "second_suggestions_sha256": second_hash,
        "metadata_sha256": metadata_hash,
        "output": str(train_path.resolve()),
        "row_count": len(accepted),
        "group_count": len(groups),
        "identity_edge_count": len(identity_pairs),
        "event_edge_count": len(event_pairs),
        "component_count": len(components),
        "cross_split_component_count": sum(len({row["split"] for row in members}) > 1 for members in groups.values()),
        "split_counts": dict(sorted(Counter(row["split"] for row in accepted).items())),
        "label_counts": dict(sorted(Counter(row["label"] for row in accepted).items())),
        "excluded_counts": dict(sorted(excluded.items())),
        "event_group_agreement_count": 0,
        "limitations": ["Agreement between two LLM passes is weak supervision, not human truth.", "Event grouping is only within the 100-row pilot pool; disputed groups are not treated as resolved.", "Rows with sentiment disagreement, mixed/invalid/context-dependent labels are excluded; event-group disagreement remains flagged for human review."],
    }
    atomic_write_json(output_dir / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--first", type=Path, required=True)
    parser.add_argument("--second", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.first, args.second, args.metadata, args.output_dir), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
