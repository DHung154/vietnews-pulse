"""Validate non-gold LLM suggestions against the frozen pilot packet."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.rss import atomic_write_json  # noqa: E402


SENTIMENTS = {"positive", "neutral", "negative", "mixed", "needs_context", "invalid"}
PRIORITIES = {"normal", "high"}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _metadata(path: Path) -> dict[str, dict]:
    rows = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        key = row.get("annotation_id")
        if not key or key in rows:
            raise ValueError(f"metadata duplicate or missing annotation_id at line {line_number}")
        if row.get("input_sha256") != _sha256(str(row.get("input_text") or "")):
            raise ValueError(f"metadata input hash mismatch for {key}")
        rows[key] = row
    return rows


def validate(metadata_path: Path, suggestions_path: Path, review_csv_path: Path | None = None) -> dict:
    metadata = _metadata(metadata_path)
    suggestions = []
    errors = []
    for line_number, line in enumerate(suggestions_path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            suggestions.append(json.loads(line))
        except json.JSONDecodeError as exc:
            errors.append(f"suggestions invalid JSON line {line_number}: {exc}")
    by_id = {}
    required = {"annotation_id", "input_sha256", "suggested_sentiment", "evidence_span", "reason", "review_priority", "label_origin", "model", "reasoning_effort", "human_verified", "suggested_event_group_id", "group_reason", "group_origin"}
    for row in suggestions:
        key = row.get("annotation_id")
        if not key or key in by_id:
            errors.append(f"duplicate_or_missing_annotation_id:{key}")
            continue
        by_id[key] = row
        if key not in metadata:
            errors.append(f"extra_annotation_id:{key}")
            continue
        missing = sorted(required - set(row))
        if missing:
            errors.append(f"missing_fields:{key}:{','.join(missing)}")
        meta = metadata[key]
        if row.get("input_sha256") != meta.get("input_sha256"):
            errors.append(f"input_hash_mismatch:{key}")
        if row.get("suggested_sentiment") not in SENTIMENTS:
            errors.append(f"invalid_sentiment:{key}")
        if row.get("review_priority") not in PRIORITIES:
            errors.append(f"invalid_priority:{key}")
        if row.get("evidence_span") and row["evidence_span"] not in str(meta.get("input_text") or ""):
            errors.append(f"evidence_not_in_input:{key}")
        if row.get("label_origin") != "llm" or row.get("model") != "gpt-5.5" or row.get("reasoning_effort") != "high" or row.get("human_verified") is not False or row.get("group_origin") != "llm":
            errors.append(f"provenance_mismatch:{key}")
        if not isinstance(row.get("reason"), str) or len(row["reason"].splitlines()) > 1:
            errors.append(f"reason_not_short:{key}")
        if not (row.get("suggested_event_group_id") or "").strip():
            errors.append(f"missing_event_suggestion:{key}")
    missing_ids = sorted(set(metadata) - set(by_id))
    errors.extend(f"missing_annotation_id:{key}" for key in missing_ids)
    csv_count = None
    if review_csv_path:
        with review_csv_path.open(encoding="utf-8", newline="") as handle:
            csv_rows = list(csv.DictReader(handle))
        csv_count = len(csv_rows)
        csv_by_id = {}
        for row in csv_rows:
            key = row.get("annotation_id")
            if not key or key in csv_by_id:
                errors.append(f"review_csv_duplicate_or_missing:{key}")
                continue
            csv_by_id[key] = row
            if key not in metadata:
                errors.append(f"review_csv_extra:{key}")
                continue
            if row.get("input_text") != metadata[key].get("input_text"):
                errors.append(f"review_csv_text_mismatch:{key}")
        errors.extend(f"review_csv_missing:{key}" for key in sorted(set(metadata) - set(csv_by_id)))
        if set(csv_by_id) == set(by_id):
            for key, row in csv_by_id.items():
                if row.get("suggested_sentiment") != by_id[key].get("suggested_sentiment") or row.get("suggested_event_group_id") != by_id[key].get("suggested_event_group_id"):
                    errors.append(f"review_csv_suggestion_mismatch:{key}")
    return {"status": "ok" if not errors and len(by_id) == len(metadata) else "invalid", "metadata_count": len(metadata), "suggestion_count": len(suggestions), "review_csv_count": csv_count, "missing_count": len(missing_ids), "error_count": len(errors), "errors": errors, "input_metadata_sha256": hashlib.sha256(metadata_path.read_bytes()).hexdigest(), "suggestions_sha256": hashlib.sha256(suggestions_path.read_bytes()).hexdigest(), "human_gold": False, "limitations": ["LLM suggestions are not human labels and cannot enter gold validation.", "Event groups are proposals only; singleton/unresolved groups do not prove no leakage outside this 100-row pool."]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--suggestions", type=Path, required=True)
    parser.add_argument("--review-csv", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = validate(args.metadata, args.suggestions, args.review_csv)
    result.update({"metadata": str(args.metadata.resolve()), "suggestions": str(args.suggestions.resolve()), "review_csv": str(args.review_csv.resolve()) if args.review_csv else None, "output": str(args.output.resolve())})
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
