"""Offline pilot review handoff; never promotes assisted labels to human gold."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.validate_labeling import _read_metadata
from scripts.validate_llm_suggestions import SENTIMENTS, validate


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)


def prepare(metadata: Path, guideline: Path, output: Path, suggestions: list[Path]) -> dict:
    # A version is an exclusive directory, not an overwriteable manifest.
    if output.exists():
        raise FileExistsError(f"Output already exists: {output}")
    rows = _read_metadata(metadata)
    if not rows or any(row.get("split") != "pilot" for row in rows.values()):
        raise ValueError("This handoff only accepts a nonempty pilot, never main test/train")
    if len(suggestions) not in (0, 2):
        raise ValueError("Use no suggestions for blind mode, or both existing LLM rounds")
    rounds = []
    for path in suggestions:
        result = validate(metadata, path)
        if result["status"] != "ok":
            raise ValueError(f"Invalid LLM suggestions: {result['errors']}")
        rounds.append({r["annotation_id"]: r for r in
                       (json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())})
    records = []
    for key, row in rows.items():
        item = {field: row[field] for field in ("annotation_id", "input_text", "input_sha256")}
        if rounds:
            item["suggestions"] = [{field: rnd[key][field] for field in
                ("suggested_sentiment", "evidence_span", "reason", "model", "review_priority")} for rnd in rounds]
            item["needs_priority_review"] = (
                rounds[0][key]["suggested_sentiment"] != rounds[1][key]["suggested_sentiment"]
                or any(rnd[key]["review_priority"] == "high" or rnd[key]["suggested_sentiment"] in
                    {"mixed", "needs_context", "invalid"} for rnd in rounds)
            )
        records.append(item)
    if rounds:
        records.sort(key=lambda row: (not row["needs_priority_review"], row["annotation_id"]))
    bundle = {
        "format_version": "sentiment_review_v1", "mode": "assisted" if rounds else "blind",
        "metadata_sha256": sha(metadata), "guideline_sha256": sha(guideline),
        "suggestions_sha256": [sha(p) for p in suggestions], "records": records,
    }
    bundle["bundle_id"] = hashlib.sha256(json.dumps(bundle, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    payload = json.dumps(bundle, ensure_ascii=False).replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    template = (ROOT / "docs/sentiment_review_template.html").read_text(encoding="utf-8")
    rendered = template.replace("__BUNDLE_JSON__", payload)
    manifest = {
        "status": "awaiting_human_review", "bundle_id": bundle["bundle_id"], "mode": bundle["mode"],
        "metadata": str(metadata.resolve()), "metadata_sha256": sha(metadata),
        "guideline": str(guideline.resolve()), "guideline_sha256": sha(guideline),
        "suggestions": [str(p.resolve()) for p in suggestions],
        "suggestions_sha256": bundle["suggestions_sha256"], "row_count": len(records),
        "priority_count": sum(r.get("needs_priority_review", False) for r in records),
        "input_dataset_counts": dict(Counter(r.get("dataset_id", "unknown") for r in rows.values())),
        "created_at": datetime.now(timezone.utc).isoformat(), "human_gold": False,
        "html_sha256": hashlib.sha256(rendered.encode()).hexdigest(),
        "limitations": ["Pilot only; not a final test.", "Assisted review is silver, not independent human gold.",
            "Identity/review claims are self-reported, not independently verified."],
    }
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "bundle.json", bundle)
    write_json(output / "manifest.json", manifest)
    with (output / "review.html").open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(rendered)
    return manifest


def import_reviews(bundle_path: Path, metadata: Path, guideline: Path, review_path: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(f"Output already exists: {output}")
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    original_id = bundle.pop("bundle_id")
    expected_id = hashlib.sha256(json.dumps(bundle, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    bundle["bundle_id"] = original_id
    if original_id != expected_id or bundle["metadata_sha256"] != sha(metadata) or bundle["guideline_sha256"] != sha(guideline):
        raise ValueError("Bundle/metadata/guideline changed since preparation")
    if bundle["mode"] not in {"blind", "assisted"} or bundle["format_version"] != "sentiment_review_v1":
        raise ValueError("Unknown review format/mode")
    source = _read_metadata(metadata)
    expected = {r["annotation_id"]: r for r in bundle["records"]}
    if len(expected) != len(bundle["records"]) or set(expected) != set(source):
        raise ValueError("Bundle IDs do not exactly match metadata")
    for key, record in expected.items():
        if record["input_sha256"] != source[key]["input_sha256"] or record["input_text"] != source[key]["input_text"]:
            raise ValueError(f"Bundle text/hash mismatch: {key}")
    reviewed = json.loads(review_path.read_text(encoding="utf-8-sig"))
    if reviewed.get("bundle_id") != original_id or reviewed.get("mode") != bundle["mode"] or reviewed.get("format_version") != bundle["format_version"]:
        raise ValueError("Review belongs to a different bundle/mode")
    items = reviewed.get("reviews", [])
    ids = [r.get("annotation_id") for r in items]
    if len(ids) != len(set(ids)) or set(ids) != set(expected):
        raise ValueError("Review IDs must match the complete packet exactly once")
    labels = []
    now = datetime.now(timezone.utc)
    for row in items:
        key = row["annotation_id"]
        if row.get("input_sha256") != source[key]["input_sha256"]:
            raise ValueError(f"Review input hash mismatch: {key}")
        if "input_text" in row and row["input_text"] != source[key]["input_text"]:
            raise ValueError(f"Review input text mismatch: {key}")
        if row.get("label") not in SENTIMENTS or row.get("reviewed") is not True:
            raise ValueError(f"Missing explicit label/review confirmation: {key}")
        if any(not isinstance(row.get(f), str) or not row[f].strip() for f in ("reviewer", "reason", "reviewed_at")):
            raise ValueError(f"Missing reviewer/reason/time: {key}")
        timestamp = datetime.fromisoformat(row["reviewed_at"].replace("Z", "+00:00"))
        if timestamp.tzinfo is None or timestamp > now:
            raise ValueError(f"Review time needs timezone and cannot be future: {key}")
        evidence = row.get("evidence_span", "")
        if not isinstance(evidence, str) or (evidence and evidence not in source[key]["input_text"]):
            raise ValueError(f"Evidence is not an exact input span: {key}")
        if row["label"] in {"positive", "negative"} and not evidence:
            raise ValueError(f"Positive/negative needs a supporting input span: {key}")
        if row.get("human_gold") or row.get("gold_ready") or row.get("human_verified") or row.get("label_origin") not in (None, ""):
            raise ValueError("Review cannot supply gold/provenance overrides")
        labels.append({
            "annotation_id": key, "input_sha256": source[key]["input_sha256"], "input_text": source[key]["input_text"],
            "label": row["label"], "reviewer": row["reviewer"].strip(), "reason": row["reason"].strip(),
            "evidence_span": evidence, "reviewed_at": row["reviewed_at"], "review_mode": bundle["mode"],
            "label_origin": "human_reviewed_llm" if bundle["mode"] == "assisted" else "human_self_reported",
            "verification": "self_reported", "human_gold": False, "split": "pilot",
            "suggestions": expected[key].get("suggestions", []),
        })
    manifest = {"status": "imported_pilot_review", "row_count": len(labels), "human_gold": False, "gold_ready": False,
        "mode": bundle["mode"], "bundle_id": original_id, "metadata_sha256": sha(metadata),
        "guideline_sha256": sha(guideline), "reviews_sha256": sha(review_path),
        "label_counts": dict(Counter(r["label"] for r in labels)),
        "reviewer_count": len({r["reviewer"] for r in labels}), "metrics": None,
        "limitations": ["Pilot/silver only; not train/test or frozen gold.", "Self-report is not independent verification.",
            "Event grouping, independent second annotator and adjudication remain separate requirements."]}
    output.mkdir(parents=True, exist_ok=False)
    with (output / "reviewed_labels.jsonl").open("x", encoding="utf-8") as handle:
        for row in labels:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest["labels_sha256"] = sha(output / "reviewed_labels.jsonl")
    write_json(output / "manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "import"))
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--guideline", type=Path, default=ROOT / "docs/NEWS_SENTIMENT_LABELING_GUIDE.md")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suggestions", type=Path, nargs="*", default=[])
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--reviews", type=Path)
    args = parser.parse_args()
    if args.action == "import" and not (args.bundle and args.reviews):
        parser.error("import requires --bundle and --reviews")
    try:
        result = prepare(args.metadata, args.guideline, args.output, args.suggestions) if args.action == "prepare" else import_reviews(args.bundle, args.metadata, args.guideline, args.reviews, args.output)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "rejected", "error": str(exc)}, ensure_ascii=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
