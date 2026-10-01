"""Validate gold metadata/splits and optionally score a frozen prediction CSV."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.evaluation import classification_metrics, split_group_leakage  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402
from scripts.validate_labeling import _read_annotator  # noqa: E402


EVALUATION_LABELS = {"positive", "neutral", "negative"}
GROUPING_FIELDS = ("article_id", "content_group_id", "event_group_id")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_gold_hash(rows: list[dict]) -> str:
    payload = "\n".join(json.dumps({"annotation_id": row["annotation_id"], "final_label": row["final_label"]}, ensure_ascii=False, sort_keys=True) for row in sorted(rows, key=lambda item: item["annotation_id"]))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_csv_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _label_rows(path: Path, selected: list[dict]) -> tuple[dict[str, dict], list[str]]:
    """Use the same text/hash/label validation as packet validation."""
    metadata = {row["annotation_id"]: row for row in selected}
    rows = _read_annotator(path, metadata)
    return rows, []


def _validate_human_chain(metadata_path: Path, split: str, selected: list[dict], annotator_a_path: Path, annotator_b_path: Path, adjudication_path: Path, freeze_path: Path) -> tuple[dict[str, str], list[str]]:
    errors = []
    annotator_a, errors_a = _label_rows(annotator_a_path, selected)
    annotator_b, errors_b = _label_rows(annotator_b_path, selected)
    errors.extend(errors_a + errors_b)
    adjudication_rows = _read_csv_rows(adjudication_path)
    selected_ids = {row["annotation_id"] for row in selected}
    adjudication = {}
    for row in adjudication_rows:
        key = row.get("annotation_id")
        if not key or key in adjudication:
            errors.append(f"adjudication:duplicate_or_missing:{key}")
            continue
        if key not in selected_ids:
            errors.append(f"adjudication:extra:{key}")
            continue
        adjudication[key] = row
    errors.extend(f"adjudication:missing:{key}" for key in sorted(selected_ids - set(adjudication)))
    metadata_by_id = {row["annotation_id"]: row for row in selected}
    for key in sorted(selected_ids & set(adjudication)):
        meta = metadata_by_id[key]
        a = (annotator_a.get(key, {}).get("label") or "").strip()
        b = (annotator_b.get(key, {}).get("label") or "").strip()
        row = adjudication[key]
        required = ("input_sha256", "label_a", "label_b", "final_label", "adjudicator_id", "adjudicated_at", "reason", "guideline_version")
        if any(not (row.get(field) or "").strip() for field in required):
            errors.append(f"adjudication:missing_provenance:{key}")
        if row.get("input_sha256") != meta.get("input_sha256"):
            errors.append(f"adjudication:input_hash_mismatch:{key}")
        if row.get("label_a", "").strip() != a or row.get("label_b", "").strip() != b:
            errors.append(f"adjudication:original_label_mismatch:{key}")
        if row.get("final_label", "").strip() not in EVALUATION_LABELS:
            errors.append(f"adjudication:invalid_final_label:{key}")
    final_rows = [{"annotation_id": key, "final_label": adjudication[key]["final_label"].strip()} for key in selected_ids if key in adjudication]
    guideline_versions = {adjudication[key].get("guideline_version", "").strip() for key in selected_ids if key in adjudication}
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    manifest_ok = (freeze.get("status") == "frozen" and freeze.get("human_adjudication") is True and freeze.get("adjudication_validated") is True and freeze.get("split") == split and freeze.get("guideline_version") in guideline_versions and freeze.get("metadata_sha256") == _file_sha256(metadata_path) and freeze.get("annotator_a_sha256") == _file_sha256(annotator_a_path) and freeze.get("annotator_b_sha256") == _file_sha256(annotator_b_path) and freeze.get("adjudication_sha256") == _file_sha256(adjudication_path) and freeze.get("gold_labels_sha256") == _stable_gold_hash(final_rows) and freeze.get("adjudication_count") == len(selected) and freeze.get("annotation_count") == len(selected))
    if not manifest_ok:
        errors.append("freeze:provenance_manifest_mismatch")
    return ({key: adjudication[key]["final_label"].strip() for key in selected_ids if key in adjudication}, errors)


def evaluate(metadata_path: Path, split: str, predictions_path: Path | None = None, gold_manifest_path: Path | None = None, annotator_a_path: Path | None = None, annotator_b_path: Path | None = None, adjudication_path: Path | None = None) -> dict:
    rows = [json.loads(line) for line in metadata_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    schema_errors = []
    seen = set()
    required = {"annotation_id", "article_id", "content_group_id", "event_group_id", "input_text", "input_sha256", "split"}
    for row in rows:
        annotation_id = row.get("annotation_id")
        if not annotation_id or annotation_id in seen:
            schema_errors.append({"annotation_id": annotation_id, "error": "duplicate_or_missing_annotation_id"})
        seen.add(annotation_id)
        missing = sorted(required - set(row))
        if missing:
            schema_errors.append({"annotation_id": annotation_id, "missing": missing})
        if row.get("input_sha256") != hashlib.sha256(str(row.get("input_text") or "").encode("utf-8")).hexdigest():
            schema_errors.append({"annotation_id": annotation_id, "error": "input_sha256_mismatch"})
    leakage = split_group_leakage(rows)
    grouping_errors = [{"annotation_id": row.get("annotation_id"), "missing": [field for field in GROUPING_FIELDS if not row.get(field)]} for row in rows if any(not row.get(field) for field in GROUPING_FIELDS)]
    result = {"status": "ok", "row_count": len(rows), "requested_split": split, "schema_errors": schema_errors, "grouping_errors": grouping_errors, "split_group_leakage": leakage, "evaluation_status": "blocked_no_news_gold", "metrics": None}
    if schema_errors or grouping_errors or any(leakage["overlap_counts"].values()):
        result.update({"status": "invalid", "evaluation_status": "blocked_invalid_gold"})
        return result
    selected = [row for row in rows if row.get("split") == split]
    result["selected_count"] = len(selected)
    if not selected:
        result.update({"status": "blocked", "evaluation_status": "blocked_no_selected_rows"})
        return result
    if split == "pilot":
        result.update({"status": "blocked", "evaluation_status": "blocked_pilot_not_evaluation_split"})
        return result
    if not (annotator_a_path and annotator_b_path and adjudication_path and gold_manifest_path):
        result.update({"status": "blocked", "evaluation_status": "blocked_missing_human_provenance"})
        return result
    try:
        final_labels, provenance_errors = _validate_human_chain(metadata_path, split, selected, annotator_a_path, annotator_b_path, adjudication_path, gold_manifest_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError) as exc:
        result.update({"status": "invalid", "evaluation_status": "blocked_invalid_human_provenance", "provenance_errors": [str(exc)]})
        return result
    result["provenance_errors"] = provenance_errors
    if provenance_errors or len(final_labels) != len(selected):
        result.update({"status": "invalid", "evaluation_status": "blocked_invalid_human_provenance"})
        return result
    result["gold_manifest_valid"] = True
    if not predictions_path:
        result.update({"status": "blocked", "evaluation_status": "blocked_no_predictions"})
        return result
    with predictions_path.open(encoding="utf-8", newline="") as handle:
        prediction_rows = list(csv.DictReader(handle))
    prediction_ids = [row.get("annotation_id") for row in prediction_rows]
    duplicate_prediction_ids = sorted({key for key in prediction_ids if key and prediction_ids.count(key) > 1})
    gold_ids = {row["annotation_id"] for row in selected}
    prediction_id_set = set(prediction_ids)
    missing_predictions = sorted(gold_ids - prediction_id_set)
    extra_predictions = sorted(prediction_id_set - gold_ids)
    invalid_predictions = sorted({row.get("prediction") for row in prediction_rows if row.get("prediction") not in EVALUATION_LABELS})
    result.update({"prediction_count": len(prediction_rows), "duplicate_prediction_ids": duplicate_prediction_ids, "missing_prediction_ids": missing_predictions, "extra_prediction_ids": extra_predictions, "invalid_predictions": invalid_predictions})
    if duplicate_prediction_ids or missing_predictions or extra_predictions or invalid_predictions:
        result.update({"status": "invalid", "evaluation_status": "blocked_prediction_contract_mismatch", "gold_manifest_valid": False})
        return result
    if not gold_manifest_path or not gold_manifest_path.exists():
        result.update({"status": "blocked", "evaluation_status": "blocked_missing_frozen_gold_manifest"})
        return result
    prediction_by_id = {row["annotation_id"]: row["prediction"] for row in prediction_rows}
    gold_rows = [{"annotation_id": row["annotation_id"], "final_label": final_labels[row["annotation_id"]]} for row in selected]
    result["metrics"] = classification_metrics([row["final_label"] for row in gold_rows], [prediction_by_id[row["annotation_id"]] for row in gold_rows])
    result["evaluation_status"] = "ok"
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--split", choices=("dev", "test"), required=True)
    parser.add_argument("--predictions", type=Path)
    parser.add_argument("--gold-manifest", type=Path)
    parser.add_argument("--annotator-a", type=Path)
    parser.add_argument("--annotator-b", type=Path)
    parser.add_argument("--adjudication", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(args.metadata, args.split, args.predictions, args.gold_manifest, args.annotator_a, args.annotator_b, args.adjudication)
    result.update({"metadata": str(args.metadata.resolve()), "annotator_a": str(args.annotator_a.resolve()) if args.annotator_a else None, "annotator_b": str(args.annotator_b.resolve()) if args.annotator_b else None, "adjudication": str(args.adjudication.resolve()) if args.adjudication else None, "predictions": str(args.predictions.resolve()) if args.predictions else None, "gold_manifest": str(args.gold_manifest.resolve()) if args.gold_manifest else None, "output": str(args.output.resolve()), "limitations": ["Metrics require the complete packet → annotators → adjudication → frozen manifest chain.", "UIT-VSFC is not used as news evaluation evidence."]})
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0 if result["status"] in {"ok", "blocked"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
