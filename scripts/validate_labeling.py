"""Validate human-label packet integrity without changing packet or labels."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.rss import atomic_write_json


ALLOWED_LABELS = {"", "positive", "negative", "neutral", "needs_context", "mixed", "invalid"}
EVALUATION_LABELS = {"positive", "negative", "neutral"}


def _read_metadata(path: Path) -> dict[str, dict]:
    rows = {}
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            annotation_id = row.get("annotation_id")
            if not annotation_id or annotation_id in rows:
                raise ValueError(f"metadata duplicate or missing annotation_id at line {line_number}")
            expected_hash = hashlib.sha256(str(row.get("input_text") or "").encode("utf-8")).hexdigest()
            if row.get("input_sha256") != expected_hash:
                raise ValueError(f"metadata input hash mismatch for {annotation_id}")
            rows[annotation_id] = row
    return rows


def _read_annotator(path: Path, metadata: dict[str, dict]) -> dict[str, dict]:
    rows = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), 2):
            annotation_id = row.get("annotation_id")
            if not annotation_id or annotation_id in rows:
                raise ValueError(f"{path.name}: duplicate or missing annotation_id at line {line_number}")
            if annotation_id not in metadata:
                raise ValueError(f"{path.name}: unknown annotation_id {annotation_id}")
            label = (row.get("label") or "").strip()
            if label not in ALLOWED_LABELS:
                raise ValueError(f"{path.name}: invalid label {label!r} for {annotation_id}")
            input_text = row.get("input_text")
            if input_text is not None and input_text != metadata[annotation_id].get("input_text"):
                raise ValueError(f"{path.name}: input text mismatch for {annotation_id}")
            if row.get("input_sha256") and row["input_sha256"] != metadata[annotation_id].get("input_sha256"):
                raise ValueError(f"{path.name}: input hash mismatch for {annotation_id}")
            rows[annotation_id] = row
    return rows


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_gold_hash(final_labels: dict[str, str]) -> str:
    payload = "\n".join(json.dumps({"annotation_id": key, "final_label": final_labels[key]}, ensure_ascii=False, sort_keys=True) for key in sorted(final_labels))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_adjudication(path: Path, metadata: dict[str, dict], labels_a: dict[str, str], labels_b: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    rows = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), 2):
            annotation_id = row.get("annotation_id")
            if not annotation_id or annotation_id in rows or annotation_id not in metadata:
                raise ValueError(f"adjudication duplicate, missing or unknown annotation_id at line {line_number}")
            final_label = (row.get("final_label") or "").strip()
            if final_label not in ALLOWED_LABELS - {""}:
                raise ValueError(f"adjudication invalid final_label for {annotation_id}")
            required_fields = ("input_sha256", "label_a", "label_b", "adjudicator_id", "adjudicated_at", "reason", "guideline_version")
            if any(not (row.get(field) or "").strip() for field in required_fields):
                raise ValueError(f"adjudication provenance fields missing for {annotation_id}")
            if row["input_sha256"] != metadata[annotation_id].get("input_sha256"):
                raise ValueError(f"adjudication input hash mismatch for {annotation_id}")
            if row["label_a"].strip() != labels_a.get(annotation_id) or row["label_b"].strip() != labels_b.get(annotation_id):
                raise ValueError(f"adjudication original labels mismatch for {annotation_id}")
            rows[annotation_id] = final_label
    disagreements = [key for key in metadata if labels_a.get(key) and labels_b.get(key) and labels_a[key] != labels_b[key]]
    return rows, disagreements


def validate_packet(metadata_path: Path, annotator_a_path: Path, annotator_b_path: Path, adjudication_path: Path | None = None, freeze_manifest_path: Path | None = None) -> dict:
    metadata = _read_metadata(metadata_path)
    annotator_a = _read_annotator(annotator_a_path, metadata)
    annotator_b = _read_annotator(annotator_b_path, metadata)
    missing_a = sorted(set(metadata) - set(annotator_a))
    missing_b = sorted(set(metadata) - set(annotator_b))
    labels_a = {key: (row.get("label") or "").strip() for key, row in annotator_a.items()}
    labels_b = {key: (row.get("label") or "").strip() for key, row in annotator_b.items()}
    compared = [key for key in metadata if labels_a.get(key) and labels_b.get(key)]
    agreement = sum(labels_a[key] == labels_b[key] for key in compared)
    disagreements = [key for key in compared if labels_a[key] != labels_b[key]]
    invalid = sorted({label for label in list(labels_a.values()) + list(labels_b.values()) if label not in ALLOWED_LABELS})
    annotation_complete = bool(metadata) and not missing_a and not missing_b and not invalid and all(labels_a.values()) and all(labels_b.values())
    result = {
        "status": "ok",
        "metadata_count": len(metadata),
        "annotator_a_count": len(annotator_a),
        "annotator_b_count": len(annotator_b),
        "missing_a": missing_a,
        "missing_b": missing_b,
        "labels_filled_a": sum(bool(value) for value in labels_a.values()),
        "labels_filled_b": sum(bool(value) for value in labels_b.values()),
        "compared_filled_pairs": len(compared),
        "agreement_count_before_adjudication": agreement,
        "agreement_rate_before_adjudication": agreement / len(compared) if compared else None,
        "disagreement_count_before_adjudication": len(disagreements),
        "annotation_complete": bool(annotation_complete),
        "invalid_labels": invalid,
        "gold_ready": False,
        "limitations": ["Agreement is not Cohen kappa and does not replace adjudication.", "Annotator views alone never produce gold_ready; adjudication and a frozen provenance manifest are required."],
    }
    if adjudication_path:
        final_labels, adjudication_disagreements = _read_adjudication(adjudication_path, metadata, labels_a, labels_b)
        missing_final = sorted(set(metadata) - set(final_labels))
        result.update({"adjudication_count": len(final_labels), "missing_adjudication": missing_final, "adjudication_disagreement_count": len(adjudication_disagreements), "final_label_counts": dict(Counter(final_labels.values()))})
        if freeze_manifest_path and not missing_final and annotation_complete and not invalid:
            freeze = json.loads(freeze_manifest_path.read_text(encoding="utf-8"))
            metadata_sha = _file_sha256(metadata_path)
            adjudication_sha = _file_sha256(adjudication_path)
            with adjudication_path.open(encoding="utf-8", newline="") as handle:
                guideline_versions = {row.get("guideline_version", "").strip() for row in csv.DictReader(handle)}
            freeze_ok = (freeze.get("status") == "frozen" and freeze.get("human_adjudication") is True and freeze.get("adjudication_validated") is True and len(guideline_versions) == 1 and freeze.get("guideline_version") in guideline_versions and freeze.get("metadata_sha256") == metadata_sha and freeze.get("annotator_a_sha256") == _file_sha256(annotator_a_path) and freeze.get("annotator_b_sha256") == _file_sha256(annotator_b_path) and freeze.get("adjudication_sha256") == adjudication_sha and freeze.get("gold_labels_sha256") == _stable_gold_hash(final_labels) and freeze.get("adjudication_count") == len(metadata) and freeze.get("annotation_count") == len(metadata))
            result["freeze_valid"] = freeze_ok
            result["gold_ready"] = bool(freeze_ok and all(label in EVALUATION_LABELS for label in final_labels.values()))
        else:
            result["freeze_valid"] = False
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--annotator-a", type=Path, required=True)
    parser.add_argument("--annotator-b", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--adjudication", type=Path)
    parser.add_argument("--freeze-manifest", type=Path)
    args = parser.parse_args()
    if args.freeze_manifest and not args.adjudication:
        parser.error("--freeze-manifest requires --adjudication")
    result = validate_packet(args.metadata, args.annotator_a, args.annotator_b, args.adjudication, args.freeze_manifest)
    result.update({"metadata": str(args.metadata.resolve()), "annotator_a": str(args.annotator_a.resolve()), "annotator_b": str(args.annotator_b.resolve()), "adjudication": str(args.adjudication.resolve()) if args.adjudication else None, "freeze_manifest": str(args.freeze_manifest.resolve()) if args.freeze_manifest else None, "output": str(args.output.resolve())})
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
