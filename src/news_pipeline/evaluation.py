"""Small, dependency-free checks for news gold provenance and evaluation."""

from __future__ import annotations

from collections import defaultdict


def split_group_leakage(rows: list[dict], fields=("annotation_id", "article_id", "input_sha256", "content_group_id", "event_group_id")) -> dict:
    overlaps = {}
    for field in fields:
        groups = defaultdict(set)
        for row in rows:
            value = row.get(field)
            split = row.get("split")
            if value and split:
                groups[str(value)].add(str(split))
        overlaps[field] = {key: sorted(value) for key, value in groups.items() if len(value) > 1}
    return {"overlap_counts": {field: len(values) for field, values in overlaps.items()}, "overlaps": overlaps}


def classification_metrics(y_true: list[str], y_pred: list[str], labels=("negative", "neutral", "positive")) -> dict:
    if len(y_true) != len(y_pred):
        raise ValueError("y_true and y_pred length mismatch")
    matrix = {(actual, predicted): 0 for actual in labels for predicted in labels}
    for actual, predicted in zip(y_true, y_pred):
        if actual not in labels or predicted not in labels:
            raise ValueError("metrics only accept the three evaluation labels")
        matrix[(actual, predicted)] += 1
    per_class = []
    for label in labels:
        tp = matrix[(label, label)]
        fp = sum(matrix[(other, label)] for other in labels if other != label)
        fn = sum(matrix[(label, other)] for other in labels if other != label)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class.append({"label": label, "support": tp + fn, "precision": precision, "recall": recall, "f1": f1})
    return {"count": len(y_true), "accuracy": sum(matrix[(label, label)] for label in labels) / len(y_true) if y_true else 0.0, "macro_f1": sum(row["f1"] for row in per_class) / len(labels), "per_class": per_class}
