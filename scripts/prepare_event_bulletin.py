"""Build a versioned, read-only event-bulletin artifact from one SQLite snapshot."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.event_bulletin import build_event_groups  # noqa: E402
from news_pipeline.rss import atomic_write_json  # noqa: E402


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_snapshot(root: Path) -> tuple[Path, str, str]:
    pointer = json.loads((root / "current.json").read_text(encoding="utf-8"))
    database = Path(pointer["db_path"])
    if not database.is_absolute():
        database = root / database
    return database, pointer["snapshot_id"], pointer.get("model_version", "unavailable")


def _read_snapshot(connection: sqlite3.Connection, dataset_id: str, snapshot_id: str) -> tuple[list[dict], tuple, str]:
    connection.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in connection.execute(
            "SELECT article_id, source, feed_id, category, title, description, canonical_url, "
            "published_at, event_time, event_date, sentiment_label "
            "FROM articles WHERE dataset_id = ? ORDER BY event_time, article_id",
            (dataset_id,),
        )
    ]
    meta_columns = {row[1] for row in connection.execute("PRAGMA table_info(snapshot_meta)")}
    if "coverage_status" in meta_columns:
        meta = connection.execute(
            "SELECT window_previous_start, window_current_start, window_end, coverage_status "
            "FROM snapshot_meta WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
    else:
        meta = None
    if meta:
        window_bounds = tuple(meta[:3])
        coverage_status = meta[3]
    else:
        window_bounds = (None, None, None)
        coverage_status = "unknown"
    return rows, window_bounds, coverage_status


def prepare(
    serving_root: Path,
    dataset_id: str,
    output_dir: Path,
    max_gap_hours: int,
    min_similarity: float,
) -> dict:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing to overwrite event-bulletin output: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    database, snapshot_id, model_version = _resolve_snapshot(serving_root)
    if not database.is_file():
        raise FileNotFoundError(database)
    connection = sqlite3.connect(database)
    try:
        rows, window_bounds, coverage_status = _read_snapshot(connection, dataset_id, snapshot_id)
    finally:
        connection.close()
    groups, review_pairs, context = build_event_groups(
        rows,
        max_gap_hours=max_gap_hours,
        min_similarity=min_similarity,
    )
    review_path = output_dir / "event_group_review.csv"
    review_fields = [
        "article_id_a",
        "article_id_b",
        "similarity",
        "title_shared_tokens",
        "shared_tokens",
        "title_jaccard",
        "text_jaccard",
        "time_gap_hours",
        "candidate_reason",
        "title_a",
        "description_a",
        "source_a",
        "event_time_a",
        "input_sha256_a",
        "title_b",
        "description_b",
        "source_b",
        "event_time_b",
        "input_sha256_b",
        "review_decision",
        "reviewer",
        "review_reason",
    ]
    with review_path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=review_fields)
        writer.writeheader()
        writer.writerows(review_pairs)

    generated_at = datetime.now(timezone.utc).isoformat()
    artifact = {
        "format_version": "event_bulletin_v1",
        "status": "pending_manual_event_review",
        "dataset_id": dataset_id,
        "snapshot_id": snapshot_id,
        "model_version": model_version,
        "generated_at": generated_at,
        "window_bounds": window_bounds,
        "coverage_status": coverage_status,
        "algorithm": {
            "max_gap_hours": max_gap_hours,
            "min_similarity": min_similarity,
            "grouping": "signal-token title/description similarity with time horizon",
        },
        "groups": groups,
        "article_to_event": context["article_to_event"],
        "limitations": [
            "Groups are algorithmic candidates and have not been manually accepted.",
            "Singleton groups are pool-local evidence, not proof that no related article exists outside this snapshot.",
            "Coverage status is inherited from the serving snapshot; incomplete coverage hides increase/decrease conclusions.",
            "Sentiment is retained only as a supplementary model field in the dashboard detail view.",
        ],
    }
    artifact_path = output_dir / "event_bulletin.json"
    atomic_write_json(artifact_path, artifact)
    audit = {
        "status": "pending_manual_event_review",
        "input_database": str(database.resolve()),
        "input_database_sha256": _sha256(database),
        "snapshot_id": snapshot_id,
        "dataset_id": dataset_id,
        "window_bounds": window_bounds,
        "coverage_status": coverage_status,
        "artifact": str(artifact_path.resolve()),
        "review_csv": str(review_path.resolve()),
        "audit": context["audit"],
        "parameters": {
            "max_gap_hours": max_gap_hours,
            "min_similarity": min_similarity,
        },
        "generated_at": generated_at,
        "human_review_required": True,
    }
    atomic_write_json(output_dir / "manifest.json", audit)
    atomic_write_json(output_dir / "audit.json", context["audit"])
    return audit


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serving-root", type=Path, required=True)
    parser.add_argument("--dataset", default="rss_live_v2")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-gap-hours", type=int, default=48)
    parser.add_argument("--min-similarity", type=float, default=0.52)
    args = parser.parse_args()
    if args.max_gap_hours <= 0 or not 0 <= args.min_similarity <= 1:
        parser.error("max-gap-hours must be positive and min-similarity must be between 0 and 1")
    result = prepare(
        args.serving_root,
        args.dataset,
        args.output,
        args.max_gap_hours,
        args.min_similarity,
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
