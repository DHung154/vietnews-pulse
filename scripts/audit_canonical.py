"""Streaming Python audit for historical canonicalization; not a Spark snapshot."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.normalization import normalize_record  # noqa: E402


def audit(input_path: Path, output_root: Path) -> dict:
    if output_root.exists():
        raise FileExistsError(f"refusing to overwrite audit output: {output_root}")
    output_root.mkdir(parents=True)
    database = sqlite3.connect(output_root / "canonical_keys.sqlite")
    database.execute("CREATE TABLE canonical (dataset_id TEXT, article_id TEXT, source TEXT, category TEXT, category_basis TEXT, published_at TEXT, event_date TEXT, content_hash TEXT, crawled_at TEXT, first_seen_at TEXT, last_seen_at TEXT, observations INTEGER, PRIMARY KEY(dataset_id, article_id))")
    counts = Counter()
    sources = Counter()
    categories = Counter()
    category_bases = Counter()
    started = time.monotonic()
    digest = hashlib.sha256()
    with input_path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            digest.update(raw_line)
            counts["raw_lines"] += 1
            if not raw_line.strip():
                counts["blank_lines"] += 1
                continue
            try:
                raw = json.loads(raw_line)
                row = normalize_record(raw)
            except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
                counts["invalid_json_or_record"] += 1
                continue
            if row["valid"]:
                counts["valid"] += 1
                sources[row["source"]] += 1
                categories[row["category"]] += 1
                category_bases[row["category_basis"]] += 1
                key = (row["dataset_id"], row["article_id"])
                existing = database.execute("SELECT observations, last_seen_at FROM canonical WHERE dataset_id=? AND article_id=?", key).fetchone()
                if existing:
                    counts["duplicate_observations"] += 1
                    observations = int(existing[0]) + 1
                    latest = row["last_seen_at"] or ""
                    old_latest = existing[1] or ""
                    if latest >= old_latest:
                        database.execute("UPDATE canonical SET source=?, category=?, category_basis=?, published_at=?, event_date=?, content_hash=?, crawled_at=?, last_seen_at=?, observations=? WHERE dataset_id=? AND article_id=?", (row["source"], row["category"], row["category_basis"], row["published_at"], row["event_date"], row["text_hash"], row["first_seen_at"], row["last_seen_at"], observations, key[0], key[1]))
                    else:
                        database.execute("UPDATE canonical SET observations=? WHERE dataset_id=? AND article_id=?", (observations, key[0], key[1]))
                else:
                    database.execute("INSERT INTO canonical VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (row["dataset_id"], row["article_id"], row["source"], row["category"], row["category_basis"], row["published_at"], row["event_date"], row["text_hash"], row["first_seen_at"], row["first_seen_at"], row["last_seen_at"], 1))
            else:
                counts["invalid"] += 1
            counts["missing_published_at"] += row["published_at"] is None
            counts["description_null"] += row["description"] is None
            counts["body_present"] += bool(row["body_text"])
            if line_number % 10000 == 0:
                database.commit()
    database.commit()
    canonical_count = database.execute("SELECT COUNT(*) FROM canonical").fetchone()[0]
    duplicate_groups = database.execute("SELECT COUNT(*) FROM canonical WHERE observations > 1").fetchone()[0]
    counts["canonical"] = canonical_count
    counts["duplicate_groups"] = duplicate_groups
    sample_path = output_root / "audit_sample_200.jsonl"
    with sample_path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in database.execute("SELECT dataset_id, article_id, source, category, category_basis, published_at, event_date, content_hash, observations FROM canonical ORDER BY article_id LIMIT 200"):
            handle.write(json.dumps({"dataset_id": row[0], "article_id": row[1], "source": row[2], "category": row[3], "category_basis": row[4], "published_at": row[5], "event_date": row[6], "content_hash": row[7], "observations": row[8], "manual_audit_status": "pending"}, ensure_ascii=False, sort_keys=True) + "\n")
    result = {"status": "ok", "mode": "python_offline_audit", "input": str(input_path.resolve()), "output": str(output_root.resolve()), "bytes": input_path.stat().st_size, "sha256": digest.hexdigest(), "counts": dict(counts), "source_counts_raw_valid": dict(sources), "category_counts_raw_valid": dict(categories), "category_basis_counts_raw_valid": dict(category_bases), "audit_sample": str(sample_path.resolve()), "elapsed_seconds": round(time.monotonic() - started, 3), "checked_at": datetime.now(timezone.utc).isoformat(), "limitations": ["This is a Python offline audit, not Spark/HDFS evidence.", "The 200-row sample is prepared for manual audit; manual audit has not been performed.", "Canonical representative uses latest crawled_at; no claim of published-time ordering because historical published_at is null."]}
    (output_root / "audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    database.close()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    audit(args.input, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
