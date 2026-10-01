"""Explicit-mapping JSONL history importer; it never guesses a dataset schema."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .rss import atomic_write_json, write_raw_batch


REQUIRED_MAPPING = {"title", "link"}


def import_jsonl(input_path: Path, mapping_path: Path, output: Path, dataset_id: str) -> dict[str, Any]:
    if not dataset_id.startswith("historical_"):
        raise ValueError("history imports require a historical_ dataset_id")
    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    missing = REQUIRED_MAPPING - set(mapping)
    if missing:
        raise ValueError(f"mapping must explicitly define: {sorted(missing)}")
    rejected = []
    records_seen = 0
    batch_id = f"history_{dataset_id}"
    def observations():
        nonlocal records_seen
        with input_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                records_seen += 1
                record = json.loads(line)
                title = record.get(mapping["title"])
                link = record.get(mapping["link"])
                if not title or not link:
                    rejected.append({"line_number": line_number, "reason": "missing_title_or_link"})
                yield {
                    "schema_version": "2", "observation_id": f"{batch_id}:{line_number}", "batch_id": batch_id, "dataset_id": dataset_id,
                    "source": record.get(mapping.get("source", "")) if mapping.get("source") else "historical", "feed_url": None,
                    "source_record_id": str(record.get(mapping.get("source_record_id", ""), line_number)) if mapping.get("source_record_id") else str(line_number),
                    "title_raw": title, "description_raw": record.get(mapping.get("description", "")) if mapping.get("description") else None,
                    "body_raw": record.get(mapping.get("body_text", "")) if mapping.get("body_text") else None,
                    "source_crawled_at_raw": record.get(mapping.get("source_crawled_at", "")) if mapping.get("source_crawled_at") else None,
                    "link_raw": link, "category_raw": record.get(mapping.get("category", "")) if mapping.get("category") else None,
                    "published_at_raw": record.get(mapping.get("published_at", "")) if mapping.get("published_at") else None,
                    "crawled_at": datetime.now(timezone.utc).isoformat(), "raw_payload": json.dumps(record, ensure_ascii=False, sort_keys=True),
                }
    result = write_raw_batch(observations(), output)
    result.update({"records_seen": records_seen, "accepted": records_seen - len(rejected), "rejected": len(rejected), "rejections": rejected})
    return result
