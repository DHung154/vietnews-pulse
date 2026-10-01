"""Small SQLite serving snapshot exporter; Spark remains the corpus processor."""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame

from .rss import atomic_write_json


def _insert_batches(connection: sqlite3.Connection, rows: Iterable, sql: str, batch_size: int = 500) -> int:
    batch = []
    count = 0
    for row in rows:
        batch.append(tuple(row))
        if len(batch) == batch_size:
            connection.executemany(sql, batch)
            count += len(batch)
            batch.clear()
    if batch:
        connection.executemany(sql, batch)
        count += len(batch)
    return count


def _source_inventory(path: Path | None) -> list[dict]:
    if not path or not path.exists():
        return []
    return [item for item in json.loads(path.read_text(encoding="utf-8")) if item.get("enabled", True)]


def validate_provenance(snapshot_id: str, model_version: str, snapshot_ids: list[str], model_versions: list[str], score_manifest: dict, enriched_count: int) -> None:
    if score_manifest.get("status") != "ok":
        raise ValueError("score manifest is not successful")
    if snapshot_ids != [snapshot_id]:
        raise ValueError(f"enriched snapshot provenance mismatch: expected {[snapshot_id]!r}, got {snapshot_ids!r}")
    if model_versions != [model_version]:
        raise ValueError(f"enriched model provenance mismatch: expected {[model_version]!r}, got {model_versions!r}")
    if score_manifest.get("input_snapshot_ids") != [snapshot_id] or score_manifest.get("model_versions") != [model_version]:
        raise ValueError("score manifest provenance mismatch")
    if score_manifest.get("enriched_count") != enriched_count:
        raise ValueError("score manifest enriched_count mismatch")


def validate_gold_provenance(gold_root: Path, score_manifest: dict, required_tables: tuple[str, ...]) -> None:
    expected_path = score_manifest.get("gold_path")
    if not expected_path or Path(expected_path).resolve() != gold_root.resolve():
        raise ValueError("gold path does not match score manifest")
    expected_counts = score_manifest.get("gold_counts")
    if not isinstance(expected_counts, dict) or set(expected_counts) != set(required_tables):
        raise ValueError("score manifest gold_counts is incomplete")
    if any(not isinstance(expected_counts[name], int) or expected_counts[name] < 0 for name in required_tables):
        raise ValueError("score manifest gold_counts contains invalid values")
    bounds = score_manifest.get("window_bounds")
    if not isinstance(bounds, dict) or set(bounds) != {"previous_start", "current_start", "window_end"} or any(not bounds[name] for name in bounds):
        raise ValueError("score manifest window_bounds is incomplete")


def export_snapshot(enriched: DataFrame, gold_root: Path, output_root: Path, snapshot_id: str, model_version: str, evidence_path: Path, source_manifest_path: Path | None = None, score_manifest_path: Path | None = None, source_config_path: Path | None = None, coverage_manifest_path: Path | None = None) -> dict:
    if not score_manifest_path or not score_manifest_path.exists():
        raise ValueError("score manifest is required for semantic publish validation")
    score_manifest = json.loads(score_manifest_path.read_text(encoding="utf-8"))
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    db_path = output_root / f"{snapshot_id}.sqlite"
    if db_path.exists():
        raise FileExistsError(f"refusing to overwrite serving database: {db_path}")
    temporary = output_root / f".{db_path.name}.{uuid.uuid4().hex}.inprogress"
    connection = sqlite3.connect(temporary)
    try:
        connection.executescript(
            """
            CREATE TABLE snapshot_meta (snapshot_id TEXT PRIMARY KEY, published_at TEXT NOT NULL, model_version TEXT, window_previous_start TEXT NOT NULL, window_current_start TEXT NOT NULL, window_end TEXT NOT NULL, coverage_status TEXT NOT NULL, coverage_manifest_path TEXT);
            CREATE TABLE snapshot_datasets (snapshot_id TEXT NOT NULL, dataset_id TEXT NOT NULL, article_count INTEGER NOT NULL, PRIMARY KEY (snapshot_id, dataset_id));
            CREATE TABLE articles (dataset_id TEXT NOT NULL, article_id TEXT NOT NULL, source TEXT, feed_id TEXT, category TEXT, title TEXT, description TEXT, clean_text TEXT, canonical_url TEXT, published_at TEXT, event_time TEXT, event_date TEXT, sentiment_label TEXT, prob_negative REAL, prob_neutral REAL, prob_positive REAL, predicted_class_probability REAL, model_version TEXT, PRIMARY KEY (dataset_id, article_id));
            CREATE INDEX articles_event_idx ON articles(dataset_id, event_date);
            CREATE INDEX articles_source_idx ON articles(source);
            CREATE INDEX articles_category_idx ON articles(category);
            CREATE INDEX articles_sentiment_idx ON articles(sentiment_label);
            CREATE TABLE volume_6h (dataset_id TEXT, source TEXT, category TEXT, window_start TEXT, window_end TEXT, count INTEGER);
            CREATE TABLE sentiment_daily (dataset_id TEXT, event_date TEXT, source TEXT, category TEXT, sentiment_label TEXT, count INTEGER);
            CREATE TABLE sentiment_summary (dataset_id TEXT, source TEXT, category TEXT, article_count INTEGER, predicted_count INTEGER, prediction_coverage REAL);
            CREATE TABLE category_trends (dataset_id TEXT, source TEXT, category TEXT, previous_count INTEGER, current_count INTEGER, delta INTEGER, growth_ratio REAL, growth_percent REAL, status TEXT, eligible INTEGER);
            CREATE TABLE keywords_window (dataset_id TEXT, token TEXT, document_frequency INTEGER);
            CREATE TABLE source_health (dataset_id TEXT NOT NULL, source TEXT NOT NULL, feed_id TEXT NOT NULL, status TEXT, last_attempt_at TEXT, last_success_at TEXT, latest_event_date TEXT, article_count INTEGER, PRIMARY KEY (dataset_id, source, feed_id));
            """
        )
        required = {"snapshot_id", "dataset_id", "article_id", "source", "category", "event_time", "event_date", "model_version"}
        missing = required - set(enriched.columns)
        if missing:
            raise ValueError(f"enriched contract missing columns: {sorted(missing)}")
        snapshot_ids = [row[0] for row in enriched.select("snapshot_id").distinct().collect()]
        actual_model_versions = [row[0] for row in enriched.select("model_version").distinct().collect()]
        article_columns = ["dataset_id", "article_id", "source", "feed_id", "category", "title", "description", "clean_text", "canonical_url", "published_at", "event_time", "event_date", "sentiment_label", "prob_negative", "prob_neutral", "prob_positive", "predicted_class_probability", "model_version"]
        article_count = _insert_batches(connection, enriched.select(*article_columns).toLocalIterator(), "INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        validate_provenance(snapshot_id, model_version, snapshot_ids, actual_model_versions, score_manifest, article_count)
        gold_specs = (
            ("volume_6h", ["dataset_id", "source", "category", "window_start", "window_end", "count"], "INSERT INTO volume_6h VALUES (?,?,?,?,?,?)"),
            ("sentiment_daily", ["dataset_id", "event_date", "source", "category", "sentiment_label", "count"], "INSERT INTO sentiment_daily VALUES (?,?,?,?,?,?)"),
            ("sentiment_summary", ["dataset_id", "source", "category", "article_count", "predicted_count", "prediction_coverage"], "INSERT INTO sentiment_summary VALUES (?,?,?,?,?,?)"),
            ("category_trends", ["dataset_id", "source", "category", "previous_count", "current_count", "delta", "growth_ratio", "growth_percent", "status", "eligible"], "INSERT INTO category_trends VALUES (?,?,?,?,?,?,?,?,?,?)"),
            ("keywords_window", ["dataset_id", "token", "document_frequency"], "INSERT INTO keywords_window VALUES (?,?,?)"),
        )
        required_gold_tables = tuple(name for name, _, _ in gold_specs)
        validate_gold_provenance(gold_root, score_manifest, required_gold_tables)
        for name, columns, sql in gold_specs:
            path = gold_root / name
            if not path.exists():
                raise FileNotFoundError(f"required gold table is missing: {path}")
            _insert_batches(connection, enriched.sparkSession.read.parquet(str(path)).select(*columns).toLocalIterator(), sql)
        counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("articles",) + required_gold_tables}
        expected_counts = score_manifest["gold_counts"]
        if any(counts[name] != expected_counts[name] for name in required_gold_tables):
            raise ValueError(f"gold count mismatch: {counts}")
        bounds = score_manifest["window_bounds"]
        coverage_status = "unknown"
        if coverage_manifest_path and coverage_manifest_path.exists():
            coverage_payload = json.loads(coverage_manifest_path.read_text(encoding="utf-8"))
            coverage_status = str(coverage_payload.get("coverage_status") or "unknown")
        connection.execute("INSERT INTO snapshot_meta VALUES (?,?,?,?,?,?,?,?)", (snapshot_id, datetime_now(), model_version, bounds["previous_start"], bounds["current_start"], bounds["window_end"], coverage_status, str(coverage_manifest_path.resolve()) if coverage_manifest_path else None))
        dataset_counts = {row[0]: int(row[1]) for row in enriched.groupBy("dataset_id").count().collect()}
        for dataset_id, count in dataset_counts.items():
            connection.execute("INSERT INTO snapshot_datasets VALUES (?,?,?)", (snapshot_id, dataset_id, count))
        source_results = {}
        manifest_time = None
        if source_manifest_path and source_manifest_path.exists():
            payload = json.loads(source_manifest_path.read_text(encoding="utf-8"))
            manifest_time = payload.get("timestamp_utc")
            source_results = {(item.get("feed_id") or item["source"]): item for item in payload.get("results", [])}
        article_sources = {(row[0], row[1], row[2], row[3], int(row[4])) for row in connection.execute("SELECT dataset_id, source, COALESCE(feed_id, source), MAX(event_date), COUNT(*) FROM articles GROUP BY dataset_id, source, COALESCE(feed_id, source)")}
        for dataset_id in dataset_counts:
            inventory = _source_inventory(source_config_path) if dataset_id in {"rss_live", "rss_live_v2"} else []
            known_feeds = {(item["source_id"], item.get("feed_id") or item["source_id"]) for item in inventory}
            known_feeds.update((row[1], row[2]) for row in article_sources if row[0] == dataset_id)
            for source, feed_id in sorted(known_feeds):
                result = source_results.get(feed_id, source_results.get(source, {}))
                status = "ok" if result.get("status") in {"ok", "no_new_data"} else result.get("status", "unavailable")
                article_row = next((row for row in article_sources if row[0] == dataset_id and row[1] == source and row[2] == feed_id), None)
                connection.execute("INSERT INTO source_health VALUES (?,?,?,?,?,?,?,?)", (dataset_id, source, feed_id, status, result.get("last_attempt_at", manifest_time), result.get("last_success_at"), article_row[3] if article_row else None, article_row[4] if article_row else 0))
        connection.commit()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"sqlite integrity_check={integrity}")
        table_counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("articles", "snapshot_datasets", "volume_6h", "sentiment_daily", "sentiment_summary", "category_trends", "keywords_window", "source_health")}
        connection.close()
        os.replace(temporary, db_path)
        pointer = output_root / "current.json"
        atomic_write_json(pointer, {"snapshot_id": snapshot_id, "db_path": db_path.name, "model_version": model_version})
        manifest = {"status": "ok", "snapshot_id": snapshot_id, "db_path": str(db_path), "current_pointer": str(pointer), "article_count": article_count, "table_counts": table_counts, "integrity_check": integrity, "model_version": model_version, "coverage_status": coverage_status, "coverage_manifest_path": str(coverage_manifest_path.resolve()) if coverage_manifest_path else None}
        atomic_write_json(evidence_path, manifest)
        return manifest
    except Exception:
        connection.close()
        temporary.unlink(missing_ok=True)
        raise


def datetime_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
