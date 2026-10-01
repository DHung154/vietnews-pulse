from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_pipeline.paths import validate_artifact_paths  # noqa: E402
from news_pipeline.normalization import canonicalize_url  # noqa: E402
from news_pipeline.rss import atomic_write_json, fetch_feed, load_sources, parse_feed, write_raw_batch  # noqa: E402
from news_pipeline.writer_lock import single_writer  # noqa: E402


def overall_status(results: list[dict]) -> str:
    success_states = {"ok", "no_new_data", "partial"}
    has_success = any(item["status"] in success_states for item in results)
    has_failure = any(item["status"] in {"failed", "partial"} for item in results)
    return "partial" if has_success and has_failure else ("ok" if has_success else "failed")


def _sleep_gap(base: float, jitter: float, floor: float = 5.0) -> None:
    if base > 0:
        time.sleep(max(floor, base * (1.0 + random.uniform(-jitter, jitter))))


def _content_fingerprint(observation: dict) -> str:
    material = "\n".join(str(observation.get(key) or "") for key in ("title_raw", "description_raw"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _collect(args, sources: dict[str, dict], selected: list[str]) -> int:
    state = json.loads(args.state.read_text(encoding="utf-8")) if args.state.exists() else {}
    legacy_keys = (set(state) & {entry["source_id"] for entry in sources.values()}) - set(sources)
    if legacy_keys:
        raise ValueError("source-keyed legacy state cannot be reused; use the new v2 state path")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    results = []
    for feed_id in selected:
        source = sources[feed_id]
        source_id = source["source_id"]
        if results:
            _sleep_gap(args.request_gap, args.jitter)
        started = datetime.now(timezone.utc)
        previous = state.get(feed_id, {})
        try:
            if previous.get("feed_url") not in (None, source["feed_url"]):
                raise ValueError("feed URL changed under existing feed_id; use a new feed version/state")
            response = fetch_feed(source["feed_url"], previous, timeout=args.timeout, retries=args.retries, jitter=args.jitter)
            now = datetime.now(timezone.utc).isoformat()
            if response["status_code"] == 304:
                state[feed_id] = {**previous, "source_id": source_id, "feed_url": source["feed_url"], "last_attempt_at": now, "last_http_success_at": now, "last_success_at": now, "status": "no_new_data"}
                results.append({"source": source_id, "feed_id": feed_id, "feed_url": source["feed_url"], "status_code": 304, "status": "no_new_data", "records": 0, "new_article_count": 0, "content_change_count": 0, "last_attempt_at": now, "last_http_success_at": now, "last_success_at": now, "last_new_article_at": previous.get("last_new_article_at"), "last_content_change_at": previous.get("last_content_change_at"), "duration_ms": int((datetime.now(timezone.utc) - started).total_seconds() * 1000)})
                continue
            batch_id = f"rss_{feed_id}_{run_id}"
            observations, parse_errors = parse_feed(response["payload"], source, batch_id)
            if not observations:
                raise ValueError("RSS produced zero usable entries; needs review, not empty healthy")
            target = args.raw_root / source_id / feed_id / f"ingest_date={ingest_date}" / f"{batch_id}.jsonl"
            raw = write_raw_batch(observations, target)
            new_state = dict(previous)
            if response.get("etag"):
                new_state["etag"] = response["etag"]
            if response.get("last_modified"):
                new_state["last_modified"] = response["last_modified"]
            current_content = {}
            for observation in observations:
                article_key = canonicalize_url(observation.get("link_raw")) or observation.get("link_raw")
                if article_key:
                    current_content[article_key] = _content_fingerprint(observation)
            previous_content = previous.get("observed_content_by_article", {})
            history = {key: list(value) if isinstance(value, list) else [value] for key, value in previous.get("article_content_history", {}).items()}
            for key, value in previous_content.items():
                history.setdefault(key, [value])
            new_article_count = sum(key not in history for key in current_content)
            content_change_count = sum(key in history and value not in history[key] for key, value in current_content.items())
            for key, value in current_content.items():
                if value not in history.setdefault(key, []):
                    history[key].append(value)
            last_new_article_at = now if new_article_count else previous.get("last_new_article_at")
            last_content_change_at = now if content_change_count else previous.get("last_content_change_at")
            status = "partial" if parse_errors else "ok"
            new_state.update({"source_id": source_id, "feed_url": source["feed_url"], "last_attempt_at": now, "last_http_success_at": now, "status": status, "last_success_batch_id": batch_id, "last_raw_path": raw["raw_path"], "last_success_at": now, "last_new_article_at": last_new_article_at, "last_content_change_at": last_content_change_at, "observed_content_by_article": current_content, "article_content_history": history})
            state[feed_id] = new_state
            results.append({"source": source_id, "feed_id": feed_id, "feed_url": source["feed_url"], "feed_category_raw": source.get("feed_category_raw"), "canonical_category": source.get("canonical_category"), "feed_set_version": source.get("feed_set_version"), "status_code": response["status_code"], "content_type": response.get("content_type"), "status": status, "records": raw["records"], "new_article_count": new_article_count, "content_change_count": content_change_count, "parse_errors": parse_errors, "last_attempt_at": now, "last_http_success_at": now, "last_success_at": now, "last_new_article_at": last_new_article_at, "last_content_change_at": last_content_change_at, "duration_ms": int((datetime.now(timezone.utc) - started).total_seconds() * 1000), **raw})
        except Exception as exc:
            now = datetime.now(timezone.utc).isoformat()
            state[feed_id] = {**previous, "last_attempt_at": now, "status": "failed", "error_type": type(exc).__name__, "error": str(exc)}
            results.append({"source": source_id, "feed_id": feed_id, "feed_url": source["feed_url"], "status_code": getattr(exc, "code", None), "status": "failed", "records": 0, "last_attempt_at": now, "last_http_success_at": previous.get("last_http_success_at"), "last_success_at": previous.get("last_success_at"), "last_new_article_at": previous.get("last_new_article_at"), "duration_ms": int((datetime.now(timezone.utc) - started).total_seconds() * 1000), "error_type": type(exc).__name__, "error": str(exc)})

    manifest = {"status": overall_status(results), "run_id": run_id, "feed_set_version": sorted({source.get("feed_set_version") for source in sources.values() if source.get("feed_set_version")}), "timestamp_utc": datetime.now(timezone.utc).isoformat(), "jitter_fraction": args.jitter, "request_gap_seconds": args.request_gap, "results": results}
    suffix = args.evidence.suffix or ".json"
    immutable_evidence = args.evidence.with_name(f"{args.evidence.stem}_{run_id}{suffix}")
    manifest["immutable_manifest_path"] = str(immutable_evidence.resolve())
    atomic_write_json(immutable_evidence, manifest)
    atomic_write_json(args.state, state)
    atomic_write_json(args.evidence, manifest)
    # ASCII stdout keeps the CLI reliable on Windows consoles with a legacy code page.
    print(json.dumps(manifest, ensure_ascii=True, sort_keys=True))
    return 0 if manifest["status"] in {"ok", "partial"} else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=ROOT / "config/rss_sources.yaml")
    parser.add_argument("--source", action="append")
    parser.add_argument("--feed", action="append")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--request-gap", type=float, default=5)
    parser.add_argument("--jitter", type=float, default=0.2)
    parser.add_argument("--raw-root", type=Path, default=ROOT / "runtime/data_collection_v2/raw/rss")
    parser.add_argument("--state", type=Path, default=ROOT / "runtime/data_collection_v2/state.json")
    parser.add_argument("--lock", type=Path, default=ROOT / "runtime/data_collection_v2/collector.lock")
    parser.add_argument("--evidence", type=Path, default=ROOT / "evidence/d02/collect_manifest.json")
    args = parser.parse_args()
    try:
        validate_artifact_paths(inputs=(args.config,), outputs=(args.raw_root, args.state, args.lock), evidence=args.evidence)
        if not 1 <= args.timeout <= 60 or not 0 <= args.retries <= 2 or args.request_gap < 0 or not 0 <= args.jitter <= 1:
            raise ValueError("timeout 1..60, retries 0..2, request-gap nonnegative and jitter 0..1 required")
        sources = load_sources(args.config)
        unknown_sources = set(args.source or []) - {entry["source_id"] for entry in sources.values()}
        unknown_feeds = set(args.feed or []) - set(sources)
        if unknown_sources or unknown_feeds:
            raise ValueError(f"unknown or disabled source/feed: {sorted(unknown_sources | unknown_feeds)}")
        selected = [key for key, source in sources.items() if (not args.source or source["source_id"] in args.source) and (not args.feed or key in args.feed)]
        if not selected:
            raise ValueError("selection contains no enabled feeds")
        if args.state.exists():
            state_keys = set(json.loads(args.state.read_text(encoding="utf-8")))
            legacy_keys = (state_keys & {entry["source_id"] for entry in sources.values()}) - set(sources)
            if legacy_keys:
                parser.error("source-keyed legacy state cannot be reused; use the new v2 state path")
        with single_writer(args.lock):
            return _collect(args, sources, selected)
    except (ValueError, RuntimeError, OSError, json.JSONDecodeError) as exc:
        print(f"collector failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
