"""Coverage profiling for immutable RSS poll manifests."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from typing import Iterable
from zoneinfo import ZoneInfo


SUCCESS_CODES = {200, 304}


def _local_day(value: str, timezone_name: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("manifest timestamp must include timezone")
    return parsed.astimezone(ZoneInfo(timezone_name)).date().isoformat()


def coverage_gate(feed_coverage: Iterable[dict], required_days: int = 14, min_success_rate: float = 0.95) -> dict:
    rows = list(feed_coverage)
    rates = [row["success_rate"] for row in rows if row["expected_poll_slots"]]
    observed_days = min((row["observed_days"] for row in rows), default=0)
    status = "ok" if rows and observed_days >= required_days and all(rate >= min_success_rate for rate in rates) else "incomplete"
    return {
        "coverage_status": status,
        "required_days": required_days,
        "min_success_rate": min_success_rate,
        "observed_days": observed_days,
        "min_observed_days": min((row["observed_days"] for row in rows), default=0),
        "min_success_rate_observed": min(rates) if rates else None,
    }


def _manifest_key(manifest: dict) -> str:
    run_id = manifest.get("run_id")
    if run_id:
        return str(run_id)
    return hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _slot(timestamp: str, poll_seconds: int) -> int:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("manifest timestamp must include timezone")
    return int(parsed.timestamp()) // poll_seconds


def _cadence_summary(manifests: Iterable[dict], sources: dict[str, dict]) -> dict:
    """Summarize collector run cadence without treating it as coverage."""
    timestamps = []
    for manifest in manifests:
        value = manifest.get("timestamp_utc")
        if value:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            timestamps.append(parsed.astimezone(timezone.utc))
    timestamps = sorted(set(timestamps))
    intervals = [int((right - left).total_seconds()) for left, right in zip(timestamps, timestamps[1:])]
    configured = [int(item.get("poll_seconds") or 600) for item in sources.values()]
    expected = min(configured) if configured else 600
    gap_threshold = max(900, int(expected * 1.5))
    return {
        "run_count": len(timestamps),
        "first_run_utc": timestamps[0].isoformat() if timestamps else None,
        "last_run_utc": timestamps[-1].isoformat() if timestamps else None,
        "expected_interval_seconds": expected,
        "gap_threshold_seconds": gap_threshold,
        "interval_count": len(intervals),
        "min_interval_seconds": min(intervals) if intervals else None,
        "max_interval_seconds": max(intervals) if intervals else None,
        "median_interval_seconds": sorted(intervals)[len(intervals) // 2] if intervals else None,
        "gaps_over_threshold": sum(interval > gap_threshold for interval in intervals),
        "intervals_seconds": intervals,
    }


def profile_manifests(manifests: Iterable[dict], sources: dict[str, dict], required_days: int = 14, min_success_rate: float = 0.95, end_local: str | None = None) -> dict:
    manifest_list = list(manifests)
    unique_manifests = {}
    for manifest in manifest_list:
        unique_manifests.setdefault(_manifest_key(manifest), manifest)
    polls = []
    for manifest in unique_manifests.values():
        for result in manifest.get("results", []):
            feed_id = result.get("feed_id")
            if feed_id not in sources:
                continue
            timestamp = result.get("last_attempt_at") or manifest.get("timestamp_utc")
            if not timestamp:
                continue
            day = _local_day(timestamp, sources[feed_id].get("timezone", "Asia/Ho_Chi_Minh"))
            polls.append((manifest, result, day, timestamp))
    all_days = sorted({day for _, _, day, _ in polls})
    expected_end = date.fromisoformat(end_local) if end_local else (date.fromisoformat(all_days[-1]) if all_days else None)
    expected_start = expected_end - timedelta(days=required_days - 1) if expected_end else None
    windowed_polls = [(manifest, result, day, timestamp) for manifest, result, day, timestamp in polls if expected_start and expected_start.isoformat() <= day <= expected_end.isoformat()]
    feed_rows = []
    category_counts = Counter()
    source_counts = defaultdict(lambda: {"polls": 0, "successful_polls": 0, "records": 0})
    by_feed_slot = defaultdict(list)
    for manifest, result, day, timestamp in windowed_polls:
        feed_id = result["feed_id"]
        interval = int(sources[feed_id].get("poll_seconds") or 600)
        by_feed_slot[(feed_id, _slot(timestamp, interval))].append((manifest, result, day))
    slot_rows = []
    for (feed_id, _), values in by_feed_slot.items():
        manifest, result, day = values[0]
        successful = any(item.get("status_code") in SUCCESS_CODES for _, item, _ in values)
        records = max(int(item.get("records") or 0) for _, item, _ in values)
        category = result.get("canonical_category") or sources[feed_id].get("canonical_category") or "other"
        slot_rows.append((feed_id, day, successful, records, category, manifest))
        category_counts[(day, category)] += records
        source = sources[feed_id]["source_id"]
        source_counts[source]["polls"] += 1
        source_counts[source]["records"] += records
        if successful:
            source_counts[source]["successful_polls"] += 1
    if expected_start and expected_end:
        expected_days = (expected_end - expected_start).days + 1
        for feed_id, source in sources.items():
            expected_slots = sum(max(1, round(86400 / int(source.get("poll_seconds") or 600))) for _ in range(expected_days))
            actual = [row for row in slot_rows if row[0] == feed_id]
            successful = sum(row[2] for row in actual)
            observed = {row[1] for row in actual}
            feed_rows.append({"feed_id": feed_id, "source_id": source["source_id"], "expected_poll_slots": expected_slots, "observed_poll_slots": len(actual), "successful_poll_slots": successful, "success_rate": successful / expected_slots if expected_slots else None, "observed_days": len(observed), "records": sum(row[3] for row in actual)})
    gate = coverage_gate(feed_rows, required_days, min_success_rate)
    source_rows = []
    for source_id, counts in sorted(source_counts.items()):
        source_rows.append({"source_id": source_id, **counts, "success_rate_observed": counts["successful_polls"] / counts["polls"] if counts["polls"] else None})
    versions = {tuple(manifest.get("feed_set_version", [])) for manifest, _, _, _ in windowed_polls if manifest.get("feed_set_version")}
    return {
        **gate,
        "window_start_local": expected_start.isoformat() if expected_start else None,
        "window_end_local": expected_end.isoformat() if expected_end else None,
        "window_end_basis": "explicit_end_local" if end_local else "latest_observed_day",
        "manifest_count_input": len(manifest_list),
        "manifest_count_unique": len(unique_manifests),
        "feed_coverage": feed_rows,
        "source_coverage_observed": source_rows,
        "category_counts_by_local_day": [{"day": day, "category": category, "records": count} for (day, category), count in sorted(category_counts.items())],
        "feed_set_versions": [list(version) for version in sorted(versions)],
        "cadence": _cadence_summary(unique_manifests.values(), sources),
        "limitations": ["A window is incomplete until every configured feed reaches the required days and poll success threshold.", "RSS coverage is coverage of observed feeds, not all publisher articles."] if gate["coverage_status"] == "incomplete" else [],
    }
