"""Small, auditable RSS collector using only the Python standard library."""

from __future__ import annotations

import email.utils
import hashlib
import json
import os
import random
import time
import uuid
import xml.etree.ElementTree as ET
from http.client import IncompleteRead
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


USER_AGENT = "news-pipeline-m02/0.1 (+RSS collector; contact project owner)"
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


def load_sources(path: Path) -> dict[str, dict[str, Any]]:
    """Read JSON-compatible YAML so PyYAML is not needed at runtime."""
    entries = json.loads(path.read_text(encoding="utf-8"))
    sources = {}
    for entry in entries:
        feed_id = entry.get("feed_id")
        if not isinstance(feed_id, str) or not feed_id.strip():
            raise ValueError("every feed requires an explicit feed_id; migrate source-keyed configs/state")
        if feed_id in sources:
            raise ValueError(f"duplicate feed_id: {feed_id}")
        sources[feed_id] = entry
    return {key: entry for key, entry in sources.items() if entry.get("enabled", True)}


def parse_published_at(value: str | None, source_timezone: str) -> datetime | None:
    if not value or not value.strip():
        return None
    raw = " ".join(value.replace("\u202f", " ").split())
    parsed: datetime | None = None
    try:
        parsed = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(source_timezone))
    return parsed.astimezone(timezone.utc)


def _text(parent: ET.Element, name: str) -> str | None:
    child = parent.find(name)
    return child.text if child is not None and child.text is not None else None


def parse_feed(payload: bytes, source: dict[str, Any], batch_id: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    root = ET.fromstring(payload)
    if root.tag != "rss" or root.find("channel") is None:
        raise ValueError("response is not an RSS channel")
    entries = root.findall("./channel/item")
    observations: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for index, entry in enumerate(entries):
        raw_entry = ET.tostring(entry, encoding="unicode")
        title = _text(entry, "title")
        link = _text(entry, "link")
        guid = _text(entry, "guid")
        published = _text(entry, "pubDate")
        try:
            published_at = parse_published_at(published, source["timezone"])
        except (KeyError, ZoneInfoNotFoundError):
            published_at = None
        if not title or not link:
            errors.append({"entry_index": str(index), "reason": "missing_title_or_link"})
            continue
        source_record_id = guid or link or str(index)
        feed_id = source.get("feed_id") or source["feed_url"]
        observation_id = hashlib.sha256(f"{batch_id}|{feed_id}|{index}".encode()).hexdigest()
        observations.append(
            {
                "schema_version": "2",
                "observation_id": observation_id,
                "batch_id": batch_id,
                "dataset_id": source.get("dataset_id", "rss_live"),
                "source": source["source_id"],
                "feed_id": feed_id,
                "feed_category_raw": source.get("feed_category_raw"),
                "feed_canonical_category": source.get("canonical_category"),
                "category_mapping_version": source.get("category_mapping_version"),
                "feed_set_version": source.get("feed_set_version"),
                "feed_url": source["feed_url"],
                "source_record_id": source_record_id,
                "title_raw": title,
                "description_raw": _text(entry, "description"),
                "link_raw": link,
                "category_raw": _text(entry, "category"),
                "published_at_raw": published,
                "crawled_at": datetime.now(timezone.utc).isoformat(),
                "raw_payload": raw_entry,
                "published_at_parse_ok": published_at is not None if published else True,
            }
        )
    return observations, errors


def _retry_after(headers: Any) -> float:
    value = headers.get("Retry-After")
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        try:
            deadline = email.utils.parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            return max(0.0, (deadline.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def _sleep_backoff(base: float, jitter: float, floor: float = 5.0) -> None:
    base = max(0.0, base)
    delay = max(floor, base, base * (1.0 + random.uniform(-jitter, jitter)))
    time.sleep(delay)


def fetch_feed(url: str, state: dict[str, Any], timeout: int = 20, retries: int = 2, jitter: float = 0.2) -> dict[str, Any]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"}
    for key, header in (("etag", "If-None-Match"), ("last_modified", "If-Modified-Since")):
        if state.get(key):
            headers[header] = state[key]
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=timeout) as response:
                return {
                    "status_code": response.status,
                    "payload": response.read(),
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "content_type": response.headers.get("Content-Type"),
                    "final_url": response.geturl() if hasattr(response, "geturl") else url,
                }
        except HTTPError as exc:
            if exc.code == 304:
                return {"status_code": 304, "payload": b"", "etag": state.get("etag"), "last_modified": state.get("last_modified")}
            if exc.code not in RETRYABLE_STATUS or attempt >= retries:
                raise
            last_error = exc
            _sleep_backoff(_retry_after(exc.headers) or 2**attempt, jitter)
        except (TimeoutError, URLError, IncompleteRead, ConnectionError) as exc:
            if attempt >= retries:
                raise
            last_error = exc
            _sleep_backoff(2**attempt, jitter)
    raise RuntimeError(f"fetch failed after retries: {last_error}")


def write_raw_batch(observations: Iterable[dict[str, Any]], target: Path) -> dict[str, Any]:
    target = target.resolve()
    if target.exists():
        raise FileExistsError(f"refusing to overwrite raw batch: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        records = 0
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for observation in observations:
                handle.write(json.dumps(observation, ensure_ascii=False, sort_keys=True) + "\n")
                records += 1
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    checksum = hashlib.sha256()
    with target.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(block)
    digest = checksum.hexdigest()
    return {"raw_path": str(target), "sha256": digest, "records": records}


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
