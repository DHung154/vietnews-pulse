"""Deterministic URL, text and timestamp normalization shared by ETL jobs."""

from __future__ import annotations

import hashlib
import html
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from .rss import parse_published_at


TAG_RE = re.compile(r"<[^>]*>")
TRACKING_KEYS = {"fbclid", "gclid", "dclid", "msclkid"}
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
# Keep this list small and auditable. Unknown or missing feed categories remain `other`.
CATEGORY_MAP = {
    "thời sự": "society", "xã hội": "society", "đời sống": "society",
    "kinh doanh": "business", "tài chính": "business", "bất động sản": "real_estate",
    "khoa học": "science", "công nghệ": "technology", "thể thao": "sports",
    "giải trí": "entertainment", "pháp luật": "law", "sức khỏe": "health",
    "du lịch": "travel", "giáo dục": "education", "thế giới": "world",
    "world": "world", "business": "business", "technology": "technology",
    "sports": "sports", "entertainment": "entertainment", "law": "law",
    "health": "health", "education": "education", "travel": "travel",
}
try:
    _category_config = Path(__file__).resolve().parents[2] / "config" / "categories.yaml"
    CATEGORY_MAP.update(json.loads(_category_config.read_text(encoding="utf-8")).get("mapping", {}))
except (OSError, json.JSONDecodeError):
    pass


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = html.unescape(value)
    value = TAG_RE.sub(" ", value)
    value = unicodedata.normalize("NFC", value)
    return " ".join(value.split())


def normalize_category(value: str | None) -> str:
    raw = clean_text(value).casefold()
    return CATEGORY_MAP.get(raw, "other") if raw else "other"


def canonicalize_url(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parts = urlsplit(value.strip())
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            return None
        host = parts.hostname.lower()
        port = parts.port
        netloc = host
        if port and not ((parts.scheme.lower() == "http" and port == 80) or (parts.scheme.lower() == "https" and port == 443)):
            netloc = f"{host}:{port}"
        query = [(key, item) for key, item in parse_qsl(parts.query, keep_blank_values=True) if not key.lower().startswith("utm_") and key.lower() not in TRACKING_KEYS]
        query.sort()
        return urlunsplit((parts.scheme.lower(), netloc, parts.path or "/", urlencode(query, doseq=True), ""))
    except ValueError:
        return None


def _iso_utc(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def normalize_record(row: dict) -> dict:
    if hasattr(row, "asDict"):
        row = row.asDict(recursive=True)
    title = clean_text(row.get("title_raw"))
    description = clean_text(row.get("description_raw")) or None
    body = clean_text(row.get("body_raw")) or None
    url = canonicalize_url(row.get("link_raw"))
    source = row.get("source") or "unknown"
    published = None
    if row.get("published_at_raw"):
        try:
            published = parse_published_at(row["published_at_raw"], "Asia/Ho_Chi_Minh")
        except Exception:
            published = None
    crawl_raw = row.get("crawled_at")
    try:
        crawled = datetime.fromisoformat(crawl_raw.replace("Z", "+00:00")) if crawl_raw else None
        if crawled and crawled.tzinfo is None:
            crawled = crawled.replace(tzinfo=timezone.utc)
        crawled_iso = _iso_utc(crawled)
    except (AttributeError, ValueError):
        crawled = None
        crawled_iso = None
    flags: list[str] = []
    if not description:
        flags.append("missing_description")
    if not url:
        flags.append("invalid_url")
    if row.get("published_at_raw") and not published:
        flags.append("published_time_unparsed")
    dataset_id = row.get("dataset_id") or "unknown"
    if published:
        event_time = _iso_utc(published)
        basis = "published"
    elif dataset_id.startswith("historical_"):
        event_time = None
        basis = "unknown"
    else:
        event_time = crawled_iso
        basis = "crawled_fallback"
        flags.append("published_time_fallback")
    if event_time and crawled:
        try:
            if datetime.fromisoformat(event_time).timestamp() > crawled.timestamp() + 86400:
                flags.append("future_time"); event_time = None; basis = "unknown"
        except ValueError:
            pass
    source_record_id = str(row.get("source_record_id") or "")
    if url:
        id_material = f"{source}|{url}"
    elif title and row.get("published_at_raw"):
        id_material = f"{source}|{title}|{row['published_at_raw']}"
    elif title and source_record_id:
        id_material = f"{source}|{title}|{source_record_id}"
    else:
        id_material = ""
    article_id = hashlib.sha256(id_material.encode("utf-8")).hexdigest() if id_material else None
    valid = bool(title and article_id)
    invalid_reason = None if valid else ("missing_title_or_stable_id" if not title else "missing_stable_id")
    event_date = None
    if event_time:
        event_dt = datetime.fromisoformat(event_time)
        event_date = event_dt.astimezone(VIETNAM_TZ).date().isoformat()
    clean = " ".join(part for part in (title, description or "") if part).strip()
    category = normalize_category(row.get("category_raw"))
    category_basis = "item_mapping" if category != "other" else "unknown"
    if category == "other" and row.get("feed_canonical_category"):
        category = row["feed_canonical_category"]
        category_basis = "feed_registry" if category != "other" else "unknown"
    return {
        "dataset_id": dataset_id,
        "article_id": article_id,
        "source": source,
        "canonical_url": url,
        "title": title or None,
        "description": description,
        "body_text": body,
        "model_input_text": clean or None,
        "model_input_type": "title_description_v1" if description else "title_only",
        "clean_text": clean or None,
        "category": category,
        "category_basis": category_basis,
        "category_mapping_version": row.get("category_mapping_version"),
        "feed_id": row.get("feed_id"),
        "feed_category_raw": row.get("feed_category_raw"),
        "feed_set_version": row.get("feed_set_version"),
        "published_at": _iso_utc(published),
        "event_time": event_time,
        "event_time_basis": basis,
        "first_seen_at": crawled_iso,
        "last_seen_at": crawled_iso,
        "event_date": event_date,
        "quality_flags": flags,
        "text_hash": hashlib.sha256(clean.encode("utf-8")).hexdigest() if clean else None,
        "source_record_id": source_record_id or None,
        "observation_id": row.get("observation_id"),
        "batch_id": row.get("batch_id"),
        "feed_url": row.get("feed_url"),
        "valid": valid,
        "invalid_reason": invalid_reason,
    }
