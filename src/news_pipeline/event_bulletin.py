"""Conservative event candidates and two-window bulletin scoring.

This module is deliberately read-only with respect to the serving snapshot.
Semantic groups are algorithmic candidates until a reviewer accepts them.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable


UTC = timezone.utc
TOKEN_RE = re.compile(r"[^\W_]+", flags=re.UNICODE)
SIGNAL_STOPWORDS = {
    "bài",
    "các",
    "cho",
    "của",
    "đã",
    "đang",
    "đây",
    "đến",
    "được",
    "giá",
    "hôm",
    "khi",
    "lại",
    "một",
    "mới",
    "nay",
    "ngày",
    "những",
    "qua",
    "sau",
    "sẽ",
    "tăng",
    "tin",
    "tại",
    "theo",
    "trong",
    "và",
    "vào",
    "với",
    "về",
}


def parse_event_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _time_key(value: str | None) -> tuple[int, str]:
    parsed = parse_event_time(value)
    return (int(parsed.timestamp()) if parsed else -1, str(value or ""))


def _tokens(value: str | None) -> set[str]:
    return {
        token.casefold()
        for token in TOKEN_RE.findall(value or "")
        if len(token) > 1 and token.casefold() not in SIGNAL_STOPWORDS
    }


def _article_text(row: dict) -> str:
    return " ".join(str(row.get(field) or "") for field in ("title", "description"))


def _content_hash(row: dict) -> str:
    return hashlib.sha256(_article_text(row).encode("utf-8")).hexdigest()


def deduplicate_articles(rows: Iterable[dict]) -> tuple[list[dict], dict]:
    """Keep one deterministic row per article_id for analysis only."""
    by_id: dict[str, list[dict]] = defaultdict(list)
    invalid = 0
    for row in rows:
        article_id = str(row.get("article_id") or "").strip()
        if not article_id:
            invalid += 1
            continue
        by_id[article_id].append(dict(row))
    selected = []
    duplicate_rows = 0
    for article_id, candidates in by_id.items():
        duplicate_rows += max(0, len(candidates) - 1)
        chosen = max(
            candidates,
            key=lambda row: (_time_key(row.get("event_time")), _content_hash(row)),
        )
        chosen["article_id"] = article_id
        selected.append(chosen)
    selected.sort(key=lambda row: (parse_event_time(row.get("event_time")) or datetime.min.replace(tzinfo=UTC), row["article_id"]))
    return selected, {
        "input_rows": sum(len(items) for items in by_id.values()) + invalid,
        "invalid_article_id": invalid,
        "duplicate_rows_removed": duplicate_rows,
        "canonical_rows": len(selected),
        "missing_event_time": sum(parse_event_time(row.get("event_time")) is None for row in selected),
    }


def _pair_similarity(left: dict, right: dict) -> tuple[float, int, int, float, float]:
    left_title = _tokens(left.get("title"))
    right_title = _tokens(right.get("title"))
    left_all = _tokens(_article_text(left))
    right_all = _tokens(_article_text(right))
    title_shared = len(left_title & right_title)
    all_shared = len(left_all & right_all)
    title_union = len(left_title | right_title)
    all_union = len(left_all | right_all)
    title_jaccard = title_shared / title_union if title_union else 0.0
    all_jaccard = all_shared / all_union if all_union else 0.0
    score = title_jaccard * 0.7 + all_jaccard * 0.3
    return score, title_shared, all_shared, title_jaccard, all_jaccard


def candidate_pairs(
    rows: list[dict],
    max_gap_hours: int = 48,
    min_similarity: float = 0.52,
) -> list[dict]:
    """Return conservative same-event candidates within a time horizon."""
    timed = sorted(
        (row for row in rows if parse_event_time(row.get("event_time"))),
        key=lambda row: (parse_event_time(row.get("event_time")), row["article_id"]),
    )
    pairs: list[dict] = []
    horizon_seconds = max_gap_hours * 3600
    for right_index, right in enumerate(timed):
        right_time = parse_event_time(right.get("event_time"))
        if right_time is None:
            continue
        for left in reversed(timed[:right_index]):
            left_time = parse_event_time(left.get("event_time"))
            if left_time is None:
                continue
            gap_seconds = (right_time - left_time).total_seconds()
            if gap_seconds > horizon_seconds:
                break
            score, title_shared, all_shared, title_jaccard, all_jaccard = _pair_similarity(left, right)
            if title_shared < 2 or all_shared < 3 or score < min_similarity:
                continue
            pairs.append(
                {
                    "article_id_a": left["article_id"],
                    "article_id_b": right["article_id"],
                    "similarity": round(score, 6),
                    "title_shared_tokens": title_shared,
                    "shared_tokens": all_shared,
                    "title_jaccard": round(title_jaccard, 6),
                    "text_jaccard": round(all_jaccard, 6),
                    "time_gap_hours": round(gap_seconds / 3600, 3),
                    "candidate_reason": "shared_signal_tokens_and_time_horizon",
                }
            )
    return sorted(
        pairs,
        key=lambda row: (-row["similarity"], row["time_gap_hours"], row["article_id_a"], row["article_id_b"]),
    )


class _UnionFind:
    def __init__(self, values: Iterable[str]):
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def build_event_groups(
    rows: Iterable[dict],
    max_gap_hours: int = 48,
    min_similarity: float = 0.52,
) -> tuple[list[dict], list[dict], dict]:
    canonical, audit = deduplicate_articles(rows)
    article_by_id = {row["article_id"]: row for row in canonical}
    pairs = candidate_pairs(canonical, max_gap_hours=max_gap_hours, min_similarity=min_similarity)
    union_find = _UnionFind(article_by_id)
    for pair in pairs:
        union_find.union(pair["article_id_a"], pair["article_id_b"])
    components: dict[str, list[str]] = defaultdict(list)
    for article_id in article_by_id:
        components[union_find.find(article_id)].append(article_id)
    groups = []
    article_to_event: dict[str, str] = {}
    for member_ids in sorted(components.values(), key=lambda values: (min(values), len(values))):
        member_ids = sorted(member_ids)
        group_id = "event_candidate_" + hashlib.sha256("|".join(member_ids).encode("utf-8")).hexdigest()[:16]
        representative = max(
            (article_by_id[article_id] for article_id in member_ids),
            key=lambda row: (_time_key(row.get("event_time")), row["article_id"]),
        )
        timed = [parse_event_time(article_by_id[article_id].get("event_time")) for article_id in member_ids]
        timed = [item for item in timed if item is not None]
        sources = sorted({str(article_by_id[article_id].get("source") or "") for article_id in member_ids if article_by_id[article_id].get("source")})
        groups.append(
            {
                "event_group_id": group_id,
                "representative_article_id": representative["article_id"],
                "representative_title": representative.get("title") or "(Không có tiêu đề)",
                "article_ids": member_ids,
                "article_count": len(member_ids),
                "source_count": len(sources),
                "sources": sources,
                "first_seen_at": min(timed).isoformat() if timed else None,
                "last_seen_at": max(timed).isoformat() if timed else None,
                "group_origin": "algorithmic_candidate",
                "review_status": "pending_manual",
                "group_reason": "shared_signal_tokens_and_time_horizon" if len(member_ids) > 1 else "single_article_in_pool",
            }
        )
        for article_id in member_ids:
            article_to_event[article_id] = group_id
    review_pairs = []
    for pair in pairs[:100]:
        left = article_by_id[pair["article_id_a"]]
        right = article_by_id[pair["article_id_b"]]
        review_pairs.append(
            {
                **pair,
                "title_a": left.get("title") or "",
                "description_a": left.get("description") or "",
                "source_a": left.get("source") or "",
                "event_time_a": left.get("event_time") or "",
                "input_sha256_a": _content_hash(left),
                "title_b": right.get("title") or "",
                "description_b": right.get("description") or "",
                "source_b": right.get("source") or "",
                "event_time_b": right.get("event_time") or "",
                "input_sha256_b": _content_hash(right),
                "review_decision": "",
                "reviewer": "",
                "review_reason": "",
            }
        )
    audit.update(
        {
            "timed_rows": sum(parse_event_time(row.get("event_time")) is not None for row in canonical),
            "candidate_pair_count": len(pairs),
            "review_pair_count": len(review_pairs),
            "event_group_count": len(groups),
            "multi_article_group_count": sum(group["article_count"] > 1 for group in groups),
            "unresolved_singleton_count": sum(group["article_count"] == 1 for group in groups),
        }
    )
    return groups, review_pairs, {
        "article_to_event": article_to_event,
        "articles": {article_id: row for article_id, row in article_by_id.items()},
        "audit": audit,
    }


def score_event_groups(
    groups: Iterable[dict],
    articles: dict[str, dict],
    window_bounds: tuple[str, str, str],
    coverage_status: str,
) -> list[dict]:
    previous_start, current_start, window_end = (parse_event_time(value) for value in window_bounds)
    if not previous_start or not current_start or not window_end:
        return []
    scored = []
    for group in groups:
        evidence = [articles[article_id] for article_id in group.get("article_ids", []) if article_id in articles]
        previous = [
            row
            for row in evidence
            if parse_event_time(row.get("event_time")) and previous_start <= parse_event_time(row.get("event_time")) < current_start
        ]
        current = [
            row
            for row in evidence
            if parse_event_time(row.get("event_time")) and current_start <= parse_event_time(row.get("event_time")) < window_end
        ]
        if not current:
            continue
        previous_count = len({row["article_id"] for row in previous})
        current_count = len({row["article_id"] for row in current})
        delta = current_count - previous_count
        current_sources = sorted({str(row.get("source") or "") for row in current if row.get("source")})
        evidence_sources = sorted({str(row.get("source") or "") for row in evidence if row.get("source")})
        if previous_count == 0:
            base_status = "new"
            growth_percent = None
        elif delta > 0:
            base_status = "up"
            growth_percent = delta / previous_count * 100
        elif delta < 0:
            base_status = "down"
            growth_percent = delta / previous_count * 100
        else:
            base_status = "stable"
            growth_percent = 0.0
        complete = coverage_status == "ok"
        scored.append(
            {
                "event_group_id": group["event_group_id"],
                "event_name": group.get("representative_title") or "(Không có tiêu đề)",
                "representative_article_id": group.get("representative_article_id"),
                "previous_count": previous_count,
                "current_count": current_count,
                "observed_delta": delta,
                "growth_percent": round(growth_percent, 2) if complete and growth_percent is not None else None,
                "status": base_status if complete else "observed_only",
                "source_count": len(current_sources),
                "sources": current_sources,
                "evidence_source_count": len(evidence_sources),
                "evidence_sources": evidence_sources,
                "multi_source": len(current_sources) >= 2,
                "first_seen_at": min(
                    (row.get("event_time") for row in current if row.get("event_time")),
                    default=None,
                ),
                "evidence_article_ids": [row["article_id"] for row in previous + current],
                "prominence_score": current_count + max(delta, 0) + 2 * len(current_sources),
                "group_origin": group.get("group_origin", "algorithmic_candidate"),
                "review_status": group.get("review_status", "pending_manual"),
                "coverage_status": coverage_status,
            }
        )
    return sorted(
        scored,
        key=lambda row: (
            -row["prominence_score"],
            -row["source_count"],
            -row["current_count"],
            row["event_name"],
        ),
    )
