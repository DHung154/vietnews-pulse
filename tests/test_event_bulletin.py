import unittest

from src.news_pipeline.event_bulletin import (
    _tokens,
    build_event_groups,
    deduplicate_articles,
    score_event_groups,
)


def _article(article_id, title, event_time, source="source-a", description=""):
    return {
        "article_id": article_id,
        "source": source,
        "title": title,
        "description": description,
        "event_time": event_time,
        "event_date": event_time[:10] if event_time else None,
        "category": "other",
        "sentiment_label": "neutral",
        "canonical_url": f"https://example.test/{article_id}",
    }


class EventBulletinTest(unittest.TestCase):
    def test_token_cache_is_bounded_immutable_and_matches_uncached_tokens(self):
        _tokens.cache_clear()
        text = "Vụ cháy nhà Chợ Lớn khiến hai người tử vong"
        expected = _tokens.__wrapped__(text)
        self.assertEqual(_tokens(text), expected)
        self.assertEqual(_tokens(text), expected)
        self.assertIsInstance(_tokens(text), frozenset)
        self.assertEqual(_tokens.cache_info().hits, 2)
        self.assertEqual(_tokens.cache_info().maxsize, 16384)
        _tokens.cache_clear()

    def test_dedup_keeps_one_article_and_tracks_missing_time(self):
        rows, audit = deduplicate_articles(
            [
                _article("a", "Bản tin A", "2026-09-17T05:00:00+00:00"),
                _article("a", "Bản tin A cập nhật", "2026-09-17T06:00:00+00:00"),
                _article("b", "Không có thời gian", None),
            ]
        )
        self.assertEqual({row["article_id"] for row in rows}, {"a", "b"})
        self.assertEqual(audit["duplicate_rows_removed"], 1)
        self.assertEqual(audit["missing_event_time"], 1)

    def test_same_event_candidate_is_time_limited_and_topic_only_is_not_enough(self):
        rows = [
            _article(
                "a",
                "Cháy nhà ở Chợ Lớn làm hai người tử vong",
                "2026-09-17T10:00:00+00:00",
                "tuoitre",
                "Vụ cháy nhà ở Chợ Lớn khiến lực lượng cứu hộ có mặt.",
            ),
            _article(
                "b",
                "Vụ cháy nhà Chợ Lớn khiến hai người tử vong",
                "2026-09-17T10:20:00+00:00",
                "thanhnien",
                "Lực lượng chức năng điều tra vụ cháy nhà tại Chợ Lớn.",
            ),
            _article(
                "c",
                "Giá vàng hôm nay tăng mạnh",
                "2026-09-17T10:30:00+00:00",
                "vnexpress",
                "Giá vàng trong nước được cập nhật trong ngày.",
            ),
        ]
        groups, pairs, audit = build_event_groups(rows)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(audit["audit"]["multi_article_group_count"], 1)
        self.assertEqual(len(groups), 2)
        self.assertNotEqual(groups[0]["article_ids"], ["c"])

    def test_incomplete_coverage_hides_growth_conclusion_and_zero_baseline_is_finite(self):
        groups = [
            {
                "event_group_id": "event-1",
                "representative_article_id": "current",
                "representative_title": "Sự kiện mới",
                "article_ids": ["current"],
                "group_origin": "algorithmic_candidate",
                "review_status": "pending_manual",
            }
        ]
        articles = {
            "current": _article(
                "current",
                "Sự kiện mới",
                "2026-09-17T06:10:00+00:00",
                "tuoitre",
            )
        }
        result = score_event_groups(
            groups,
            articles,
            (
                "2026-09-16T23:00:00+00:00",
                "2026-09-17T05:00:00+00:00",
                "2026-09-17T11:00:00+00:00",
            ),
            "incomplete",
        )
        self.assertEqual(result[0]["status"], "observed_only")
        self.assertIsNone(result[0]["growth_percent"])
        self.assertEqual(result[0]["observed_delta"], 1)

    def test_complete_coverage_marks_zero_baseline_as_new_without_infinity(self):
        groups = [
            {
                "event_group_id": "event-1",
                "representative_article_id": "current",
                "representative_title": "Sự kiện mới",
                "article_ids": ["current"],
                "group_origin": "algorithmic_candidate",
                "review_status": "pending_manual",
            }
        ]
        articles = {
            "current": _article(
                "current",
                "Sự kiện mới",
                "2026-09-17T06:10:00+00:00",
            )
        }
        result = score_event_groups(
            groups,
            articles,
            (
                "2026-09-16T23:00:00+00:00",
                "2026-09-17T05:00:00+00:00",
                "2026-09-17T11:00:00+00:00",
            ),
            "ok",
        )
        self.assertEqual(result[0]["status"], "new")
        self.assertIsNone(result[0]["growth_percent"])


if __name__ == "__main__":
    unittest.main()
