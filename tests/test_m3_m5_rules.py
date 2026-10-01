import unittest

from datetime import datetime, timezone

from src.news_pipeline.analytics import completed_window_bounds, growth_status, resolve_as_of
from src.news_pipeline.normalization import canonicalize_url, clean_text, normalize_record, normalize_category


class M3M5RulesTest(unittest.TestCase):
    def test_url_keeps_identifier_and_removes_tracking(self):
        self.assertEqual(canonicalize_url("HTTPS://Example.COM/a?id=7&utm_source=x#part"), "https://example.com/a?id=7")

    def test_unicode_html_and_negation_are_preserved(self):
        cleaned = clean_text("A&nbsp; <b>không</b> tốt")
        self.assertIn("không", cleaned)
        self.assertEqual(cleaned, "A không tốt")

    def test_missing_historical_date_is_unknown_not_import_day(self):
        row = normalize_record({"dataset_id": "historical_v1", "source": "history", "title_raw": "Bài lịch sử", "link_raw": "https://example.com/a", "crawled_at": "2026-09-15T00:00:00+00:00"})
        self.assertIsNone(row["event_time"])
        self.assertEqual(row["event_time_basis"], "unknown")

    def test_growth_zero_baselines(self):
        self.assertAlmostEqual(growth_status(185, 60)["growth_percent"], 208.33333333333331)
        self.assertEqual(growth_status(10, 0)["status"], "new")
        self.assertEqual(growth_status(0, 10)["growth_percent"], -100.0)
        self.assertEqual(growth_status(0, 0)["status"], "no_activity")

    def test_event_date_uses_vietnam_local_day(self):
        row = normalize_record({"dataset_id": "rss_live", "source": "x", "title_raw": "Bài", "link_raw": "https://example.com/a", "published_at_raw": "2026-09-14T18:30:00+00:00", "crawled_at": "2026-09-15T00:00:00+00:00"})
        self.assertEqual(row["event_date"], "2026-09-15")

    def test_category_mapping_preserves_unknown_as_other(self):
        self.assertEqual(normalize_category(" Kinh doanh "), "business")
        self.assertEqual(normalize_category(None), "other")

    def test_completed_windows_are_recent_closed_local_boundaries(self):
        bounds = completed_window_bounds(datetime(2026, 9, 15, 17, 10, tzinfo=timezone.utc))
        self.assertEqual(bounds[1], "2026-09-15T11:00:00+00:00")
        self.assertEqual(bounds[2], "2026-09-15T17:00:00+00:00")

    def test_historical_anchor_uses_latest_event(self):
        anchor, source = resolve_as_of(None, ["historical_v1"], "2020-01-02T10:00:00+00:00")
        self.assertEqual(source, "historical_latest_event")
        self.assertEqual(anchor.year, 2020)


if __name__ == "__main__":
    unittest.main()
