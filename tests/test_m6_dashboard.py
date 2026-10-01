import unittest

import sqlite3
from dashboard.app import _keyword_rows, _trend_rows, _trend_window_bounds, _where


class DashboardRuleTest(unittest.TestCase):
    def test_single_selected_day_uses_its_closed_local_six_hour_windows(self):
        self.assertEqual(_trend_window_bounds(("old", "old", "old"), "2026-09-16", "2026-09-16"), ("2026-09-15T11:00:00+00:00", "2026-09-15T17:00:00+00:00", "2026-09-15T23:00:00+00:00"))

    def test_filters_are_parameterized_and_scoped(self):
        where, args = _where("rss_live", "tuoitre", "other", "Tất cả", "không tốt")
        self.assertEqual(where, " WHERE dataset_id = ? AND source = ? AND category = ? AND (title LIKE ? OR description LIKE ?)")
        self.assertEqual(args, ["rss_live", "tuoitre", "other", "%không tốt%", "%không tốt%"])

    def test_trend_and_keywords_use_same_scope(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("CREATE TABLE articles (dataset_id TEXT, article_id TEXT, source TEXT, category TEXT, event_time TEXT, event_date TEXT, sentiment_label TEXT, title TEXT, description TEXT, clean_text TEXT)")
        connection.executemany("INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?)", [("rss_live", "1", "a", "x", "2026-09-14T23:00:00+00:00", "2026-09-14", "positive", "old", "", "old word"), ("rss_live", "2", "a", "x", "2026-09-15T01:00:00+00:00", "2026-09-15", "positive", "new", "", "new word"), ("rss_live", "3", "b", "y", "2026-09-15T01:00:00+00:00", "2026-09-15", "negative", "other", "", "other word"), ("rss_live", "4", "a", "x", "2026-09-15T07:00:00+00:00", "2026-09-15", "positive", "ongoing", "", "")])
        rows = _trend_rows(connection, "rss_live", "a", "Tất cả", "positive", "", ("2026-09-14T18:00:00+00:00", "2026-09-15T00:00:00+00:00", "2026-09-15T06:00:00+00:00"))
        self.assertEqual(rows[0][0:4], ("a", "x", 1, 1))
        self.assertEqual(_keyword_rows(connection, "rss_live", "a", "Tất cả", "positive", "", "2026-09-15", "2026-09-15"), [("new", 1), ("word", 1)])
        connection.close()


if __name__ == "__main__":
    unittest.main()
