"""Integration and unit tests for the redesigned Streamlit dashboard."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from streamlit.testing.v1 import AppTest

from dashboard.app import (
    CATEGORY_LABELS,
    SENTIMENT_LABELS,
    SOURCE_LABELS,
    _format_date,
    _format_number,
    _format_timestamp,
    _keyword_rows,
    _safe_url,
    _trend_rows,
    _trend_window_bounds,
    _where,
)


class DashboardUITest(unittest.TestCase):
    def setUp(self):
        self.original_serving_root = os.environ.get("NEWS_SERVING_ROOT")

    def tearDown(self):
        if self.original_serving_root is not None:
            os.environ["NEWS_SERVING_ROOT"] = self.original_serving_root
        else:
            os.environ.pop("NEWS_SERVING_ROOT", None)

    def test_default_kpis_match_live_sqlite(self):
        """Reconcile dashboard KPIs with actual serving SQLite database without hardcoding."""
        serving_root = Path("serving")
        pointer = json.loads((serving_root / "current.json").read_text(encoding="utf-8"))
        db_path = serving_root / pointer["db_path"]

        with sqlite3.connect(db_path) as con:
            dataset_id = "rss_live"
            # Verify dataset in snapshot
            datasets = [r[0] for r in con.execute("SELECT dataset_id FROM snapshot_datasets WHERE snapshot_id = ?", (pointer["snapshot_id"],))]
            target_ds = "rss_live_v2" if "rss_live_v2" in datasets else datasets[0]

            total = con.execute("SELECT COUNT(*) FROM articles WHERE dataset_id = ?", (target_ds,)).fetchone()[0]
            sources_count = con.execute("SELECT COUNT(DISTINCT source) FROM articles WHERE dataset_id = ?", (target_ds,)).fetchone()[0]
            topics_count = con.execute("SELECT COUNT(DISTINCT category) FROM articles WHERE dataset_id = ?", (target_ds,)).fetchone()[0]
            predicted = con.execute("SELECT COUNT(*) FROM articles WHERE dataset_id = ? AND sentiment_label IS NOT NULL", (target_ds,)).fetchone()[0]
            rate = (predicted / total) if total else 0.0
            min_date, max_date = con.execute("SELECT MIN(event_date), MAX(event_date) FROM articles WHERE dataset_id = ?", (target_ds,)).fetchone()

        os.environ.pop("NEWS_SERVING_ROOT", None)
        at = AppTest.from_file("dashboard/app.py")
        at.run(timeout=30)
        self.assertEqual(len(at.exception), 0, f"AppTest threw exceptions: {[e.value for e in at.exception]}")

        # Check first 4 metric labels and values
        labels = [m.label for m in at.metric[:4]]
        values = [m.value for m in at.metric[:4]]

        self.assertEqual(labels[0], "Tổng bài trong bộ lọc")
        self.assertEqual(values[0], _format_number(total))

        self.assertEqual(labels[1], "Số nguồn")
        self.assertEqual(values[1], _format_number(sources_count))

        self.assertEqual(labels[2], "Số chủ đề")
        self.assertEqual(values[2], _format_number(topics_count))

        self.assertEqual(labels[3], "Tỷ lệ bài có dự đoán")
        self.assertEqual(values[3], f"{rate:.1%}")
        date_caption = next(c.value for c in at.caption if c.value.startswith("Ngày đăng của toàn dataset"))
        self.assertIn(f"{_format_date(min_date)} → {_format_date(max_date)}", date_caption)
        self.assertIn("không phải khoảng thời gian thu thập RSS", date_caption)
        self.assertNotIn("Khoảng ngày bao phủ", [m.label for m in at.metric])
        for warning in at.warning:
            if "Coverage hiện tại" in warning.value:
                self.assertIn("14 ngày và tỷ lệ slot", warning.value)
                self.assertNotIn("14 ngày hoặc", warning.value)

    def test_search_filter_and_empty_state(self):
        """Verify search filter narrowing and empty state message when no results match."""
        os.environ.pop("NEWS_SERVING_ROOT", None)
        at = AppTest.from_file("dashboard/app.py")
        at.run(timeout=30)
        self.assertEqual(len(at.exception), 0)

        # Set search to a nonexistent keyword
        search_input = [ti for ti in at.sidebar.text_input if ti.key == "filter_search"][0]
        search_input.input("XYZ_NONEXISTENT_KEYWORD_9999").run(timeout=30)
        self.assertEqual(len(at.exception), 0)

        # Total KPI should be 0
        self.assertEqual(at.metric[0].value, "0")
        # Info element in articles tab should display no results message
        info_texts = [inf.value for inf in at.info]
        self.assertTrue(any("Không tìm thấy bài báo nào phù hợp" in t for t in info_texts))

    def test_pagination_controls(self):
        """Verify article pagination widgets exist and operate smoothly."""
        os.environ.pop("NEWS_SERVING_ROOT", None)
        at = AppTest.from_file("dashboard/app.py")
        at.run(timeout=30)
        self.assertEqual(len(at.exception), 0)

        # Check page size selectbox
        page_size_sb = [sb for sb in at.selectbox if sb.key == "article_page_size"][0]
        self.assertEqual(page_size_sb.value, 25)

        # Change page size to 10
        page_size_sb.select(10).run(timeout=30)
        self.assertEqual(len(at.exception), 0)
        self.assertEqual(page_size_sb.value, 10)

    def test_reset_filters_button(self):
        """Verify the reset button can be triggered without errors."""
        os.environ.pop("NEWS_SERVING_ROOT", None)
        at = AppTest.from_file("dashboard/app.py")
        at.run(timeout=30)
        self.assertEqual(len(at.exception), 0)

        # Find reset button in sidebar
        reset_btn = at.sidebar.button[0]
        self.assertIn("Đặt lại bộ lọc", reset_btn.label)
        reset_btn.click().run(timeout=30)
        self.assertEqual(len(at.exception), 0)

    def test_graceful_handling_missing_snapshot(self):
        """Verify graceful error presentation when pointing to missing/corrupted serving root."""
        with tempfile.TemporaryDirectory() as empty_dir:
            os.environ["NEWS_SERVING_ROOT"] = empty_dir
            at = AppTest.from_file("dashboard/app.py")
            at.run(timeout=30)
            self.assertEqual(len(at.exception), 0)
            # App should display error message instead of uncaught crash
            error_texts = [err.value for err in at.error]
            self.assertTrue(any("Không đọc được serving snapshot" in t for t in error_texts))

    def test_null_sentiment_and_helpers(self):
        """Verify helper formatting functions and safe URLs."""
        self.assertEqual(_safe_url("http://example.com/test"), "http://example.com/test")
        self.assertEqual(_safe_url("javascript:alert(1)"), "")
        self.assertEqual(_safe_url(""), "")
        self.assertEqual(_safe_url(None), "")
        self.assertEqual(_format_date("2026-09-15"), "15/09/2026")
        self.assertEqual(_format_date(None), "Chưa có")
        self.assertEqual(_format_number(1234567), "1.234.567")
        self.assertEqual(_format_number(None), "—")


if __name__ == "__main__":
    unittest.main()
