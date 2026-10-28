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
    _article_card,
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
        self.assertNotIn("Khoảng ngày bao phủ", [m.label for m in at.metric])
        self.assertEqual(len(at.warning), 0)
        overview_text = " ".join(m.value for m in at.tabs[0].markdown)
        self.assertNotIn("Manifest:", overview_text)
        self.assertNotIn("Model version:", overview_text)
        processing = next(e for e in at.expander if e.label == "Thông tin xử lý")
        self.assertFalse(processing.proto.expanded)
        metadata_text = " ".join(m.value for m in processing.markdown)
        self.assertIn(pointer["model_version"], metadata_text)
        self.assertIn("Coverage (metadata)", metadata_text)

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

        default_total = at.metric[0].value
        at.sidebar.text_input(key="filter_search").input("XYZ_NONEXISTENT_KEYWORD_9999").run(timeout=30)
        self.assertEqual(at.metric[0].value, "0")
        # Reset must clear the visible text as well as restore the count.
        reset_btn = at.sidebar.button[0]
        self.assertIn("Đặt lại bộ lọc", reset_btn.label)
        reset_btn.click().run(timeout=30)
        self.assertEqual(len(at.exception), 0)
        self.assertEqual(at.sidebar.text_input(key="filter_search").value, "")
        self.assertEqual(at.metric[0].value, default_total)
        self.assertEqual(at.number_input(key="article_page").value, 1)

    def test_article_card_escapes_feed_content_and_rejects_script_links(self):
        card = _article_card(("1", '<script>alert("x")</script>', "vnexpress", "sports", "2026-10-08", None, "javascript:alert(1)"))
        self.assertIn("&lt;script&gt;", card)
        self.assertNotIn("<script>", card)
        self.assertNotIn("javascript:", card)
        self.assertIn("Chưa có dự đoán", card)
        safe = _article_card(("2", "Tiêu đề đầy đủ", "vnexpress", "sports", "2026-10-08", "positive", 'https://example.com/?a=1&b="x"'))
        self.assertIn("rel=\"noopener noreferrer\"", safe)
        self.assertIn("&amp;b=&quot;x&quot;", safe)

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
