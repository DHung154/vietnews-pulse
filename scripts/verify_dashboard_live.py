"""Read-only SQLite/AppTest and optional real-browser verification, fresh output."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    pointer_path = args.serving_root / "current.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    database = args.serving_root / pointer["db_path"]
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    pointer_before = pointer_path.read_bytes()
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as con:
        assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        total, unique, predicted, sources, categories = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT article_id), COUNT(sentiment_label), "
            "COUNT(DISTINCT source), COUNT(DISTINCT category) FROM articles WHERE dataset_id='rss_live_v2'").fetchone()
        assert total == unique and total > 0
        meta = con.execute("SELECT model_version,coverage_status FROM snapshot_meta WHERE snapshot_id=?",
            (pointer["snapshot_id"],)).fetchone()
        assert meta[0] == pointer["model_version"]
        source = con.execute("SELECT source FROM articles WHERE dataset_id='rss_live_v2' ORDER BY source LIMIT 1").fetchone()[0]
        source_count = con.execute("SELECT COUNT(*) FROM articles WHERE dataset_id='rss_live_v2' AND source=?", (source,)).fetchone()[0]
    bulletin = json.loads(args.bulletin.read_text(encoding="utf-8"))
    assert (bulletin["snapshot_id"], bulletin["model_version"], bulletin["coverage_status"]) == (pointer["snapshot_id"], meta[0], meta[1])
    os.environ["NEWS_SERVING_ROOT"] = str(args.serving_root.resolve())
    os.environ["NEWS_EVENT_BULLETIN_PATH"] = str(args.bulletin.resolve())
    from streamlit.testing.v1 import AppTest
    app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run(timeout=60)
    assert not app.exception, [e.value for e in app.exception]
    assert app.sidebar.selectbox(key="filter_dataset").value == "rss_live_v2"
    assert int(app.metric[0].value.replace(".", "")) == total
    app.sidebar.selectbox(key="filter_source").select(source).run(timeout=60)
    assert not app.exception
    assert int(app.metric[0].value.replace(".", "")) == source_count
    app.number_input(key="article_page").set_value(2).run(timeout=60)
    assert not app.exception
    app.sidebar.text_input(key="filter_search").input("NO_MATCH_38df610b_test").run(timeout=60)
    assert not app.exception and app.metric[0].value == "0"
    result = {"status": "ok", "snapshot_id": pointer["snapshot_id"], "model_version": meta[0],
        "coverage_status": meta[1], "rss_live_v2_articles": total, "distinct_article_ids": unique,
        "predicted_articles": predicted, "sources": sources, "categories": categories,
        "apptest": {"exception_count": 0, "source": source, "source_count": source_count,
            "pagination": "ok", "empty_filter": "ok"}, "browser": "not_requested"}
    if args.browser:
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(executable_path=args.chrome, headless=True)
                page = browser.new_page(viewport={"width": 1366, "height": 768})
                page.goto(args.url, wait_until="networkidle", timeout=30000)
                page.wait_for_selector('[data-testid="stMetric"]', timeout=30000)
                page.wait_for_function("![...document.querySelectorAll('[data-testid=stStatusWidget]')].some(e=>/RUNNING|Running/.test(e.innerText))", timeout=60000)
                assert page.locator('[data-testid="stMetricValue"]').first.inner_text().strip() == f"{total:,}".replace(",", ".")
                assert "rss_live_v2" in page.locator('[data-testid="stSidebar"]').inner_text()
                hero = page.locator('.news-hero').bounding_box()
                header = page.locator('[data-testid="stHeader"]').bounding_box()
                assert hero and header and hero['y'] >= header['y'] + header['height'], (hero, header)
                dimensions = page.evaluate("({width:innerWidth,scroll:document.documentElement.scrollWidth})")
                assert dimensions["scroll"] <= dimensions["width"], dimensions
                page.screenshot(path=str(output / "overview.png"), full_page=True)
                page.get_by_text("Nhịp độ xuất bản bài báo theo thời gian", exact=True).scroll_into_view_if_needed()
                page.screenshot(path=str(output / "charts.png"))
                page.get_by_text("Top từ khóa xuất hiện nhiều nhất", exact=True).scroll_into_view_if_needed()
                page.screenshot(path=str(output / "keywords.png"))
                for text, filename in (("Khám phá bài báo", "articles.png"), ("Chất lượng dữ liệu", "quality.png")):
                    page.get_by_role("tab", name=text, exact=False).click()
                    heading = "Khám phá danh sách bài báo" if filename == "articles.png" else "Chất lượng dữ liệu và Giới hạn hệ thống"
                    page.get_by_text(heading, exact=True).scroll_into_view_if_needed()
                    if filename == "quality.png":
                        date_caption = page.locator('[data-testid="stCaptionContainer"]').filter(has_text="Ngày đăng của toàn dataset").first
                        assert date_caption.is_visible()
                        assert date_caption.evaluate("element => element.scrollWidth <= element.clientWidth")
                    page.screenshot(path=str(output / filename), full_page=True)
                page.get_by_placeholder("Ví dụ: kinh tế, AI, thể thao...").fill("NO_MATCH_38df610b_test")
                page.get_by_placeholder("Ví dụ: kinh tế, AI, thể thao...").press("Enter")
                page.wait_for_function("document.querySelector('[data-testid=stMetricValue]')?.innerText.trim()==='0'", timeout=30000)
                page.screenshot(path=str(output / "empty.png"), full_page=True)
                browser.close()
                result["browser"] = {"status": "ok", "viewport": [1366, 768], "dimensions": dimensions,
                    "horizontal_overflow": False, "hero_clear_of_header": True,
                    "published_date_caption_unclipped": True,
                    "screenshots": ["overview.png", "charts.png", "keywords.png", "articles.png", "quality.png", "empty.png"]}
        except Exception as exc:
            result["browser"] = {"status": "failed", "error": str(exc)}
            result["status"] = "partial"
    assert before == hashlib.sha256(database.read_bytes()).hexdigest()
    assert pointer_before == pointer_path.read_bytes()
    result["database_sha256"] = before
    result["database_and_pointer_unchanged"] = True
    (output / "checks.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=True))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serving-root", type=Path, required=True)
    parser.add_argument("--bulletin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--browser", action="store_true")
    parser.add_argument("--url", default="http://localhost:8502")
    parser.add_argument("--chrome", default="C:/Program Files/Google/Chrome/Application/chrome.exe")
    raise SystemExit(run(parser.parse_args()))
