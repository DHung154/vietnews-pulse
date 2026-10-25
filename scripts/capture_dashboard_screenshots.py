"""Script to capture detailed 1366x768 screenshots of all three tabs of the dashboard."""

import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

sys.stdout.reconfigure(encoding="utf-8")

shots_dir = Path("reports/screenshots")
shots_dir.mkdir(parents=True, exist_ok=True)

with sync_playwright() as p:
    browser = p.chromium.launch(
        executable_path="C:/Program Files/Google/Chrome/Application/chrome.exe",
        headless=True,
    )
    context = browser.new_context(viewport={"width": 1366, "height": 768})
    page = context.new_page()

    print("Navigating to http://localhost:8502...")
    page.goto("http://localhost:8502", wait_until="networkidle")

    # Wait for Streamlit to render metrics and hero
    page.wait_for_selector('[data-testid="stMetric"]', timeout=30000)
    time.sleep(3)

    # Check horizontal overflow
    scroll_w = page.evaluate("document.documentElement.scrollWidth")
    inner_w = page.evaluate("window.innerWidth")
    print(f"Viewport width: {inner_w}, Scroll width: {scroll_w}, Horizontal overflow: {scroll_w > inner_w}")

    # 1. Tab 1: Top (KPIs, Hero, Warnings)
    page.screenshot(path=str(shots_dir / "tab1_overview_header.png"))
    print("Captured tab1_overview_header.png")

    # Scroll down to charts
    page.mouse.move(700, 400)
    page.mouse.wheel(0, 600)
    time.sleep(2)
    page.screenshot(path=str(shots_dir / "tab1_overview_charts.png"))
    print("Captured tab1_overview_charts.png")

    # Scroll down further to keywords
    page.mouse.wheel(0, 600)
    time.sleep(2)
    page.screenshot(path=str(shots_dir / "tab1_overview_keywords.png"))
    print("Captured tab1_overview_keywords.png")

    # 2. Tab 2: Articles
    page.mouse.wheel(0, -1500)
    time.sleep(1)
    btn_tab2 = page.locator('button:has-text("Khám phá bài báo")')
    btn_tab2.click()
    page.wait_for_selector('[data-testid="stDataFrame"]', timeout=15000)
    time.sleep(2)
    page.screenshot(path=str(shots_dir / "tab2_articles_header.png"))
    print("Captured tab2_articles_header.png")

    page.mouse.wheel(0, 500)
    time.sleep(2)
    page.screenshot(path=str(shots_dir / "tab2_articles_table.png"))
    print("Captured tab2_articles_table.png")

    # 3. Tab 3: Data Quality
    page.mouse.wheel(0, -1500)
    time.sleep(1)
    btn_tab3 = page.locator('button:has-text("Chất lượng dữ liệu")')
    btn_tab3.click()
    time.sleep(2)
    page.screenshot(path=str(shots_dir / "tab3_quality_metrics.png"))
    print("Captured tab3_quality_metrics.png")

    page.mouse.wheel(0, 950)
    time.sleep(2)
    page.screenshot(path=str(shots_dir / "tab3_quality_feed_health.png"))
    print("Captured tab3_quality_feed_health.png")

    browser.close()
    print("All screenshots successfully captured!")
