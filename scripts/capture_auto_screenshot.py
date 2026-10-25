"""Capture screenshot of the dashboard running against the auto-serving dataset (rss_live_v2 with 10k+ articles)."""

import os
import subprocess
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

shots_dir = Path("reports/screenshots")
shots_dir.mkdir(parents=True, exist_ok=True)

env = os.environ.copy()
env["NEWS_SERVING_ROOT"] = "runtime/dashboard_auto_20261002T144200Z/serving"

proc = subprocess.Popen(
    [
        "python",
        "-m",
        "streamlit",
        "run",
        "dashboard/app.py",
        "--server.headless",
        "true",
        "--server.port",
        "8503",
        "--browser.gatherUsageStats",
        "false",
    ],
    env=env,
)

try:
    print("Waiting for Streamlit instance on port 8503...")
    time.sleep(7)
    with sync_playwright() as p:
        browser = p.chromium.launch(
            executable_path="C:/Program Files/Google/Chrome/Application/chrome.exe",
            headless=True,
        )
        page = browser.new_page(viewport={"width": 1366, "height": 768})
        page.goto("http://localhost:8503", wait_until="networkidle")
        page.wait_for_selector('[data-testid="stMetric"]', timeout=30000)
        time.sleep(3)

        # 1. Header & metrics
        page.screenshot(path=str(shots_dir / "auto_dataset_overview_top.png"))
        print("Captured auto_dataset_overview_top.png")

        # 2. Event bulletin & charts
        page.mouse.move(700, 400)
        page.mouse.wheel(0, 650)
        time.sleep(2)
        page.screenshot(path=str(shots_dir / "auto_dataset_bulletin_events.png"))
        print("Captured auto_dataset_bulletin_events.png")

        # 3. Volume and sentiment charts
        page.mouse.wheel(0, 650)
        time.sleep(2)
        page.screenshot(path=str(shots_dir / "auto_dataset_charts.png"))
        print("Captured auto_dataset_charts.png")

        browser.close()
finally:
    proc.terminate()
    print("Completed auto dataset capture.")
