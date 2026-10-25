"""Read-only presentation dashboard for one immutable serving snapshot."""

from __future__ import annotations

import html
import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from src.news_pipeline.event_bulletin import parse_event_time, score_event_groups
from src.news_pipeline.time_windows import completed_window_bounds


try:
    VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
except Exception:  # pragma: no cover - platform fallback
    VIETNAM_TZ = timezone.utc

NAVY = "#0F172A"
NAVY_LIGHT = "#1E293B"
TEAL = "#0F766E"
TEAL_LIGHT = "#0D9488"
SLATE = "#64748B"
RED = "#DC2626"
AMBER = "#D97706"

SENTIMENT_LABELS = {
    "positive": "Tích cực",
    "neutral": "Trung tính",
    "negative": "Tiêu cực",
    "mixed": "Hỗn hợp",
    "needs_context": "Cần ngữ cảnh",
    "invalid": "Không hợp lệ",
}

SENTIMENT_COLORS = {
    "positive": TEAL,
    "neutral": SLATE,
    "negative": RED,
    "mixed": AMBER,
    "needs_context": "#7C3AED",
    "invalid": "#94A3B8",
}

CATEGORY_LABELS = {
    "society": "Thời sự - Xã hội",
    "business": "Kinh doanh",
    "technology": "Công nghệ",
    "sports": "Thể thao",
    "entertainment": "Giải trí",
    "law": "Pháp luật",
    "health": "Sức khỏe",
    "education": "Giáo dục",
    "world": "Thế giới",
    "travel": "Du lịch",
    "real_estate": "Bất động sản",
    "science": "Khoa học",
    "other": "Khác",
}

SOURCE_LABELS = {
    "vnexpress": "VnExpress",
    "tuoitre": "Tuổi Trẻ",
    "thanhnien": "Thanh Niên",
}


def _db_path(root: Path) -> tuple[str, str, str]:
    pointer = json.loads((root / "current.json").read_text(encoding="utf-8"))
    db = Path(pointer["db_path"])
    if not db.is_absolute():
        db = root / db
    return str(db), pointer["snapshot_id"], pointer.get("model_version", "unavailable")


def _where(
    dataset_id: str,
    source: str,
    category: str,
    sentiment: str,
    search: str,
    date_start: str | None = None,
    date_end: str | None = None,
) -> tuple[str, list]:
    clauses = ["dataset_id = ?"]
    args: list = [dataset_id]
    for column, value in (("source", source), ("category", category), ("sentiment_label", sentiment)):
        if value != "Tất cả":
            clauses.append(f"{column} = ?")
            args.append(value)
    if date_start:
        clauses.append("event_date >= ?")
        args.append(date_start)
    if date_end:
        clauses.append("event_date <= ?")
        args.append(date_end)
    if search:
        clauses.append("(title LIKE ? OR description LIKE ?)")
        args.extend([f"%{search}%", f"%{search}%"])
    return " WHERE " + " AND ".join(clauses), args


def _selected_dates(value, minimum: str | None, maximum: str | None) -> tuple[str | None, str | None]:
    if not minimum or not maximum:
        return None, None
    if isinstance(value, (tuple, list)) and len(value) == 2:
        return value[0].isoformat(), value[1].isoformat()
    if isinstance(value, date):
        item = value.isoformat()
        return item, item
    return minimum, maximum


def _trend_window_bounds(
    snapshot_bounds: tuple[str, str, str],
    date_start: str | None,
    date_end: str | None,
) -> tuple[str, str, str]:
    if date_start and date_start == date_end:
        return completed_window_bounds(datetime.fromisoformat(f"{date_start}T06:00:00+07:00"))
    return snapshot_bounds


def _trend_rows(
    connection: sqlite3.Connection,
    dataset_id: str,
    source: str,
    category: str,
    sentiment: str,
    search: str,
    window_bounds: tuple[str, str, str],
    date_start: str | None = None,
    date_end: str | None = None,
) -> list[tuple]:
    previous_start, current_start, window_end = window_bounds
    counts = defaultdict(lambda: [0, 0])
    where, args = _where(dataset_id, source, category, sentiment, search, date_start, date_end)
    where += " AND event_time >= ? AND event_time < ?"
    args.extend([previous_start, window_end])
    for row_source, row_category, event_time in connection.execute(
        f"SELECT source, category, event_time FROM articles{where}", args
    ):
        if event_time is None:
            continue
        counts[(row_source, row_category)][1 if event_time >= current_start else 0] += 1
    dim_where, dim_args = _where(dataset_id, source, category, sentiment, search, date_start, date_end)
    dimensions = connection.execute(
        f"SELECT DISTINCT source, category FROM articles{dim_where}", dim_args
    ).fetchall()
    rows = []
    for row_source, row_category in dimensions:
        previous_count, current_count = counts[(row_source, row_category)]
        delta = current_count - previous_count
        ratio = delta / previous_count if previous_count else None
        status = (
            "new"
            if previous_count == 0 and current_count > 0
            else "no_activity"
            if previous_count == 0
            else "ok"
        )
        rows.append(
            (
                row_source,
                row_category,
                previous_count,
                current_count,
                delta,
                ratio * 100 if ratio is not None else None,
                status,
                int(current_count >= 10 and previous_count >= 5),
            )
        )
    return sorted(
        rows,
        key=lambda row: (
            not row[7],
            -(row[5] if row[5] is not None else float("-inf")),
            -row[3],
            row[1],
        ),
    )


def _keyword_rows(
    connection: sqlite3.Connection,
    dataset_id: str,
    source: str,
    category: str,
    sentiment: str,
    search: str,
    date_start: str | None,
    date_end: str | None,
) -> list[tuple]:
    where, args = _where(dataset_id, source, category, sentiment, search, date_start, date_end)
    document_frequency = defaultdict(int)
    for article_id, clean_text in connection.execute(
        f"SELECT article_id, clean_text FROM articles{where}", args
    ):
        del article_id
        tokens = {
            token
            for token in re.findall(r"[^\W_]+", (clean_text or "").casefold(), flags=re.UNICODE)
            if len(token) > 1
        }
        for token in tokens:
            document_frequency[token] += 1
    return sorted(document_frequency.items(), key=lambda row: (-row[1], row[0]))[:30]


def _format_timestamp(value: str | None) -> str:
    if not value:
        return "Chưa có"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(VIETNAM_TZ).strftime("%d/%m/%Y %H:%M")
    except (TypeError, ValueError):
        return str(value)


def _format_date(value: str | None) -> str:
    if not value:
        return "Chưa có"
    try:
        parts = str(value).split("T")[0].split("-")
        if len(parts) == 3:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return str(value)
    except Exception:
        return str(value)


def _format_number(value: int | float | None) -> str:
    if value is None:
        return "—"
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.1f}".replace(",", ".")
    return f"{int(value):,}".replace(",", ".")


def _safe_url(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(str(value))
    return str(value) if parsed.scheme in {"http", "https"} and parsed.netloc else ""


def _aggregate_trends(rows: list[tuple]) -> list[dict]:
    grouped: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for _, row_category, previous_count, current_count, *_ in rows:
        grouped[row_category][0] += previous_count
        grouped[row_category][1] += current_count
    result = []
    for row_category, (previous_count, current_count) in grouped.items():
        delta = current_count - previous_count
        growth_pct = (delta / previous_count * 100) if previous_count else None
        status = "new" if previous_count == 0 and current_count else "no_activity" if not previous_count else "ok"
        result.append(
            {
                "category": row_category or "Chưa phân loại",
                "previous": previous_count,
                "current": current_count,
                "delta": delta,
                "growth_pct": growth_pct,
                "status": status,
                "eligible": current_count >= 10 and previous_count >= 5,
            }
        )
    return sorted(
        result,
        key=lambda row: (-row["delta"], -row["current"], row["category"]),
    )


def _style(st) -> None:
    st.markdown(
        """
        <style>
        :root {
            --navy-900: #0f172a;
            --navy-800: #1e293b;
            --teal-600: #0d9488;
            --teal-700: #0f766e;
            --slate-500: #64748b;
            --slate-200: #e2e8f0;
            --bg-main: #f8fafc;
        }

        .stApp {
            background-color: var(--bg-main);
            color: #1e293b;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        }

        /* 1366x768 and desktop container ergonomics */
        .block-container {
            max-width: 1360px;
            padding-top: 4.5rem;
            padding-bottom: 2.5rem;
            padding-left: 1.5rem;
            padding-right: 1.5rem;
        }

        [data-testid="stSidebar"] {
            background-color: #ffffff;
            border-right: 1px solid #e2e8f0;
        }
        [data-testid="stSidebar"] > div:first-child {
            padding-top: 1.25rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }

        /* Hero banner */
        .news-hero {
            background: linear-gradient(135deg, #0f172a 0%, #115e59 58%, #0f766e 100%);
            border-radius: 14px;
            padding: 1.35rem 1.75rem 1.25rem;
            margin-bottom: 1rem;
            box-shadow: 0 4px 18px rgba(15, 23, 42, 0.12);
        }
        .news-hero-kicker {
            color: #5eead4;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-bottom: 0.25rem;
        }
        .news-hero-title {
            color: #ffffff;
            font-size: clamp(1.45rem, 2.3vw, 2.05rem);
            font-weight: 750;
            line-height: 1.2;
            margin: 0 0 0.35rem 0;
        }
        .news-hero-desc {
            color: #ccfbf1;
            font-size: 0.95rem;
            margin: 0;
            line-height: 1.45;
            max-width: 840px;
        }

        /* Metric cards */
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            padding: 0.85rem 1rem;
            box-shadow: 0 2px 6px rgba(15, 23, 42, 0.03);
            min-width: 0;
        }
        [data-testid="stMetricLabel"] {
            color: #64748b;
            font-weight: 600;
            font-size: 0.82rem;
            letter-spacing: 0.02em;
        }
        [data-testid="stMetricValue"] {
            color: #0f172a;
            font-weight: 750;
            font-size: clamp(1.1rem, 1.7vw, 1.65rem);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* Event card styling */
        .event-card {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            padding: 0.95rem 1.15rem;
            margin-bottom: 0.75rem;
            box-shadow: 0 2px 6px rgba(15, 23, 42, 0.03);
        }
        .event-badge {
            display: inline-block;
            padding: 0.18rem 0.5rem;
            border-radius: 6px;
            font-size: 0.72rem;
            font-weight: 600;
            margin-right: 0.35rem;
            letter-spacing: 0.02em;
        }
        .badge-new { background: #dcfce7; color: #15803d; }
        .badge-obs { background: #e0f2fe; color: #0369a1; }
        .badge-multi { background: #f3e8ff; color: #7e22ce; }
        .badge-single { background: #f1f5f9; color: #475569; }

        /* Tabs and expanders */
        [data-testid="stTabs"] button {
            font-weight: 600;
            font-size: 0.95rem;
            padding-top: 0.5rem;
            padding-bottom: 0.5rem;
        }
        [data-testid="stExpander"] {
            border: 1px solid #e2e8f0;
            border-radius: 10px;
            background: #ffffff;
        }

        /* Section headers */
        .section-tag {
            color: #0f766e;
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            margin-bottom: -0.3rem;
            margin-top: 0.5rem;
        }

        @media (max-width: 900px) {
            .block-container { padding-left: 0.85rem; padding-right: 0.85rem; }
            .news-hero { padding: 1.1rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _chart(alt, data: list[dict], mark: dict, encoding: dict, height: int = 280):
    import pandas as pd

    mark_type = mark.get("type", "bar")
    mark_options = {key: value for key, value in mark.items() if key != "type"}
    data_source = pd.DataFrame(data)
    if mark_type == "line":
        chart = alt.Chart(data_source).mark_line(**mark_options)
    elif mark_type == "point":
        chart = alt.Chart(data_source).mark_point(**mark_options)
    elif mark_type == "area":
        chart = alt.Chart(data_source).mark_area(**mark_options)
    else:
        chart = alt.Chart(data_source).mark_bar(**mark_options)
    return (
        chart
        .encode(**encoding)
        .properties(height=height)
        .configure_view(strokeOpacity=0)
        .configure_axis(labelColor=SLATE, titleColor=NAVY, gridColor="#E2E8F0")
    )


def _read_snapshot_meta(connection: sqlite3.Connection, snapshot_id: str) -> tuple:
    meta_columns = {row[1] for row in connection.execute("PRAGMA table_info(snapshot_meta)")}
    meta_query = (
        "SELECT published_at, model_version, window_previous_start, window_current_start, "
        "window_end, coverage_status, coverage_manifest_path FROM snapshot_meta WHERE snapshot_id = ?"
        if "coverage_status" in meta_columns
        else
        "SELECT published_at, model_version, window_previous_start, window_current_start, "
        "window_end, 'unknown', NULL FROM snapshot_meta WHERE snapshot_id = ?"
    )
    return connection.execute(meta_query, (snapshot_id,)).fetchone()


def _event_bulletin_path(snapshot_id: str | None = None) -> Path:
    configured = os.environ.get("NEWS_EVENT_BULLETIN_PATH")
    if configured:
        path = Path(configured)
        return path if path.is_absolute() else Path(__file__).resolve().parents[1] / path

    base = Path(__file__).resolve().parents[1]
    # Check if any bulletin matches snapshot_id in evidence directory
    if snapshot_id:
        for candidate in sorted(base.glob("evidence/**/event_bulletin.json"), reverse=True):
            try:
                candidate_data = json.loads(candidate.read_text(encoding="utf-8"))
                if candidate_data.get("snapshot_id") == snapshot_id:
                    return candidate
            except Exception:
                continue

    return base / "evidence" / "event_bulletin_v1_20260918_r2" / "event_bulletin.json"


def _read_event_bulletin(path: Path, snapshot_id: str, dataset_id: str) -> tuple[dict | None, str | None]:
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"Không đọc được artifact Bản tin biến động: {exc}"
    if artifact.get("snapshot_id") != snapshot_id:
        return None, "Artifact Bản tin biến động không cùng snapshot với serving hiện tại."
    if artifact.get("dataset_id") != dataset_id:
        return None, "Artifact Bản tin biến động không cùng dataset đang chọn."
    if artifact.get("format_version") != "event_bulletin_v1":
        return None, "Artifact Bản tin biến động không đúng format version."
    return artifact, None


def _event_bulletin_scope_rows(
    connection: sqlite3.Connection,
    artifact: dict,
    dataset_id: str,
    source: str,
    category: str,
    sentiment: str,
    search: str,
    window_bounds: tuple[str, str, str],
    date_start: str | None,
    date_end: str | None,
) -> tuple[list[dict], dict[str, dict]]:
    previous_start, _, window_end = window_bounds
    where, args = _where(dataset_id, source, category, sentiment, search, date_start, date_end)
    cursor = connection.execute(
        f"SELECT article_id, source, category, title, description, canonical_url, "
        f"event_time, event_date, sentiment_label FROM articles{where} "
        "AND event_time >= ? AND event_time < ? ORDER BY event_time, article_id",
        args + [previous_start, window_end],
    )
    columns = [item[0] for item in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor]
    mapping = artifact.get("article_to_event") or {}
    filtered = {
        row["article_id"]: row
        for row in rows
        if row.get("article_id") in mapping and parse_event_time(row.get("event_time")) is not None
    }
    return rows, filtered


def _event_detail_rows(
    item: dict,
    articles: dict[str, dict],
    window_bounds: tuple[str, str, str],
) -> list[dict]:
    previous_start, current_start, window_end = (parse_event_time(value) for value in window_bounds)
    result = []
    for article_id in item.get("evidence_article_ids", []):
        row = articles.get(article_id)
        event_time = parse_event_time(row.get("event_time")) if row else None
        if not row or not event_time or not previous_start or not current_start or not window_end:
            continue
        if not previous_start <= event_time < window_end:
            continue
        result.append(
            {
                "Cửa sổ": "Hiện tại" if event_time >= current_start else "Trước",
                "Thời điểm": _format_timestamp(row.get("event_time")),
                "Nguồn": SOURCE_LABELS.get(row.get("source"), row.get("source") or "Chưa rõ"),
                "Chủ đề": CATEGORY_LABELS.get(row.get("category"), row.get("category") or "Khác"),
                "Tiêu đề": row.get("title") or "(Không có tiêu đề)",
                "Sắc thái": SENTIMENT_LABELS.get(row.get("sentiment_label"), "Chưa có"),
                "Mở bài": _safe_url(row.get("canonical_url")),
            }
        )
    return result


def main() -> None:
    import altair as alt
    import pandas as pd
    import streamlit as st

    st.set_page_config(
        page_title="Theo dõi xu hướng tin tức Việt Nam",
        page_icon="📰",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _style(st)
    st.sidebar.link_button("Trình diễn 9 bước Big Data", "http://localhost:8504")

    root = Path(os.environ.get("NEWS_SERVING_ROOT", "serving"))
    connection = None
    try:
        db_path, snapshot_id, pointer_model = _db_path(root)
        connection = sqlite3.connect(db_path)
        meta = _read_snapshot_meta(connection, snapshot_id)
        datasets = [
            row[0]
            for row in connection.execute(
                "SELECT dataset_id FROM snapshot_datasets WHERE snapshot_id = ? ORDER BY dataset_id",
                (snapshot_id,),
            )
        ]
    except (OSError, KeyError, json.JSONDecodeError, sqlite3.Error) as exc:
        st.error(f"Không đọc được serving snapshot: {exc}")
        return

    if not meta or not datasets:
        st.error("Snapshot thiếu metadata hoặc danh sách dataset.")
        if connection:
            connection.close()
        return

    try:
        published_at, model_version, previous_start, current_start, window_end, coverage_status, coverage_manifest_path = meta
        window_bounds = (previous_start, current_start, window_end)
        if not all(window_bounds):
            window_bounds = completed_window_bounds()
        active_model = model_version or pointer_model or "unavailable"

        # ---------------------------------------------------------
        # SIDEBAR FILTERS
        # ---------------------------------------------------------
        st.sidebar.markdown("### 🔍 Bộ lọc dữ liệu")
        st.sidebar.caption("Thu hẹp phạm vi phân tích trên snapshot serving hiện tại.")

        # Reset button
        if st.sidebar.button("🔄 Đặt lại bộ lọc", use_container_width=True, type="secondary"):
            for key in [
                "filter_source",
                "filter_category",
                "filter_sentiment",
                "filter_search",
                "filter_date",
                "article_page",
            ]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

        # Dataset selector: default rss_live_v2 if available
        dataset_index = datasets.index("rss_live_v2") if "rss_live_v2" in datasets else 0
        dataset_id = st.sidebar.selectbox("Dataset", datasets, index=dataset_index, key="filter_dataset")

        # Handle dataset switch to avoid date bounds clash
        if "active_dataset_id" not in st.session_state or st.session_state["active_dataset_id"] != dataset_id:
            st.session_state["active_dataset_id"] = dataset_id
            for key in ["filter_date", "filter_source", "filter_category", "filter_sentiment", "filter_search", "article_page"]:
                if key in st.session_state:
                    del st.session_state[key]

        min_date, max_date = connection.execute(
            "SELECT MIN(event_date), MAX(event_date) FROM articles WHERE dataset_id = ?",
            (dataset_id,),
        ).fetchone()

        date_value = None
        if min_date and max_date:
            d_min = date.fromisoformat(min_date)
            d_max = date.fromisoformat(max_date)
            date_value = st.sidebar.date_input(
                "Khoảng ngày (giờ Việt Nam)",
                value=(d_min, d_max),
                min_value=d_min,
                max_value=d_max,
                key="filter_date",
            )
        date_start, date_end = _selected_dates(date_value, min_date, max_date)

        sources = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT source FROM articles WHERE dataset_id = ? ORDER BY source",
                (dataset_id,),
            )
        ]
        categories = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT category FROM articles WHERE dataset_id = ? ORDER BY category",
                (dataset_id,),
            )
        ]
        sentiments = [
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT sentiment_label FROM articles "
                "WHERE dataset_id = ? AND sentiment_label IS NOT NULL ORDER BY sentiment_label",
                (dataset_id,),
            )
        ]

        source = st.sidebar.selectbox(
            "Nguồn báo",
            ["Tất cả"] + sources,
            key="filter_source",
            format_func=lambda value: SOURCE_LABELS.get(value, value),
        )
        category = st.sidebar.selectbox(
            "Chủ đề",
            ["Tất cả"] + categories,
            key="filter_category",
            format_func=lambda value: CATEGORY_LABELS.get(value, value),
        )
        sentiment = st.sidebar.selectbox(
            "Sắc thái bài báo",
            ["Tất cả"] + sentiments,
            key="filter_sentiment",
            format_func=lambda value: SENTIMENT_LABELS.get(value, value),
        )
        search = st.sidebar.text_input(
            "Tìm trong tiêu đề / mô tả",
            placeholder="Ví dụ: kinh tế, AI, thể thao...",
            key="filter_search",
        )

        st.sidebar.divider()
        st.sidebar.caption("Snapshot chỉ đọc. Các thống kê được tính toán trực tiếp từ dữ liệu hiện có.")

        # ---------------------------------------------------------
        # QUERY MAIN KPIS
        # ---------------------------------------------------------
        where, args = _where(dataset_id, source, category, sentiment, search, date_start, date_end)
        total = connection.execute(f"SELECT COUNT(*) FROM articles{where}", args).fetchone()[0]
        sources_count = connection.execute(
            f"SELECT COUNT(DISTINCT source) FROM articles{where}", args
        ).fetchone()[0]
        topics_count = connection.execute(
            f"SELECT COUNT(DISTINCT category) FROM articles{where}", args
        ).fetchone()[0]
        predicted = connection.execute(
            f"SELECT COUNT(*) FROM articles{where} AND sentiment_label IS NOT NULL", args
        ).fetchone()[0]
        prediction_rate = (predicted / total) if total else 0.0

        # ---------------------------------------------------------
        # HEADER
        # ---------------------------------------------------------
        st.markdown(
            """
            <div class="news-hero">
              <div class="news-hero-kicker">HỆ THỐNG PHÂN TÍCH TIN TỨC TIẾNG VIỆT · VIETNEWS PULSE</div>
              <h1 class="news-hero-title">Theo dõi xu hướng tin tức Việt Nam</h1>
              <p class="news-hero-desc">Theo dõi nhịp độ xuất bản, phát hiện biến động chủ đề và phân tích sắc thái nội dung từ các luồng RSS báo chí chính thống.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        formatted_pub_time = _format_timestamp(published_at)
        st.caption(
            f"🕒 Cập nhật snapshot: **{formatted_pub_time} (Giờ Việt Nam)** · "
            f"Dataset: **{dataset_id}** · Snapshot ID: `{snapshot_id}`"
        )

        # MANDATORY WARNINGS
        st.warning(
            f"⚠️ **Model đang dùng: `{active_model}`** — **Model thử nghiệm, chưa được đánh giá trên tập tin tức độc lập do người gán nhãn.** "
            "Sắc thái bài báo mô tả nội dung tiêu đề và mô tả, không phải cảm xúc độc giả hoặc thái độ công chúng."
        )

        if coverage_status != "ok":
            st.warning(
                f"⚠️ **Tình trạng thu thập (Coverage): `{coverage_status}`** — Dữ liệu quan sát chưa hoàn chỉnh (incomplete). "
                "Khoảng trống nguồn không được coi là 0 bài; chưa đủ căn cứ để kết luận xu hướng tăng hoặc giảm là đáng tin cậy."
                + (f" (Manifest: `{coverage_manifest_path}`)" if coverage_manifest_path else "")
            )

        # ---------------------------------------------------------
        # 4 KPI METRIC CARDS
        # ---------------------------------------------------------
        kpi_1, kpi_2, kpi_3, kpi_4 = st.columns(4)
        kpi_1.metric("Tổng bài trong bộ lọc", _format_number(total))
        kpi_2.metric("Số nguồn", _format_number(sources_count))
        kpi_3.metric("Số chủ đề", _format_number(topics_count))
        kpi_4.metric(
            "Tỷ lệ bài có dự đoán",
            f"{prediction_rate:.1%}",
            help="Tỷ lệ bài viết đã được mô hình gán nhãn dự đoán sắc thái (prediction coverage). Đây không phải là độ chính xác (accuracy) của mô hình.",
        )

        # TECHNICAL PROCESSING EXPANDER
        with st.expander("ℹ️ Thông tin xử lý"):
            info_left, info_right = st.columns(2)
            with info_left:
                st.markdown(
                    f"""
                    - **Snapshot ID:** `{snapshot_id}`
                    - **Dataset ID:** `{dataset_id}`
                    - **Mô hình phục vụ:** `{active_model}`
                    - **Thời điểm xuất bản snapshot:** {_format_timestamp(published_at)}
                    - **Cơ sở dữ liệu serving:** `{Path(db_path).name}`
                    """
                )
            with info_right:
                st.markdown(
                    f"""
                    - **Cửa sổ so sánh trend (giờ VN):** {_format_timestamp(window_bounds[0])} → {_format_timestamp(window_bounds[2])}
                    - **Mốc ranh giới cửa sổ hiện tại:** {_format_timestamp(window_bounds[1])}
                    - **Múi giờ phân tích:** Asia/Ho_Chi_Minh (UTC+07:00)
                    - **Tình trạng coverage:** `{coverage_status}`
                    - **Đường dẫn manifest:** `{coverage_manifest_path or "Chưa gắn"}`
                    """
                )

        # ---------------------------------------------------------
        # 3 MAIN TABS
        # ---------------------------------------------------------
        overview_tab, articles_tab, quality_tab = st.tabs(
            ["📊 Tổng quan", "📰 Khám phá bài báo", "🛡️ Chất lượng dữ liệu"]
        )

        # =========================================================
        # TAB 1: TỔNG QUAN
        # =========================================================
        with overview_tab:
            st.markdown('<div class="section-tag">DIỄN BIẾN ĐÁNG CHÚ Ý</div>', unsafe_allow_html=True)
            st.subheader("Bản tin biến động")
            st.caption(
                "Tối đa 5 ứng viên biến động nổi bật so sánh giữa hai cửa sổ 6 giờ đã kết thúc. "
                "Nhóm sự kiện là kỹ thuật gom cụm heuristic thử nghiệm; cùng chủ đề chưa chắc cùng sự kiện và chưa được thẩm định độc lập."
            )

            bulletin_bounds = _trend_window_bounds(window_bounds, date_start, date_end)
            bulletin_file_path = _event_bulletin_path(snapshot_id)
            bulletin_artifact, bulletin_error = _read_event_bulletin(
                bulletin_file_path,
                snapshot_id,
                dataset_id,
            )

            if bulletin_error:
                st.info(f"ℹ️ {bulletin_error} Có thể tiếp tục theo dõi biến động theo chủ đề ở các biểu đồ bên dưới.")
            else:
                _, bulletin_articles = _event_bulletin_scope_rows(
                    connection,
                    bulletin_artifact,
                    dataset_id,
                    source,
                    category,
                    sentiment,
                    search,
                    bulletin_bounds,
                    date_start,
                    date_end,
                )
                bulletin_rows = score_event_groups(
                    bulletin_artifact.get("groups", []),
                    bulletin_articles,
                    bulletin_bounds,
                    coverage_status or "unknown",
                )

                if coverage_status != "ok":
                    st.warning(
                        "⚠️ Biến động trong dữ liệu đã thu thập. Coverage chưa đủ nên không kết luận tăng hoặc giảm là xu hướng thực tế."
                    )

                if not bulletin_rows:
                    st.info("Chưa có ứng viên sự kiện trong hai cửa sổ và bộ lọc hiện tại.")
                else:
                    for item in bulletin_rows[:5]:
                        with st.container():
                            card_left, card_right = st.columns([3, 1])
                            with card_left:
                                safe_event_name = html.escape(item.get("event_name", "Sự kiện chưa đặt tên"))
                                st.markdown(f"**{safe_event_name}**")

                                # Status badge and metrics description
                                if item["status"] == "observed_only":
                                    badge_html = '<span class="event-badge badge-obs">QUAN SÁT</span>'
                                    delta_text = f"Cửa sổ trước: {item['previous_count']} bài → Cửa sổ hiện tại: {item['current_count']} bài"
                                elif item["status"] == "new":
                                    badge_html = '<span class="event-badge badge-new">MỚI XUẤT HIỆN</span>'
                                    delta_text = f"Mới xuất hiện · {item['current_count']} bài"
                                else:
                                    badge_html = f'<span class="event-badge badge-obs">{item["status"].upper()}</span>'
                                    delta_text = f"Trước: {item['previous_count']} → Hiện tại: {item['current_count']} bài (Δ {item['observed_delta']:+d})"

                                source_names = [SOURCE_LABELS.get(s, s) for s in item.get("sources", [])]
                                if item["multi_source"]:
                                    src_badge = '<span class="event-badge badge-multi">ĐA NGUỒN</span>'
                                    src_text = f"Nhiều nguồn cùng đưa tin: {', '.join(source_names)}"
                                else:
                                    src_badge = '<span class="event-badge badge-single">ĐƠN NGUỒN</span>'
                                    src_text = f"Một nguồn trong cửa sổ: {', '.join(source_names) or 'chưa rõ'}"

                                st.markdown(f"{badge_html} {src_badge} <span style='color:#64748b;font-size:0.85rem;'>{delta_text} · {src_text}</span>", unsafe_allow_html=True)

                            with card_right:
                                c1, c2 = st.columns(2)
                                c1.metric("Bài hiện tại", _format_number(item["current_count"]))
                                c2.metric("Số nguồn", _format_number(item["source_count"]))

                            with st.expander("📄 Xem diễn biến và bài bằng chứng"):
                                st.caption(
                                    f"Xuất hiện đầu tiên trong bằng chứng: {_format_timestamp(item['first_seen_at'])}. "
                                    "Sắc thái chỉ là thông tin tham khảo từ mô hình baseline thử nghiệm."
                                )
                                detail_rows = _event_detail_rows(
                                    item,
                                    bulletin_articles,
                                    bulletin_bounds,
                                )
                                if detail_rows:
                                    st.dataframe(
                                        detail_rows,
                                        use_container_width=True,
                                        hide_index=True,
                                        column_config={
                                            "Tiêu đề": st.column_config.TextColumn("Tiêu đề", width="large"),
                                            "Mở bài": st.column_config.LinkColumn("Mở bài", validate="^https?://"),
                                        },
                                    )
                                else:
                                    st.info("Không còn bài báo bằng chứng nào trong phạm vi bộ lọc hiện tại.")

                                if item.get("review_status") != "human_reviewed":
                                    st.caption("⚠️ Nhóm sự kiện do thuật toán heuristic đề xuất, chưa được chuyên viên thẩm định.")

            st.divider()

            # -----------------------------------------------------
            # VOLUME OVER TIME
            # -----------------------------------------------------
            st.markdown('<div class="section-tag">NHỊP ĐỘ XUẤT BẢN</div>', unsafe_allow_html=True)
            st.subheader("Nhịp độ xuất bản bài báo theo thời gian")
            st.caption(
                "Số lượng bài viết theo ngày đăng (giờ Việt Nam). Áp dụng toàn bộ bộ lọc hiện tại (dataset, ngày, nguồn, chủ đề, sắc thái, tìm kiếm)."
            )

            volume_rows = [
                {"event_date": row[0], "count": row[1], "display_date": _format_date(row[0])}
                for row in connection.execute(
                    f"SELECT event_date, COUNT(*) FROM articles{where} "
                    "AND event_date IS NOT NULL GROUP BY event_date ORDER BY event_date",
                    args,
                )
            ]

            if volume_rows:
                vol_df = pd.DataFrame(volume_rows)
                if len(volume_rows) == 1:
                    vol_chart = (
                        alt.Chart(vol_df)
                        .mark_bar(color=TEAL, size=36, cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
                        .encode(
                            x=alt.X("display_date:N", title="Ngày xuất bản (giờ Việt Nam)"),
                            y=alt.Y("count:Q", title="Số bài viết", axis=alt.Axis(format=",.0f")),
                            tooltip=[
                                alt.Tooltip("display_date:N", title="Ngày xuất bản"),
                                alt.Tooltip("count:Q", title="Số bài báo", format=",.0f"),
                            ],
                        )
                        .properties(height=260)
                        .configure_view(strokeOpacity=0)
                        .configure_axis(labelColor=SLATE, titleColor=NAVY, gridColor="#E2E8F0")
                    )
                else:
                    base = alt.Chart(vol_df).encode(
                        x=alt.X("event_date:T", title="Ngày xuất bản (giờ Việt Nam)", axis=alt.Axis(format="%d/%m/%Y")),
                        y=alt.Y("count:Q", title="Số bài viết", axis=alt.Axis(format=",.0f")),
                        tooltip=[
                            alt.Tooltip("display_date:N", title="Ngày"),
                            alt.Tooltip("count:Q", title="Số bài báo", format=",.0f"),
                        ],
                    )
                    vol_line = base.mark_line(color=TEAL, strokeWidth=2.5)
                    vol_pts = base.mark_point(color=TEAL, size=65, filled=True)
                    vol_chart = (
                        (vol_line + vol_pts)
                        .properties(height=260)
                        .configure_view(strokeOpacity=0)
                        .configure_axis(labelColor=SLATE, titleColor=NAVY, gridColor="#E2E8F0")
                    )
                st.altair_chart(vol_chart, use_container_width=True)
            else:
                st.info("ℹ️ Không có dữ liệu bài báo trong khoảng thời gian và bộ lọc đã chọn.")

            st.divider()

            # -----------------------------------------------------
            # TWO COLUMNS: SENTIMENT & TRENDS
            # -----------------------------------------------------
            col_sentiment, col_trend = st.columns(2, gap="large")

            with col_sentiment:
                st.markdown('<div class="section-tag">SẮC THÁI NỘI DUNG</div>', unsafe_allow_html=True)
                st.subheader("Phân bố sắc thái nội dung")
                st.caption("Sắc thái được mô hình dự đoán từ tiêu đề và mô tả bài báo; không đại diện cho cảm xúc độc giả.")

                sentiment_counts = connection.execute(
                    f"SELECT sentiment_label, COUNT(*) FROM articles{where} "
                    "AND sentiment_label IS NOT NULL GROUP BY sentiment_label",
                    args,
                ).fetchall()
                total_sent = sum(r[1] for r in sentiment_counts)

                sentiment_rows = [
                    {
                        "sentiment": row[0],
                        "label": SENTIMENT_LABELS.get(row[0], row[0]),
                        "count": row[1],
                        "percentage": (row[1] / total_sent * 100) if total_sent else 0.0,
                    }
                    for row in sentiment_counts
                ]

                if sentiment_rows:
                    sent_df = pd.DataFrame(sentiment_rows)
                    sent_chart = (
                        alt.Chart(sent_df)
                        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                        .encode(
                            x=alt.X("count:Q", title="Số bài viết", axis=alt.Axis(format=",.0f")),
                            y=alt.Y("label:N", title=None, sort="-x"),
                            color=alt.Color(
                                "sentiment:N",
                                title="Sắc thái",
                                scale=alt.Scale(
                                    domain=list(SENTIMENT_COLORS.keys()),
                                    range=list(SENTIMENT_COLORS.values()),
                                ),
                                legend=None,
                            ),
                            tooltip=[
                                alt.Tooltip("label:N", title="Sắc thái"),
                                alt.Tooltip("count:Q", title="Số bài", format=",.0f"),
                                alt.Tooltip("percentage:Q", title="Tỷ lệ %", format=".1f"),
                            ],
                        )
                        .properties(height=260)
                        .configure_view(strokeOpacity=0)
                        .configure_axis(labelColor=SLATE, titleColor=NAVY, gridColor="#E2E8F0")
                    )
                    st.altair_chart(sent_chart, use_container_width=True)
                else:
                    st.info("ℹ️ Chưa có nhãn sắc thái trong phạm vi bộ lọc đang chọn.")

            with col_trend:
                st.markdown('<div class="section-tag">XU HƯỚNG CHỦ ĐỀ</div>', unsafe_allow_html=True)
                st.subheader("Chủ đề biến động (2 cửa sổ 6 giờ)")
                st.caption(
                    "So sánh hai cửa sổ 6 giờ đã kết thúc theo giờ Việt Nam. Dấu (+) tăng, (-) giảm. "
                    "Nếu coverage chưa đủ, biến động chỉ phản ánh số bài thu thập được."
                )

                trend_bounds = _trend_window_bounds(window_bounds, date_start, date_end)
                trend_data = _aggregate_trends(
                    _trend_rows(
                        connection,
                        dataset_id,
                        source,
                        category,
                        sentiment,
                        search,
                        trend_bounds,
                        date_start,
                        date_end,
                    )
                )

                if trend_data:
                    for td in trend_data:
                        td["category_display"] = CATEGORY_LABELS.get(td["category"], td["category"])

                    trend_df = pd.DataFrame(trend_data[:15])
                    trend_chart = (
                        alt.Chart(trend_df)
                        .mark_bar(cornerRadiusEnd=4)
                        .encode(
                            x=alt.X("delta:Q", title="Thay đổi số bài (Δ)"),
                            y=alt.Y("category_display:N", title=None, sort="-x"),
                            color=alt.condition(
                                "datum.delta >= 0",
                                alt.value(TEAL),
                                alt.value(RED),
                            ),
                            tooltip=[
                                alt.Tooltip("category_display:N", title="Chủ đề"),
                                alt.Tooltip("previous:Q", title="Cửa sổ trước", format=",.0f"),
                                alt.Tooltip("current:Q", title="Cửa sổ hiện tại", format=",.0f"),
                                alt.Tooltip("delta:Q", title="Biến động (Δ)", format="+,.0f"),
                                alt.Tooltip("growth_pct:Q", title="Tăng trưởng %", format=".1f"),
                                alt.Tooltip("status:N", title="Trạng thái"),
                            ],
                        )
                        .properties(height=260)
                        .configure_view(strokeOpacity=0)
                        .configure_axis(labelColor=SLATE, titleColor=NAVY, gridColor="#E2E8F0")
                    )
                    st.altair_chart(trend_chart, use_container_width=True)
                else:
                    st.info("ℹ️ Chưa có đủ dữ liệu để so sánh biến động chủ đề trong hai cửa sổ 6 giờ.")

            st.divider()

            # -----------------------------------------------------
            # KEYWORDS
            # -----------------------------------------------------
            st.markdown('<div class="section-tag">TỪ KHÓA BÀI VIẾT</div>', unsafe_allow_html=True)
            st.subheader("Top từ khóa xuất hiện nhiều nhất")
            st.caption(
                "Tần suất xuất hiện (document frequency) trên văn bản đã chuẩn hóa (`clean_text`) của các bài báo đang lọc. "
                "Đây là tín hiệu mô tả thống kê, không phải mô hình phân loại chủ đề chuyên sâu."
            )

            keyword_data = [
                {"keyword": row[0], "articles": row[1]}
                for row in _keyword_rows(
                    connection,
                    dataset_id,
                    source,
                    category,
                    sentiment,
                    search,
                    date_start,
                    date_end,
                )[:15]
            ]

            if keyword_data:
                kw_df = pd.DataFrame(keyword_data)
                kw_chart = (
                    alt.Chart(kw_df)
                    .mark_bar(color=TEAL, cornerRadiusEnd=4)
                    .encode(
                        x=alt.X("articles:Q", title="Số bài báo chứa từ khóa", axis=alt.Axis(format=",.0f")),
                        y=alt.Y("keyword:N", title=None, sort="-x"),
                        tooltip=[
                            alt.Tooltip("keyword:N", title="Từ khóa"),
                            alt.Tooltip("articles:Q", title="Số bài xuất hiện", format=",.0f"),
                        ],
                    )
                    .properties(height=320)
                    .configure_view(strokeOpacity=0)
                    .configure_axis(labelColor=SLATE, titleColor=NAVY, gridColor="#E2E8F0")
                )
                st.altair_chart(kw_chart, use_container_width=True)
            else:
                st.info("ℹ️ Không tìm thấy từ khóa nào phù hợp với bộ lọc hiện tại.")

        # =========================================================
        # TAB 2: KHÁM PHÁ BÀI BÁO
        # =========================================================
        with articles_tab:
            st.markdown('<div class="section-tag">DANH SÁCH BÀI VIẾT</div>', unsafe_allow_html=True)
            st.subheader("Khám phá danh sách bài báo")
            st.caption("Truy vấn trực tiếp và phân trang an toàn từ SQLite; tiêu đề và nội dung được xử lý chống chèn mã độc.")

            ctrl_c1, ctrl_c2, ctrl_c3 = st.columns([2, 2, 4])
            with ctrl_c1:
                page_size = st.selectbox("Số bài mỗi trang", [10, 25, 50], index=1, key="article_page_size")
            total_pages = max(1, (total + page_size - 1) // page_size)
            with ctrl_c2:
                page = st.number_input(
                    f"Trang (1–{total_pages})",
                    min_value=1,
                    max_value=total_pages,
                    value=1,
                    step=1,
                    key="article_page",
                )
            with ctrl_c3:
                start_record = (int(page) - 1) * page_size + 1 if total > 0 else 0
                end_record = min(total, int(page) * page_size)
                st.markdown(
                    f"<div style='padding-top:1.85rem;color:#64748b;font-size:0.88rem;font-weight:500;'>"
                    f"Hiển thị bài <b>{_format_number(start_record)} – {_format_number(end_record)}</b> trên tổng số <b>{_format_number(total)}</b> bài "
                    f"· Trang <b>{int(page)} / {total_pages}</b>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

            offset = (int(page) - 1) * page_size
            article_rows = connection.execute(
                f"SELECT article_id, title, source, category, event_date, sentiment_label, canonical_url "
                f"FROM articles{where} ORDER BY event_date DESC, article_id LIMIT ? OFFSET ?",
                args + [page_size, offset],
            ).fetchall()

            if not article_rows:
                st.info(
                    "🔍 Không tìm thấy bài báo nào phù hợp với bộ lọc hiện tại. "
                    "Vui lòng nới lỏng từ khóa hoặc nhấn 'Đặt lại bộ lọc' trên thanh bên."
                )
            else:
                table_rows = []
                for article_id, title, row_source, row_category, event_date, row_sentiment, canonical_url in article_rows:
                    table_rows.append(
                        {
                            "Tiêu đề": title or "(Không có tiêu đề)",
                            "Nguồn": SOURCE_LABELS.get(row_source, row_source or "Chưa rõ"),
                            "Chủ đề": CATEGORY_LABELS.get(row_category, row_category or "Khác"),
                            "Ngày đăng": _format_date(event_date),
                            "Sắc thái": SENTIMENT_LABELS.get(row_sentiment, "Chưa có"),
                            "Mở bài": _safe_url(canonical_url),
                        }
                    )
                st.dataframe(
                    table_rows,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Tiêu đề": st.column_config.TextColumn("Tiêu đề", width="large"),
                        "Mở bài": st.column_config.LinkColumn("Mở bài", validate="^https?://"),
                    },
                )
                st.caption(f"Đang hiển thị {len(table_rows)} bài · Trang {int(page)}/{total_pages}.")

        # =========================================================
        # TAB 3: CHẤT LƯỢNG DỮ LIỆU
        # =========================================================
        with quality_tab:
            st.markdown('<div class="section-tag">ĐỘ TIN CẬY & GIỚI HẠN</div>', unsafe_allow_html=True)
            st.subheader("Chất lượng dữ liệu và Giới hạn hệ thống")
            st.caption(
                "Các chỉ số đánh giá độ hoàn chỉnh của dữ liệu thu thập và tính minh bạch về giới hạn của mô hình."
            )

            missing_date = connection.execute(
                f"SELECT COUNT(*) FROM articles{where} AND event_date IS NULL",
                args,
            ).fetchone()[0]
            missing_prediction = total - predicted

            q1, q2, q3 = st.columns(3)
            q1.metric("Trạng thái Coverage", coverage_status or "unknown")
            q2.metric("Thiếu ngày đăng", _format_number(missing_date))
            q3.metric("Chưa có dự đoán", _format_number(missing_prediction))
            st.caption(
                f"Ngày đăng của toàn dataset (giờ Việt Nam): {_format_date(min_date)} → {_format_date(max_date)}. "
                "Đây không phải khoảng thời gian thu thập RSS hoặc bằng chứng coverage đã đủ."
            )

            st.markdown("#### 🔬 Giới hạn và Độ tin cậy của Mô hình")
            st.warning(
                f"⚠️ **Model đang dùng: `{active_model}`** — **Model thử nghiệm, chưa được đánh giá trên tập tin tức độc lập do người gán nhãn.**\n\n"
                f"- **Tỷ lệ có dự đoán:** `{prediction_rate:.1%}` ({predicted}/{total} bài). "
                "Con số này chỉ phản ánh độ phủ gán nhãn kỹ thuật (prediction coverage), **tuyệt đối không phải độ chính xác (accuracy)** của mô hình.\n"
                "- **Phạm vi phân tích:** Sắc thái mô tả nội dung tiêu đề và đoạn trích mô tả của bài báo từ nguồn RSS, "
                "không phân tích toàn văn và **không đại diện cho cảm xúc độc giả** hay thái độ của công chúng."
            )

            st.markdown("#### 📡 Đánh giá Tính hoàn chỉnh (Coverage)")
            if coverage_status != "ok":
                st.warning(
                    f"⚠️ **Coverage hiện tại là `{coverage_status}`:**\n\n"
                    "- Chưa đồng thời đáp ứng tối thiểu 14 ngày và tỷ lệ slot thu thập thành công >= 95% cho mỗi feed.\n"
                    "- Khoảng trống dữ liệu do lỗi mạng hoặc gián đoạn feed **không được xem là 0 bài**.\n"
                    "- Do đó, không kết luận xu hướng tăng hoặc giảm là đáng tin cậy.\n"
                    + (f"- **Đường dẫn manifest:** `{coverage_manifest_path}`" if coverage_manifest_path else "")
                )
            else:
                st.success("✅ Trạng thái coverage đạt chuẩn (`ok`). Dữ liệu quan sát đáp ứng yêu cầu tính toán.")

            st.markdown("#### 📋 Chi tiết Tình trạng Feed Nguồn (Source Health)")
            health_columns = {row[1] for row in connection.execute("PRAGMA table_info(source_health)")}
            if "feed_id" in health_columns:
                health_query = (
                    "SELECT source, feed_id, status, last_attempt_at, last_success_at, "
                    "latest_event_date, article_count FROM source_health "
                    "WHERE dataset_id = ? ORDER BY source, feed_id"
                )
            else:
                health_query = (
                    "SELECT source, NULL, status, last_attempt_at, last_success_at, "
                    "latest_event_date, article_count FROM source_health "
                    "WHERE dataset_id = ? ORDER BY source"
                )
            health_data = []
            for row_source, feed_id, status, last_attempt, last_success, latest_event_date, article_count in connection.execute(
                health_query, (dataset_id,)
            ):
                health_data.append(
                    {
                        "Nguồn": SOURCE_LABELS.get(row_source, row_source or "Chưa rõ"),
                        "Feed": feed_id or "Chưa có",
                        "Trạng thái": status or "unknown",
                        "Lần thử gần nhất": _format_timestamp(last_attempt),
                        "Lần thành công gần nhất": _format_timestamp(last_success),
                        "Bài mới nhất": _format_date(latest_event_date),
                        "Số bài": _format_number(article_count),
                    }
                )
            if health_data:
                st.dataframe(health_data, use_container_width=True, hide_index=True)
            else:
                st.info("ℹ️ Snapshot chưa có dữ liệu health cho dataset này.")

            st.caption(
                f"Model version đọc trực tiếp từ snapshot: `{active_model}`. "
                "Không tự động thay đổi model hay sửa đổi dữ liệu."
            )

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
