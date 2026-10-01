"""Read-only presentation dashboard for one immutable serving snapshot."""

from __future__ import annotations

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

NAVY = "#102A43"
TEAL = "#0F766E"
SLATE = "#64748B"
RED = "#DC2626"
AMBER = "#B45309"
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
        :root { --navy: #102a43; --teal: #0f766e; --mist: #f5f8fb; }
        .stApp { background: #f7fafc; }
        [data-testid="stHeader"] { background: rgba(247,250,252,.92); }
        [data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid #e2e8f0; }
        [data-testid="stSidebar"] > div:first-child { padding-top: 1.35rem; }
        .block-container { max-width: 1420px; padding: 1.4rem 2.2rem 3rem; }
        .hero {
            background: linear-gradient(120deg, #102a43 0%, #155e75 58%, #0f766e 100%);
            border-radius: 18px; padding: 1.55rem 1.8rem 1.35rem; margin-bottom: 1rem;
            box-shadow: 0 10px 28px rgba(16,42,67,.16);
        }
        .hero h1 { color: #ffffff; font-size: clamp(1.75rem, 3vw, 2.5rem); line-height: 1.12; margin: 0 0 .45rem; }
        .hero p { color: #d9f0f1; font-size: 1rem; margin: 0; max-width: 780px; }
        .section-kicker { color: #0f766e; font-size: .75rem; font-weight: 700; letter-spacing: .11em; text-transform: uppercase; margin-bottom: -.45rem; }
        [data-testid="stMetric"] { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 14px; padding: .8rem .95rem; box-shadow: 0 4px 14px rgba(15,23,42,.04); }
        [data-testid="stMetricLabel"] { color: #64748b; }
        [data-testid="stMetricValue"] { color: #102a43; }
        [data-testid="stTabs"] button { font-weight: 650; }
        [data-testid="stExpander"] { border: 1px solid #dbe5ef; border-radius: 12px; background: #ffffff; }
        div[data-testid="stDataFrame"] { border-radius: 10px; }
        @media (max-width: 900px) {
            .block-container { padding-left: 1rem; padding-right: 1rem; }
            .hero { padding: 1.2rem; }
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


def _event_bulletin_path() -> Path:
    configured = os.environ.get("NEWS_EVENT_BULLETIN_PATH")
    if configured:
        path = Path(configured)
        return path if path.is_absolute() else Path(__file__).resolve().parents[1] / path
    return Path(__file__).resolve().parents[1] / "evidence" / "event_bulletin_v1_20260918_r2" / "event_bulletin.json"


def _read_event_bulletin(path: Path, snapshot_id: str, dataset_id: str) -> tuple[dict | None, str | None]:
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"Không đọc được artifact Bản tin biến động: {exc}"
    if artifact.get("snapshot_id") != snapshot_id:
        return None, "Artifact Bản tin biến động không cùng snapshot với dashboard."
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
                "Nguồn": row.get("source") or "Chưa rõ",
                "Chủ đề": row.get("category") or "Chưa phân loại",
                "Tiêu đề": row.get("title") or "(Không có tiêu đề)",
                "Sentiment": SENTIMENT_LABELS.get(row.get("sentiment_label"), "Chưa có"),
                "Mở bài": _safe_url(row.get("canonical_url")),
            }
        )
    return result


def main() -> None:
    import altair as alt
    import streamlit as st

    st.set_page_config(
        page_title="Theo dõi xu hướng tin tức Việt Nam",
        page_icon="📰",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _style(st)

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

    published_at, model_version, previous_start, current_start, window_end, coverage_status, coverage_manifest_path = meta
    window_bounds = (previous_start, current_start, window_end)
    if not all(window_bounds):
        window_bounds = completed_window_bounds()
    active_model = model_version or pointer_model or "unavailable"

    dataset_index = datasets.index("rss_live_v2") if "rss_live_v2" in datasets else 0
    st.sidebar.markdown("### Bộ lọc")
    st.sidebar.caption("Thu hẹp phạm vi phân tích mà không thay đổi snapshot.")
    dataset_id = st.sidebar.selectbox("Dataset", datasets, index=dataset_index)
    min_date, max_date = connection.execute(
        "SELECT MIN(event_date), MAX(event_date) FROM articles WHERE dataset_id = ?",
        (dataset_id,),
    ).fetchone()
    date_value = None
    if min_date and max_date:
        date_value = st.sidebar.date_input(
            "Khoảng ngày (giờ Việt Nam)",
            value=(date.fromisoformat(min_date), date.fromisoformat(max_date)),
            min_value=date.fromisoformat(min_date),
            max_value=date.fromisoformat(max_date),
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
    source = st.sidebar.selectbox("Nguồn báo", ["Tất cả"] + sources)
    category = st.sidebar.selectbox("Chủ đề", ["Tất cả"] + categories)
    sentiment = st.sidebar.selectbox(
        "Sắc thái bài báo",
        ["Tất cả"] + sentiments,
        format_func=lambda value: SENTIMENT_LABELS.get(value, value),
    )
    search = st.sidebar.text_input("Tìm trong tiêu đề / mô tả", placeholder="Ví dụ: AI, kinh tế, bóng đá")
    st.sidebar.divider()
    st.sidebar.caption("Mặc định: dataset rss_live_v2. Các biểu đồ chỉ phản ánh dữ liệu đã có trong snapshot.")

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
    prediction_rate = predicted / total if total else 0

    st.markdown(
        """
        <div class="hero">
          <div class="section-kicker" style="color:#a7f3d0;">NEWS INTELLIGENCE · VIETNAM</div>
          <h1>Theo dõi xu hướng tin tức Việt Nam</h1>
          <p>Khám phá nhịp độ xuất bản, chủ đề nổi bật và sắc thái nội dung bài báo từ các nguồn RSS.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(
        f"Cập nhật snapshot {_format_timestamp(published_at)} · dataset {dataset_id} · "
        f"snapshot {snapshot_id}"
    )
    if active_model.startswith("tfidf_lr_weak_"):
        st.warning(
            f"Model đang dùng: {active_model}. Đây là model thử nghiệm trên nhãn yếu do LLM, "
            "chưa được đánh giá trên tập tin tức do người gán nhãn."
        )
    elif active_model == "unavailable":
        st.error("Không đọc được model version từ snapshot; không nên diễn giải kết quả sentiment.")
    else:
        st.info(
            f"Model đang dùng: {active_model}. Chất lượng model trên news gold human chưa được xác nhận."
        )
    if coverage_status != "ok":
        st.warning(
            f"Coverage: {coverage_status}. Khoảng trống nguồn không được coi là 0 bài."
            + (
                f" Manifest: {coverage_manifest_path}"
                if coverage_manifest_path
                else " Snapshot chưa gắn manifest coverage."
            )
        )

    kpi_1, kpi_2, kpi_3, kpi_4 = st.columns(4)
    kpi_1.metric("Tổng bài trong bộ lọc", _format_number(total))
    kpi_2.metric("Nguồn báo", _format_number(sources_count))
    kpi_3.metric("Chủ đề", _format_number(topics_count))
    kpi_4.metric(
        "Đã có dự đoán",
        f"{prediction_rate:.1%}",
        help="Tỷ lệ bản ghi có sentiment_label, không phải độ chính xác của model.",
    )

    with st.expander("Thông tin xử lý"):
        info_left, info_right = st.columns(2)
        info_left.markdown(
            f"""
            **Snapshot ID:** {snapshot_id}  
            **Dataset:** {dataset_id}  
            **Model version:** {active_model}  
            **Published:** {_format_timestamp(published_at)}
            """
        )
        info_right.markdown(
            f"""
            **Trend window (giờ Việt Nam):** {_format_timestamp(window_bounds[0])} → {_format_timestamp(window_bounds[2])}  
            **Mốc cửa sổ hiện tại (giờ Việt Nam):** {_format_timestamp(window_bounds[1])}  
            **Timezone hiển thị:** Asia/Ho_Chi_Minh (UTC+07:00)  
            **Coverage evidence:** {coverage_manifest_path or "Chưa có"}
            """
        )

    overview_tab, articles_tab, quality_tab = st.tabs(
        ["Tổng quan", "Khám phá bài báo", "Chất lượng dữ liệu"]
    )

    with overview_tab:
        st.markdown('<div class="section-kicker">TỔNG QUAN</div>', unsafe_allow_html=True)
        st.subheader("Bản tin biến động")
        st.caption(
            "Tối đa 5 ứng viên nổi bật từ hai cửa sổ 6 giờ đã kết thúc. "
            "Tên sự kiện dùng tiêu đề bài đại diện; nhóm sự kiện vẫn đang thử nghiệm."
        )
        bulletin_bounds = _trend_window_bounds(window_bounds, date_start, date_end)
        bulletin_artifact, bulletin_error = _read_event_bulletin(
            _event_bulletin_path(),
            snapshot_id,
            dataset_id,
        )
        if bulletin_error:
            st.info(
                f"{bulletin_error} Có thể tiếp tục xem biến động theo chủ đề ở phần bên dưới."
            )
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
                    "Biến động trong dữ liệu đã thu thập. Coverage chưa đủ nên không kết luận tăng hoặc giảm."
                )
            if not bulletin_rows:
                st.info("Chưa có ứng viên sự kiện trong hai cửa sổ và bộ lọc hiện tại.")
            for item in bulletin_rows[:5]:
                with st.container():
                    card_left, card_right = st.columns([3, 1])
                    with card_left:
                        st.write(item["event_name"])
                        if item["status"] == "observed_only":
                            st.caption(
                                f"Quan sát: {item['previous_count']} bài ở cửa sổ trước → "
                                f"{item['current_count']} bài ở cửa sổ hiện tại"
                            )
                        elif item["status"] == "new":
                            st.caption(f"Mới xuất hiện · {item['current_count']} bài")
                        else:
                            st.caption(
                                f"{item['status']} · {item['previous_count']} → "
                                f"{item['current_count']} bài · Δ {item['observed_delta']:+d}"
                            )
                        if item["multi_source"]:
                            st.caption(
                                f"Nhiều nguồn cùng đưa tin: {', '.join(item['sources'])}"
                            )
                        else:
                            st.caption(
                                f"Một nguồn trong cửa sổ hiện tại: "
                                f"{', '.join(item['sources']) or 'chưa rõ'}"
                            )
                    with card_right:
                        st.metric("Bài hiện tại", _format_number(item["current_count"]))
                        st.metric("Số nguồn", _format_number(item["source_count"]))
                    with st.expander("Xem diễn biến và bài bằng chứng"):
                        st.caption(
                            f"Xuất hiện đầu tiên trong evidence: {_format_timestamp(item['first_seen_at'])}. "
                            "Sentiment chỉ là thông tin bổ sung từ model thử nghiệm."
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
                                    "Mở bài": st.column_config.LinkColumn(
                                        "Mở bài",
                                        validate="^https?://",
                                    ),
                                },
                            )
                        else:
                            st.info("Không còn bài bằng chứng trong bộ lọc hiện tại.")
                        if item["review_status"] != "human_reviewed":
                            st.caption(
                                "Nhóm sự kiện do thuật toán đề xuất, chưa được reviewer chấp nhận."
                            )

        st.subheader("Nhịp độ xuất bản")
        st.caption("Số bài theo ngày Việt Nam; áp dụng dataset, khoảng ngày, nguồn, chủ đề, sentiment và tìm kiếm.")
        volume_rows = [
            {"event_date": row[0], "count": row[1]}
            for row in connection.execute(
                f"SELECT event_date, COUNT(*) FROM articles{where} "
                "GROUP BY event_date ORDER BY event_date",
                args,
            )
        ]
        if volume_rows:
            volume_chart = _chart(
                alt,
                volume_rows,
                {"type": "line", "point": {"filled": True, "size": 55}, "color": TEAL},
                {
                    "x": alt.X("event_date:T", title="Ngày (giờ Việt Nam)"),
                    "y": alt.Y("count:Q", title="Số bài", axis=alt.Axis(format=",.0f")),
                    "tooltip": [
                        alt.Tooltip("event_date:T", title="Ngày", format="%d/%m/%Y"),
                        alt.Tooltip("count:Q", title="Số bài", format=",.0f"),
                    ],
                },
                285,
            )
            st.altair_chart(volume_chart, use_container_width=True)
        else:
            st.info("Không có dữ liệu trong bộ lọc hiện tại.")

        sentiment_rows = [
            {"sentiment": row[0], "label": SENTIMENT_LABELS.get(row[0], row[0]), "count": row[1]}
            for row in connection.execute(
                f"SELECT sentiment_label, COUNT(*) FROM articles{where} "
                "AND sentiment_label IS NOT NULL GROUP BY sentiment_label",
                args,
            )
        ]
        trend_bounds = _trend_window_bounds(window_bounds, date_start, date_end)
        trend_rows = _aggregate_trends(
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
        sentiment_col, trend_col = st.columns(2, gap="large")
        with sentiment_col:
            st.subheader("Sắc thái nội dung")
            st.caption("Phân bố nhãn trên bài báo, không phải cảm xúc độc giả.")
            if sentiment_rows:
                sentiment_chart = _chart(
                    alt,
                    sentiment_rows,
                    {"type": "bar", "cornerRadiusTopLeft": 5, "cornerRadiusTopRight": 5},
                    {
                        "x": alt.X("count:Q", title="Số bài", axis=alt.Axis(format=",.0f")),
                        "y": alt.Y(
                            "label:N",
                            title=None,
                            sort=[SENTIMENT_LABELS.get(item, item) for item in sentiments],
                        ),
                        "color": alt.Color(
                            "sentiment:N",
                            title="Nhãn",
                            scale=alt.Scale(
                                domain=list(SENTIMENT_COLORS),
                                range=list(SENTIMENT_COLORS.values()),
                            ),
                            legend=None,
                        ),
                        "tooltip": [
                            alt.Tooltip("label:N", title="Nhãn"),
                            alt.Tooltip("count:Q", title="Số bài", format=",.0f"),
                        ],
                    },
                    255,
                )
                st.altair_chart(sentiment_chart, use_container_width=True)
            else:
                st.info("Chưa có nhãn sentiment trong bộ lọc.")

        with trend_col:
            st.subheader("Chủ đề tăng / giảm")
            st.caption(
                "So sánh hai cửa sổ 6 giờ đã kết thúc; bảng này dùng bộ lọc hiện tại, "
                "nhưng không thay coverage_status."
            )
            if trend_rows:
                trend_chart = _chart(
                    alt,
                    trend_rows[:15],
                    {"type": "bar", "cornerRadiusEnd": 5},
                    {
                        "x": alt.X("delta:Q", title="Thay đổi số bài"),
                        "y": alt.Y("category:N", title=None, sort="-x"),
                        "color": alt.condition(
                            "datum.delta >= 0",
                            alt.value(TEAL),
                            alt.value(RED),
                        ),
                        "tooltip": [
                            alt.Tooltip("category:N", title="Chủ đề"),
                            alt.Tooltip("previous:Q", title="Cửa sổ trước"),
                            alt.Tooltip("current:Q", title="Cửa sổ hiện tại"),
                            alt.Tooltip("delta:Q", title="Thay đổi"),
                            alt.Tooltip("growth_pct:Q", title="Tăng trưởng %", format=".1f"),
                            alt.Tooltip("status:N", title="Trạng thái"),
                        ],
                    },
                    255,
                )
                st.altair_chart(trend_chart, use_container_width=True)
            else:
                st.info("Chưa đủ dữ liệu cho so sánh trend.")

        st.subheader("Từ khóa nổi bật")
        st.caption("Document frequency trên clean_text của các bài đang lọc; đây là tín hiệu mô tả, không phải model topic.")
        keyword_rows = [
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
        if keyword_rows:
            keyword_chart = _chart(
                alt,
                keyword_rows,
                {"type": "bar", "cornerRadiusEnd": 5, "color": TEAL},
                {
                    "x": alt.X("articles:Q", title="Số bài", axis=alt.Axis(format=",.0f")),
                    "y": alt.Y("keyword:N", title=None, sort="-x"),
                    "tooltip": [
                        alt.Tooltip("keyword:N", title="Từ khóa"),
                        alt.Tooltip("articles:Q", title="Số bài", format=",.0f"),
                    ],
                },
                330,
            )
            st.altair_chart(keyword_chart, use_container_width=True)
        else:
            st.info("Chưa có từ khóa trong bộ lọc.")

    with articles_tab:
        st.markdown('<div class="section-kicker">KHÁM PHÁ</div>', unsafe_allow_html=True)
        st.subheader("Khám phá bài báo")
        st.caption("Chỉ tải trang đang xem từ SQLite; tiêu đề và nội dung được hiển thị như dữ liệu văn bản an toàn.")
        page_size = st.selectbox("Số bài mỗi trang", [10, 25, 50], index=1, key="article_page_size")
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = st.number_input(
            f"Trang (1–{total_pages})",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key="article_page",
        )
        article_rows = connection.execute(
            f"SELECT article_id, title, source, category, event_date, sentiment_label, canonical_url "
            f"FROM articles{where} ORDER BY event_date DESC, article_id LIMIT ? OFFSET ?",
            args + [page_size, (int(page) - 1) * page_size],
        ).fetchall()
        if not article_rows:
            st.info("Không tìm thấy bài báo phù hợp với bộ lọc hiện tại.")
        else:
            table_rows = []
            for article_id, title, row_source, row_category, event_date, row_sentiment, canonical_url in article_rows:
                table_rows.append(
                    {
                        "Tiêu đề": title or "(Không có tiêu đề)",
                        "Nguồn": row_source or "Chưa rõ",
                        "Chủ đề": row_category or "Chưa phân loại",
                        "Ngày đăng": event_date or "Chưa có",
                        "Sentiment": SENTIMENT_LABELS.get(row_sentiment, "Chưa có"),
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
            st.caption(f"Đang hiển thị {len(table_rows)} bài · trang {int(page)}/{total_pages}.")

    with quality_tab:
        st.markdown('<div class="section-kicker">TIN CẬY DỮ LIỆU</div>', unsafe_allow_html=True)
        st.subheader("Chất lượng dữ liệu")
        st.caption("Các chỉ số dưới đây mô tả độ đầy đủ của snapshot; coverage và prediction coverage không phải accuracy.")
        missing_date = connection.execute(
            f"SELECT COUNT(*) FROM articles{where} AND event_date IS NULL",
            args,
        ).fetchone()[0]
        missing_prediction = total - predicted
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Coverage status", coverage_status or "unknown")
        q2.metric("Thiếu ngày đăng", _format_number(missing_date))
        q3.metric("Thiếu dự đoán", _format_number(missing_prediction))
        q4.metric("Khoảng ngày", f"{date_start or '—'} → {date_end or '—'}")
        st.info(
            f"{prediction_rate:.1%} có dự đoán chỉ có nghĩa là {predicted}/{total} bài có nhãn model. "
            "Không được đọc thành độ chính xác 100%."
        )
        if coverage_status != "ok":
            st.warning(
                "Snapshot hiện incomplete: chưa đủ chuỗi quan sát để khẳng định không có bài ở các khoảng trống."
            )
        st.subheader("Tình trạng feed")
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
                    "Nguồn": row_source or "Chưa rõ",
                    "Feed": feed_id or "Chưa có",
                    "Trạng thái": status or "unknown",
                    "Lần thử gần nhất": _format_timestamp(last_attempt),
                    "Lần thành công gần nhất": _format_timestamp(last_success),
                    "Bài mới nhất": latest_event_date or "Chưa có",
                    "Số bài": article_count if article_count is not None else "—",
                }
            )
        if health_data:
            st.dataframe(health_data, use_container_width=True, hide_index=True)
        else:
            st.info("Snapshot chưa có dữ liệu health cho feed này.")
        st.caption(
            f"Model version đọc từ snapshot: {active_model}. "
            "Chưa chạy thay model và chưa đánh dấu D6/D8 nghiệm thu."
        )

    connection.close()


if __name__ == "__main__":
    main()
