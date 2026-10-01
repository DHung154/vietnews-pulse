"""Pure time-window helpers shared by Spark analytics and serving UI."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def completed_window_bounds(as_of: datetime | None = None) -> tuple[str, str, str]:
    """Return previous/current six-hour windows ending at a closed VN boundary."""
    anchor = (as_of or datetime.now(timezone.utc)).astimezone(VIETNAM_TZ)
    end_local = anchor.replace(hour=(anchor.hour // 6) * 6, minute=0, second=0, microsecond=0)
    current_start = end_local - timedelta(hours=6)
    previous_start = end_local - timedelta(hours=12)
    return tuple(item.astimezone(timezone.utc).isoformat() for item in (previous_start, current_start, end_local))
