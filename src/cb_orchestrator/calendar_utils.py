from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from pathlib import Path


def normalize_date_value(date_value: str) -> str:
    normalized = str(date_value).strip()
    if len(normalized) == 8 and normalized.isdigit():
        return f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}"
    return normalized


def _next_session_from_exchange_calendar(date_value: str) -> str | None:
    try:
        import pandas as pd
        import exchange_calendars as xcals
    except ImportError:
        return None

    start = pd.Timestamp(normalize_date_value(date_value))
    end = start + pd.Timedelta(days=30)
    calendar = xcals.get_calendar("XSHG")
    sessions = calendar.sessions_in_range(start, end)
    for session in sessions:
        session_date = session.strftime("%Y-%m-%d")
        if session_date > normalize_date_value(date_value):
            return session_date
    return None


@dataclass(frozen=True)
class TradingCalendar:
    dates: tuple[str, ...]

    @classmethod
    def from_path(cls, calendar_path: Path) -> "TradingCalendar":
        if not calendar_path.exists():
            raise FileNotFoundError(f"trade calendar not found: {calendar_path}")
        dates = tuple(line.strip() for line in calendar_path.read_text(encoding="utf-8").splitlines() if line.strip())
        if not dates:
            raise ValueError(f"trade calendar is empty: {calendar_path}")
        return cls(dates=dates)

    def contains(self, date_value: str) -> bool:
        normalized = normalize_date_value(date_value)
        index = bisect_left(self.dates, normalized)
        return index < len(self.dates) and self.dates[index] == normalized

    def next_after(self, date_value: str) -> str:
        normalized = normalize_date_value(date_value)
        index = bisect_right(self.dates, normalized)
        if index >= len(self.dates):
            fallback = _next_session_from_exchange_calendar(date_value)
            if fallback:
                return fallback
            raise ValueError(f"no next trading day after {normalized}")
        return self.dates[index]

    def previous_before(self, date_value: str) -> str:
        normalized = normalize_date_value(date_value)
        index = bisect_left(self.dates, normalized) - 1
        if index < 0:
            raise ValueError(f"no previous trading day before {normalized}")
        return self.dates[index]

    def first_of_month(self, date_value: str) -> str:
        month_prefix = normalize_date_value(date_value)[:7]
        for item in self.dates:
            if item.startswith(month_prefix):
                return item
        raise ValueError(f"no trading day found in month {month_prefix}")
