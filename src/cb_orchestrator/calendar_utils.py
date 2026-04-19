from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from pathlib import Path


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
        index = bisect_left(self.dates, date_value)
        return index < len(self.dates) and self.dates[index] == date_value

    def next_after(self, date_value: str) -> str:
        index = bisect_right(self.dates, date_value)
        if index >= len(self.dates):
            raise ValueError(f"no next trading day after {date_value}")
        return self.dates[index]

    def previous_before(self, date_value: str) -> str:
        index = bisect_left(self.dates, date_value) - 1
        if index < 0:
            raise ValueError(f"no previous trading day before {date_value}")
        return self.dates[index]

    def first_of_month(self, date_value: str) -> str:
        month_prefix = date_value[:7]
        for item in self.dates:
            if item.startswith(month_prefix):
                return item
        raise ValueError(f"no trading day found in month {month_prefix}")
