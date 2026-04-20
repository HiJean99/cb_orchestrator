from __future__ import annotations

import pytest

from cb_orchestrator import calendar_utils
from cb_orchestrator.calendar_utils import TradingCalendar


def test_next_after_uses_exchange_calendar_fallback(monkeypatch: pytest.MonkeyPatch):
    calendar = TradingCalendar(("2026-04-17", "2026-04-20"))
    monkeypatch.setattr(
        calendar_utils,
        "_next_session_from_exchange_calendar",
        lambda date_value: "2026-04-21" if date_value == "2026-04-20" else None,
    )

    assert calendar.next_after("2026-04-20") == "2026-04-21"
