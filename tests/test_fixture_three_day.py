from __future__ import annotations

from pathlib import Path

from cb_orchestrator.notion_sync import sync_run_to_notion
from cb_orchestrator.planner import plan_next_trade
from tests.fixture_support import FakeNotionClient, build_three_day_config, copy_three_day_fixture

RUN_IDS = (
    "20260421_150716",
    "20260422_150716",
    "20260423_150716",
)


def _page_by_signal_date(pages: list[dict], signal_date: str) -> dict:
    for page in pages:
        prop = page["properties"]["Signal Date"]
        if prop["date"]["start"] == signal_date:
            return page
    raise AssertionError(f"page not found for signal_date={signal_date}")


def _page_by_plan_key(pages: list[dict], plan_key: str) -> dict:
    for page in pages:
        plain_text = "".join(item["plain_text"] for item in page["properties"]["Plan Key"]["rich_text"])
        if plain_text == plan_key:
            return page
    raise AssertionError(f"page not found for plan_key={plan_key}")


def _block_plain_texts(blocks: list[dict]) -> list[str]:
    lines: list[str] = []
    for block in blocks:
        block_type = block["type"]
        rich_text = block.get(block_type, {}).get("rich_text", [])
        lines.append("".join(item.get("plain_text", "") for item in rich_text))
    return lines


def test_three_day_fixture_drives_planner_outputs_across_runs(tmp_path: Path) -> None:
    case_root = copy_three_day_fixture(tmp_path)
    config = build_three_day_config(case_root)

    day1 = plan_next_trade(config, run_id=RUN_IDS[0])
    day2 = plan_next_trade(config, run_id=RUN_IDS[1])
    day3 = plan_next_trade(config, run_id=RUN_IDS[2])

    day1_by_instrument = {item["instrument"]: item for item in day1["orders"]}
    day2_by_instrument = {item["instrument"]: item for item in day2["orders"]}
    day3_by_instrument = {item["instrument"]: item for item in day3["orders"]}

    assert day1["buy_count"] == 3
    assert day1["sell_count"] == 3
    assert day1["hold_count"] == 2
    assert day1["watch_count"] == 2
    assert day1_by_instrument["SH110073"]["strategy_reason"] == "deferred_entry"
    assert day1_by_instrument["SZ128134"]["strategy_reason"] == "deferred_drop"

    assert day2["buy_count"] == 2
    assert day2["sell_count"] == 2
    assert day2["hold_count"] == 4
    assert day2["watch_count"] == 0
    assert day2_by_instrument["SH110073"]["strategy_action"] == "buy"
    assert day2_by_instrument["SZ123100"]["strategy_action"] == "buy"
    assert day2_by_instrument["SZ127025"]["strategy_action"] == "sell"
    assert day2_by_instrument["SZ128134"]["strategy_action"] == "sell"
    assert day2_by_instrument["SZ123064"]["strategy_action"] == "hold"

    assert day3["buy_count"] == 2
    assert day3["sell_count"] == 2
    assert day3["hold_count"] == 4
    assert day3["watch_count"] == 0
    assert day3["holdings_confirmed_at"] is None
    assert day3_by_instrument["SZ128134"]["strategy_action"] == "buy"
    assert day3_by_instrument["SH118057"]["strategy_action"] == "buy"
    assert day3_by_instrument["SH113584"]["strategy_action"] == "sell"
    assert day3_by_instrument["SZ127017"]["strategy_action"] == "sell"
    assert day3_by_instrument["SH110073"]["strategy_action"] == "hold"

    for payload in (day1, day2, day3):
        assert Path(payload["json_path"]).exists()
        assert Path(payload["csv_path"]).exists()
        assert Path(payload["html_path"]).exists()


def test_three_day_fixture_drives_notion_sync_and_latest_flags(tmp_path: Path) -> None:
    case_root = copy_three_day_fixture(tmp_path)
    config = build_three_day_config(case_root)
    client = FakeNotionClient()

    day1 = sync_run_to_notion(config, run_id=RUN_IDS[0], client=client)
    day2 = sync_run_to_notion(config, run_id=RUN_IDS[1], client=client)
    day3 = sync_run_to_notion(config, run_id=RUN_IDS[2], client=client)

    assert day1["notion"]["positions_created"] == 6
    assert day1["notion"]["orders_created"] == 10
    assert day1["notion"]["decision_latest_updates"] == 0
    assert day1["notion"]["order_latest_updates"] == 0

    assert day2["notion"]["positions_created"] == 6
    assert day2["notion"]["orders_created"] == 8
    assert day2["notion"]["decision_latest_updates"] == 1
    assert day2["notion"]["order_latest_updates"] == 10

    assert day3["notion"]["positions_created"] == 6
    assert day3["notion"]["orders_created"] == 8
    assert day3["notion"]["decision_latest_updates"] == 1
    assert day3["notion"]["order_latest_updates"] == 8

    holdings_pages = client.pages_for_database("holdings-db")
    ranking_pages = client.pages_for_database("rankings-db")
    decision_pages = client.pages_for_database("decision-days-db")
    order_pages = client.pages_for_database("plan-orders-db")
    position_pages = client.pages_for_database("positions-db")

    assert len(holdings_pages) == 3
    assert len(ranking_pages) == 3
    assert len(decision_pages) == 3
    assert len(order_pages) == 26
    assert len(position_pages) == 18

    day1_decision = _page_by_signal_date(decision_pages, "2026-04-20")
    day2_decision = _page_by_signal_date(decision_pages, "2026-04-21")
    day3_decision = _page_by_signal_date(decision_pages, "2026-04-22")

    assert day1_decision["properties"]["Decision Status"]["select"]["name"] == "review_pending"
    assert day2_decision["properties"]["Decision Status"]["select"]["name"] == "review_pending"
    assert day3_decision["properties"]["Decision Status"]["select"]["name"] == "blocked"
    assert day1_decision["properties"]["Is Latest"]["checkbox"] is False
    assert day2_decision["properties"]["Is Latest"]["checkbox"] is False
    assert day3_decision["properties"]["Is Latest"]["checkbox"] is True

    day3_body = _block_plain_texts(client.list_block_children(day3_decision["id"]))
    assert "Decision status: blocked" in day3_body

    day2_buy = _page_by_plan_key(order_pages, "2026-04-22:SH110073")
    assert day2_buy["properties"]["Prev Display Rank"]["number"] == 6.0
    assert day2_buy["properties"]["Rank Delta"]["number"] == 3.0
    assert day2_buy["properties"]["Is Latest"]["checkbox"] is False

    day3_buy = _page_by_plan_key(order_pages, "2026-04-23:SH118057")
    assert day3_buy["properties"]["Prev Display Rank"]["number"] == 8.0
    assert day3_buy["properties"]["Rank Delta"]["number"] == 2.0
    assert day3_buy["properties"]["Is Latest"]["checkbox"] is True

    day3_sell = _page_by_plan_key(order_pages, "2026-04-23:SH113584")
    assert day3_sell["properties"]["Action"]["select"]["name"] == "sell"
    assert day3_sell["properties"]["Action Color"]["select"]["name"] == "sell"
    assert day3_sell["properties"]["Portfolio Move"]["select"]["name"] == "exit"
    assert day3_sell["properties"]["Rank Delta"]["number"] == -5.0
