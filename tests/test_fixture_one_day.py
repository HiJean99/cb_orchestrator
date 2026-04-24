from __future__ import annotations

from pathlib import Path

from cb_orchestrator.notion_sync import sync_run_to_notion
from cb_orchestrator.planner import plan_next_trade
from tests.fixture_support import FakeNotionClient, build_one_day_config, copy_one_day_fixture, write_one_day_env_file


def _block_plain_texts(blocks: list[dict]) -> list[str]:
    lines: list[str] = []
    for block in blocks:
        block_type = block["type"]
        rich_text = block.get(block_type, {}).get("rich_text", [])
        lines.append("".join(item.get("plain_text", "") for item in rich_text))
    return lines


def test_one_day_fixture_drives_planner_outputs(tmp_path: Path) -> None:
    case_root = copy_one_day_fixture(tmp_path)
    config = build_one_day_config(case_root)

    summary = plan_next_trade(config)
    by_instrument = {item["instrument"]: item for item in summary["orders"]}

    assert summary["run_id"] == "20260421_150716"
    assert summary["signal_date"] == "2026-04-20"
    assert summary["trade_date"] == "2026-04-21"
    assert summary["bootstrap"] is False
    assert summary["buy_count"] == 3
    assert summary["sell_count"] == 3
    assert summary["hold_count"] == 2
    assert summary["watch_count"] == 2
    assert by_instrument["SZ128125"]["strategy_action"] == "hold"
    assert by_instrument["SH113584"]["strategy_action"] == "hold"
    assert by_instrument["SH118057"]["strategy_action"] == "sell"
    assert by_instrument["SZ123092"]["strategy_action"] == "sell"
    assert by_instrument["SZ127067"]["strategy_action"] == "sell"
    assert by_instrument["SZ123064"]["strategy_action"] == "buy"
    assert by_instrument["SZ127025"]["strategy_action"] == "buy"
    assert by_instrument["SZ127017"]["strategy_action"] == "buy"
    assert by_instrument["SZ128134"]["strategy_reason"] == "deferred_drop"
    assert by_instrument["SH110073"]["strategy_reason"] == "deferred_entry"
    assert Path(summary["json_path"]).exists()
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["html_path"]).exists()


def test_one_day_fixture_drives_notion_sync_outputs(tmp_path: Path) -> None:
    case_root = copy_one_day_fixture(tmp_path)
    config = build_one_day_config(case_root)
    client = FakeNotionClient()

    summary = sync_run_to_notion(config, client=client)
    notion_summary = summary["notion"]

    assert Path(summary["plan_json_path"]).exists()
    assert Path(summary["ranking_csv_path"]).exists()
    assert notion_summary["positions_created"] == 6
    assert notion_summary["orders_created"] == 10
    assert notion_summary["decision_latest_updates"] == 0
    assert notion_summary["order_latest_updates"] == 0

    decision_pages = client.pages_for_database("decision-days-db")
    assert len(decision_pages) == 1
    decision_page = decision_pages[0]
    assert decision_page["properties"]["Decision Status"]["select"]["name"] == "review_pending"
    assert decision_page["properties"]["Buy Count"]["number"] == 3
    assert decision_page["properties"]["Sell Count"]["number"] == 3
    assert decision_page["properties"]["Keep Count"]["number"] == 2
    assert decision_page["properties"]["Watch Count"]["number"] == 2
    assert "华阳转债(SZ128125)" in decision_page["properties"]["Top 6 Summary"]["rich_text"][0]["plain_text"]
    assert client.list_block_children(decision_page["id"])

    order_pages = client.pages_for_database("plan-orders-db")
    assert len(order_pages) == 10
    order_pages_by_instrument = {
        page["properties"]["Instrument"]["rich_text"][0]["plain_text"]: page for page in order_pages
    }
    assert order_pages_by_instrument["SZ123064"]["properties"]["Action"]["select"]["name"] == "buy"
    assert order_pages_by_instrument["SZ123064"]["properties"]["Action Color"]["select"]["name"] == "buy"
    assert order_pages_by_instrument["SH118057"]["properties"]["Action"]["select"]["name"] == "sell"
    assert order_pages_by_instrument["SH118057"]["properties"]["Action Color"]["select"]["name"] == "sell"
    assert order_pages_by_instrument["SZ127017"]["properties"]["Portfolio Move"]["select"]["name"] == "enter"
    assert order_pages_by_instrument["SZ128134"]["properties"]["Portfolio Move"]["select"]["name"] == "keep"
    assert order_pages_by_instrument["SH110073"]["properties"]["Action"]["select"]["name"] == "watch"
    assert order_pages_by_instrument["SH110073"]["properties"]["Action Color"]["select"]["name"] == "watch"
    assert order_pages_by_instrument["SH110073"]["properties"]["Portfolio Move"]["select"]["name"] == "ignore"
    assert order_pages_by_instrument["SZ123064"]["properties"]["Bond Name"]["rich_text"][0]["plain_text"] == "万孚转债"


def test_one_day_fixture_refreshes_existing_notion_page_bodies(tmp_path: Path) -> None:
    case_root_first = copy_one_day_fixture(tmp_path / "first")
    config_first = build_one_day_config(case_root_first)
    client = FakeNotionClient()

    first_summary = sync_run_to_notion(config_first, client=client)
    first_decision_page_id = first_summary["notion"]["decision_day_page_id"]
    first_blocks = _block_plain_texts(client.list_block_children(first_decision_page_id))
    assert any(str(case_root_first) in line for line in first_blocks)

    case_root_second = copy_one_day_fixture(tmp_path / "second")
    config_second = build_one_day_config(case_root_second)
    second_summary = sync_run_to_notion(config_second, client=client)
    second_decision_page_id = second_summary["notion"]["decision_day_page_id"]
    second_blocks = _block_plain_texts(client.list_block_children(second_decision_page_id))

    assert first_decision_page_id == second_decision_page_id
    assert any(str(case_root_second) in line for line in second_blocks)
    assert all(str(case_root_first) not in line for line in second_blocks)
    assert second_blocks.count("Decision Summary") == 1


def test_write_one_day_env_file_can_overlay_real_notion_keys(tmp_path: Path) -> None:
    case_root = copy_one_day_fixture(tmp_path)
    base_env = tmp_path / "base.env"
    base_env.write_text(
        "\n".join(
            [
                "NOTION_TOKEN=real-token",
                "NOTION_VERSION=2022-06-28",
                "NOTION_DAILY_HOLDINGS_DB_ID=real-holdings",
                "NOTION_DECISION_DAYS_DB_ID=real-decision-days",
                "",
            ]
        ),
        encoding="utf-8",
    )

    env_file = write_one_day_env_file(case_root, base_env_file=base_env)
    payload = env_file.read_text(encoding="utf-8")

    assert "NOTION_TOKEN=real-token" in payload
    assert "NOTION_DAILY_HOLDINGS_DB_ID=real-holdings" in payload
    assert "NOTION_DECISION_DAYS_DB_ID=real-decision-days" in payload
    assert "PLAN_INPUT_ROOT=" in payload
