from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.planner import (
    load_holdings_snapshot,
    load_ranking_snapshot,
    plan_input_paths,
    plan_next_trade,
    plan_output_dir,
    resolve_run_summary,
)

NOTION_API_ROOT = "https://api.notion.com/v1"
MAX_RICH_TEXT_CHARS = 1800
MAX_TITLE_CHARS = 1800


class NotionSyncError(RuntimeError):
    pass


@dataclass(frozen=True)
class NotionResources:
    holdings_snapshots_db_id: str
    holding_positions_db_id: str
    daily_rankings_db_id: str
    decision_days_db_id: str
    plan_orders_db_id: str

    @classmethod
    def from_config(cls, config: OrchestratorConfig) -> "NotionResources":
        if not config.notion_sync_enabled():
            raise NotionSyncError("notion sync is not configured: missing token or database ids")
        return cls(
            holdings_snapshots_db_id=str(config.notion_daily_holdings_db_id),
            holding_positions_db_id=str(config.notion_holding_positions_db_id),
            daily_rankings_db_id=str(config.notion_daily_rankings_db_id),
            decision_days_db_id=str(config.notion_decision_days_db_id),
            plan_orders_db_id=str(config.notion_plan_orders_db_id),
        )


class NotionClient:
    def __init__(self, *, token: str, notion_version: str) -> None:
        self._token = token
        self._notion_version = notion_version

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(
            f"{NOTION_API_ROOT}{path}",
            data=body,
            method=method,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Notion-Version": self._notion_version,
                "Content-Type": "application/json",
            },
        )

        attempts = 0
        while True:
            attempts += 1
            try:
                with request.urlopen(req, timeout=30) as resp:
                    raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
            except error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    details = json.loads(raw)
                except json.JSONDecodeError:
                    details = {"message": raw}
                if exc.code == 429 and attempts < 4:
                    retry_after = exc.headers.get("Retry-After")
                    time.sleep(float(retry_after) if retry_after else 2.0)
                    continue
                raise NotionSyncError(
                    f"notion api {method} {path} failed: status={exc.code} message={details.get('message', raw)}"
                ) from exc
            except error.URLError as exc:
                if attempts < 4:
                    time.sleep(1.0)
                    continue
                raise NotionSyncError(f"notion api {method} {path} failed: {exc}") from exc

    def query_database(
        self,
        database_id: str,
        *,
        filter_: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"page_size": page_size}
        if filter_:
            payload["filter"] = filter_
        if sorts:
            payload["sorts"] = sorts

        results: list[dict[str, Any]] = []
        next_cursor: str | None = None
        while True:
            if next_cursor:
                payload["start_cursor"] = next_cursor
            data = self._request("POST", f"/databases/{database_id}/query", payload)
            results.extend(data.get("results", []))
            if not data.get("has_more"):
                return results
            next_cursor = data.get("next_cursor")

    def create_page(
        self,
        *,
        database_id: str,
        properties: dict[str, Any],
        children: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "parent": {"database_id": database_id},
            "properties": properties,
        }
        if children:
            payload["children"] = children
        return self._request("POST", "/pages", payload)

    def update_page(
        self,
        page_id: str,
        *,
        properties: dict[str, Any] | None = None,
        archived: bool | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if properties is not None:
            payload["properties"] = properties
        if archived is not None:
            payload["archived"] = archived
        return self._request("PATCH", f"/pages/{page_id}", payload)

    def list_block_children(self, block_id: str) -> list[dict[str, Any]]:
        data = self._request("GET", f"/blocks/{block_id}/children?page_size=100")
        return data.get("results", [])

    def append_block_children(self, block_id: str, children: list[dict[str, Any]]) -> dict[str, Any]:
        return self._request("PATCH", f"/blocks/{block_id}/children", {"children": children})

    def update_block(self, block_id: str, *, archived: bool | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if archived is not None:
            payload["archived"] = archived
        return self._request("PATCH", f"/blocks/{block_id}", payload)


def sync_run_to_notion(
    config: OrchestratorConfig,
    *,
    run_id: str | None = None,
    trade_date: str | None = None,
    client: NotionClient | None = None,
) -> dict[str, Any]:
    resources = NotionResources.from_config(config)
    notion_client = client or NotionClient(token=str(config.notion_token), notion_version=config.notion_version)

    summary = resolve_run_summary(config, run_id=run_id, trade_date=trade_date)
    if summary.get("status") != "success":
        raise NotionSyncError(f"run is not successful: {summary.get('run_id')} status={summary.get('status')}")

    resolved_run_id = _require_text(summary, "run_id", label="run summary")
    signal_date = _require_text(summary, "signal_date", label="run summary")
    resolved_trade_date = _require_text(summary, "trade_date", label="run summary")
    holdings_path, ranking_path = plan_input_paths(config, signal_date)

    raw_holdings = _load_json_object(holdings_path)
    raw_ranking = _load_json_object(ranking_path)
    holdings = load_holdings_snapshot(holdings_path)
    ranking = load_ranking_snapshot(ranking_path)
    ranking_csv_path = _write_ranking_snapshot_csv(ranking_path, raw_ranking)
    plan = _load_or_build_plan(config, run_id=resolved_run_id, trade_date=resolved_trade_date)
    previous_rank_map = _load_previous_rank_map(config, signal_date)
    bond_name_map = _load_bond_name_map(config)

    holding_page = _sync_holdings_snapshot(
        client=notion_client,
        database_id=resources.holdings_snapshots_db_id,
        raw_snapshot=raw_holdings,
        normalized_snapshot=holdings,
        snapshot_path=holdings_path,
    )
    position_result = _sync_holding_positions(
        client=notion_client,
        database_id=resources.holding_positions_db_id,
        holding_page_id=holding_page["id"],
        normalized_snapshot=holdings,
    )
    ranking_page = _sync_daily_rankings(
        client=notion_client,
        database_id=resources.daily_rankings_db_id,
        ranking_snapshot=raw_ranking,
        ranking_snapshot_path=ranking_path,
        ranking_csv_path=ranking_csv_path,
        source_run_path=str(summary.get("source_run_path") or ""),
        holding_page_id=holding_page["id"],
        top_n=config.next_trade_top_n,
        bond_name_map=bond_name_map,
    )
    decision_day_page = _sync_decision_day(
        client=notion_client,
        database_id=resources.decision_days_db_id,
        plan=plan,
        holdings_snapshot=holdings,
        ranking_snapshot=raw_ranking,
        holding_page_id=holding_page["id"],
        ranking_page_id=ranking_page["id"],
        bond_name_map=bond_name_map,
    )
    _link_page_to_decision_day(
        client=notion_client,
        page_id=holding_page["id"],
        decision_day_page_id=decision_day_page["id"],
    )
    _link_page_to_decision_day(
        client=notion_client,
        page_id=ranking_page["id"],
        decision_day_page_id=decision_day_page["id"],
    )
    orders_result = _sync_plan_orders(
        client=notion_client,
        database_id=resources.plan_orders_db_id,
        decision_day_page_id=decision_day_page["id"],
        plan=plan,
        previous_rank_map=previous_rank_map,
        bond_name_map=bond_name_map,
    )
    decision_latest_updates = _sync_is_latest_flag(
        client=notion_client,
        database_id=resources.decision_days_db_id,
        signal_date=signal_date,
    )
    order_latest_updates = _sync_is_latest_flag(
        client=notion_client,
        database_id=resources.plan_orders_db_id,
        signal_date=signal_date,
    )

    return {
        "run_id": resolved_run_id,
        "signal_date": signal_date,
        "trade_date": resolved_trade_date,
        "holdings_snapshot_path": str(holdings_path),
        "ranking_snapshot_path": str(ranking_path),
        "ranking_csv_path": str(ranking_csv_path),
        "plan_json_path": str(plan["json_path"]),
        "notion": {
            "holdings_page_id": holding_page["id"],
            "positions_created": position_result["created"],
            "positions_updated": position_result["updated"],
            "positions_archived": position_result["archived"],
            "ranking_page_id": ranking_page["id"],
            "decision_day_page_id": decision_day_page["id"],
            "orders_created": orders_result["created"],
            "orders_updated": orders_result["updated"],
            "orders_archived": orders_result["archived"],
            "decision_latest_updates": decision_latest_updates,
            "order_latest_updates": order_latest_updates,
        },
    }


def _sync_holdings_snapshot(
    *,
    client: NotionClient,
    database_id: str,
    raw_snapshot: dict[str, Any],
    normalized_snapshot: dict[str, Any],
    snapshot_path: Path,
) -> dict[str, Any]:
    signal_date = _require_text(normalized_snapshot, "signal_date", label="holdings snapshot")
    snapshot_key = _require_text(normalized_snapshot, "snapshot_key", label="holdings snapshot")
    source_type = str(raw_snapshot.get("source_type") or _infer_holdings_source_type(raw_snapshot, normalized_snapshot))
    raw_ocr_text = _coalesce_text(
        raw_snapshot.get("raw_ocr_text"),
        raw_snapshot.get("ocr_text"),
        raw_snapshot.get("raw_text"),
    )
    preview = _build_ocr_preview(raw_ocr_text=raw_ocr_text, positions=normalized_snapshot["positions"])
    properties = {
        "Name": _title_value(f"{signal_date} | {snapshot_key}"),
        "Signal Date": _date_value(signal_date),
        "Snapshot Key": _rich_text_value(snapshot_key),
        "Parse Status": _select_value(str(normalized_snapshot.get("parse_status") or "")),
        "Source Type": _select_value(source_type),
        "Positions Count": _number_value(len(normalized_snapshot["positions"])),
        "Holdings JSON Path": _rich_text_value(str(snapshot_path)),
        "OCR Preview": _rich_text_value(preview),
        "Submitted At": _date_value(normalized_snapshot.get("submitted_at")),
        "Parsed At": _date_value(normalized_snapshot.get("parsed_at")),
        "Confirmed At": _date_value(normalized_snapshot.get("confirmed_at")),
    }
    body_children = _build_holdings_body(
        raw_snapshot=raw_snapshot,
        raw_ocr_text=raw_ocr_text,
        positions=normalized_snapshot["positions"],
    )

    page = _query_single_page(
        client,
        database_id,
        {"property": "Snapshot Key", "rich_text": {"equals": snapshot_key}},
    )
    if page is None:
        created = client.create_page(database_id=database_id, properties=properties, children=body_children or None)
        return {"id": created["id"], "created": True}

    client.update_page(page["id"], properties=properties)
    _sync_page_body(client=client, page_id=page["id"], body_children=body_children)
    return {"id": page["id"], "created": False}


def _sync_holding_positions(
    *,
    client: NotionClient,
    database_id: str,
    holding_page_id: str,
    normalized_snapshot: dict[str, Any],
) -> dict[str, int]:
    signal_date = _require_text(normalized_snapshot, "signal_date", label="holdings snapshot")
    snapshot_key = _require_text(normalized_snapshot, "snapshot_key", label="holdings snapshot")
    desired: dict[str, dict[str, Any]] = {}
    for instrument, quantity in sorted(normalized_snapshot["positions"].items()):
        desired[instrument] = {
            "Name": _title_value(f"{signal_date} | {instrument}"),
            "Signal Date": _date_value(signal_date),
            "Snapshot Key": _rich_text_value(snapshot_key),
            "Instrument": _rich_text_value(instrument),
            "Holding Qty": _number_value(quantity),
            "Holdings Snapshot": _relation_value([holding_page_id]),
        }

    existing_pages = client.query_database(
        database_id,
        filter_={"property": "Holdings Snapshot", "relation": {"contains": holding_page_id}},
    )
    existing_by_instrument = {
        _plain_rich_text(page, "Instrument"): page
        for page in existing_pages
        if _plain_rich_text(page, "Instrument")
    }

    created = 0
    updated = 0
    for instrument, properties in desired.items():
        page = existing_by_instrument.get(instrument)
        if page is None:
            client.create_page(database_id=database_id, properties=properties)
            created += 1
            continue
        client.update_page(page["id"], properties=properties)
        updated += 1

    archived = 0
    for instrument, page in existing_by_instrument.items():
        if instrument in desired:
            continue
        client.update_page(page["id"], archived=True)
        archived += 1
    return {"created": created, "updated": updated, "archived": archived}


def _sync_daily_rankings(
    *,
    client: NotionClient,
    database_id: str,
    ranking_snapshot: dict[str, Any],
    ranking_snapshot_path: Path,
    ranking_csv_path: Path,
    source_run_path: str,
    holding_page_id: str,
    top_n: int,
    bond_name_map: dict[str, str],
) -> dict[str, Any]:
    signal_date = _require_text(ranking_snapshot, "signal_date", label="ranking snapshot")
    snapshot_key = _require_text(ranking_snapshot, "ranking_snapshot_key", label="ranking snapshot")
    run_id = _require_text(ranking_snapshot, "run_id", label="ranking snapshot")
    ranked_entries = _iter_ranked_entries(ranking_snapshot)
    if not ranked_entries:
        raise NotionSyncError("ranking snapshot must contain a non-empty ranked_entries list")
    properties = {
        "Name": _title_value(f"{signal_date} | {run_id}"),
        "Signal Date": _date_value(signal_date),
        "Run ID": _rich_text_value(run_id),
        "Ranking Snapshot Key": _rich_text_value(snapshot_key),
        "Policy": _select_value(str(ranking_snapshot.get("policy_name") or "")),
        "Generated At": _date_value(_coalesce_text(ranking_snapshot.get("generated_at"))),
        "Ranked Universe Count": _number_value(int(ranking_snapshot.get("ranked_universe_count") or 0)),
        "Top 6 Summary": _rich_text_value(_build_top_summary(ranking_snapshot, bond_name_map=bond_name_map, limit=top_n)),
        "Snapshot Status": _select_value("usable_for_plan"),
        "Ranking JSON Path": _rich_text_value(str(ranking_snapshot_path)),
        "Ranking CSV Path": _rich_text_value(str(ranking_csv_path)),
        "Source Run Path": _rich_text_value(source_run_path),
        "Holdings Snapshot": _relation_value([holding_page_id]),
    }
    body_children = _build_ranking_body(ranking_snapshot=ranking_snapshot, bond_name_map=bond_name_map)

    page = _query_single_page(
        client,
        database_id,
        {"property": "Ranking Snapshot Key", "rich_text": {"equals": snapshot_key}},
    )
    if page is None:
        created = client.create_page(database_id=database_id, properties=properties, children=body_children or None)
        return {"id": created["id"], "created": True}

    client.update_page(page["id"], properties=properties)
    _sync_page_body(client=client, page_id=page["id"], body_children=body_children)
    return {"id": page["id"], "created": False}


def _sync_decision_day(
    *,
    client: NotionClient,
    database_id: str,
    plan: dict[str, Any],
    holdings_snapshot: dict[str, Any],
    ranking_snapshot: dict[str, Any],
    holding_page_id: str,
    ranking_page_id: str,
    bond_name_map: dict[str, str],
) -> dict[str, Any]:
    signal_date = _require_text(plan, "signal_date", label="plan")
    holdings_status = str(holdings_snapshot.get("parse_status") or "missing")
    ranking_status = "usable_for_plan"
    page = _query_single_page(
        client,
        database_id,
        {"property": "Signal Date", "date": {"equals": signal_date}},
    )
    review_verdict = _plain_select(page, "Review Verdict") if page else "pending"
    decision_status = _derive_decision_status(
        review_verdict=review_verdict,
        holdings_status=holdings_status,
        ranking_status=ranking_status,
    )
    properties = {
        "Name": _title_value(signal_date),
        "Signal Date": _date_value(signal_date),
        "Trade Date": _date_value(_coalesce_text(plan.get("trade_date"))),
        "Decision Status": _select_value(decision_status),
        "Is Latest": _checkbox_value(True),
        "Holdings Status": _select_value(holdings_status),
        "Ranking Status": _select_value(ranking_status),
        "Current Positions Count": _number_value(plan.get("current_positions_count")),
        "Buy Count": _number_value(plan.get("buy_count")),
        "Sell Count": _number_value(plan.get("sell_count")),
        "Keep Count": _number_value(plan.get("hold_count")),
        "Watch Count": _number_value(plan.get("watch_count")),
        "Top 6 Summary": _rich_text_value(_build_top_summary(ranking_snapshot, bond_name_map=bond_name_map, limit=6)),
        "Policy": _select_value(str(plan.get("policy_name") or "")),
        "Target N": _number_value(plan.get("target_n")),
        "Max Drop": _number_value(plan.get("max_drop")),
        "Ranked Universe Count": _number_value(plan.get("ranked_universe_count")),
        "Holdings Snapshot At": _date_value(_coalesce_text(plan.get("holdings_confirmed_at"))),
        "Generated At": _date_value(_coalesce_text(plan.get("generated_at"))),
        "Run ID": _rich_text_value(str(plan.get("run_id") or "")),
        "Source Run Path": _rich_text_value(str(plan.get("source_run_path") or "")),
        "Plan JSON Path": _rich_text_value(str(plan.get("json_path") or "")),
        "Plan CSV Path": _rich_text_value(str(plan.get("csv_path") or "")),
        "Brief HTML Path": _rich_text_value(str(plan.get("html_path") or "")),
        "Holdings Snapshot": _relation_value([holding_page_id]),
        "Ranking Snapshot": _relation_value([ranking_page_id]),
    }
    body_children = _build_decision_day_body(
        plan=plan,
        holdings_snapshot=holdings_snapshot,
        ranking_snapshot=ranking_snapshot,
        bond_name_map=bond_name_map,
        decision_status=decision_status,
    )

    if page is None:
        create_properties = {
            **properties,
            "Review Verdict": _select_value("pending"),
        }
        created = client.create_page(database_id=database_id, properties=create_properties, children=body_children or None)
        return {"id": created["id"], "created": True}

    client.update_page(page["id"], properties=properties)
    _sync_page_body(client=client, page_id=page["id"], body_children=body_children)
    return {"id": page["id"], "created": False}


def _link_page_to_decision_day(
    *,
    client: NotionClient,
    page_id: str,
    decision_day_page_id: str,
) -> None:
    client.update_page(
        page_id,
        properties={"Decision Day": _relation_value([decision_day_page_id])},
    )


def _sync_plan_orders(
    *,
    client: NotionClient,
    database_id: str,
    decision_day_page_id: str,
    plan: dict[str, Any],
    previous_rank_map: dict[str, int],
    bond_name_map: dict[str, str],
) -> dict[str, int]:
    desired: dict[str, dict[str, Any]] = {}
    signal_date = _require_text(plan, "signal_date", label="plan")
    for order in plan["orders"]:
        plan_key = _require_text(order, "plan_key", label="plan order")
        instrument = _require_text(order, "instrument", label="plan order")
        normalized_instrument = _normalize_instrument(instrument)
        source_strategies = _split_source_strategies(order.get("source_strategies"))
        display_rank = _coerce_int(order.get("display_rank"))
        prev_display_rank = previous_rank_map.get(normalized_instrument)
        create_properties = {
            "Name": _title_value(f"{signal_date} | {instrument}"),
            "Plan Key": _rich_text_value(plan_key),
            "Signal Date": _date_value(_coalesce_text(order.get("signal_date"))),
            "Trade Date": _date_value(_coalesce_text(order.get("trade_date"))),
            "Bond Name": _rich_text_value(bond_name_map.get(normalized_instrument, "")),
            "Instrument": _rich_text_value(instrument),
            "Display Rank": _number_value(display_rank),
            "Prev Display Rank": _number_value(prev_display_rank),
            "Rank Delta": _number_value(_derive_rank_delta(prev_display_rank, display_rank)),
            "Portfolio Move": _select_value(
                _derive_portfolio_move(
                    current_in_portfolio=bool(order.get("current_in_portfolio")),
                    planned_in_portfolio=bool(order.get("planned_in_portfolio")),
                )
            ),
            "Action": _select_value(str(order.get("strategy_action") or "")),
            "Action Color": _select_value(str(order.get("strategy_action") or "")),
            "Reason": _select_value(str(order.get("strategy_reason") or "")),
            "Current Holding Qty": _number_value(order.get("current_holding_qty")),
            "Current In Portfolio": _checkbox_value(bool(order.get("current_in_portfolio"))),
            "Planned In Portfolio": _checkbox_value(bool(order.get("planned_in_portfolio"))),
            "Ranked Top N": _checkbox_value(bool(order.get("ranked_top_n"))),
            "Policy": _select_value(str(order.get("policy_name") or "")),
            "Source Strategies": _multi_select_value(source_strategies),
            "Source Scores": _rich_text_value(str(order.get("source_scores") or "")),
            "Updated At": _date_value(_coalesce_text(order.get("updated_at"))),
            "Is Latest": _checkbox_value(True),
            "Decision Day": _relation_value([decision_day_page_id]),
            "Checked": _checkbox_value(False),
        }
        update_properties = {key: value for key, value in create_properties.items() if key != "Checked"}
        desired[plan_key] = {
            "create_properties": create_properties,
            "update_properties": update_properties,
        }

    existing_pages = client.query_database(
        database_id,
        filter_={"property": "Decision Day", "relation": {"contains": decision_day_page_id}},
    )
    existing_by_key = {
        _plain_rich_text(page, "Plan Key"): page
        for page in existing_pages
        if _plain_rich_text(page, "Plan Key")
    }

    created = 0
    updated = 0
    for plan_key, payload in desired.items():
        page = existing_by_key.get(plan_key)
        if page is None:
            client.create_page(database_id=database_id, properties=payload["create_properties"])
            created += 1
            continue
        client.update_page(page["id"], properties=payload["update_properties"])
        updated += 1

    archived = 0
    for plan_key, page in existing_by_key.items():
        if plan_key in desired:
            continue
        client.update_page(page["id"], archived=True)
        archived += 1
    return {"created": created, "updated": updated, "archived": archived}


def _sync_is_latest_flag(
    *,
    client: NotionClient,
    database_id: str,
    signal_date: str,
) -> int:
    pages = client.query_database(database_id)
    updated = 0
    for page in pages:
        desired = _plain_date(page, "Signal Date") == signal_date
        if _plain_checkbox(page, "Is Latest") == desired:
            continue
        client.update_page(page["id"], properties={"Is Latest": _checkbox_value(desired)})
        updated += 1
    return updated


def _load_or_build_plan(config: OrchestratorConfig, *, run_id: str, trade_date: str) -> dict[str, Any]:
    output_dir = plan_output_dir(config, trade_date, run_id)
    json_path = output_dir / "next_trade_plan.json"
    if json_path.exists():
        payload = _load_json_object(json_path)
        payload["json_path"] = str(json_path)
        payload["csv_path"] = str(output_dir / "next_trade_plan.csv")
        payload["html_path"] = str(output_dir / "daily_brief.html")
        return payload
    return plan_next_trade(config, run_id=run_id)


def _write_ranking_snapshot_csv(snapshot_path: Path, ranking_snapshot: dict[str, Any]) -> Path:
    csv_path = snapshot_path.with_suffix(".csv")
    ranked_entries = ranking_snapshot.get("ranked_entries")
    if not isinstance(ranked_entries, list):
        raise NotionSyncError("ranking snapshot missing ranked_entries")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["instrument", "display_rank", "final_score", "source_strategies", "source_scores"],
        )
        writer.writeheader()
        for item in ranked_entries:
            writer.writerow(
                {
                    "instrument": item.get("instrument"),
                    "display_rank": item.get("display_rank"),
                    "final_score": item.get("final_score"),
                    "source_strategies": ",".join(_coerce_list(item.get("source_strategies"))),
                    "source_scores": _stringify_source_scores(item.get("source_scores")),
                }
            )
    return csv_path


def _sync_page_body(
    *,
    client: NotionClient,
    page_id: str,
    body_children: list[dict[str, Any]],
) -> None:
    if not body_children:
        return
    existing_children = client.list_block_children(page_id)
    if _block_signatures(existing_children) == _block_signatures(body_children):
        return
    for block in existing_children:
        block_id = str(block.get("id") or "")
        if block_id:
            client.update_block(block_id, archived=True)
    client.append_block_children(page_id, body_children)


def _block_signatures(blocks: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [_block_signature(block) for block in blocks]


def _block_signature(block: dict[str, Any]) -> tuple[str, str]:
    block_type = str(block.get("type") or "")
    payload = block.get(block_type)
    if not isinstance(payload, dict):
        return (block_type, "")
    rich_text = payload.get("rich_text")
    if not isinstance(rich_text, list):
        return (block_type, "")
    parts: list[str] = []
    for item in rich_text:
        if not isinstance(item, dict):
            continue
        plain_text = item.get("plain_text")
        if plain_text is None:
            plain_text = item.get("text", {}).get("content", "")
        parts.append(str(plain_text or ""))
    return (block_type, "".join(parts))


def _build_decision_day_body(
    *,
    plan: dict[str, Any],
    holdings_snapshot: dict[str, Any],
    ranking_snapshot: dict[str, Any],
    bond_name_map: dict[str, str],
    decision_status: str,
) -> list[dict[str, Any]]:
    top_summary = _build_top_summary(ranking_snapshot, bond_name_map=bond_name_map, limit=6) or "N/A"
    blocks = [_heading_block("Decision Summary")]
    blocks.extend(
        _bulleted_blocks(
            [
                f"Decision status: {decision_status}",
                f"Trade date: {plan.get('trade_date') or ''}",
                f"Holdings status: {holdings_snapshot.get('parse_status') or 'missing'}",
                f"Buy / Sell / Keep / Watch: {int(plan.get('buy_count') or 0)} / {int(plan.get('sell_count') or 0)} / {int(plan.get('hold_count') or 0)} / {int(plan.get('watch_count') or 0)}",
            ]
        )
    )
    blocks.append(_heading_block("Holdings Snapshot"))
    blocks.extend(
        _bulleted_blocks(
            [
                f"Snapshot key: {holdings_snapshot.get('snapshot_key') or ''}",
                f"Positions count: {len(holdings_snapshot.get('positions', {}))}",
                f"Confirmed at: {holdings_snapshot.get('confirmed_at') or ''}",
            ]
        )
    )
    blocks.append(_heading_block("Top 6 Snapshot"))
    blocks.extend(_paragraph_blocks(top_summary))
    blocks.append(_heading_block("Portfolio Changes"))
    blocks.extend(
        _bulleted_blocks(
            [
                f"Enter: {_summarize_move(plan['orders'], move='enter', bond_name_map=bond_name_map)}",
                f"Keep: {_summarize_move(plan['orders'], move='keep', bond_name_map=bond_name_map)}",
                f"Exit: {_summarize_move(plan['orders'], move='exit', bond_name_map=bond_name_map)}",
            ]
        )
    )
    blocks.append(_heading_block("Artifacts"))
    blocks.extend(
        _bulleted_blocks(
            [
                f"Plan JSON: {plan.get('json_path') or ''}",
                f"Plan CSV: {plan.get('csv_path') or ''}",
                f"Brief HTML: {plan.get('html_path') or ''}",
            ]
        )
    )
    return blocks


def _build_ranking_body(
    *,
    ranking_snapshot: dict[str, Any],
    bond_name_map: dict[str, str],
) -> list[dict[str, Any]]:
    blocks = [_heading_block("Top 20 Ranking")]
    lines: list[str] = []
    for item in _iter_ranked_entries(ranking_snapshot)[:20]:
        instrument = _normalize_instrument(str(item.get("instrument") or ""))
        if not instrument:
            continue
        label = _format_instrument_label(instrument, bond_name_map)
        score = _stringify_source_scores(item.get("source_scores"))
        final_score = _format_score(item.get("final_score"))
        lines.append(
            f"{int(item.get('display_rank') or 0)}. {label} | final_score={final_score} | source_scores={score or '-'}"
        )
    blocks.extend(_bulleted_blocks(lines or ["No ranking entries."]))
    return blocks


def _build_holdings_body(
    *,
    raw_snapshot: dict[str, Any],
    raw_ocr_text: str | None,
    positions: dict[str, float],
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    source_note = _coalesce_text(raw_snapshot.get("source_note"))
    source_type = _coalesce_text(raw_snapshot.get("source_type")) or _infer_holdings_source_type(raw_snapshot, {"positions": positions})
    if raw_ocr_text:
        blocks.append(_heading_block("Raw OCR Text"))
        blocks.extend(_paragraph_blocks(raw_ocr_text))
    else:
        blocks.append(_heading_block("Snapshot Note"))
        blocks.extend(
            _paragraph_blocks(
                source_note or f"No raw OCR text was attached for this snapshot. Source type: {source_type}."
            )
        )

    if positions:
        blocks.append(_heading_block("Parsed Positions"))
        lines = [f"{instrument}: {quantity:g}" for instrument, quantity in sorted(positions.items())]
        blocks.extend(_bulleted_blocks(lines))
    return blocks


def _build_ocr_preview(*, raw_ocr_text: str | None, positions: dict[str, float]) -> str:
    if raw_ocr_text:
        compact = " ".join(raw_ocr_text.split())
        return compact[:280]
    if not positions:
        return "No positions."
    preview = ", ".join(f"{instrument}:{quantity:g}" for instrument, quantity in sorted(positions.items()))
    return preview[:280]


def _build_top_summary(
    snapshot: dict[str, Any],
    *,
    bond_name_map: dict[str, str],
    limit: int,
) -> str:
    parts: list[str] = []
    for item in _iter_ranked_entries(snapshot)[:limit]:
        instrument = _normalize_instrument(str(item.get("instrument") or ""))
        if not instrument:
            continue
        parts.append(_format_instrument_label(instrument, bond_name_map))
    return " / ".join(parts)


def _iter_ranked_entries(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    ranked_entries = snapshot.get("ranked_entries")
    if isinstance(ranked_entries, list):
        return ranked_entries
    ranked_orders = snapshot.get("ranked_orders")
    if isinstance(ranked_orders, list):
        return ranked_orders
    return []


def _summarize_move(
    orders: list[dict[str, Any]],
    *,
    move: str,
    bond_name_map: dict[str, str],
) -> str:
    labels: list[str] = []
    for order in orders:
        portfolio_move = _derive_portfolio_move(
            current_in_portfolio=bool(order.get("current_in_portfolio")),
            planned_in_portfolio=bool(order.get("planned_in_portfolio")),
        )
        if portfolio_move != move:
            continue
        instrument = _normalize_instrument(str(order.get("instrument") or ""))
        if not instrument:
            continue
        labels.append(_format_instrument_label(instrument, bond_name_map))
    if not labels:
        return "-"
    if len(labels) <= 6:
        return ", ".join(labels)
    return f'{", ".join(labels[:6])} (+{len(labels) - 6} more)'


def _format_instrument_label(instrument: str, bond_name_map: dict[str, str]) -> str:
    bond_name = bond_name_map.get(_normalize_instrument(instrument), "").strip()
    if bond_name:
        return f"{bond_name}({instrument})"
    return instrument


def _derive_portfolio_move(*, current_in_portfolio: bool, planned_in_portfolio: bool) -> str:
    if current_in_portfolio and planned_in_portfolio:
        return "keep"
    if current_in_portfolio and not planned_in_portfolio:
        return "exit"
    if planned_in_portfolio:
        return "enter"
    return "ignore"


def _derive_rank_delta(prev_display_rank: int | None, display_rank: int | None) -> int | None:
    if prev_display_rank is None or display_rank is None:
        return None
    return int(prev_display_rank) - int(display_rank)


def _derive_decision_status(
    *,
    review_verdict: str,
    holdings_status: str,
    ranking_status: str,
) -> str:
    if holdings_status == "missing":
        return "waiting_holdings"
    if ranking_status == "missing":
        return "waiting_ranking"
    if holdings_status in {"parsed", "needs_review", "error"} or ranking_status == "blocked":
        return "blocked"
    if review_verdict in {"accepted", "ignored"}:
        return "reviewed"
    return "review_pending"


def _load_previous_rank_map(config: OrchestratorConfig, signal_date: str) -> dict[str, int]:
    plan_root = config.plan_input_root
    if not plan_root.exists():
        return {}
    for candidate in sorted(plan_root.iterdir(), key=lambda item: item.name, reverse=True):
        if not candidate.is_dir() or candidate.name >= signal_date:
            continue
        snapshot_path = candidate / "ranking_snapshot.json"
        if not snapshot_path.exists():
            continue
        try:
            previous_snapshot = load_ranking_snapshot(snapshot_path)
        except Exception:
            continue
        return {
            _normalize_instrument(str(item.get("instrument") or "")): int(item["display_rank"])
            for item in _iter_ranked_entries(previous_snapshot)
            if item.get("instrument") and _coerce_int(item.get("display_rank")) is not None
        }
    return {}


def _load_bond_name_map(config: OrchestratorConfig) -> dict[str, str]:
    candidate_paths: list[Path] = []
    if config.notion_bond_name_map_path:
        candidate_paths.append(config.notion_bond_name_map_path)
    candidate_paths.extend(
        [
            config.runtime_repo_root / "local_assets" / "cb_basic.csv",
            config.runtime_repo_root / "local_assets" / "cb_basic.json",
            config.runtime_repo_root / "local_state" / "cb_basic.csv",
            config.upstream_repo_root / "data" / "cb_basic.csv",
            config.upstream_repo_root / "cb_basic.csv",
        ]
    )

    seen: set[Path] = set()
    for path in candidate_paths:
        resolved = path.expanduser()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        mapping = _load_bond_name_map_file(resolved)
        if mapping:
            return mapping
    return {}


def _load_bond_name_map_file(path: Path) -> dict[str, str]:
    if path.suffix.lower() == ".json":
        return _load_bond_name_map_json(path)
    return _load_bond_name_map_csv(path)


def _load_bond_name_map_json(path: Path) -> dict[str, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return {
            _normalize_instrument(str(key)): str(value).strip()
            for key, value in payload.items()
            if str(key).strip() and str(value).strip()
        }
    if not isinstance(payload, list):
        return {}
    mapping: dict[str, str] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        code, bond_name = _extract_bond_name_row(row)
        if code and bond_name:
            mapping[code] = bond_name
    return mapping


def _load_bond_name_map_csv(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code, bond_name = _extract_bond_name_row(row)
            if code and bond_name:
                mapping[code] = bond_name
    return mapping


def _extract_bond_name_row(row: dict[str, Any]) -> tuple[str | None, str | None]:
    instrument = _coalesce_text(
        row.get("instrument"),
        row.get("qlib_code"),
        row.get("code"),
    )
    if not instrument:
        ts_code = _coalesce_text(row.get("ts_code"))
        if ts_code:
            instrument = _ts_code_to_instrument(ts_code)
    if not instrument:
        return None, None
    bond_name = _coalesce_text(
        row.get("bond_short_name"),
        row.get("bond_name"),
        row.get("name"),
    )
    if not bond_name:
        return None, None
    return _normalize_instrument(instrument), bond_name


def _ts_code_to_instrument(value: str) -> str:
    normalized = str(value).strip().upper()
    if "." not in normalized:
        return _normalize_instrument(normalized)
    code, market = normalized.split(".", 1)
    market = market.strip()
    code = code.strip()
    if not market or not code:
        return _normalize_instrument(normalized)
    return _normalize_instrument(f"{market}{code}")


def _query_single_page(
    client: NotionClient,
    database_id: str,
    filter_: dict[str, Any],
) -> dict[str, Any] | None:
    results = client.query_database(database_id, filter_=filter_, page_size=10)
    return results[0] if results else None


def _heading_block(text: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {"rich_text": [{"type": "text", "text": {"content": text[:MAX_TITLE_CHARS]}}]},
    }


def _paragraph_blocks(text: str) -> list[dict[str, Any]]:
    return [
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": chunk}}]},
        }
        for chunk in _chunk_text(text, limit=MAX_RICH_TEXT_CHARS)
    ]


def _bulleted_blocks(lines: list[str]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for line in lines:
        for chunk in _chunk_text(line, limit=MAX_RICH_TEXT_CHARS):
            blocks.append(
                {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": [{"type": "text", "text": {"content": chunk}}]},
                }
            )
    return blocks


def _chunk_text(text: str, *, limit: int) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []
    return [normalized[index : index + limit] for index in range(0, len(normalized), limit)]


def _title_value(text: str) -> dict[str, Any]:
    normalized = text.strip()[:MAX_TITLE_CHARS]
    return {"title": [{"type": "text", "text": {"content": normalized}}]}


def _rich_text_value(value: str | None) -> dict[str, Any]:
    normalized = (value or "").strip()
    return {
        "rich_text": [
            {"type": "text", "text": {"content": chunk}}
            for chunk in _chunk_text(normalized, limit=MAX_RICH_TEXT_CHARS)
        ]
    }


def _date_value(value: str | None) -> dict[str, Any]:
    return {"date": {"start": value}} if value else {"date": None}


def _select_value(value: str | None) -> dict[str, Any]:
    normalized = (value or "").strip()
    return {"select": {"name": normalized}} if normalized else {"select": None}


def _multi_select_value(values: list[str] | tuple[str, ...] | None) -> dict[str, Any]:
    return {"multi_select": [{"name": item} for item in _coerce_list(values)]}


def _checkbox_value(value: bool) -> dict[str, Any]:
    return {"checkbox": bool(value)}


def _number_value(value: int | float | None) -> dict[str, Any]:
    if value is None:
        return {"number": None}
    return {"number": float(value)}


def _relation_value(page_ids: list[str] | tuple[str, ...]) -> dict[str, Any]:
    return {"relation": [{"id": page_id} for page_id in page_ids]}


def _plain_rich_text(page: dict[str, Any] | None, property_name: str) -> str:
    if not page:
        return ""
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") != "rich_text":
        return ""
    return "".join(item.get("plain_text", "") for item in prop.get("rich_text", []))


def _plain_select(page: dict[str, Any] | None, property_name: str) -> str:
    if not page:
        return ""
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") != "select":
        return ""
    value = prop.get("select")
    if not isinstance(value, dict):
        return ""
    return str(value.get("name") or "")


def _plain_checkbox(page: dict[str, Any] | None, property_name: str) -> bool:
    if not page:
        return False
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") != "checkbox":
        return False
    return bool(prop.get("checkbox"))


def _plain_date(page: dict[str, Any] | None, property_name: str) -> str:
    if not page:
        return ""
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") != "date":
        return ""
    value = prop.get("date")
    if not isinstance(value, dict):
        return ""
    return str(value.get("start") or "")


def _stringify_source_scores(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        parts = []
        for key in sorted(value):
            parts.append(f"{key}={_format_score(value[key])}")
        return ";".join(parts)
    return ""


def _format_score(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{numeric:.6f}".rstrip("0").rstrip(".")


def _split_source_strategies(value: Any) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return _coerce_list(value)


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _infer_holdings_source_type(raw_snapshot: dict[str, Any], normalized_snapshot: dict[str, Any]) -> str:
    parse_status = str(raw_snapshot.get("parse_status") or normalized_snapshot.get("parse_status") or "")
    if parse_status == "confirmed_empty":
        return "bootstrap_empty"
    if parse_status == "confirmed_from_plan":
        return "executed_plan_replay"
    if raw_snapshot.get("raw_ocr_text") or raw_snapshot.get("ocr_text") or raw_snapshot.get("raw_text"):
        return "ocr_text"
    return "snapshot_bundle"


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    try:
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _normalize_instrument(value: str) -> str:
    return str(value).strip().upper()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise NotionSyncError(f"missing json artifact: {path}") from exc
    if not isinstance(payload, dict):
        raise NotionSyncError(f"json artifact must be an object: {path}")
    return payload


def _coalesce_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _require_text(payload: dict[str, Any], key: str, *, label: str) -> str:
    value = payload.get(key)
    if not value or not str(value).strip():
        raise NotionSyncError(f"{label} missing {key}")
    return str(value).strip()
