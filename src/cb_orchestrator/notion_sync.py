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
    daily_holdings_db_id: str
    holding_positions_db_id: str
    daily_rankings_db_id: str
    ranking_focus_db_id: str
    daily_plans_db_id: str
    plan_orders_db_id: str

    @classmethod
    def from_config(cls, config: OrchestratorConfig) -> "NotionResources":
        if not config.notion_sync_enabled():
            raise NotionSyncError("notion sync is not configured: missing token or database ids")
        return cls(
            daily_holdings_db_id=str(config.notion_daily_holdings_db_id),
            holding_positions_db_id=str(config.notion_holding_positions_db_id),
            daily_rankings_db_id=str(config.notion_daily_rankings_db_id),
            ranking_focus_db_id=str(config.notion_ranking_focus_db_id),
            daily_plans_db_id=str(config.notion_daily_plans_db_id),
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

    holding_page = _sync_daily_holdings(
        client=notion_client,
        database_id=resources.daily_holdings_db_id,
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
    )
    focus_result = _sync_ranking_focus(
        client=notion_client,
        database_id=resources.ranking_focus_db_id,
        ranking_page_id=ranking_page["id"],
        ranking_snapshot=raw_ranking,
        current_positions=holdings["positions"],
        top_n=config.next_trade_top_n,
        focus_top_k=config.notion_focus_top_k,
    )

    plan_page = _sync_daily_plan(
        client=notion_client,
        database_id=resources.daily_plans_db_id,
        plan=plan,
        holding_page_id=holding_page["id"],
        ranking_page_id=ranking_page["id"],
    )
    orders_result = _sync_plan_orders(
        client=notion_client,
        database_id=resources.plan_orders_db_id,
        plan_page_id=plan_page["id"],
        plan=plan,
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
            "focus_created": focus_result["created"],
            "focus_updated": focus_result["updated"],
            "focus_archived": focus_result["archived"],
            "plan_page_id": plan_page["id"],
            "orders_created": orders_result["created"],
            "orders_updated": orders_result["updated"],
            "orders_archived": orders_result["archived"],
        },
    }


def _sync_daily_holdings(
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
    body_children = _build_holdings_body(raw_snapshot=raw_snapshot, raw_ocr_text=raw_ocr_text, positions=normalized_snapshot["positions"])

    page = _query_single_page(
        client,
        database_id,
        {"property": "Snapshot Key", "rich_text": {"equals": snapshot_key}},
    )
    if page is None:
        created = client.create_page(database_id=database_id, properties=properties, children=body_children or None)
        return {"id": created["id"], "created": True}

    client.update_page(page["id"], properties=properties)
    if body_children and not client.list_block_children(page["id"]):
        client.append_block_children(page["id"], body_children)
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
        title = f"{signal_date} | {instrument}"
        desired[instrument] = {
            "Name": _title_value(title),
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
) -> dict[str, Any]:
    signal_date = _require_text(ranking_snapshot, "signal_date", label="ranking snapshot")
    snapshot_key = _require_text(ranking_snapshot, "ranking_snapshot_key", label="ranking snapshot")
    run_id = _require_text(ranking_snapshot, "run_id", label="ranking snapshot")
    ranked_entries = ranking_snapshot.get("ranked_entries")
    if not isinstance(ranked_entries, list) or not ranked_entries:
        raise NotionSyncError("ranking snapshot must contain a non-empty ranked_entries list")
    top_instruments = ",".join(str(item.get("instrument") or "").strip() for item in ranked_entries[:top_n] if item.get("instrument"))
    properties = {
        "Name": _title_value(f"{signal_date} | {run_id}"),
        "Signal Date": _date_value(signal_date),
        "Run ID": _rich_text_value(run_id),
        "Ranking Snapshot Key": _rich_text_value(snapshot_key),
        "Policy": _select_value(str(ranking_snapshot.get("policy_name") or "")),
        "Generated At": _date_value(ranking_snapshot.get("generated_at")),
        "Ranked Universe Count": _number_value(int(ranking_snapshot.get("ranked_universe_count") or 0)),
        "Top K": _number_value(top_n),
        "Top 6 Instruments": _rich_text_value(top_instruments),
        "Snapshot Status": _select_value("usable_for_plan"),
        "Ranking JSON Path": _rich_text_value(str(ranking_snapshot_path)),
        "Ranking CSV Path": _rich_text_value(str(ranking_csv_path)),
        "Source Run Path": _rich_text_value(source_run_path),
        "Holdings Snapshot": _relation_value([holding_page_id]),
    }

    page = _query_single_page(
        client,
        database_id,
        {"property": "Ranking Snapshot Key", "rich_text": {"equals": snapshot_key}},
    )
    if page is None:
        created = client.create_page(database_id=database_id, properties=properties)
        return {"id": created["id"], "created": True}

    client.update_page(page["id"], properties=properties)
    return {"id": page["id"], "created": False}


def _sync_ranking_focus(
    *,
    client: NotionClient,
    database_id: str,
    ranking_page_id: str,
    ranking_snapshot: dict[str, Any],
    current_positions: dict[str, float],
    top_n: int,
    focus_top_k: int,
) -> dict[str, int]:
    signal_date = _require_text(ranking_snapshot, "signal_date", label="ranking snapshot")
    snapshot_key = _require_text(ranking_snapshot, "ranking_snapshot_key", label="ranking snapshot")
    focus_entries = _build_focus_entries(
        ranking_snapshot=ranking_snapshot,
        current_positions=current_positions,
        top_n=top_n,
        focus_top_k=focus_top_k,
    )

    desired: dict[str, dict[str, Any]] = {}
    for entry in focus_entries:
        instrument = _require_text(entry, "instrument", label="focus entry")
        desired[instrument] = {
            "Name": _title_value(f"{signal_date} | {instrument}"),
            "Signal Date": _date_value(signal_date),
            "Ranking Snapshot Key": _rich_text_value(snapshot_key),
            "Instrument": _rich_text_value(instrument),
            "Display Rank": _number_value(entry.get("display_rank")),
            "Final Score": _number_value(entry.get("final_score")),
            "In Top 6": _checkbox_value(bool(entry.get("in_top_n"))),
            "In Top 20": _checkbox_value(bool(entry.get("in_focus_top_k"))),
            "Current In Portfolio": _checkbox_value(bool(entry.get("current_in_portfolio"))),
            "Current Holding Qty": _number_value(entry.get("current_holding_qty")),
            "Source Strategies": _multi_select_value(entry.get("source_strategies")),
            "Source Scores": _rich_text_value(_stringify_source_scores(entry.get("source_scores"))),
            "Ranking Snapshot": _relation_value([ranking_page_id]),
        }

    existing_pages = client.query_database(
        database_id,
        filter_={"property": "Ranking Snapshot", "relation": {"contains": ranking_page_id}},
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


def _sync_daily_plan(
    *,
    client: NotionClient,
    database_id: str,
    plan: dict[str, Any],
    holding_page_id: str,
    ranking_page_id: str,
) -> dict[str, Any]:
    run_id = _require_text(plan, "run_id", label="plan")
    trade_date = _require_text(plan, "trade_date", label="plan")
    create_properties = {
        "Name": _title_value(f"{trade_date} | {run_id}"),
        "Trade Date": _date_value(trade_date),
        "Signal Date": _date_value(plan.get("signal_date")),
        "Run ID": _rich_text_value(run_id),
        "Policy": _select_value(str(plan.get("policy_name") or "")),
        "Bootstrap": _checkbox_value(bool(plan.get("bootstrap"))),
        "Buy Count": _number_value(plan.get("buy_count")),
        "Sell Count": _number_value(plan.get("sell_count")),
        "Hold Count": _number_value(plan.get("hold_count")),
        "Watch Count": _number_value(plan.get("watch_count")),
        "Target N": _number_value(plan.get("target_n")),
        "Max Drop": _number_value(plan.get("max_drop")),
        "Ranked Universe Count": _number_value(plan.get("ranked_universe_count")),
        "Current Positions Count": _number_value(plan.get("current_positions_count")),
        "Holdings Snapshot At": _date_value(plan.get("holdings_confirmed_at")),
        "Source Run Path": _rich_text_value(str(plan.get("source_run_path") or "")),
        "Plan JSON Path": _rich_text_value(str(plan.get("json_path") or "")),
        "Plan CSV Path": _rich_text_value(str(plan.get("csv_path") or "")),
        "Brief HTML Path": _rich_text_value(str(plan.get("html_path") or "")),
        "Generated At": _date_value(plan.get("generated_at")),
        "Holdings Snapshot": _relation_value([holding_page_id]),
        "Ranking Snapshot": _relation_value([ranking_page_id]),
        "Review Status": _select_value("new"),
    }
    update_properties = {key: value for key, value in create_properties.items() if key != "Review Status"}
    page = _query_single_page(
        client,
        database_id,
        {
            "and": [
                {"property": "Run ID", "rich_text": {"equals": run_id}},
                {"property": "Trade Date", "date": {"equals": trade_date}},
            ]
        },
    )
    if page is None:
        created = client.create_page(database_id=database_id, properties=create_properties)
        return {"id": created["id"], "created": True}

    client.update_page(page["id"], properties=update_properties)
    return {"id": page["id"], "created": False}


def _sync_plan_orders(
    *,
    client: NotionClient,
    database_id: str,
    plan_page_id: str,
    plan: dict[str, Any],
) -> dict[str, int]:
    desired: dict[str, dict[str, Any]] = {}
    for order in plan["orders"]:
        plan_key = _require_text(order, "plan_key", label="plan order")
        instrument = _require_text(order, "instrument", label="plan order")
        trade_date = _require_text(order, "trade_date", label="plan order")
        source_strategies = _split_source_strategies(order.get("source_strategies"))
        create_properties = {
            "Name": _title_value(f"{trade_date} | {instrument}"),
            "Plan Key": _rich_text_value(plan_key),
            "Trade Date": _date_value(trade_date),
            "Signal Date": _date_value(order.get("signal_date")),
            "Instrument": _rich_text_value(instrument),
            "Display Rank": _number_value(order.get("display_rank")),
            "Current Holding Qty": _number_value(order.get("current_holding_qty")),
            "Current In Portfolio": _checkbox_value(bool(order.get("current_in_portfolio"))),
            "Ranked Top N": _checkbox_value(bool(order.get("ranked_top_n"))),
            "Planned In Portfolio": _checkbox_value(bool(order.get("planned_in_portfolio"))),
            "Action": _select_value(str(order.get("strategy_action") or "")),
            "Reason": _select_value(str(order.get("strategy_reason") or "")),
            "Source Strategies": _multi_select_value(source_strategies),
            "Source Scores": _rich_text_value(str(order.get("source_scores") or "")),
            "Policy": _select_value(str(order.get("policy_name") or "")),
            "Updated At": _date_value(order.get("updated_at")),
            "Daily Plan": _relation_value([plan_page_id]),
            "Checked": _checkbox_value(False),
        }
        update_properties = {key: value for key, value in create_properties.items() if key != "Checked"}
        desired[plan_key] = {
            "create_properties": create_properties,
            "update_properties": update_properties,
        }

    existing_pages = client.query_database(
        database_id,
        filter_={"property": "Daily Plan", "relation": {"contains": plan_page_id}},
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


def _build_focus_entries(
    *,
    ranking_snapshot: dict[str, Any],
    current_positions: dict[str, float],
    top_n: int,
    focus_top_k: int,
) -> list[dict[str, Any]]:
    ranked_entries = ranking_snapshot.get("ranked_entries")
    if not isinstance(ranked_entries, list):
        raise NotionSyncError("ranking snapshot missing ranked_entries")

    current_position_map = {str(instrument): float(quantity) for instrument, quantity in current_positions.items()}
    ranked_by_instrument = {
        str(item.get("instrument")): item
        for item in ranked_entries
        if item.get("instrument")
    }
    selected: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(ranked_entries, start=1):
        instrument = str(item.get("instrument") or "").strip()
        if not instrument:
            continue
        if index <= focus_top_k or instrument in current_position_map:
            selected[instrument] = item

    for instrument, quantity in current_position_map.items():
        if instrument in selected:
            continue
        selected[instrument] = {
            "instrument": instrument,
            "display_rank": None,
            "final_score": None,
            "source_strategies": [],
            "source_scores": {},
        }

    rows: list[dict[str, Any]] = []
    for instrument, item in selected.items():
        rank = item.get("display_rank")
        rows.append(
            {
                "instrument": instrument,
                "display_rank": rank,
                "final_score": item.get("final_score"),
                "in_top_n": isinstance(rank, int) and rank <= top_n,
                "in_focus_top_k": isinstance(rank, int) and rank <= focus_top_k,
                "current_in_portfolio": instrument in current_position_map,
                "current_holding_qty": current_position_map.get(instrument),
                "source_strategies": _coerce_list(item.get("source_strategies")),
                "source_scores": item.get("source_scores") or {},
            }
        )
    rows.sort(key=lambda item: (_sort_rank(item.get("display_rank")), item["instrument"]))
    return rows


def _query_single_page(
    client: NotionClient,
    database_id: str,
    filter_: dict[str, Any],
) -> dict[str, Any] | None:
    results = client.query_database(database_id, filter_=filter_, page_size=10)
    return results[0] if results else None


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


def _plain_rich_text(page: dict[str, Any], property_name: str) -> str:
    prop = page.get("properties", {}).get(property_name, {})
    if prop.get("type") != "rich_text":
        return ""
    return "".join(item.get("plain_text", "") for item in prop.get("rich_text", []))


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


def _sort_rank(value: Any) -> tuple[int, float]:
    if isinstance(value, int):
        return (0, float(value))
    if isinstance(value, float) and value.is_integer():
        return (0, value)
    return (1, float("inf"))


def _infer_holdings_source_type(raw_snapshot: dict[str, Any], normalized_snapshot: dict[str, Any]) -> str:
    parse_status = str(raw_snapshot.get("parse_status") or normalized_snapshot.get("parse_status") or "")
    if parse_status == "confirmed_empty":
        return "bootstrap_empty"
    if parse_status == "confirmed_from_plan":
        return "executed_plan_replay"
    if raw_snapshot.get("raw_ocr_text") or raw_snapshot.get("ocr_text") or raw_snapshot.get("raw_text"):
        return "ocr_text"
    return "snapshot_bundle"


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
