from __future__ import annotations

import csv
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cb_orchestrator.config import OrchestratorConfig

HOLDINGS_SNAPSHOT_FILENAME = "holdings_snapshot.json"
RANKING_SNAPSHOT_FILENAME = "ranking_snapshot.json"


def plan_next_trade(
    config: OrchestratorConfig,
    *,
    run_id: str | None = None,
    trade_date: str | None = None,
) -> dict[str, Any]:
    summary = resolve_run_summary(config, run_id=run_id, trade_date=trade_date)
    if summary.get("status") != "success":
        raise ValueError(f"run is not successful: {summary.get('run_id')} status={summary.get('status')}")
    resolved_run_id = _require_text(summary, "run_id", label="run summary")
    signal_date = _require_text(summary, "signal_date", label="run summary")
    resolved_trade_date = _require_text(summary, "trade_date", label="run summary")

    holdings_snapshot_path, ranking_snapshot_path = plan_input_paths(config, signal_date)
    holdings_snapshot = load_holdings_snapshot(holdings_snapshot_path)
    ranking_snapshot = load_ranking_snapshot(ranking_snapshot_path)
    _validate_snapshot_alignment(
        summary=summary,
        holdings_snapshot=holdings_snapshot,
        ranking_snapshot=ranking_snapshot,
    )

    payload = build_next_trade_plan(
        run_id=resolved_run_id,
        signal_date=signal_date,
        trade_date=resolved_trade_date,
        source_run_path=summary.get("source_run_path"),
        holdings_snapshot_ref=holdings_snapshot["snapshot_key"],
        holdings_snapshot_path=holdings_snapshot_path,
        holdings_confirmed_at=holdings_snapshot.get("confirmed_at"),
        ranking_snapshot_ref=ranking_snapshot["ranking_snapshot_key"],
        ranking_snapshot_path=ranking_snapshot_path,
        ranked_orders=ranking_snapshot["ranked_orders"],
        ranked_universe_count=ranking_snapshot["ranked_universe_count"],
        current_positions=holdings_snapshot["positions"],
        top_n=config.next_trade_top_n,
        max_drop=config.next_trade_max_drop,
    )

    output_dir = plan_output_dir(config, str(payload["trade_date"]), str(payload["run_id"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "next_trade_plan.json"
    csv_path = output_dir / "next_trade_plan.csv"
    html_path = output_dir / "daily_brief.html"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_plan_csv(csv_path, payload["orders"])
    write_plan_html(html_path, payload)

    return {
        **payload,
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "html_path": str(html_path),
    }


def resolve_run_summary(
    config: OrchestratorConfig,
    *,
    run_id: str | None = None,
    trade_date: str | None = None,
) -> dict[str, Any]:
    if run_id:
        run_path = config.state_root / "runs" / f"{run_id}.json"
        if not run_path.exists():
            raise FileNotFoundError(f"run summary not found: {run_path}")
        summary = _load_json(run_path)
        summary["source_run_path"] = str(run_path)
        return summary

    if trade_date:
        runs_dir = config.state_root / "runs"
        candidates = sorted(runs_dir.glob("*.json"), reverse=True)
        for item in candidates:
            payload = _load_json(item)
            if payload.get("trade_date") == trade_date and payload.get("status") == "success":
                payload["source_run_path"] = str(item)
                return payload
        raise FileNotFoundError(f"no successful run found for trade_date={trade_date}")

    latest_path = config.state_root / "latest.json"
    if not latest_path.exists():
        raise FileNotFoundError(f"latest run summary not found: {latest_path}")
    summary = _load_json(latest_path)
    summary["source_run_path"] = str(latest_path)
    return summary


def plan_input_dir(config: OrchestratorConfig, signal_date: str) -> Path:
    return config.plan_input_root / signal_date


def plan_input_paths(config: OrchestratorConfig, signal_date: str) -> tuple[Path, Path]:
    input_dir = plan_input_dir(config, signal_date)
    return input_dir / HOLDINGS_SNAPSHOT_FILENAME, input_dir / RANKING_SNAPSHOT_FILENAME


def load_holdings_snapshot(snapshot_path: Path) -> dict[str, Any]:
    payload = _load_json_object(snapshot_path, label="holdings snapshot")
    raw_positions = payload.get("positions")
    if not isinstance(raw_positions, list):
        raise ValueError("holdings snapshot must contain a positions list")

    positions: dict[str, float] = {}
    for index, item in enumerate(raw_positions, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"positions[{index}] must be an object")
        instrument = item.get("instrument")
        if not instrument or not str(instrument).strip():
            raise ValueError(f"positions[{index}] missing instrument")
        if "holding_qty" not in item:
            raise ValueError(f"positions[{index}] missing holding_qty")
        try:
            quantity = float(item["holding_qty"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"positions[{index}] holding_qty must be numeric") from exc
        if abs(quantity) <= 1e-12:
            continue
        normalized = _normalize_instrument(str(instrument))
        positions[normalized] = positions.get(normalized, 0.0) + quantity

    return {
        "signal_date": _require_text(payload, "signal_date", label="holdings snapshot"),
        "snapshot_key": _require_text(payload, "snapshot_key", label="holdings snapshot"),
        "parse_status": _require_text(payload, "parse_status", label="holdings snapshot"),
        "submitted_at": _optional_text(payload.get("submitted_at")),
        "parsed_at": _optional_text(payload.get("parsed_at")),
        "confirmed_at": _optional_text(payload.get("confirmed_at")),
        "positions": dict(sorted(positions.items())),
    }


def build_next_trade_plan(
    *,
    run_id: str,
    signal_date: str | None,
    trade_date: str | None,
    source_run_path: str | None,
    holdings_snapshot_ref: str,
    holdings_snapshot_path: Path,
    holdings_confirmed_at: str | None,
    ranking_snapshot_ref: str,
    ranking_snapshot_path: Path,
    ranked_orders: list[dict[str, Any]],
    ranked_universe_count: int,
    current_positions: dict[str, float],
    top_n: int,
    max_drop: int,
) -> dict[str, Any]:
    if not signal_date:
        raise ValueError("signal_date is required")
    if not trade_date:
        raise ValueError("trade_date is required")
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if max_drop < 0:
        raise ValueError("max_drop must be non-negative")

    top_ranked_orders = ranked_orders[:top_n]
    ranking_map = {_normalize_instrument(item["instrument"]): item for item in ranked_orders}
    current_holdings = {
        _normalize_instrument(instrument): float(quantity)
        for instrument, quantity in current_positions.items()
        if abs(float(quantity)) > 1e-12
    }
    current_set = set(current_holdings)
    top_keys = [_normalize_instrument(item["instrument"]) for item in top_ranked_orders]
    top_set = set(top_keys)
    bootstrap = not current_set

    if bootstrap:
        sell_keys: set[str] = set()
        deferred_drop_keys: set[str] = set()
        remaining_keys = set()
    else:
        exit_candidates = sorted(
            current_set - top_set,
            key=lambda instrument: (
                _rank_sort_value(ranking_map.get(instrument, {}).get("display_rank")),
                instrument,
            ),
            reverse=True,
        )
        sell_keys = set(exit_candidates[:max_drop])
        deferred_drop_keys = set(exit_candidates[max_drop:])
        remaining_keys = current_set - sell_keys

    missing_target_keys = [instrument for instrument in top_keys if instrument not in remaining_keys]
    open_slots = max(top_n - len(remaining_keys), 0)
    buy_keys = set(missing_target_keys[:open_slots])
    deferred_entry_keys = set(missing_target_keys[open_slots:])

    plan_orders: list[dict[str, Any]] = []
    for instrument_key in sorted(current_set | top_set):
        ranking_order = ranking_map.get(instrument_key)
        instrument = ranking_order.get("instrument") if ranking_order else instrument_key
        display_rank = ranking_order.get("display_rank") if ranking_order else None
        source_strategies = ranking_order.get("source_strategies", "") if ranking_order else ""
        source_scores = ranking_order.get("source_scores", "") if ranking_order else ""
        ranked_top = instrument_key in top_set
        current_in_portfolio = instrument_key in current_set
        planned_in_portfolio = instrument_key in remaining_keys or instrument_key in buy_keys

        if instrument_key in sell_keys:
            action = "sell"
            reason = "drop_by_rule"
        elif instrument_key in buy_keys:
            action = "buy"
            reason = "bootstrap_top6" if bootstrap else "enter_top6"
        elif instrument_key in deferred_drop_keys:
            action = "watch"
            reason = "deferred_drop"
        elif instrument_key in deferred_entry_keys:
            action = "watch"
            reason = "deferred_entry"
        elif ranked_top and current_in_portfolio:
            action = "hold"
            reason = "stay_top6"
        else:
            action = "watch"
            reason = "not_selected"

        plan_orders.append(
            {
                "plan_key": f"{trade_date}:{instrument}",
                "run_id": run_id,
                "signal_date": signal_date,
                "trade_date": trade_date,
                "instrument": instrument,
                "display_rank": display_rank,
                "current_holding_qty": current_holdings.get(instrument_key),
                "current_in_portfolio": current_in_portfolio,
                "ranked_top_n": ranked_top,
                "planned_in_portfolio": planned_in_portfolio,
                "strategy_action": action,
                "strategy_reason": reason,
                "source_strategies": source_strategies,
                "source_scores": source_scores,
                "policy_name": f"top{top_n}_drop{max_drop}",
                "updated_at": _utcnow(),
            }
        )

    orders = sorted(plan_orders, key=_next_trade_order_sort_key)
    counts = {
        "buy": sum(1 for item in orders if item["strategy_action"] == "buy"),
        "sell": sum(1 for item in orders if item["strategy_action"] == "sell"),
        "hold": sum(1 for item in orders if item["strategy_action"] == "hold"),
        "watch": sum(1 for item in orders if item["strategy_action"] == "watch"),
    }
    return {
        "run_id": run_id,
        "signal_date": signal_date,
        "trade_date": trade_date,
        "source_run_path": source_run_path,
        "target_n": top_n,
        "max_drop": max_drop,
        "holdings_snapshot_ref": holdings_snapshot_ref,
        "holdings_snapshot_path": str(holdings_snapshot_path),
        "holdings_confirmed_at": holdings_confirmed_at,
        "ranking_snapshot_ref": ranking_snapshot_ref,
        "ranking_snapshot_path": str(ranking_snapshot_path),
        "current_positions_count": len(current_holdings),
        "ranked_universe_count": ranked_universe_count,
        "bootstrap": bootstrap,
        "policy_name": f"top{top_n}_drop{max_drop}",
        "buy_count": counts["buy"],
        "sell_count": counts["sell"],
        "hold_count": counts["hold"],
        "watch_count": counts["watch"],
        "generated_at": _utcnow(),
        "orders": orders,
    }


def plan_output_dir(config: OrchestratorConfig, trade_date: str, run_id: str) -> Path:
    return config.plan_output_root / trade_date / run_id


def write_plan_csv(csv_path: Path, orders: list[dict[str, Any]]) -> None:
    fieldnames = [
        "plan_key",
        "run_id",
        "signal_date",
        "trade_date",
        "instrument",
        "display_rank",
        "current_holding_qty",
        "current_in_portfolio",
        "ranked_top_n",
        "planned_in_portfolio",
        "strategy_action",
        "strategy_reason",
        "source_strategies",
        "source_scores",
        "policy_name",
        "updated_at",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for order in orders:
            writer.writerow({key: order.get(key) for key in fieldnames})


def write_plan_html(html_path: Path, payload: dict[str, Any]) -> None:
    rows_html = []
    for order in payload["orders"]:
        rows_html.append(
            "<tr>"
            f"<td>{html.escape(str(order['instrument']))}</td>"
            f"<td>{html.escape(str(order['strategy_action']))}</td>"
            f"<td>{html.escape(str(order['strategy_reason']))}</td>"
            f"<td>{html.escape(str(order.get('display_rank') or ''))}</td>"
            f"<td>{html.escape(str(order.get('current_holding_qty') or ''))}</td>"
            f"<td>{html.escape(str(order.get('source_strategies') or ''))}</td>"
            f"<td>{html.escape(str(order.get('source_scores') or ''))}</td>"
            "</tr>"
        )
    html_body = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>CB Next Trade Plan {html.escape(str(payload['trade_date']))}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; color: #222; }}
    h1 {{ margin-bottom: 8px; }}
    .summary {{ display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 12px; margin: 20px 0; }}
    .card {{ padding: 12px 16px; border: 1px solid #ddd; border-radius: 12px; background: #fafafa; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 16px; }}
    th, td {{ border-bottom: 1px solid #eee; padding: 8px 10px; text-align: left; font-size: 14px; }}
    th {{ background: #f4f4f4; }}
  </style>
</head>
<body>
  <h1>下一交易日执行计划</h1>
  <p>run_id: {html.escape(str(payload['run_id']))}</p>
  <p>signal_date: {html.escape(str(payload.get('signal_date') or ''))} / trade_date: {html.escape(str(payload['trade_date']))}</p>
  <p>holdings_snapshot: {html.escape(str(payload.get('holdings_snapshot_ref') or ''))} / {html.escape(str(payload.get('holdings_snapshot_path') or ''))}</p>
  <p>holdings_confirmed_at: {html.escape(str(payload.get('holdings_confirmed_at') or ''))}</p>
  <p>ranking_snapshot: {html.escape(str(payload.get('ranking_snapshot_ref') or ''))} / {html.escape(str(payload.get('ranking_snapshot_path') or ''))}</p>
  <p>policy: {html.escape(str(payload['policy_name']))} / bootstrap: {html.escape(str(payload['bootstrap']))}</p>
  <div class="summary">
    <div class="card"><strong>买入</strong><br>{int(payload.get('buy_count') or 0)}</div>
    <div class="card"><strong>卖出</strong><br>{int(payload.get('sell_count') or 0)}</div>
    <div class="card"><strong>持有</strong><br>{int(payload.get('hold_count') or 0)}</div>
    <div class="card"><strong>观察</strong><br>{int(payload.get('watch_count') or 0)}</div>
  </div>
  <table>
    <thead>
      <tr>
        <th>标的</th>
        <th>动作</th>
        <th>原因</th>
        <th>排序</th>
        <th>当前持仓</th>
        <th>来源策略</th>
        <th>来源分数</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</body>
</html>
"""
    html_path.write_text(html_body, encoding="utf-8")


def load_ranking_snapshot(snapshot_path: Path) -> dict[str, Any]:
    payload = _load_json_object(snapshot_path, label="ranking snapshot")
    raw_entries = payload.get("ranked_entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValueError("ranking snapshot must contain a non-empty ranked_entries list")

    ranked_universe_count = _require_positive_int(payload, "ranked_universe_count", label="ranking snapshot")
    ranked_orders: list[dict[str, Any]] = []
    seen_instruments: set[str] = set()
    for index, item in enumerate(raw_entries, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"ranked_entries[{index}] must be an object")
        instrument = _normalize_instrument(_require_text(item, "instrument", label=f"ranked_entries[{index}]"))
        if instrument in seen_instruments:
            raise ValueError(f"ranking snapshot contains duplicate instrument: {instrument}")
        seen_instruments.add(instrument)
        ranked_orders.append(
            {
                "instrument": instrument,
                "display_rank": _require_positive_int(item, "display_rank", label=f"ranked_entries[{index}]"),
                "final_score": _require_float(item, "final_score", label=f"ranked_entries[{index}]"),
                "source_strategies": _normalize_string_list_or_text(
                    item.get("source_strategies"),
                    label=f"ranked_entries[{index}].source_strategies",
                ),
                "source_scores": _normalize_source_scores(
                    item.get("source_scores"),
                    label=f"ranked_entries[{index}].source_scores",
                ),
            }
        )

    if len(ranked_orders) != ranked_universe_count:
        raise ValueError(
            "ranking snapshot ranked_universe_count does not match ranked_entries length: "
            f"{ranked_universe_count} != {len(ranked_orders)}"
        )

    return {
        "signal_date": _require_text(payload, "signal_date", label="ranking snapshot"),
        "run_id": _require_text(payload, "run_id", label="ranking snapshot"),
        "ranking_snapshot_key": _require_text(payload, "ranking_snapshot_key", label="ranking snapshot"),
        "policy_name": _require_text(payload, "policy_name", label="ranking snapshot"),
        "generated_at": _require_text(payload, "generated_at", label="ranking snapshot"),
        "ranked_universe_count": ranked_universe_count,
        "ranked_orders": sorted(
            ranked_orders,
            key=lambda item: (_rank_sort_value(item.get("display_rank")), item["instrument"]),
        ),
    }


def _validate_snapshot_alignment(
    *,
    summary: dict[str, Any],
    holdings_snapshot: dict[str, Any],
    ranking_snapshot: dict[str, Any],
) -> None:
    summary_signal_date = _require_text(summary, "signal_date", label="run summary")
    summary_run_id = _require_text(summary, "run_id", label="run summary")
    if holdings_snapshot["signal_date"] != summary_signal_date:
        raise ValueError(
            "holdings snapshot signal_date does not match run summary: "
            f"{holdings_snapshot['signal_date']} != {summary_signal_date}"
        )
    if ranking_snapshot["signal_date"] != summary_signal_date:
        raise ValueError(
            "ranking snapshot signal_date does not match run summary: "
            f"{ranking_snapshot['signal_date']} != {summary_signal_date}"
        )
    if ranking_snapshot["run_id"] != summary_run_id:
        raise ValueError(
            "ranking snapshot run_id does not match run summary: "
            f"{ranking_snapshot['run_id']} != {summary_run_id}"
        )


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    try:
        payload = _load_json(path)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid {label} json: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _require_text(payload: dict[str, Any], key: str, *, label: str) -> str:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"{label} missing {key}")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{label} {key} must be non-empty")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _require_positive_int(payload: dict[str, Any], key: str, *, label: str) -> int:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"{label} missing {key}")
    try:
        numeric_value = float(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{label} {key} must be numeric") from exc
    if not numeric_value.is_integer():
        raise ValueError(f"{label} {key} must be an integer")
    normalized = int(numeric_value)
    if normalized <= 0:
        raise ValueError(f"{label} {key} must be positive")
    return normalized


def _require_float(payload: dict[str, Any], key: str, *, label: str) -> float:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"{label} missing {key}")
    try:
        return float(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{label} {key} must be numeric") from exc


def _normalize_string_list_or_text(value: Any, *, label: str) -> str:
    if value is None:
        raise ValueError(f"{label} is required")
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{label} must be non-empty")
        return normalized
    if isinstance(value, (list, tuple)):
        parts = [str(item).strip() for item in value if str(item).strip()]
        if not parts:
            raise ValueError(f"{label} must be non-empty")
        return ",".join(parts)
    raise ValueError(f"{label} must be a string or string list")


def _normalize_source_scores(value: Any, *, label: str) -> str:
    if value is None:
        raise ValueError(f"{label} is required")
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{label} must be non-empty")
        return normalized
    if isinstance(value, dict):
        normalized_pairs = []
        for key, item in value.items():
            normalized_key = str(key).strip()
            if not normalized_key:
                continue
            normalized_pairs.append(f"{normalized_key}={_format_scalar(item)}")
        if not normalized_pairs:
            raise ValueError(f"{label} must be non-empty")
        return ";".join(normalized_pairs)
    raise ValueError(f"{label} must be a string or object")


def _format_scalar(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value).strip()


def _next_trade_order_sort_key(order: dict[str, Any]) -> tuple[int, int, str]:
    priority = {
        "sell": 0,
        "watch": 1,
        "buy": 2,
        "hold": 3,
    }.get(order["strategy_action"], 9)
    rank = _rank_sort_value(order.get("display_rank"))
    return priority, rank, order["instrument"]


def _rank_sort_value(value: Any) -> int:
    if value is None:
        return 999999
    return int(value)


def _normalize_instrument(value: str) -> str:
    return str(value).strip().upper()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
