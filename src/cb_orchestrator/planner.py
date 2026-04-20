from __future__ import annotations

import csv
import html
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cb_orchestrator.config import OrchestratorConfig

INSTRUMENT_COLUMNS = ("instrument", "symbol", "code", "ticker", "stock_code", "ts_code")
RANK_COLUMNS = ("rank", "score_rank", "position_rank")
SCORE_COLUMNS = ("score", "pred", "prediction", "rank_score", "prob")


@dataclass(frozen=True)
class ParsedRow:
    instrument: str
    rank: int | None
    score: float | None


def plan_next_trade(
    config: OrchestratorConfig,
    *,
    run_id: str | None = None,
    trade_date: str | None = None,
) -> dict[str, Any]:
    summary = resolve_run_summary(config, run_id=run_id, trade_date=trade_date)
    if summary.get("status") != "success":
        raise ValueError(f"run is not successful: {summary.get('run_id')} status={summary.get('status')}")

    ranked_orders = build_ranked_orders(summary)
    holdings_snapshot = load_current_positions(config.current_positions_json_path)
    payload = build_next_trade_plan(
        run_id=str(summary["run_id"]),
        signal_date=summary.get("signal_date"),
        trade_date=summary.get("trade_date"),
        source_run_path=summary.get("source_run_path"),
        current_positions_path=config.current_positions_json_path,
        current_positions_snapshot_at=holdings_snapshot.get("snapshot_at"),
        ranked_orders=ranked_orders,
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


def build_ranked_orders(summary: dict[str, Any]) -> list[dict[str, Any]]:
    strategy_summaries = list(summary.get("strategies") or [])
    if not strategy_summaries:
        raise ValueError("run summary has no strategies")

    bucket: dict[str, dict[str, Any]] = {}
    for strategy_summary in strategy_summaries:
        strategy_id = str(strategy_summary.get("strategy_id") or "")
        if not strategy_id:
            raise ValueError("missing strategy_id in run summary")
        if strategy_summary.get("status") not in {None, "success"}:
            raise ValueError(f"strategy is not successful: {strategy_id} status={strategy_summary.get('status')}")
        rows = load_strategy_rows(strategy_summary)
        for row in rows:
            aggregate = bucket.setdefault(
                row.instrument,
                {
                    "instrument": row.instrument,
                    "source_entries": [],
                },
            )
            aggregate["source_entries"].append(
                {
                    "strategy_id": strategy_id,
                    "rank": row.rank,
                    "score": row.score,
                }
            )

    ranked_orders: list[dict[str, Any]] = []
    for instrument, aggregate in bucket.items():
        entries = list(aggregate.pop("source_entries"))
        ranks = [entry["rank"] for entry in entries if entry["rank"] is not None]
        aggregate["display_rank"] = min(ranks) if ranks else None
        aggregate["source_strategies"] = ",".join(entry["strategy_id"] for entry in entries)
        aggregate["source_scores"] = ";".join(
            f"{entry['strategy_id']}={_format_float(entry['score'])}"
            for entry in entries
            if entry["score"] is not None
        )
        ranked_orders.append(aggregate)

    return sorted(
        ranked_orders,
        key=lambda item: (_rank_sort_value(item.get("display_rank")), item["instrument"]),
    )


def load_strategy_rows(strategy_summary: dict[str, Any]) -> list[ParsedRow]:
    prediction_summary = _load_prediction_summary(strategy_summary)
    next_positions_path = _resolve_existing_path(
        prediction_summary.get("next_positions_path"),
        prediction_summary.get("next_positions_csv"),
        prediction_summary.get("positions_path"),
    )
    top_prediction_path = _resolve_existing_path(
        prediction_summary.get("top_prediction_path"),
        strategy_summary.get("top_prediction_path"),
    )
    current_path = next_positions_path or top_prediction_path
    if current_path is None:
        raise FileNotFoundError(f"missing prediction artifact for strategy {strategy_summary.get('strategy_id')}")
    return parse_prediction_rows(current_path)


def parse_prediction_rows(csv_path: Path) -> list[ParsedRow]:
    rows = read_csv_rows(csv_path)
    parsed: list[ParsedRow] = []
    for index, row in enumerate(rows, start=1):
        instrument = _first_text(row, INSTRUMENT_COLUMNS)
        if not instrument:
            continue
        rank_value = _first_int(row, RANK_COLUMNS)
        if rank_value is None:
            rank_value = index
        parsed.append(
            ParsedRow(
                instrument=_normalize_instrument(instrument),
                rank=rank_value,
                score=_first_float(row, SCORE_COLUMNS),
            )
        )
    return parsed


def load_current_positions(positions_json_path: Path) -> dict[str, Any]:
    if not positions_json_path.exists():
        raise FileNotFoundError(f"holdings json not found: {positions_json_path}")
    try:
        payload = json.loads(positions_json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid holdings json: {positions_json_path}") from exc

    if not isinstance(payload, dict):
        raise ValueError("holdings json must be an object")
    raw_positions = payload.get("positions")
    if not isinstance(raw_positions, list):
        raise ValueError("holdings json must contain a positions list")

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
        "snapshot_at": payload.get("snapshot_at"),
        "positions": dict(sorted(positions.items())),
    }


def build_next_trade_plan(
    *,
    run_id: str,
    signal_date: str | None,
    trade_date: str | None,
    source_run_path: str | None,
    current_positions_path: Path,
    current_positions_snapshot_at: str | None,
    ranked_orders: list[dict[str, Any]],
    current_positions: dict[str, float],
    top_n: int,
    max_drop: int,
) -> dict[str, Any]:
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
        "current_positions_path": str(current_positions_path),
        "current_positions_snapshot_at": current_positions_snapshot_at,
        "current_positions_count": len(current_holdings),
        "ranked_universe_count": len(ranked_orders),
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


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"csv not found: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


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
  <p>holdings: {html.escape(str(payload.get('current_positions_path') or ''))}</p>
  <p>snapshot_at: {html.escape(str(payload.get('current_positions_snapshot_at') or ''))}</p>
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


def _load_prediction_summary(strategy_summary: dict[str, Any]) -> dict[str, Any]:
    prediction_summary_path = strategy_summary.get("prediction_summary_path")
    if prediction_summary_path:
        path = Path(str(prediction_summary_path))
        if path.exists():
            return _load_json(path)
    return strategy_summary.get("prediction_summary") or {}


def _resolve_existing_path(*candidates: Any) -> Path | None:
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(str(candidate))
        if path.exists():
            return path
    return None


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _first_text(row: dict[str, Any], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        value = row.get(candidate)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _first_float(row: dict[str, Any], candidates: tuple[str, ...]) -> float | None:
    for candidate in candidates:
        value = row.get(candidate)
        if value is None:
            continue
        try:
            return float(str(value).strip())
        except ValueError:
            continue
    return None


def _first_int(row: dict[str, Any], candidates: tuple[str, ...]) -> int | None:
    value = _first_float(row, candidates)
    if value is None:
        return None
    return int(value)


def _format_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6f}".rstrip("0").rstrip(".")


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
