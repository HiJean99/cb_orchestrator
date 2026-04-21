from __future__ import annotations

import json
from pathlib import Path

import pytest

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.planner import (
    build_next_trade_plan,
    load_holdings_snapshot,
    load_ranking_snapshot,
    plan_next_trade,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    _write(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _base_config(tmp_path: Path) -> OrchestratorConfig:
    runtime_root = tmp_path / "runtime"
    state_root = tmp_path / "state"
    upstream_root = tmp_path / "infra"
    provider_uri = tmp_path / "provider"
    (upstream_root / "scripts").mkdir(parents=True, exist_ok=True)
    (provider_uri / "calendars").mkdir(parents=True, exist_ok=True)
    _write(provider_uri / "calendars" / "day.txt", "2026-04-17\n2026-04-20\n")

    return OrchestratorConfig(
        upstream_python_bin=Path("/usr/bin/python3"),
        upstream_repo_root=upstream_root,
        upstream_state_file=state_root / "latest.json",
        upstream_env_file=None,
        provider_uri=provider_uri,
        trade_calendar_path=provider_uri / "calendars" / "day.txt",
        runtime_repo_root=runtime_root,
        packs_root=runtime_root / "local_assets" / "packs",
        model_root=runtime_root / "local_state" / "models",
        build_root=runtime_root / "local_state" / "builds",
        prediction_root=runtime_root / "local_state" / "predictions",
        log_root=runtime_root / "local_state" / "logs",
        state_root=state_root,
        lock_path=state_root / "orchestrator.lock",
        strategy_ids=("cb_batch_15", "cb_batch_27"),
        train_start="2018-01-01",
        rolling_valid_months=12,
        model_num_threads=2,
        top_count=10,
        upstream_lookback_trade_days=7,
        upstream_repair_trade_days=20,
        upstream_allow_missing_symbols="",
        email_env={},
        plan_input_root=state_root / "plan_inputs",
        plan_output_root=state_root / "next_trade_plans",
        next_trade_top_n=6,
        next_trade_max_drop=3,
    )


def _seed_success_run(
    config: OrchestratorConfig,
    *,
    signal_date: str = "2026-04-17",
    trade_date: str = "2026-04-20",
    run_id: str = "20260420_083000",
) -> str:
    payload = {
        "run_id": run_id,
        "signal_date": signal_date,
        "trade_date": trade_date,
        "status": "success",
        "strategies": [],
    }
    latest_path = config.state_root / "latest.json"
    run_path = config.state_root / "runs" / f"{run_id}.json"
    _write_json(latest_path, payload)
    _write_json(run_path, payload)
    return run_id


def _make_ranked_entries(instruments: tuple[str, ...]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for index, instrument in enumerate(instruments, start=1):
        entries.append(
            {
                "instrument": instrument,
                "display_rank": index,
                "final_score": round(1.0 - index * 0.01, 6),
                "source_strategies": ["cb_batch_15", "cb_batch_27"],
                "source_scores": {
                    "cb_batch_15": round(1.0 - index * 0.01, 6),
                    "cb_batch_27": round(0.9 - index * 0.01, 6),
                },
            }
        )
    return entries


def _seed_plan_inputs(
    config: OrchestratorConfig,
    *,
    signal_date: str,
    run_id: str,
    positions: list[dict[str, object]],
    ranked_entries: list[dict[str, object]] | None = None,
    parse_status: str = "parsed",
    confirmed_at: str | None = None,
    holdings_signal_date: str | None = None,
    ranking_signal_date: str | None = None,
    ranking_run_id: str | None = None,
) -> None:
    ranked_entries = ranked_entries or _make_ranked_entries(("A", "B", "C", "D", "E", "F", "G"))
    bundle_dir = config.plan_input_root / signal_date

    _write_json(
        bundle_dir / "holdings_snapshot.json",
        {
            "signal_date": holdings_signal_date or signal_date,
            "snapshot_key": f"holding-{signal_date}",
            "parse_status": parse_status,
            "submitted_at": f"{signal_date}T15:00:00+08:00",
            "parsed_at": f"{signal_date}T15:01:00+08:00",
            "confirmed_at": confirmed_at,
            "positions": positions,
        },
    )
    _write_json(
        bundle_dir / "ranking_snapshot.json",
        {
            "signal_date": ranking_signal_date or signal_date,
            "run_id": ranking_run_id or run_id,
            "ranking_snapshot_key": f"ranking-{signal_date}",
            "policy_name": "ensemble_daily",
            "generated_at": f"{signal_date}T20:00:00+08:00",
            "ranked_universe_count": len(ranked_entries),
            "ranked_entries": ranked_entries,
        },
    )


def test_plan_next_trade_bootstrap_outputs_top6_from_snapshot_bundle(tmp_path: Path):
    config = _base_config(tmp_path)
    run_id = _seed_success_run(config)
    _seed_plan_inputs(config, signal_date="2026-04-17", run_id=run_id, positions=[])

    summary = plan_next_trade(config)

    assert summary["run_id"] == run_id
    assert summary["bootstrap"] is True
    assert summary["target_n"] == 6
    assert summary["max_drop"] == 3
    assert summary["holdings_snapshot_ref"] == "holding-2026-04-17"
    assert summary["ranking_snapshot_ref"] == "ranking-2026-04-17"
    assert summary["holdings_confirmed_at"] is None
    assert [item["instrument"] for item in summary["orders"]] == ["A", "B", "C", "D", "E", "F"]
    assert all(item["strategy_action"] == "buy" for item in summary["orders"])
    assert summary["orders"][0]["source_strategies"] == "cb_batch_15,cb_batch_27"
    assert "cb_batch_15=" in summary["orders"][0]["source_scores"]
    assert Path(summary["json_path"]).exists()
    assert Path(summary["csv_path"]).exists()
    assert Path(summary["html_path"]).exists()


def test_build_next_trade_plan_respects_top6_drop3_with_actual_holdings():
    ranked_orders = [
        {"instrument": item, "display_rank": index, "source_strategies": "cb_batch_15,cb_batch_27", "source_scores": ""}
        for index, item in enumerate(("A", "B", "C", "D", "E", "F", "G", "H"), start=1)
    ]
    payload = build_next_trade_plan(
        run_id="20260420_083000",
        signal_date="2026-04-20",
        trade_date="2026-04-21",
        source_run_path="/tmp/run.json",
        holdings_snapshot_ref="holding-2026-04-20",
        holdings_snapshot_path=Path("/tmp/plan_inputs/2026-04-20/holdings_snapshot.json"),
        holdings_confirmed_at=None,
        ranking_snapshot_ref="ranking-2026-04-20",
        ranking_snapshot_path=Path("/tmp/plan_inputs/2026-04-20/ranking_snapshot.json"),
        ranked_orders=ranked_orders,
        ranked_universe_count=len(ranked_orders),
        current_positions={
            "A": 100.0,
            "B": 100.0,
            "G": 100.0,
            "H": 100.0,
            "I": 100.0,
            "J": 100.0,
        },
        top_n=6,
        max_drop=3,
    )
    by_instrument = {item["instrument"]: item for item in payload["orders"]}

    assert payload["bootstrap"] is False
    assert payload["current_positions_count"] == 6
    assert payload["holdings_snapshot_ref"] == "holding-2026-04-20"
    assert payload["ranking_snapshot_ref"] == "ranking-2026-04-20"
    assert {code for code, item in by_instrument.items() if item["strategy_action"] == "sell"} == {"H", "I", "J"}
    assert by_instrument["G"]["strategy_action"] == "watch"
    assert by_instrument["G"]["strategy_reason"] == "deferred_drop"
    assert {code for code, item in by_instrument.items() if item["strategy_action"] == "buy"} == {"C", "D", "E"}
    assert by_instrument["F"]["strategy_action"] == "watch"
    assert by_instrument["F"]["strategy_reason"] == "deferred_entry"
    assert by_instrument["A"]["strategy_action"] == "hold"
    assert by_instrument["B"]["strategy_action"] == "hold"


def test_load_holdings_snapshot_sums_duplicates_and_ignores_zero(tmp_path: Path):
    snapshot_path = tmp_path / "holdings_snapshot.json"
    _write_json(
        snapshot_path,
        {
            "signal_date": "2026-04-20",
            "snapshot_key": "holding-2026-04-20",
            "parse_status": "parsed",
            "positions": [
                {"instrument": "a", "holding_qty": 100},
                {"instrument": "A", "holding_qty": 20},
                {"instrument": "b", "holding_qty": 0},
                {"instrument": "C", "holding_qty": 50},
            ],
        },
    )

    payload = load_holdings_snapshot(snapshot_path)

    assert payload["positions"] == {"A": 120.0, "C": 50.0}


def test_load_ranking_snapshot_normalizes_complete_ranked_entries(tmp_path: Path):
    snapshot_path = tmp_path / "ranking_snapshot.json"
    _write_json(
        snapshot_path,
        {
            "signal_date": "2026-04-20",
            "run_id": "20260420_083000",
            "ranking_snapshot_key": "ranking-2026-04-20",
            "policy_name": "ensemble_daily",
            "generated_at": "2026-04-20T20:00:00+08:00",
            "ranked_universe_count": 2,
            "ranked_entries": [
                {
                    "instrument": "a",
                    "display_rank": 1,
                    "final_score": 0.95,
                    "source_strategies": ["cb_batch_15", "cb_batch_27"],
                    "source_scores": {"cb_batch_15": 0.95, "cb_batch_27": 0.85},
                },
                {
                    "instrument": "B",
                    "display_rank": 2,
                    "final_score": 0.9,
                    "source_strategies": "cb_batch_15",
                    "source_scores": "cb_batch_15=0.9",
                },
            ],
        },
    )

    payload = load_ranking_snapshot(snapshot_path)

    assert payload["ranked_orders"][0]["instrument"] == "A"
    assert payload["ranked_orders"][0]["source_strategies"] == "cb_batch_15,cb_batch_27"
    assert payload["ranked_orders"][0]["source_scores"] == "cb_batch_15=0.95;cb_batch_27=0.85"


def test_plan_next_trade_missing_holdings_snapshot_fails(tmp_path: Path):
    config = _base_config(tmp_path)
    _seed_success_run(config)

    with pytest.raises(FileNotFoundError):
        plan_next_trade(config)


def test_plan_next_trade_resolves_run_by_trade_date(tmp_path: Path):
    config = _base_config(tmp_path)
    _seed_success_run(config, signal_date="2026-04-15", trade_date="2026-04-18", run_id="20260418_083000")
    expected_run_id = _seed_success_run(config, signal_date="2026-04-17", trade_date="2026-04-20", run_id="20260420_083000")
    _seed_plan_inputs(config, signal_date="2026-04-17", run_id=expected_run_id, positions=[])

    summary = plan_next_trade(config, trade_date="2026-04-20")

    assert summary["run_id"] == expected_run_id


def test_plan_next_trade_allows_unconfirmed_holdings_snapshot(tmp_path: Path):
    config = _base_config(tmp_path)
    run_id = _seed_success_run(config)
    _seed_plan_inputs(config, signal_date="2026-04-17", run_id=run_id, positions=[], confirmed_at=None, parse_status="parsed")

    summary = plan_next_trade(config)

    assert summary["holdings_confirmed_at"] is None
    assert summary["bootstrap"] is True


def test_load_holdings_snapshot_rejects_invalid_payload(tmp_path: Path):
    snapshot_path = tmp_path / "holdings_snapshot.json"
    _write_json(snapshot_path, {"signal_date": "2026-04-20", "snapshot_key": "holding-2026-04-20", "parse_status": "parsed"})

    with pytest.raises(ValueError):
        load_holdings_snapshot(snapshot_path)


def test_load_ranking_snapshot_rejects_partial_payload(tmp_path: Path):
    snapshot_path = tmp_path / "ranking_snapshot.json"
    _write_json(
        snapshot_path,
        {
            "signal_date": "2026-04-20",
            "run_id": "20260420_083000",
            "ranking_snapshot_key": "ranking-2026-04-20",
            "policy_name": "ensemble_daily",
            "generated_at": "2026-04-20T20:00:00+08:00",
            "ranked_universe_count": 3,
            "ranked_entries": _make_ranked_entries(("A", "B")),
        },
    )

    with pytest.raises(ValueError):
        load_ranking_snapshot(snapshot_path)


def test_plan_next_trade_rejects_signal_date_mismatch(tmp_path: Path):
    config = _base_config(tmp_path)
    run_id = _seed_success_run(config)
    _seed_plan_inputs(
        config,
        signal_date="2026-04-17",
        run_id=run_id,
        positions=[],
        holdings_signal_date="2026-04-18",
    )

    with pytest.raises(ValueError):
        plan_next_trade(config)


def test_plan_next_trade_rejects_ranking_run_id_mismatch(tmp_path: Path):
    config = _base_config(tmp_path)
    run_id = _seed_success_run(config)
    _seed_plan_inputs(
        config,
        signal_date="2026-04-17",
        run_id=run_id,
        positions=[],
        ranking_run_id="20260420_999999",
    )

    with pytest.raises(ValueError):
        plan_next_trade(config)
