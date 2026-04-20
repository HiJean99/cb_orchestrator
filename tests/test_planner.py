from __future__ import annotations

import json
from pathlib import Path

import pytest

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.planner import build_next_trade_plan, load_current_positions, plan_next_trade


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
        current_positions_json_path=state_root / "current_positions.json",
        plan_output_root=state_root / "next_trade_plans",
        next_trade_top_n=6,
        next_trade_max_drop=3,
    )


def _seed_success_run(config: OrchestratorConfig, *, trade_date: str = "2026-04-20", run_id: str = "20260420_083000", with_next_positions: bool = True) -> str:
    payload = {
        "run_id": run_id,
        "signal_date": "2026-04-17",
        "trade_date": trade_date,
        "status": "success",
        "strategies": [],
    }
    datasets = {
        "cb_batch_15": "instrument,rank,score\nA,1,0.91\nB,2,0.85\nC,3,0.84\nD,4,0.82\n",
        "cb_batch_27": "instrument,rank,score\nA,1,0.88\nE,5,0.78\nF,6,0.76\nG,7,0.72\n",
    }
    for strategy_id, current_csv in datasets.items():
        current_dir = config.prediction_root / strategy_id / trade_date
        top_path = current_dir / "top10.csv"
        next_path = current_dir / "next_positions.csv"
        prediction_summary_path = current_dir / "prediction_summary.json"

        _write(top_path, current_csv)
        if with_next_positions:
            _write(next_path, current_csv)
        _write_json(
            prediction_summary_path,
            {
                "top_prediction_path": str(top_path),
                "next_positions_path": str(next_path) if with_next_positions else str(current_dir / "missing_next_positions.csv"),
            },
        )
        payload["strategies"].append(
            {
                "strategy_id": strategy_id,
                "status": "success",
                "top_prediction_path": str(top_path),
                "prediction_summary_path": str(prediction_summary_path),
            }
        )

    latest_path = config.state_root / "latest.json"
    run_path = config.state_root / "runs" / f"{run_id}.json"
    _write_json(latest_path, payload)
    _write_json(run_path, payload)
    return run_id


def _seed_holdings(config: OrchestratorConfig, positions: list[dict[str, object]], *, snapshot_at: str = "2026-04-20T15:00:00+08:00") -> None:
    _write_json(
        config.current_positions_json_path,
        {
            "snapshot_at": snapshot_at,
            "positions": positions,
        },
    )


def test_plan_next_trade_bootstrap_outputs_top6(tmp_path: Path):
    config = _base_config(tmp_path)
    run_id = _seed_success_run(config)
    _seed_holdings(config, [])

    summary = plan_next_trade(config)

    assert summary["run_id"] == run_id
    assert summary["bootstrap"] is True
    assert summary["buy_count"] == 6
    assert [item["instrument"] for item in summary["orders"]] == ["A", "B", "C", "D", "E", "F"]
    assert all(item["strategy_action"] == "buy" for item in summary["orders"])
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
        current_positions_path=Path("/tmp/holdings.json"),
        current_positions_snapshot_at="2026-04-20T15:00:00+08:00",
        ranked_orders=ranked_orders,
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
    assert {code for code, item in by_instrument.items() if item["strategy_action"] == "sell"} == {"H", "I", "J"}
    assert by_instrument["G"]["strategy_action"] == "watch"
    assert by_instrument["G"]["strategy_reason"] == "deferred_drop"
    assert {code for code, item in by_instrument.items() if item["strategy_action"] == "buy"} == {"C", "D", "E"}
    assert by_instrument["F"]["strategy_action"] == "watch"
    assert by_instrument["F"]["strategy_reason"] == "deferred_entry"
    assert by_instrument["A"]["strategy_action"] == "hold"
    assert by_instrument["B"]["strategy_action"] == "hold"


def test_load_current_positions_sums_duplicates_and_ignores_zero(tmp_path: Path):
    positions_path = tmp_path / "holdings.json"
    _write_json(
        positions_path,
        {
            "snapshot_at": "2026-04-20T15:00:00+08:00",
            "positions": [
                {"instrument": "a", "holding_qty": 100},
                {"instrument": "A", "holding_qty": 20},
                {"instrument": "b", "holding_qty": 0},
                {"instrument": "C", "holding_qty": 50},
            ],
        },
    )

    payload = load_current_positions(positions_path)

    assert payload["positions"] == {"A": 120.0, "C": 50.0}


def test_plan_next_trade_falls_back_to_top_csv_when_next_positions_missing(tmp_path: Path):
    config = _base_config(tmp_path)
    _seed_success_run(config, with_next_positions=False)
    _seed_holdings(config, [])

    summary = plan_next_trade(config)

    assert summary["bootstrap"] is True
    assert [item["instrument"] for item in summary["orders"]][:3] == ["A", "B", "C"]


def test_plan_next_trade_missing_holdings_file_fails(tmp_path: Path):
    config = _base_config(tmp_path)
    _seed_success_run(config)

    with pytest.raises(FileNotFoundError):
        plan_next_trade(config)


def test_plan_next_trade_resolves_run_by_trade_date(tmp_path: Path):
    config = _base_config(tmp_path)
    _seed_success_run(config, trade_date="2026-04-18", run_id="20260418_083000")
    expected_run_id = _seed_success_run(config, trade_date="2026-04-20", run_id="20260420_083000")
    _seed_holdings(config, [])

    summary = plan_next_trade(config, trade_date="2026-04-20")

    assert summary["run_id"] == expected_run_id


def test_load_current_positions_rejects_invalid_payload(tmp_path: Path):
    positions_path = tmp_path / "holdings.json"
    _write_json(positions_path, {"snapshot_at": "2026-04-20T15:00:00+08:00"})

    with pytest.raises(ValueError):
        load_current_positions(positions_path)
