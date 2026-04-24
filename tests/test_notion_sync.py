from __future__ import annotations

import csv
import json
from pathlib import Path

from cb_orchestrator.notion_sync import (
    _build_top_summary,
    _derive_portfolio_move,
    _derive_rank_delta,
    _infer_holdings_source_type,
    _load_bond_name_map_file,
    _stringify_source_scores,
    _write_ranking_snapshot_csv,
)


def test_build_top_summary_prefers_bond_name_and_falls_back_to_code() -> None:
    ranking_snapshot = {
        "ranked_entries": [
            {"instrument": "SH113584", "display_rank": 1},
            {"instrument": "SZ128134", "display_rank": 2},
        ]
    }

    summary = _build_top_summary(
        ranking_snapshot,
        bond_name_map={"SH113584": "闻泰转债"},
        limit=2,
    )

    assert summary == "闻泰转债(SH113584) / SZ128134"


def test_derive_portfolio_move_maps_current_and_planned_flags() -> None:
    assert _derive_portfolio_move(current_in_portfolio=False, planned_in_portfolio=True) == "enter"
    assert _derive_portfolio_move(current_in_portfolio=True, planned_in_portfolio=True) == "keep"
    assert _derive_portfolio_move(current_in_portfolio=True, planned_in_portfolio=False) == "exit"
    assert _derive_portfolio_move(current_in_portfolio=False, planned_in_portfolio=False) == "ignore"


def test_derive_rank_delta_uses_previous_rank_minus_current_rank() -> None:
    assert _derive_rank_delta(8, 3) == 5
    assert _derive_rank_delta(3, 8) == -5
    assert _derive_rank_delta(None, 8) is None
    assert _derive_rank_delta(8, None) is None


def test_load_bond_name_map_file_supports_tushare_ts_code_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "cb_basic.csv"
    csv_path.write_text(
        "ts_code,bond_short_name\n113584.SH,闻泰转债\n128134.SZ,绿动转债\n",
        encoding="utf-8",
    )

    mapping = _load_bond_name_map_file(csv_path)

    assert mapping == {
        "SH113584": "闻泰转债",
        "SZ128134": "绿动转债",
    }


def test_write_ranking_snapshot_csv_materializes_full_snapshot(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "ranking_snapshot.json"
    payload = {
        "signal_date": "2026-04-20",
        "run_id": "20260421_150716",
        "ranking_snapshot_key": "ranking-2026-04-20",
        "policy_name": "ensemble_daily",
        "generated_at": "2026-04-21T15:00:00+08:00",
        "ranked_universe_count": 2,
        "ranked_entries": [
            {"instrument": "A", "display_rank": 1, "final_score": 0.9, "source_strategies": ["cb_batch_15", "cb_batch_27"], "source_scores": {"cb_batch_15": 0.9, "cb_batch_27": 0.8}},
            {"instrument": "B", "display_rank": 2, "final_score": 0.7, "source_strategies": ["cb_batch_27"], "source_scores": {"cb_batch_27": 0.7}},
        ],
    }
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    csv_path = _write_ranking_snapshot_csv(snapshot_path, payload)

    rows = list(csv.DictReader(csv_path.open("r", encoding="utf-8")))
    assert csv_path == snapshot_path.with_suffix(".csv")
    assert [row["instrument"] for row in rows] == ["A", "B"]
    assert rows[0]["source_strategies"] == "cb_batch_15,cb_batch_27"
    assert rows[0]["source_scores"] == "cb_batch_15=0.9;cb_batch_27=0.8"


def test_infer_holdings_source_type_prefers_explicit_status() -> None:
    assert _infer_holdings_source_type({"parse_status": "confirmed_empty"}, {"positions": {}}) == "bootstrap_empty"
    assert _infer_holdings_source_type({"parse_status": "confirmed_from_plan"}, {"positions": {"A": 1}}) == "executed_plan_replay"
    assert _infer_holdings_source_type({"raw_ocr_text": "foo"}, {"positions": {"A": 1}}) == "ocr_text"


def test_stringify_source_scores_sorts_keys() -> None:
    assert _stringify_source_scores({"b": 0.2, "a": 0.1}) == "a=0.1;b=0.2"
