from __future__ import annotations

import csv
import json
from pathlib import Path

from cb_orchestrator.notion_sync import (
    _build_focus_entries,
    _infer_holdings_source_type,
    _stringify_source_scores,
    _write_ranking_snapshot_csv,
)


def test_build_focus_entries_keeps_top_focus_and_current_holdings() -> None:
    ranking_snapshot = {
        "signal_date": "2026-04-20",
        "ranking_snapshot_key": "ranking-2026-04-20",
        "ranked_entries": [
            {"instrument": "A", "display_rank": 1, "final_score": 0.9, "source_strategies": ["s1"], "source_scores": {"s1": 0.9}},
            {"instrument": "B", "display_rank": 2, "final_score": 0.8, "source_strategies": ["s1"], "source_scores": {"s1": 0.8}},
            {"instrument": "C", "display_rank": 3, "final_score": 0.7, "source_strategies": ["s1"], "source_scores": {"s1": 0.7}},
            {"instrument": "D", "display_rank": 4, "final_score": 0.6, "source_strategies": ["s1"], "source_scores": {"s1": 0.6}},
            {"instrument": "E", "display_rank": 5, "final_score": 0.5, "source_strategies": ["s1"], "source_scores": {"s1": 0.5}},
        ],
    }

    rows = _build_focus_entries(
        ranking_snapshot=ranking_snapshot,
        current_positions={"E": 100.0, "Z": 100.0},
        top_n=3,
        focus_top_k=2,
    )

    assert [row["instrument"] for row in rows] == ["A", "B", "E", "Z"]
    assert rows[0]["in_top_n"] is True
    assert rows[1]["in_focus_top_k"] is True
    assert rows[2]["current_holding_qty"] == 100.0
    assert rows[3]["display_rank"] is None
    assert rows[3]["current_in_portfolio"] is True


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
