from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import pytest

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.orchestrator import CommandResult, LockNotAcquiredError, orchestrate_daily


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    _write(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _calendar_payload() -> str:
    return "\n".join(
        [
            "2026-04-15",
            "2026-04-16",
            "2026-04-17",
            "2026-04-20",
            "2026-04-30",
            "2026-05-06",
            "2026-05-07",
        ]
    )


def _base_config(tmp_path: Path) -> OrchestratorConfig:
    runtime_root = tmp_path / "runtime"
    state_root = tmp_path / "state"
    upstream_root = tmp_path / "infra"
    provider_uri = tmp_path / "provider"
    calendar_path = provider_uri / "calendars" / "day.txt"
    _write(calendar_path, _calendar_payload())
    _write(upstream_root / "scripts" / "orchestrate_daily_update.py", "print('noop')\n")

    for strategy_id in ("cb_batch_15", "cb_batch_27"):
        pack_dir = runtime_root / "local_assets" / "packs" / strategy_id
        _write(pack_dir / "pack_manifest.json", "{}")
        _write(pack_dir / "event_exit_audit.csv", "trade_date,instrument\n")

    return OrchestratorConfig(
        upstream_python_bin=Path("/usr/bin/python3"),
        upstream_repo_root=upstream_root,
        upstream_state_file=tmp_path / "upstream_state" / "latest.json",
        upstream_env_file=None,
        provider_uri=provider_uri,
        trade_calendar_path=calendar_path,
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
    )


def _success_state(target_trade_date: str = "2026-04-17") -> dict:
    return {
        "exit_class": "success",
        "cb_status": "success",
        "qlib_status": "success",
        "target_trade_date": target_trade_date,
    }


def _fake_runner_factory(config: OrchestratorConfig, *, email_sent: bool = True):
    calls: list[tuple[str, str]] = []

    def _runner(command, env=None, cwd=None):
        env = dict(env or {})
        joined = " ".join(command)
        pack_name = env.get("PACK_NAME", "")
        calls.append((Path(command[-1]).name if command else "python", pack_name))

        if "train_monthly.sh" in joined:
            model_dir = config.model_root / pack_name / env["MODEL_VERSION"]
            model_dir.mkdir(parents=True, exist_ok=True)
            _write_json(model_dir / "train_manifest.json", {"strategy_id": pack_name})
        if "predict_daily.sh" in joined:
            out_dir = config.prediction_root / pack_name / env["TRADE_DATE"]
            out_dir.mkdir(parents=True, exist_ok=True)
            top_path = out_dir / f"top{env['TOP_COUNT']}.csv"
            _write(top_path, "instrument,score\nA,1.0\n")
            _write_json(
                out_dir / "prediction_summary.json",
                {
                    "strategy_id": pack_name,
                    "trade_date": env["TRADE_DATE"],
                    "top_prediction_path": str(top_path),
                    "email_sent": email_sent,
                    "email_error": None if email_sent else "network is unreachable",
                },
            )
        return CommandResult(tuple(command), str(cwd) if cwd else None, 0, "", "")

    return calls, _runner


def test_skip_non_trading_day(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, {"exit_class": "non_trading_day", "cb_status": "skipped", "qlib_status": "skipped", "target_trade_date": None})

    summary = orchestrate_daily(config, dry_run=True, skip_upstream=True)

    assert summary["status"] == "skipped_non_trading_day"
    assert summary["strategies"] == []


def test_success_path_runs_train_then_predict_serially(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, _success_state())
    calls, runner = _fake_runner_factory(config)

    summary = orchestrate_daily(config, skip_upstream=True, runner=runner)

    assert summary["status"] == "success"
    assert [item["status"] for item in summary["strategies"]] == ["success", "success"]
    assert calls == [
        ("train_monthly.sh", "cb_batch_15"),
        ("predict_daily.sh", "cb_batch_15"),
        ("train_monthly.sh", "cb_batch_27"),
        ("predict_daily.sh", "cb_batch_27"),
    ]


def test_existing_model_skips_train(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, _success_state())
    existing_model = config.model_root / "cb_batch_15" / "2026_04_monthly"
    existing_model.mkdir(parents=True, exist_ok=True)
    _write_json(existing_model / "train_manifest.json", {"strategy_id": "cb_batch_15"})
    calls, runner = _fake_runner_factory(config)

    summary = orchestrate_daily(config, skip_upstream=True, runner=runner)

    assert summary["status"] == "success"
    assert calls[0] == ("predict_daily.sh", "cb_batch_15")
    assert ("train_monthly.sh", "cb_batch_15") not in calls


def test_partial_upstream_state_stops_downstream(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, {"exit_class": "success", "cb_status": "success", "qlib_status": "running", "target_trade_date": "2026-04-17"})

    summary = orchestrate_daily(config, dry_run=True, skip_upstream=True)

    assert summary["status"] == "skipped_upstream_not_ready"
    assert summary["strategies"] == []


def test_email_failure_does_not_fail_orchestration(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, _success_state())
    calls, runner = _fake_runner_factory(config, email_sent=False)

    summary = orchestrate_daily(config, skip_upstream=True, runner=runner)

    assert summary["status"] == "success"
    assert summary["strategies"][0]["prediction_summary"]["email_sent"] is False
    assert len(calls) == 4


def test_lock_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, _success_state())

    @contextmanager
    def _busy(_lock_path):
        raise LockNotAcquiredError("busy")
        yield

    monkeypatch.setattr("cb_orchestrator.orchestrator.file_lock", _busy)

    summary = orchestrate_daily(config, dry_run=True, skip_upstream=True)

    assert summary["status"] == "skipped_locked"
