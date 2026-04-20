from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.nightly import run_release_window


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _base_config(tmp_path: Path) -> OrchestratorConfig:
    runtime_root = tmp_path / "runtime"
    state_root = tmp_path / "state"
    provider_uri = tmp_path / "provider"
    calendar_path = provider_uri / "calendars" / "day.txt"
    _write(calendar_path, "2026-04-20\n2026-04-21\n")

    for strategy_id in ("cb_batch_15", "cb_batch_27"):
        _write(runtime_root / "local_assets" / "packs" / strategy_id / "pack_manifest.json", "{}")

    return OrchestratorConfig(
        upstream_python_bin=Path("/usr/bin/python3"),
        upstream_repo_root=tmp_path / "infra",
        upstream_state_file=state_root / "latest.json",
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
        current_positions_json_path=state_root / "current_positions.json",
        plan_output_root=state_root / "next_trade_plans",
        upstream_mode="github_release",
        github_release_token="token",
        release_install_dir=provider_uri,
        release_work_dir=state_root / "release_tmp",
        release_poll_interval_minutes=15,
        release_window_end_hour=4,
    )


class _FakeClock:
    def __init__(self, current: datetime):
        self.current = current

    def now(self) -> datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


def test_release_window_polls_until_new_release_then_runs(monkeypatch, tmp_path: Path):
    config = _base_config(tmp_path)
    clock = _FakeClock(datetime(2026, 4, 20, 22, 30, 0, tzinfo=SHANGHAI_TZ))
    probes = iter(
        [
            {"release_poll_status": "missing_release"},
            {
                "release_poll_status": "ready",
                "release_latest_complete_trade_date": "2026-04-20",
                "release_target_trade_date": "2026-04-20",
                "release_content_fingerprint": "abc",
                "release_asset_name": "cb-qlib-data-latest.tar.zst",
            },
        ]
    )

    monkeypatch.setattr("cb_orchestrator.nightly.inspect_latest_release", lambda *args, **kwargs: next(probes))
    monkeypatch.setattr("cb_orchestrator.nightly.load_release_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("cb_orchestrator.nightly.release_is_new", lambda probe, *_args, **_kwargs: probe.get("release_poll_status") == "ready")
    monkeypatch.setattr(
        "cb_orchestrator.nightly.orchestrate_daily",
        lambda *_args, **_kwargs: {
            "status": "success",
            "run_id": "20260420_223000",
            "signal_date": "2026-04-20",
            "trade_date": "2026-04-21",
            "release_install_status": "success",
            "release_content_fingerprint": "abc",
        },
    )

    summary = run_release_window(config, now_fn=clock.now, sleep_fn=clock.sleep)

    assert summary["status"] == "success"
    assert summary["attempt_count"] == 2
    assert summary["consumed_trade_date"] == "2026-04-20"


def test_release_window_exits_when_no_release_before_window_end(monkeypatch, tmp_path: Path):
    config = _base_config(tmp_path)
    config = OrchestratorConfig(**{**config.__dict__, "release_poll_interval_minutes": 30})
    clock = _FakeClock(datetime(2026, 4, 21, 3, 50, 0, tzinfo=SHANGHAI_TZ))

    monkeypatch.setattr(
        "cb_orchestrator.nightly.inspect_latest_release",
        lambda *args, **kwargs: {"release_poll_status": "missing_release"},
    )
    monkeypatch.setattr("cb_orchestrator.nightly.load_release_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("cb_orchestrator.nightly.release_is_new", lambda *_args, **_kwargs: False)

    summary = run_release_window(config, now_fn=clock.now, sleep_fn=clock.sleep)

    assert summary["status"] == "skipped_release_not_ready"
    assert summary["attempt_count"] == 2
