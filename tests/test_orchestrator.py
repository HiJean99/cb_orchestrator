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
        plan_input_root=state_root / "plan_inputs",
        plan_output_root=state_root / "next_trade_plans",
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


def test_config_preserves_upstream_python_symlink(tmp_path: Path):
    real_python = tmp_path / "real_python"
    real_python.write_text("#!/bin/sh\n", encoding="utf-8")
    symlink_python = tmp_path / "venv_python"
    symlink_python.symlink_to(real_python)
    env_file = tmp_path / "cb-orchestrator.env"
    env_file.write_text(
        (
            f"UPSTREAM_PYTHON_BIN={symlink_python}\n"
            f"RUNTIME_REPO_ROOT={tmp_path / 'runtime'}\n"
            f"PLAN_INPUT_ROOT={tmp_path / 'plan_inputs'}\n"
            f"PLAN_OUTPUT_ROOT={tmp_path / 'plans'}\n"
            "NEXT_TRADE_TOP_N=6\n"
            "NEXT_TRADE_MAX_DROP=3\n"
        ),
        encoding="utf-8",
    )

    config = OrchestratorConfig.from_sources(env_file=env_file, environ={})

    assert config.upstream_python_bin == symlink_python
    assert config.plan_input_root == tmp_path / "plan_inputs"
    assert config.plan_output_root == tmp_path / "plans"
    assert config.next_trade_top_n == 6
    assert config.next_trade_max_drop == 3


def test_redacted_dict_masks_top_level_secrets(tmp_path: Path):
    config = OrchestratorConfig(
        **{
            **_base_config(tmp_path).__dict__,
            "github_release_token": "secret-token",
        }
    )

    payload = config.redacted_dict()

    assert payload["github_release_token"] == "***"


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


def test_runtime_slugged_prediction_paths_are_accepted(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, _success_state())
    for strategy_id in ("cb_batch_15", "cb_batch_27"):
        model_dir = config.model_root / strategy_id / "2026_04_monthly"
        model_dir.mkdir(parents=True, exist_ok=True)
        _write_json(model_dir / "train_manifest.json", {"strategy_id": strategy_id})

    def _runner(command, env=None, cwd=None):
        env = dict(env or {})
        if "predict_daily.sh" in " ".join(command):
            out_dir = config.prediction_root / env["PACK_NAME"] / env["TRADE_DATE"].replace("-", "_")
            out_dir.mkdir(parents=True, exist_ok=True)
            top_path = out_dir / f"top{env['TOP_COUNT']}.csv"
            _write(top_path, "instrument,score\nA,1.0\n")
            _write_json(
                out_dir / "prediction_summary.json",
                {
                    "strategy_id": env["PACK_NAME"],
                    "trade_date": env["TRADE_DATE"],
                    "top_prediction_path": str(top_path),
                    "email_sent": False,
                },
            )
        return CommandResult(tuple(command), str(cwd) if cwd else None, 0, "", "")

    summary = orchestrate_daily(config, skip_upstream=True, runner=_runner)

    assert summary["status"] == "success"
    assert summary["strategies"][0]["top_prediction_path"].endswith("2026_04_20/top10.csv")


def test_base_env_includes_upstream_env_file_values(tmp_path: Path):
    config = _base_config(tmp_path)
    upstream_env = tmp_path / "upstream.env"
    upstream_env.write_text("TUSHARE_TOKEN=secret\nPYTHON_BIN=/tmp/python\n", encoding="utf-8")
    config = OrchestratorConfig(
        **{**config.__dict__, "upstream_env_file": upstream_env}
    )

    from cb_orchestrator.orchestrator import _base_env

    env = _base_env(config)

    assert env["UPSTREAM_ENV_FILE"] == str(upstream_env)
    assert env["TUSHARE_TOKEN"] == "secret"
    assert env["PYTHON_BIN"] == "/tmp/python"


def test_runtime_env_passes_runtime_binary_overrides(tmp_path: Path):
    config = OrchestratorConfig(
        **{
            **_base_config(tmp_path).__dict__,
            "runtime_python_bin": tmp_path / "q_lab_python",
            "runtime_pythonpath": (tmp_path / "runtime_src", tmp_path / "extra_src"),
            "runtime_train_bin": tmp_path / "custom_train",
            "runtime_predict_bin": tmp_path / "custom_predict",
        }
    )

    from cb_orchestrator.orchestrator import _runtime_env

    env = _runtime_env(
        config,
        strategy_id="cb_batch_15",
        model_version="2026_04_monthly",
        signal_date="2026-04-15",
        trade_date="2026-04-16",
        previous_positions=None,
        event_exit_csv=None,
    )

    assert env["RUNTIME_PYTHON_BIN"] == str(tmp_path / "q_lab_python")
    assert env["RUNTIME_PYTHONPATH"] == f"{tmp_path / 'runtime_src'}:{tmp_path / 'extra_src'}"
    assert env["TRAIN_BIN"] == str(tmp_path / "custom_train")
    assert env["PREDICT_BIN"] == str(tmp_path / "custom_predict")


def test_partial_upstream_state_stops_downstream(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(config.upstream_state_file, {"exit_class": "success", "cb_status": "success", "qlib_status": "running", "target_trade_date": "2026-04-17"})

    summary = orchestrate_daily(config, dry_run=True, skip_upstream=True)

    assert summary["status"] == "skipped_upstream_not_ready"
    assert summary["strategies"] == []


def test_upstream_source_not_ready_exit_is_classified_from_state(tmp_path: Path):
    config = _base_config(tmp_path)
    _write_json(
        config.upstream_state_file,
        {
            "exit_class": "source_not_ready",
            "cb_status": "partial",
            "qlib_status": "success",
            "target_trade_date": "2026-04-20",
        },
    )

    def _runner(command, env=None, cwd=None):
        return CommandResult(tuple(command), str(cwd) if cwd else None, 75, "", "source not ready")

    summary = orchestrate_daily(config, skip_upstream=False, runner=_runner)

    assert summary["status"] == "skipped_upstream_not_ready"
    assert summary["upstream_command"]["returncode"] == 75
    assert summary["upstream_state"]["exit_class"] == "source_not_ready"
    assert summary["strategies"] == []


def test_github_release_mode_installs_and_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = OrchestratorConfig(
        **{
            **_base_config(tmp_path).__dict__,
            "upstream_mode": "github_release",
            "github_release_token": "token",
            "release_install_dir": _base_config(tmp_path).provider_uri,
            "release_work_dir": tmp_path / "release_tmp",
        }
    )
    calls, runner = _fake_runner_factory(config)
    saved_payloads: list[dict] = []

    release_payload = {
        "release_poll_status": "ready",
        "release_repo": "HiJean99/CB-Qlib-Infra",
        "release_tag": "cb-data-latest",
        "release_asset_name": "cb-qlib-data-latest.tar.zst",
        "release_latest_complete_trade_date": "2026-04-17",
        "release_target_trade_date": "2026-04-17",
        "release_content_fingerprint": "abc",
        "release_download_url": "https://example.com/file",
        "release_sha256_url": "https://example.com/file.sha256",
        "release_sha256": "deadbeef",
    }

    monkeypatch.setattr("cb_orchestrator.orchestrator.inspect_latest_release", lambda *args, **kwargs: dict(release_payload))
    monkeypatch.setattr("cb_orchestrator.orchestrator.load_release_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr("cb_orchestrator.orchestrator.release_is_new", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "cb_orchestrator.orchestrator.install_release",
        lambda **kwargs: {
            **release_payload,
            "release_install_status": "success",
            "release_installed_at": "2026-04-20T14:28:48+00:00",
            "release_install_dir": str(config.release_install_dir),
            "release_already_consumed": False,
        },
    )
    monkeypatch.setattr(
        "cb_orchestrator.orchestrator.save_release_state",
        lambda _state_root, payload: saved_payloads.append(payload) or (_state_root / "release" / "latest.json"),
    )

    summary = orchestrate_daily(config, skip_upstream=False, runner=runner)

    assert summary["status"] == "success"
    assert summary["release_install_status"] == "success"
    assert summary["signal_date"] == "2026-04-17"
    assert len(saved_payloads) == 1
    assert calls[:2] == [("train_monthly.sh", "cb_batch_15"), ("predict_daily.sh", "cb_batch_15")]


def test_github_release_mode_skips_when_latest_already_consumed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = OrchestratorConfig(
        **{
            **_base_config(tmp_path).__dict__,
            "upstream_mode": "github_release",
            "github_release_token": "token",
            "release_install_dir": _base_config(tmp_path).provider_uri,
            "release_work_dir": tmp_path / "release_tmp",
        }
    )

    monkeypatch.setattr(
        "cb_orchestrator.orchestrator.inspect_latest_release",
        lambda *args, **kwargs: {
            "release_poll_status": "ready",
            "release_latest_complete_trade_date": "2026-04-17",
            "release_target_trade_date": "2026-04-17",
            "release_content_fingerprint": "abc",
            "release_asset_name": "cb-qlib-data-latest.tar.zst",
        },
    )
    monkeypatch.setattr("cb_orchestrator.orchestrator.load_release_state", lambda *_args, **_kwargs: {"release_content_fingerprint": "abc"})
    monkeypatch.setattr("cb_orchestrator.orchestrator.release_is_new", lambda *args, **kwargs: False)

    summary = orchestrate_daily(config, skip_upstream=False)

    assert summary["status"] == "skipped_no_new_release"
    assert summary["strategies"] == []


def test_github_release_skip_upstream_uses_cached_release_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config = OrchestratorConfig(
        **{
            **_base_config(tmp_path).__dict__,
            "upstream_mode": "github_release",
            "github_release_token": "token",
        }
    )

    monkeypatch.setattr(
        "cb_orchestrator.orchestrator.load_release_state",
        lambda *_args, **_kwargs: {
            "release_latest_complete_trade_date": "2026-04-17",
            "release_target_trade_date": "2026-04-17",
            "release_content_fingerprint": "cached",
        },
    )

    summary = orchestrate_daily(config, dry_run=True, skip_upstream=True)

    assert summary["status"] == "dry_run"
    assert summary["release_install_status"] == "skipped_use_cached"
    assert summary["signal_date"] == "2026-04-17"


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
