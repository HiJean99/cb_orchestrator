from __future__ import annotations

import json
import os
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from cb_orchestrator.calendar_utils import TradingCalendar
from cb_orchestrator.config import OrchestratorConfig

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


Runner = Callable[[Sequence[str], Optional[Mapping[str, str]], Optional[Path]], "CommandResult"]


@dataclass(frozen=True)
class CommandResult:
    command: tuple[str, ...]
    cwd: str | None
    returncode: int
    stdout: str
    stderr: str

    def to_summary(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "cwd": self.cwd,
            "returncode": self.returncode,
            "stdout_tail": _tail(self.stdout),
            "stderr_tail": _tail(self.stderr),
        }


class LockNotAcquiredError(RuntimeError):
    pass


def _tail(text: str, *, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_command(command: Sequence[str], env: Mapping[str, str] | None = None, cwd: Path | None = None) -> CommandResult:
    completed = subprocess.run(
        list(command),
        cwd=str(cwd) if cwd else None,
        env=dict(env) if env is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(
        command=tuple(command),
        cwd=str(cwd) if cwd else None,
        returncode=int(completed.returncode),
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


@contextmanager
def file_lock(lock_path: Path):
    if fcntl is None:  # pragma: no cover
        yield
        return
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise LockNotAcquiredError(str(lock_path)) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _state_classification(upstream_state: dict[str, Any]) -> tuple[bool, str, str]:
    exit_class = str(upstream_state.get("exit_class") or "")
    cb_status = str(upstream_state.get("cb_status") or "")
    qlib_status = str(upstream_state.get("qlib_status") or "")
    target_trade_date = upstream_state.get("target_trade_date")

    if exit_class == "non_trading_day":
        return False, "skipped_non_trading_day", "upstream marked non-trading day"
    if exit_class != "success":
        return False, "skipped_upstream_not_ready", f"exit_class={exit_class or 'missing'}"
    if cb_status != "success" or qlib_status != "success":
        return False, "skipped_upstream_not_ready", f"cb_status={cb_status}, qlib_status={qlib_status}"
    if not target_trade_date:
        return False, "skipped_upstream_not_ready", "missing target_trade_date"
    return True, "ready", "upstream success"


def _model_version_for_date(trade_date: str) -> str:
    return f"{trade_date[:7].replace('-', '_')}_monthly"


def _detect_previous_positions(prediction_root: Path, strategy_id: str, previous_trade_date: str | None) -> Path | None:
    if not previous_trade_date:
        return None
    candidate = prediction_root / strategy_id / previous_trade_date / "next_positions.csv"
    return candidate if candidate.exists() else None


def _event_exit_path(pack_dir: Path) -> Path | None:
    candidate = pack_dir / "event_exit_audit.csv"
    return candidate if candidate.exists() else None


def _prediction_summary_path(prediction_root: Path, strategy_id: str, trade_date: str) -> Path:
    return prediction_root / strategy_id / trade_date / "prediction_summary.json"


def _top_prediction_path(prediction_root: Path, strategy_id: str, trade_date: str, top_count: int) -> Path:
    return prediction_root / strategy_id / trade_date / f"top{int(top_count)}.csv"


def _base_env(config: OrchestratorConfig) -> dict[str, str]:
    env = dict(os.environ)
    if config.upstream_env_file and config.upstream_env_file.exists():
        env["UPSTREAM_ENV_FILE"] = str(config.upstream_env_file)
    return env


def _runtime_env(
    config: OrchestratorConfig,
    *,
    strategy_id: str,
    model_version: str,
    signal_date: str,
    trade_date: str,
    previous_positions: Path | None,
    event_exit_csv: Path | None,
) -> dict[str, str]:
    env = _base_env(config)
    env.update(
        {
            "PACK_NAME": strategy_id,
            "PACK_DIR": str((config.packs_root / strategy_id).resolve()),
            "PROVIDER_URI": str(config.provider_uri),
            "MODEL_VERSION": model_version,
            "SIGNAL_DATE": signal_date,
            "TRADE_DATE": trade_date,
            "TOP_COUNT": str(config.top_count),
            "MODEL_ROOT": str(config.model_root),
            "BUILD_ROOT": str(config.build_root),
            "OUTPUT_ROOT": str(config.prediction_root),
            "LOG_ROOT": str(config.log_root),
            "TRAIN_START": config.train_start,
            "ROLLING_VALID_MONTHS": str(config.rolling_valid_months),
            "MODEL_NUM_THREADS": str(config.model_num_threads),
        }
    )
    if previous_positions is not None:
        env["PREVIOUS_POSITIONS"] = str(previous_positions)
    if event_exit_csv is not None:
        env["EVENT_EXIT_CSV"] = str(event_exit_csv)
    env.update(config.email_env)
    return env


def _upstream_env(config: OrchestratorConfig) -> dict[str, str]:
    env = _base_env(config)
    if config.upstream_allow_missing_symbols:
        env["ALLOW_MISSING_SYMBOLS"] = config.upstream_allow_missing_symbols
    return env


def _run_upstream(config: OrchestratorConfig, runner: Runner) -> CommandResult:
    command = [
        str(config.upstream_python_bin),
        str((config.upstream_repo_root / "scripts" / "orchestrate_daily_update.py").resolve()),
        "--mode",
        "daily",
        "--lookback-trade-days",
        str(config.upstream_lookback_trade_days),
        "--repair-trade-days",
        str(config.upstream_repair_trade_days),
    ]
    if config.upstream_allow_missing_symbols:
        command.extend(["--allow-missing-symbols", config.upstream_allow_missing_symbols])
    return runner(tuple(command), _upstream_env(config), config.upstream_repo_root)


def _run_strategy(
    *,
    config: OrchestratorConfig,
    strategy_id: str,
    signal_date: str,
    trade_date: str,
    previous_trade_date: str | None,
    first_trade_date_of_month: str,
    dry_run: bool,
    runner: Runner,
) -> dict[str, Any]:
    pack_dir = (config.packs_root / strategy_id).resolve()
    if not pack_dir.exists():
        raise FileNotFoundError(f"pack dir not found: {pack_dir}")

    model_version = _model_version_for_date(trade_date)
    model_dir = (config.model_root / strategy_id / model_version).resolve()
    previous_positions = _detect_previous_positions(config.prediction_root, strategy_id, previous_trade_date)
    event_exit_csv = _event_exit_path(pack_dir)

    strategy_summary: dict[str, Any] = {
        "strategy_id": strategy_id,
        "pack_dir": str(pack_dir),
        "model_version": model_version,
        "model_dir": str(model_dir),
        "first_trade_date_of_month": first_trade_date_of_month,
        "previous_positions_path": str(previous_positions) if previous_positions else None,
        "event_exit_csv": str(event_exit_csv) if event_exit_csv else None,
    }

    train_manifest = model_dir / "train_manifest.json"
    train_needed = not train_manifest.exists()
    strategy_summary["train_needed"] = train_needed

    if train_needed:
        train_command = ("bash", str((config.runtime_repo_root / "scripts" / "train_monthly.sh").resolve()))
        strategy_summary["train_command"] = list(train_command)
        if not dry_run:
            train_env = _runtime_env(
                config,
                strategy_id=strategy_id,
                model_version=model_version,
                signal_date=signal_date,
                trade_date=trade_date,
                previous_positions=previous_positions,
                event_exit_csv=event_exit_csv,
            )
            train_env["TEST_START"] = first_trade_date_of_month
            train_result = runner(train_command, train_env, config.runtime_repo_root)
            strategy_summary["train_result"] = train_result.to_summary()
            if train_result.returncode != 0:
                strategy_summary["status"] = "failed_train"
                return strategy_summary
        strategy_summary["train_performed"] = True
    else:
        strategy_summary["train_performed"] = False

    predict_command = ("bash", str((config.runtime_repo_root / "scripts" / "predict_daily.sh").resolve()))
    strategy_summary["predict_command"] = list(predict_command)

    if dry_run:
        strategy_summary["status"] = "dry_run"
        strategy_summary["top_prediction_path"] = str(_top_prediction_path(config.prediction_root, strategy_id, trade_date, config.top_count))
        strategy_summary["prediction_summary_path"] = str(_prediction_summary_path(config.prediction_root, strategy_id, trade_date))
        return strategy_summary

    predict_result = runner(
        predict_command,
        _runtime_env(
            config,
            strategy_id=strategy_id,
            model_version=model_version,
            signal_date=signal_date,
            trade_date=trade_date,
            previous_positions=previous_positions,
            event_exit_csv=event_exit_csv,
        ),
        config.runtime_repo_root,
    )
    strategy_summary["predict_result"] = predict_result.to_summary()
    if predict_result.returncode != 0:
        strategy_summary["status"] = "failed_predict"
        return strategy_summary

    prediction_summary_path = _prediction_summary_path(config.prediction_root, strategy_id, trade_date)
    strategy_summary["prediction_summary_path"] = str(prediction_summary_path)
    if prediction_summary_path.exists():
        prediction_summary = _load_json(prediction_summary_path)
        strategy_summary["prediction_summary"] = prediction_summary
        strategy_summary["top_prediction_path"] = prediction_summary.get("top_prediction_path")
        strategy_summary["email_sent"] = bool(prediction_summary.get("email_sent"))
    else:
        strategy_summary["top_prediction_path"] = str(_top_prediction_path(config.prediction_root, strategy_id, trade_date, config.top_count))

    top_prediction_path = Path(strategy_summary["top_prediction_path"])
    if not top_prediction_path.exists():
        strategy_summary["status"] = "failed_missing_prediction_artifact"
        return strategy_summary

    strategy_summary["status"] = "success"
    return strategy_summary


def orchestrate_daily(
    config: OrchestratorConfig,
    *,
    dry_run: bool = False,
    skip_upstream: bool = False,
    runner: Runner = run_command,
) -> dict[str, Any]:
    started_at = _utcnow()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "dry_run": dry_run,
        "skip_upstream": skip_upstream,
        "config": config.redacted_dict(),
        "strategies": [],
    }

    latest_path = config.state_root / "latest.json"
    run_path = config.state_root / "runs" / f"{run_id}.json"

    def finalize(status: str, message: str) -> dict[str, Any]:
        summary["status"] = status
        summary["message"] = message
        summary["finished_at"] = _utcnow()
        _write_json(latest_path, summary)
        _write_json(run_path, summary)
        return summary

    try:
        with file_lock(config.lock_path):
            if not skip_upstream and not dry_run:
                upstream_result = _run_upstream(config, runner)
                summary["upstream_command"] = upstream_result.to_summary()
                if upstream_result.returncode != 0:
                    return finalize("failed_upstream_command", "upstream daily update command failed")

            if not config.upstream_state_file.exists():
                return finalize("failed_missing_upstream_state", f"missing upstream state file: {config.upstream_state_file}")

            upstream_state = _load_json(config.upstream_state_file)
            summary["upstream_state"] = upstream_state

            ready, status, message = _state_classification(upstream_state)
            if not ready:
                return finalize(status, message)

            signal_date = str(upstream_state["target_trade_date"])
            calendar = TradingCalendar.from_path(config.trade_calendar_path or (config.provider_uri / "calendars" / "day.txt"))
            trade_date = calendar.next_after(signal_date)
            try:
                previous_trade_date = calendar.previous_before(trade_date)
            except ValueError:
                previous_trade_date = None
            first_trade_date_of_month = calendar.first_of_month(trade_date)

            summary["signal_date"] = signal_date
            summary["trade_date"] = trade_date
            summary["previous_trade_date"] = previous_trade_date
            summary["first_trade_date_of_month"] = first_trade_date_of_month

            for strategy_id in config.strategy_ids:
                strategy_summary = _run_strategy(
                    config=config,
                    strategy_id=strategy_id,
                    signal_date=signal_date,
                    trade_date=trade_date,
                    previous_trade_date=previous_trade_date,
                    first_trade_date_of_month=first_trade_date_of_month,
                    dry_run=dry_run,
                    runner=runner,
                )
                summary["strategies"].append(strategy_summary)
                if strategy_summary.get("status") not in {"success", "dry_run"}:
                    return finalize(strategy_summary["status"], f"strategy failed: {strategy_id}")

            return finalize("success" if not dry_run else "dry_run", "orchestration completed")
    except LockNotAcquiredError:
        return finalize("skipped_locked", f"lock already held: {config.lock_path}")
    except Exception as exc:
        summary["exception"] = repr(exc)
        return finalize("failed_exception", str(exc))
