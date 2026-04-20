from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.orchestrator import orchestrate_daily
from cb_orchestrator.release_consumer import inspect_latest_release, load_release_state, release_is_new


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def _now_shanghai() -> datetime:
    return datetime.now(SHANGHAI_TZ)


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _window_state_paths(state_root: Path, window_id: str) -> tuple[Path, Path]:
    latest_path = state_root / "release_window" / "latest.json"
    run_path = state_root / "release_window" / "runs" / f"{window_id}.json"
    return latest_path, run_path


def _resolve_window_date(now_local: datetime, end_hour: int) -> str:
    if now_local.hour < end_hour:
        return (now_local.date() - timedelta(days=1)).strftime("%Y%m%d")
    return now_local.date().strftime("%Y%m%d")


def _resolve_window_end(window_date: str, end_hour: int) -> datetime:
    start_date = datetime.strptime(window_date, "%Y%m%d").date()
    return datetime(
        year=(start_date + timedelta(days=1)).year,
        month=(start_date + timedelta(days=1)).month,
        day=(start_date + timedelta(days=1)).day,
        hour=end_hour,
        minute=0,
        second=0,
        tzinfo=SHANGHAI_TZ,
    )


def _planned_slots(started_at: datetime, window_end: datetime, interval_minutes: int) -> list[str]:
    slots: list[str] = []
    current = started_at
    while current <= window_end:
        slots.append(current.isoformat(timespec="seconds"))
        current += timedelta(minutes=interval_minutes)
    if not slots or slots[-1] != window_end.isoformat(timespec="seconds"):
        slots.append(window_end.isoformat(timespec="seconds"))
    return slots


def run_release_window(
    config: OrchestratorConfig,
    *,
    dry_run: bool = False,
    now_fn: Callable[[], datetime] = _now_shanghai,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    started_at = now_fn()
    window_date = _resolve_window_date(started_at, config.release_window_end_hour)
    window_id = started_at.strftime("%Y%m%d_%H%M%S")
    window_end = _resolve_window_end(window_date, config.release_window_end_hour)

    summary: dict[str, Any] = {
        "window_id": window_id,
        "window_date": window_date,
        "started_at": started_at.isoformat(timespec="seconds"),
        "dry_run": dry_run,
        "poll_interval_minutes": config.release_poll_interval_minutes,
        "window_end": window_end.isoformat(timespec="seconds"),
        "planned_slots": _planned_slots(started_at, window_end, config.release_poll_interval_minutes),
        "attempts": [],
    }
    latest_path, run_path = _window_state_paths(config.state_root, window_id)

    def finalize(status: str, message: str) -> dict[str, Any]:
        summary["status"] = status
        summary["message"] = message
        summary["finished_at"] = now_fn().isoformat(timespec="seconds")
        summary["attempt_count"] = len(summary["attempts"])
        _write_json(latest_path, summary)
        _write_json(run_path, summary)
        return summary

    if config.upstream_mode != "github_release":
        return finalize("failed_release_window_mode", "release nightly window requires UPSTREAM_MODE=github_release")
    if not config.github_release_token:
        return finalize("failed_missing_release_token", "missing GITHUB_RELEASE_TOKEN for github_release mode")

    while True:
        current = now_fn()
        if current > window_end:
            return finalize("skipped_release_not_ready", "window ended before a new release was available")

        probe = inspect_latest_release(
            config.github_release_repo,
            config.github_release_tag,
            config.github_release_asset_name,
            config.github_release_token,
        )
        installed_state = load_release_state(config.state_root)
        is_new_release = release_is_new(probe, installed_state, config.release_install_dir)
        attempt: dict[str, Any] = {
            "started_at": current.isoformat(timespec="seconds"),
            "slot_label": current.strftime("%H:%M"),
            **{key: value for key, value in probe.items() if key.startswith("release_")},
            "release_is_new": is_new_release,
        }

        if probe.get("release_poll_status") != "ready":
            attempt["status"] = "release_not_ready"
            attempt["finished_at"] = now_fn().isoformat(timespec="seconds")
            summary["attempts"].append(attempt)
        elif dry_run:
            attempt["status"] = "dry_run"
            attempt["finished_at"] = now_fn().isoformat(timespec="seconds")
            summary["attempts"].append(attempt)
            summary["release_poll_status"] = probe.get("release_poll_status")
            return finalize("dry_run", "release window dry run completed")
        elif not is_new_release:
            attempt["status"] = "skipped_no_new_release"
            attempt["finished_at"] = now_fn().isoformat(timespec="seconds")
            summary["attempts"].append(attempt)
        else:
            result = orchestrate_daily(config, dry_run=False, skip_upstream=False)
            attempt["status"] = result.get("status")
            attempt["finished_at"] = now_fn().isoformat(timespec="seconds")
            attempt["orchestrator_run_id"] = result.get("run_id")
            attempt["signal_date"] = result.get("signal_date")
            attempt["trade_date"] = result.get("trade_date")
            summary["attempts"].append(attempt)
            for key, value in result.items():
                if key.startswith("release_"):
                    summary[key] = value
            if result.get("status") == "success":
                summary["consumed_trade_date"] = result.get("signal_date")
                return finalize("success", "consumed new release and completed prediction orchestration")
            if result.get("status") != "skipped_no_new_release":
                summary["orchestrator_status"] = result.get("status")
                summary["orchestrator_message"] = result.get("message")
                return finalize("failed_orchestrator", f"orchestrator failed: {result.get('status')}")

        remaining = (window_end - now_fn()).total_seconds()
        if remaining <= 0:
            break
        sleep_fn(min(config.release_poll_interval_minutes * 60, remaining))

    return finalize("skipped_release_not_ready", "window ended before a new release was available")
