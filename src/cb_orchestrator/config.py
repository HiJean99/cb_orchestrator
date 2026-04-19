from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

DEFAULT_STRATEGIES = ("cb_batch_15", "cb_batch_27")
EMAIL_ENV_KEYS = (
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USER",
    "SMTP_PASSWORD",
    "EMAIL_TO",
    "EMAIL_FROM",
    "EMAIL_SUBJECT_PREFIX",
)
SECRET_ENV_KEYS = {"SMTP_PASSWORD", "TUSHARE_TOKEN"}


def _split_csv(raw_value: str | None, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if raw_value is None:
        return default
    parts = tuple(item.strip() for item in raw_value.split(",") if item.strip())
    return parts or default


def _read_env_file(env_file: Path | None) -> dict[str, str]:
    if env_file is None or not env_file.exists():
        return {}
    payload: dict[str, str] = {}
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        payload[key.strip()] = value.strip().strip('"').strip("'")
    return payload


def _coalesce_path(raw_value: str | None, *, default: Path) -> Path:
    return Path(raw_value).expanduser() if raw_value else default


@dataclass(frozen=True)
class OrchestratorConfig:
    upstream_python_bin: Path
    upstream_repo_root: Path
    upstream_state_file: Path
    upstream_env_file: Path | None
    provider_uri: Path
    trade_calendar_path: Path | None
    runtime_repo_root: Path
    packs_root: Path
    model_root: Path
    build_root: Path
    prediction_root: Path
    log_root: Path
    state_root: Path
    lock_path: Path
    strategy_ids: tuple[str, ...]
    train_start: str
    rolling_valid_months: int
    model_num_threads: int
    top_count: int
    upstream_lookback_trade_days: int
    upstream_repair_trade_days: int
    upstream_allow_missing_symbols: str | None
    email_env: dict[str, str]

    @classmethod
    def from_sources(
        cls,
        *,
        env_file: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "OrchestratorConfig":
        resolved_environ = dict(environ or os.environ)
        file_payload = _read_env_file(env_file)

        upstream_env_file_raw = resolved_environ.get("UPSTREAM_ENV_FILE") or file_payload.get("UPSTREAM_ENV_FILE")
        upstream_env_file = Path(upstream_env_file_raw).expanduser() if upstream_env_file_raw else None

        merged: dict[str, str] = {}
        merged.update(file_payload)
        merged.update(_read_env_file(upstream_env_file))
        merged.update(resolved_environ)

        runtime_repo_root = _coalesce_path(
            merged.get("RUNTIME_REPO_ROOT"),
            default=Path("/home/hermes/cb_online_runtime"),
        ).resolve()
        provider_uri = _coalesce_path(
            merged.get("PROVIDER_URI") or merged.get("QLIB_DIR"),
            default=Path("/home/hermes/workspace/cb-qlib-data/qlib_data"),
        ).resolve()
        state_root = _coalesce_path(
            merged.get("ORCH_STATE_ROOT"),
            default=runtime_repo_root / "local_state" / "orchestrator",
        ).resolve()
        default_calendar = provider_uri / "calendars" / "day.txt"
        trade_calendar_path_raw = merged.get("TRADE_CALENDAR_PATH")
        upstream_state_default = Path("/home/hermes/workspace/cb-qlib-data/state/latest.json")
        state_dir = merged.get("STATE_DIR")

        email_env = {key: merged[key] for key in EMAIL_ENV_KEYS if merged.get(key)}

        return cls(
            upstream_python_bin=_coalesce_path(
                merged.get("UPSTREAM_PYTHON_BIN") or merged.get("PYTHON_BIN"),
                default=Path("/home/hermes/workspace/CB-Qlib-Infra/.venv/bin/python"),
            ),
            upstream_repo_root=_coalesce_path(
                merged.get("UPSTREAM_REPO_ROOT"),
                default=Path("/home/hermes/workspace/CB-Qlib-Infra"),
            ).resolve(),
            upstream_state_file=_coalesce_path(
                merged.get("UPSTREAM_STATE_FILE") or (str(Path(state_dir) / "latest.json") if state_dir else None),
                default=upstream_state_default,
            ).resolve(),
            upstream_env_file=upstream_env_file.resolve() if upstream_env_file else None,
            provider_uri=provider_uri,
            trade_calendar_path=Path(trade_calendar_path_raw).expanduser().resolve() if trade_calendar_path_raw else default_calendar.resolve(),
            runtime_repo_root=runtime_repo_root,
            packs_root=_coalesce_path(
                merged.get("PACKS_ROOT"),
                default=runtime_repo_root / "local_assets" / "packs",
            ).resolve(),
            model_root=_coalesce_path(
                merged.get("MODEL_ROOT"),
                default=runtime_repo_root / "local_state" / "models",
            ).resolve(),
            build_root=_coalesce_path(
                merged.get("BUILD_ROOT"),
                default=runtime_repo_root / "local_state" / "builds",
            ).resolve(),
            prediction_root=_coalesce_path(
                merged.get("PREDICTION_ROOT"),
                default=runtime_repo_root / "local_state" / "predictions",
            ).resolve(),
            log_root=_coalesce_path(
                merged.get("LOG_ROOT"),
                default=runtime_repo_root / "local_state" / "logs",
            ).resolve(),
            state_root=state_root,
            lock_path=_coalesce_path(
                merged.get("ORCH_LOCK_PATH"),
                default=state_root / "orchestrator.lock",
            ).resolve(),
            strategy_ids=_split_csv(merged.get("ORCH_STRATEGIES"), default=DEFAULT_STRATEGIES),
            train_start=merged.get("TRAIN_START", "2018-01-01"),
            rolling_valid_months=int(merged.get("ROLLING_VALID_MONTHS", "12")),
            model_num_threads=int(merged.get("MODEL_NUM_THREADS", "2")),
            top_count=int(merged.get("TOP_COUNT", "10")),
            upstream_lookback_trade_days=int(merged.get("UPSTREAM_LOOKBACK_TRADE_DAYS", "7")),
            upstream_repair_trade_days=int(merged.get("UPSTREAM_REPAIR_TRADE_DAYS", "20")),
            upstream_allow_missing_symbols=merged.get("ALLOW_MISSING_SYMBOLS") or merged.get("UPSTREAM_ALLOW_MISSING_SYMBOLS"),
            email_env=email_env,
        )

    def redacted_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["strategy_ids"] = list(self.strategy_ids)
        payload["email_env"] = {
            key: ("***" if key in SECRET_ENV_KEYS else value)
            for key, value in self.email_env.items()
        }
        for key, value in list(payload.items()):
            if isinstance(value, Path):
                payload[key] = str(value)
        return payload
