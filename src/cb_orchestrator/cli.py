from __future__ import annotations

import argparse
import json
from pathlib import Path

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.orchestrator import orchestrate_daily


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one thin daily orchestrator for CB pipeline + runtime.")
    parser.add_argument("--env-file", default=None, help="orchestrator env file path")
    parser.add_argument("--dry-run", action="store_true", help="do not launch subprocesses; only evaluate current state")
    parser.add_argument("--skip-upstream", action="store_true", help="skip upstream daily update command and only read latest.json")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = OrchestratorConfig.from_sources(env_file=Path(args.env_file).expanduser() if args.env_file else None)
    summary = orchestrate_daily(config, dry_run=bool(args.dry_run), skip_upstream=bool(args.skip_upstream))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
