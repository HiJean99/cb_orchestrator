from __future__ import annotations

import argparse
import json
from pathlib import Path

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.nightly import run_release_window


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll GitHub Release and trigger CB runtime after a fresh install.")
    parser.add_argument("--env-file", default=None, help="orchestrator env file path")
    parser.add_argument("--dry-run", action="store_true", help="inspect release state without installing or predicting")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = OrchestratorConfig.from_sources(env_file=Path(args.env_file).expanduser() if args.env_file else None)
    summary = run_release_window(config, dry_run=bool(args.dry_run))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
