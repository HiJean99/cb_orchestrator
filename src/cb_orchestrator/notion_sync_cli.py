from __future__ import annotations

import argparse
import json
from pathlib import Path

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.notion_sync import sync_run_to_notion


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync holdings, rankings, and next-trade plan artifacts to the configured Notion workspace."
    )
    parser.add_argument("--env-file", default=None, help="orchestrator env file path")
    parser.add_argument("--run-id", default=None, help="specific orchestrator run_id to sync")
    parser.add_argument("--trade-date", default=None, help="resolve the latest successful run by trade_date")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = OrchestratorConfig.from_sources(env_file=Path(args.env_file).expanduser() if args.env_file else None)
    summary = sync_run_to_notion(config, run_id=args.run_id, trade_date=args.trade_date)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
