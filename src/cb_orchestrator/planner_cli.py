from __future__ import annotations

import argparse
import json
from pathlib import Path

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.planner import plan_next_trade


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the next-trade plan from the latest successful run and signal_date snapshot bundle."
    )
    parser.add_argument("--env-file", default=None, help="orchestrator env file path")
    parser.add_argument("--run-id", default=None, help="specific orchestrator run_id to plan from")
    parser.add_argument("--trade-date", default=None, help="resolve the latest successful run by trade_date")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = OrchestratorConfig.from_sources(env_file=Path(args.env_file).expanduser() if args.env_file else None)
    summary = plan_next_trade(config, run_id=args.run_id, trade_date=args.trade_date)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
