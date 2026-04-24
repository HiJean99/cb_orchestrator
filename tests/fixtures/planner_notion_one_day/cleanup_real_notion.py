#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.notion_sync import NotionClient, NotionResources


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive existing pages in the configured V2 Notion databases.")
    parser.add_argument("--env-file", required=True, help="env file containing the real NOTION_* settings")
    parser.add_argument(
        "--keep-latest",
        action="store_true",
        help="keep pages that already have Is Latest checked instead of archiving everything",
    )
    return parser


def _should_archive(page: dict, *, keep_latest: bool) -> bool:
    if not keep_latest:
        return True
    properties = page.get("properties", {})
    latest = properties.get("Is Latest", {})
    if latest.get("type") != "checkbox":
        return True
    return not bool(latest.get("checkbox"))


def main() -> None:
    args = build_parser().parse_args()
    env_file = Path(args.env_file).expanduser().resolve()
    config = OrchestratorConfig.from_sources(env_file=env_file, environ={})
    resources = NotionResources.from_config(config)
    client = NotionClient(token=str(config.notion_token), notion_version=config.notion_version)

    databases = {
        "holdings": resources.holdings_snapshots_db_id,
        "positions": resources.holding_positions_db_id,
        "rankings": resources.daily_rankings_db_id,
        "decision_days": resources.decision_days_db_id,
        "plan_orders": resources.plan_orders_db_id,
    }

    summary: dict[str, dict[str, int]] = {}
    for name, db_id in databases.items():
        pages = client.query_database(db_id)
        archived = 0
        kept = 0
        for page in pages:
            if _should_archive(page, keep_latest=args.keep_latest):
                client.update_page(page["id"], archived=True)
                archived += 1
            else:
                kept += 1
        summary[name] = {
            "before": len(pages),
            "archived": archived,
            "kept": kept,
        }

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
