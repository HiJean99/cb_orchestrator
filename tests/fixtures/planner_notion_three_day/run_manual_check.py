#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from cb_orchestrator.config import OrchestratorConfig
from cb_orchestrator.notion_sync import sync_run_to_notion
from tests.fixture_support import (
    FakeNotionClient,
    copy_three_day_fixture,
    write_three_day_env_file,
)

RUN_IDS = (
    "20260421_150716",
    "20260422_150716",
    "20260423_150716",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy the three-day fixture to a temp dir and replay planner / notion checks.")
    parser.add_argument("--python", default=sys.executable, help="python binary used to invoke the planner and notion CLIs")
    parser.add_argument("--work-root", default=None, help="existing directory to copy the fixture into")
    parser.add_argument("--base-env-file", default=None, help="overlay real Notion settings from an existing env file")
    parser.add_argument("--cleanup", action="store_true", help="delete the copied fixture directory after the check")
    parser.add_argument("--real-notion", action="store_true", help="run the real notion sync CLI instead of the fake client")
    return parser


def _build_env() -> dict[str, str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(SRC_ROOT) if not existing_pythonpath else f"{SRC_ROOT}{os.pathsep}{existing_pythonpath}"
    return env


def _run_planner_cli(*, python_bin: str, env_file: Path, run_id: str) -> dict:
    command = [
        python_bin,
        "-m",
        "cb_orchestrator.planner_cli",
        "--env-file",
        str(env_file),
        "--run-id",
        run_id,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False, env=_build_env(), cwd=str(REPO_ROOT))
    if completed.returncode != 0:
        raise RuntimeError(
            "planner CLI failed\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return json.loads(completed.stdout)


def _run_fake_notion_sync(*, env_file: Path, run_id: str, client: FakeNotionClient) -> dict:
    config = OrchestratorConfig.from_sources(env_file=env_file, environ={})
    return sync_run_to_notion(config, run_id=run_id, client=client)


def _run_real_notion_sync(*, python_bin: str, env_file: Path, run_id: str) -> dict:
    command = [
        python_bin,
        "-m",
        "cb_orchestrator.notion_sync_cli",
        "--env-file",
        str(env_file),
        "--run-id",
        run_id,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False, env=_build_env(), cwd=str(REPO_ROOT))
    if completed.returncode != 0:
        raise RuntimeError(
            "notion sync CLI failed\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return json.loads(completed.stdout)


def main() -> None:
    args = build_parser().parse_args()
    temp_root = Path(args.work_root).expanduser().resolve() if args.work_root else Path(
        tempfile.mkdtemp(prefix="cb_orchestrator_fixture_")
    )
    created_temp_root = args.work_root is None
    case_root = copy_three_day_fixture(temp_root)
    env_file = write_three_day_env_file(
        case_root,
        base_env_file=Path(args.base_env_file).expanduser().resolve() if args.base_env_file else None,
    )

    fake_client = FakeNotionClient() if not args.real_notion else None
    results: list[dict[str, object]] = []
    for run_id in RUN_IDS:
        planner_summary = _run_planner_cli(python_bin=args.python, env_file=env_file, run_id=run_id)
        notion_summary = (
            _run_real_notion_sync(python_bin=args.python, env_file=env_file, run_id=run_id)
            if args.real_notion
            else _run_fake_notion_sync(env_file=env_file, run_id=run_id, client=fake_client)
        )
        results.append(
            {
                "run_id": run_id,
                "planner": planner_summary,
                "notion": notion_summary,
            }
        )

    final_summary: dict[str, object] = {}
    if fake_client is not None:
        final_summary = {
            "holding_pages": len(fake_client.pages_for_database("holdings-db")),
            "position_pages": len(fake_client.pages_for_database("positions-db")),
            "ranking_pages": len(fake_client.pages_for_database("rankings-db")),
            "decision_day_pages": len(fake_client.pages_for_database("decision-days-db")),
            "plan_order_pages": len(fake_client.pages_for_database("plan-orders-db")),
        }

    result = {
        "repo_root": str(REPO_ROOT),
        "case_root": str(case_root),
        "env_file": str(env_file),
        "runs": results,
        "final": final_summary,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if args.cleanup:
        shutil.rmtree(temp_root if created_temp_root else case_root)


if __name__ == "__main__":
    main()
