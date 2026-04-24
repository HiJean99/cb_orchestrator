from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from cb_orchestrator.config import OrchestratorConfig

FIXTURE_ROOT = Path(__file__).parent / "fixtures"
FIXTURE_ONE_DAY_ROOT = FIXTURE_ROOT / "planner_notion_one_day"
FIXTURE_THREE_DAY_ROOT = FIXTURE_ROOT / "planner_notion_three_day"
NOTION_ENV_KEYS = (
    "NOTION_TOKEN",
    "NOTION_VERSION",
    "NOTION_DAILY_HOLDINGS_DB_ID",
    "NOTION_HOLDING_POSITIONS_DB_ID",
    "NOTION_DAILY_RANKINGS_DB_ID",
    "NOTION_DECISION_DAYS_DB_ID",
    "NOTION_PLAN_ORDERS_DB_ID",
    "NOTION_BOND_NAME_MAP_PATH",
)


class FakeNotionClient:
    def __init__(self) -> None:
        self._counter = 0
        self._block_counter = 0
        self._pages_by_id: dict[str, dict[str, Any]] = {}
        self._pages_by_database: dict[str, list[dict[str, Any]]] = {}
        self._blocks_by_id: dict[str, dict[str, Any]] = {}

    def query_database(
        self,
        database_id: str,
        *,
        filter_: dict[str, Any] | None = None,
        sorts: list[dict[str, Any]] | None = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        del sorts
        pages = [page for page in self._pages_by_database.get(database_id, []) if not page.get("archived")]
        if filter_:
            pages = [page for page in pages if self._match_filter(page, filter_)]
        return pages[:page_size]

    def create_page(
        self,
        *,
        database_id: str,
        properties: dict[str, Any],
        children: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self._counter += 1
        page_id = f"page-{self._counter}"
        page = {
            "id": page_id,
            "database_id": database_id,
            "archived": False,
            "properties": self._normalize_properties(properties),
            "children": self._normalize_children(children or []),
        }
        self._pages_by_id[page_id] = page
        self._pages_by_database.setdefault(database_id, []).append(page)
        return {"id": page_id}

    def update_page(
        self,
        page_id: str,
        *,
        properties: dict[str, Any] | None = None,
        archived: bool | None = None,
    ) -> dict[str, Any]:
        page = self._pages_by_id[page_id]
        if properties:
            page["properties"].update(self._normalize_properties(properties))
        if archived is not None:
            page["archived"] = bool(archived)
        return {"id": page_id}

    def list_block_children(self, block_id: str) -> list[dict[str, Any]]:
        return [child for child in self._pages_by_id[block_id]["children"] if not child.get("archived")]

    def append_block_children(self, block_id: str, children: list[dict[str, Any]]) -> dict[str, Any]:
        normalized = self._normalize_children(children)
        self._pages_by_id[block_id]["children"].extend(normalized)
        return {"results": list(normalized)}

    def update_block(self, block_id: str, *, archived: bool | None = None) -> dict[str, Any]:
        block = self._blocks_by_id[block_id]
        if archived is not None:
            block["archived"] = bool(archived)
        return {"id": block_id}

    def pages_for_database(self, database_id: str) -> list[dict[str, Any]]:
        return [page for page in self._pages_by_database.get(database_id, []) if not page.get("archived")]

    @staticmethod
    def _normalize_properties(properties: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in properties.items():
            normalized[key] = FakeNotionClient._normalize_property(value)
        return normalized

    @staticmethod
    def _normalize_property(value: dict[str, Any]) -> dict[str, Any]:
        for prop_type in ("title", "rich_text", "date", "select", "multi_select", "relation", "checkbox", "number"):
            if prop_type in value:
                payload = value[prop_type]
                if prop_type in {"title", "rich_text"}:
                    payload = [FakeNotionClient._normalize_rich_text_item(item) for item in payload]
                return {"type": prop_type, prop_type: payload}
        raise AssertionError(f"unsupported fake notion property: {value}")

    @staticmethod
    def _normalize_rich_text_item(item: dict[str, Any]) -> dict[str, Any]:
        content = str(item.get("text", {}).get("content") or item.get("plain_text") or "")
        return {
            **item,
            "plain_text": content,
        }

    def _normalize_children(self, children: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for child in children:
            self._block_counter += 1
            block_id = f"block-{self._block_counter}"
            normalized_block = {
                **child,
                "id": block_id,
                "archived": False,
            }
            block_type = str(child.get("type") or "")
            payload = child.get(block_type)
            if isinstance(payload, dict) and isinstance(payload.get("rich_text"), list):
                normalized_block[block_type] = {
                    **payload,
                    "rich_text": [self._normalize_rich_text_item(item) for item in payload["rich_text"]],
                }
            self._blocks_by_id[block_id] = normalized_block
            normalized.append(normalized_block)
        return normalized

    def _match_filter(self, page: dict[str, Any], filter_: dict[str, Any]) -> bool:
        if "and" in filter_:
            return all(self._match_filter(page, item) for item in filter_["and"])
        if "or" in filter_:
            return any(self._match_filter(page, item) for item in filter_["or"])
        property_name = filter_["property"]
        prop = page["properties"].get(property_name)
        if prop is None:
            return False
        if "rich_text" in filter_:
            expected = str(filter_["rich_text"].get("equals") or "")
            return self._plain_text(prop) == expected
        if "date" in filter_:
            expected = str(filter_["date"].get("equals") or "")
            return self._date_start(prop) == expected
        if "relation" in filter_:
            expected = str(filter_["relation"].get("contains") or "")
            return expected in self._relation_ids(prop)
        raise AssertionError(f"unsupported fake notion filter: {filter_}")

    @staticmethod
    def _plain_text(prop: dict[str, Any]) -> str:
        prop_type = prop.get("type")
        if prop_type == "title":
            return "".join(item.get("plain_text", "") for item in prop.get("title", []))
        if prop_type == "rich_text":
            return "".join(item.get("plain_text", "") for item in prop.get("rich_text", []))
        return ""

    @staticmethod
    def _date_start(prop: dict[str, Any]) -> str:
        if prop.get("type") != "date":
            return ""
        value = prop.get("date")
        if not isinstance(value, dict):
            return ""
        return str(value.get("start") or "")

    @staticmethod
    def _relation_ids(prop: dict[str, Any]) -> list[str]:
        if prop.get("type") != "relation":
            return []
        return [str(item.get("id") or "") for item in prop.get("relation", [])]


def _copy_fixture(work_root: Path, *, fixture_root: Path) -> Path:
    work_root.mkdir(parents=True, exist_ok=True)
    case_root = work_root / fixture_root.name
    shutil.copytree(fixture_root, case_root)
    return case_root


def copy_one_day_fixture(work_root: Path) -> Path:
    return _copy_fixture(work_root, fixture_root=FIXTURE_ONE_DAY_ROOT)


def copy_three_day_fixture(work_root: Path) -> Path:
    return _copy_fixture(work_root, fixture_root=FIXTURE_THREE_DAY_ROOT)


def _build_fixture_config(case_root: Path) -> OrchestratorConfig:
    runtime_root = case_root / "runtime"
    state_root = case_root / "state"
    provider_uri = case_root / "provider"
    return OrchestratorConfig(
        upstream_python_bin=Path("/usr/bin/python3"),
        upstream_repo_root=case_root,
        upstream_state_file=state_root / "latest.json",
        upstream_env_file=None,
        provider_uri=provider_uri,
        trade_calendar_path=provider_uri / "calendars" / "day.txt",
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
        next_trade_top_n=6,
        next_trade_max_drop=3,
        notion_token="test-token",
        notion_daily_holdings_db_id="holdings-db",
        notion_holding_positions_db_id="positions-db",
        notion_daily_rankings_db_id="rankings-db",
        notion_decision_days_db_id="decision-days-db",
        notion_plan_orders_db_id="plan-orders-db",
        notion_bond_name_map_path=runtime_root / "local_assets" / "cb_basic.csv",
    )


def build_one_day_config(case_root: Path) -> OrchestratorConfig:
    return _build_fixture_config(case_root)


def build_three_day_config(case_root: Path) -> OrchestratorConfig:
    return _build_fixture_config(case_root)


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


def _write_fixture_env_file(
    case_root: Path,
    env_file: Path | None = None,
    *,
    base_env_file: Path | None = None,
) -> Path:
    runtime_root = case_root / "runtime"
    state_root = case_root / "state"
    provider_uri = case_root / "provider"
    resolved_env_file = env_file or (case_root / "cb-orchestrator.env")
    base_payload = _read_env_file(base_env_file)
    payload = {
        "UPSTREAM_PYTHON_BIN": "/usr/bin/python3",
        "UPSTREAM_REPO_ROOT": str(case_root),
        "UPSTREAM_STATE_FILE": str(state_root / "latest.json"),
        "RUNTIME_REPO_ROOT": str(runtime_root),
        "PROVIDER_URI": str(provider_uri),
        "TRADE_CALENDAR_PATH": str(provider_uri / "calendars" / "day.txt"),
        "ORCH_STATE_ROOT": str(state_root),
        "PLAN_INPUT_ROOT": str(state_root / "plan_inputs"),
        "PLAN_OUTPUT_ROOT": str(state_root / "next_trade_plans"),
        "NEXT_TRADE_TOP_N": "6",
        "NEXT_TRADE_MAX_DROP": "3",
        "NOTION_TOKEN": "test-token",
        "NOTION_DAILY_HOLDINGS_DB_ID": "holdings-db",
        "NOTION_HOLDING_POSITIONS_DB_ID": "positions-db",
        "NOTION_DAILY_RANKINGS_DB_ID": "rankings-db",
        "NOTION_DECISION_DAYS_DB_ID": "decision-days-db",
        "NOTION_PLAN_ORDERS_DB_ID": "plan-orders-db",
        "NOTION_BOND_NAME_MAP_PATH": str(runtime_root / "local_assets" / "cb_basic.csv"),
    }
    for key in NOTION_ENV_KEYS:
        if base_payload.get(key):
            payload[key] = base_payload[key]
    resolved_env_file.write_text(
        "\n".join([f"{key}={value}" for key, value in payload.items()] + [""]),
        encoding="utf-8",
    )
    return resolved_env_file


def write_one_day_env_file(
    case_root: Path,
    env_file: Path | None = None,
    *,
    base_env_file: Path | None = None,
) -> Path:
    return _write_fixture_env_file(case_root, env_file, base_env_file=base_env_file)


def write_three_day_env_file(
    case_root: Path,
    env_file: Path | None = None,
    *,
    base_env_file: Path | None = None,
) -> Path:
    return _write_fixture_env_file(case_root, env_file, base_env_file=base_env_file)
