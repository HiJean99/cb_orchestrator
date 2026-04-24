# 仓库指南

## 项目结构与模块组织
核心代码位于 [`src/cb_orchestrator`](./src/cb_orchestrator)。编排主逻辑按职责拆分在 `orchestrator.py`、`planner.py`、`nightly.py`、`notion_sync.py` 等模块；CLI 入口保持轻量，分别放在 `cli.py`、`planner_cli.py`、`nightly_cli.py`、`notion_sync_cli.py`。测试放在 [`tests`](./tests) 下，文件名与模块对应，采用 `test_*.py` 命名。运行说明放在 [`docs`](./docs)，环境变量示例放在 [`deploy`](./deploy)，重点参考 `deploy/cb-orchestrator.env.example`。

## 构建、测试与开发命令
默认开发环境为 Conda 环境 `rdagent_cb`，路径是 `/home/qjh/miniconda3/envs/rdagent_cb`。先激活该环境，再以可编辑模式安装：

```bash
conda activate rdagent_cb
python -m pip install -U pip setuptools wheel
python -m pip install -e .[dev]
```

如果需要显式指定解释器，可使用 `/home/qjh/miniconda3/envs/rdagent_cb/bin/python`。运行测试使用 `pytest` 或 `pytest -q`。常用本地检查命令如下：

```bash
cb-orchestrator-daily --env-file ~/.config/cb-orchestrator/cb-orchestrator.env --dry-run --skip-upstream
cb-orchestrator-plan-next-trade --env-file ~/.config/cb-orchestrator/cb-orchestrator.env
cb-orchestrator-sync-notion --env-file ~/.config/cb-orchestrator/cb-orchestrator.env --run-id 20260420_083000
```

## 编码风格与命名约定
项目目标 Python 3.9+，延续现有代码中的类型标注、`pathlib.Path` 和显式 JSON 处理方式。统一使用 4 空格缩进；函数、模块使用 `snake_case`，数据类使用 `PascalCase`，常量和环境变量键名使用全大写。优先写职责单一的小函数，把子进程调用和文件系统读写封装在薄层辅助函数中。仓库当前未配置格式化工具，提交前请主动对齐周边代码风格。

## 测试规范
测试框架为 `pytest`，发现路径在 `pyproject.toml` 中固定为 `tests/`。凡是修改日期计算、配置加载、CLI 编排流程、planner 或 Notion 同步逻辑，都应补充或更新对应测试。测试文件命名采用 `test_<feature>.py`，优先复用夹具或局部辅助函数，避免重复搭建测试环境。

## 提交与 Pull Request 规范
近期提交信息以简短祈使句为主，例如 `Add Notion artifact sync`、`Fix next trade date fallback...`；更早的提交也出现过 `fix:`、`feat:` 这类 Conventional Commit 前缀。建议保持单一主题、短标题、祈使语气。PR 需要说明变更的运行影响，列出新增或调整的环境变量、路径或外部依赖，关联对应 issue/任务；如果修改 CLI 行为，附上关键命令输出示例。

## 配置与运维注意事项
新增配置时优先复用 `deploy/cb-orchestrator.env.example` 中已有命名，不要随意扩展一套新变量。不要提交密钥、运行态状态文件、生成的计划产物，或指向 `~/local_state`、`~/local_data` 的本地路径。如果调整 planner 输入契约或 Notion 同步字段，请在同一个 PR 中同步更新 [`docs/next_trade_workflow.md`](./docs/next_trade_workflow.md)。
