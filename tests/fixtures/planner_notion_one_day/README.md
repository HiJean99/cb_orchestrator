单交易日 fixture，用于先验证 `planner` 和 `notion sync` 主路径。

内容包括：

- `state/latest.json` 和 `state/runs/20260421_150716.json`
- `state/plan_inputs/2026-04-20/holdings_snapshot.json`
- `state/plan_inputs/2026-04-20/ranking_snapshot.json`
- `provider/calendars/day.txt`
- `runtime/local_assets/cb_basic.csv`

这套数据刻意覆盖一个非 bootstrap 的换仓日：

- `keep`: `SZ128125`, `SH113584`
- `sell`: `SH118057`, `SZ123092`, `SZ127067`
- `buy`: `SZ123064`, `SZ127025`, `SZ127017`
- `watch`: `SZ128134` (`deferred_drop`), `SH110073` (`deferred_entry`)

手工检查脚本：

```bash
python tests/fixtures/planner_notion_one_day/run_manual_check.py
```

默认行为：

- 复制 fixture 到临时目录
- 运行 planner CLI
- 用 fake Notion client 跑一次 `sync_run_to_notion`
- 输出 JSON 结果，方便检查 `orders` 和 Notion 摘要

如果要在真实 Notion 工作区联调，可以传：

```bash
python tests/fixtures/planner_notion_one_day/run_manual_check.py \
  --base-env-file ~/.config/cb-orchestrator/cb-orchestrator-notion.env \
  --real-notion
```

这时脚本会用 fixture 路径覆盖本地 state / provider / plan 输入输出路径，但会从 `--base-env-file` 里继承真实 `NOTION_*` 配置，再调用真实的 `notion_sync_cli`。

如果需要先清空这 5 张主表里的旧样例页，再做真实联调，可以先执行：

```bash
python tests/fixtures/planner_notion_one_day/cleanup_real_notion.py \
  --env-file ~/.config/cb-orchestrator/cb-orchestrator-notion.env
```

这个清理脚本只会归档当前 V2 主表中的活动页：

- `CB Holdings Snapshots`
- `CB Holding Positions`
- `CB Daily Rankings`
- `CB Decision Days`
- `CB Plan Orders`

如果希望 Notion 页面里记录的 `json/csv/html` 本地路径在联调后继续可用，不要给 `run_manual_check.py` 加 `--cleanup`，并且最好显式传一个保留的 `--work-root`。
