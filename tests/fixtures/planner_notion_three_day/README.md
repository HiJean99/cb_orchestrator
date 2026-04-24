连续 3 个交易日的 fixture，用于验证 `planner` 和 `notion sync` 的连续行为。

覆盖重点：

- 连续 3 个 signal_date / trade_date 的计划生成
- `CB Decision Days` / `CB Plan Orders` 的 `Is Latest` 轮换
- `CB Plan Orders` 的 `Prev Display Rank` / `Rank Delta`
- 第 3 天持仓状态为 `needs_review` 时，`Decision Status=blocked`
- 同一工作区连续重跑时的正文刷新和历史页面保留

日期序列：

- `2026-04-20 -> 2026-04-21`
- `2026-04-21 -> 2026-04-22`
- `2026-04-22 -> 2026-04-23`

手工检查脚本：

```bash
python tests/fixtures/planner_notion_three_day/run_manual_check.py
```

默认行为：

- 复制 fixture 到临时目录
- 依次对 3 个 run_id 运行 planner CLI
- 用同一个 fake Notion client 依次跑 3 次 `sync_run_to_notion`
- 输出每一天的 planner / notion 摘要，以及最终累积页数

如果要在真实 Notion 工作区联调，可以传：

```bash
python tests/fixtures/planner_notion_three_day/run_manual_check.py \
  --base-env-file ~/.config/cb-orchestrator/cb-orchestrator-notion.env \
  --real-notion
```

如果要先清空真实工作区里的主表样例页，再做联调，可以复用一日样例里的清理脚本：

```bash
python tests/fixtures/planner_notion_one_day/cleanup_real_notion.py \
  --env-file ~/.config/cb-orchestrator/cb-orchestrator-notion.env
```
