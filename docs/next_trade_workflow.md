# Next-Trade Workflow Design

这份文档描述的是 `cb_orchestrator` 的正式流程和对象定义，其中 planner 读侧已经按这里的快照思路实现，快照写侧和 Notion 同步仍在外部系统。

当前代码已经是：

- 从 `PLAN_INPUT_ROOT/<signal_date>/` 读取 `holdings_snapshot.json`
- 从 `PLAN_INPUT_ROOT/<signal_date>/` 读取 `ranking_snapshot.json`
- 直接生成 `next_trade_plan.json/csv/html`

目标流程是在不改变 planner 核心职责的前提下，把输入稳定在“按日期对齐的结构化快照”。

## 角色分工

- `iPhone 快捷指令`
  - 每个交易日结束后上传持仓 OCR 文本
- `do-sgp`
  - 保存原始 OCR 文本
  - 解析并落结构化持仓快照
  - 保存完整排名快照
  - 保存计划产物
- `cb_orchestrator`
  - 承接上游状态
  - 触发训练和预测
  - 读取结构化快照
  - 生成 `next_trade_plan`
- `Notion`
  - 查看和确认每日持仓
  - 查看每日排名摘要和重点标的
  - 复核下一交易日计划
  - 留人工备注和历史审计痕迹

这里的边界是：

- `do-sgp` 是机器侧主存储和快照入口
- `Notion` 是人工查看、确认和留痕层
- `cb_orchestrator` 是编排和计划生成层

Notion 不是：

- OCR 解析器
- 唯一 source of truth
- planner 的底层数据库
- 交易执行系统

## 每日流程

以交易日 `T` 为例：

1. 交易日 `T` 完成后，通过 iPhone 快捷指令把当日持仓 OCR 文本发送到 `do-sgp`
2. `do-sgp` 保存原始 OCR 文本，并解析出 `holdings_snapshot(T)`
3. 当日数据更新和预测完成后，生成 `ranking_snapshot(T)`
4. planner 读取：
   - `holdings_snapshot(T)`
   - `ranking_snapshot(T)`
   - 策略参数 `target_n=6`、`max_drop=3`
5. 生成 `next_trade_plan(T -> T+1)`
6. 本地产物先落在 `do-sgp` / orchestrator 侧
7. Notion 同步摘要，用于持仓确认、排名查看和计划复核

这里的日期语义必须固定：

- `signal_date = T`
- `trade_date = signal_date` 的下一交易日

`next_trade_plan` 的输入是“同一 `signal_date` 的两份快照 + 策略参数”，不是一份“今天持仓”和一份“今天 top6”。

## Planner Input Bundle

planner 固定读取：

- `PLAN_INPUT_ROOT/<signal_date>/holdings_snapshot.json`
- `PLAN_INPUT_ROOT/<signal_date>/ranking_snapshot.json`

并做两层一致性校验：

- 两份快照的 `signal_date` 必须和 run summary 一致
- `ranking_snapshot.run_id` 必须和 resolved run 的 `run_id` 一致

## 为什么 `top6` 不够

规则是：

- 目标持仓数固定为 6
- 每次最多只换掉最差的 3 只

如果只有 `top6`，只能知道“谁值得买进”，不能知道“当前持仓里哪 3 只最差，应该先卖掉”。

要做 `max_drop3` 裁剪，至少要知道：

- 当前持仓里每个标的在当日完整排序中的位置

因此，正式输入必须是：

- `holdings_snapshot`
- `ranking_snapshot`
- `target_n`
- `max_drop`

而不是：

- `holdings_snapshot`
- `top6`

## 三个核心对象

### `holdings_snapshot`

机器侧最小字段：

- `signal_date`
- `snapshot_key`
- `parse_status`
- `submitted_at`
- `parsed_at`
- `confirmed_at`

子项 `positions[]`：

- `instrument`
- `holding_qty`

### `ranking_snapshot`

机器侧最小字段：

- `signal_date`
- `run_id`
- `ranking_snapshot_key`
- `policy_name`
- `ranked_universe_count`
- `generated_at`
- `ranked_entries`

子项 `ranked_entries[]`：

- `instrument`
- `display_rank`
- `final_score`
- `source_strategies`
- `source_scores`

`ranked_entries[]` 必须是完整排序，不能只给 `top6`。`focus_entries[]` 这种面向展示的裁剪视图可以继续存在，但不能作为 planner 真值输入。

### `next_trade_plan`

机器侧最小字段：

- `signal_date`
- `trade_date`
- `run_id`
- `policy_name`
- `target_n`
- `max_drop`
- `holdings_snapshot_ref`
- `holdings_snapshot_path`
- `holdings_confirmed_at`
- `ranking_snapshot_ref`
- `ranking_snapshot_path`
- `ranked_universe_count`
- `current_positions_count`
- `buy_count`
- `sell_count`
- `hold_count`
- `watch_count`
- `bootstrap`
- `generated_at`
- `source_run_path`
- `plan_json_path`
- `plan_csv_path`
- `brief_html_path`

子项 `orders[]`：

- `plan_key`
- `instrument`
- `display_rank`
- `current_holding_qty`
- `current_in_portfolio`
- `ranked_top_n`
- `planned_in_portfolio`
- `strategy_action`
- `strategy_reason`
- `source_strategies`
- `source_scores`
- `updated_at`

## Notion 映射

当前已定的 6 张数据库：

- `CB Daily Holdings`
- `CB Holding Positions`
- `CB Daily Rankings`
- `CB Ranking Focus`
- `CB Daily Plans`
- `CB Plan Orders`

关系方向：

- `CB Daily Holdings` <-> `CB Holding Positions`
- `CB Daily Rankings` <-> `CB Ranking Focus`
- `CB Daily Plans` <-> `CB Plan Orders`
- `CB Daily Rankings` -> `Holdings Snapshot`
- `CB Daily Plans` -> `Holdings Snapshot`
- `CB Daily Plans` -> `Ranking Snapshot`

首页顺序：

1. `今日持仓`
2. `今日排名`
3. `排名焦点`
4. `待复核计划`
5. `最近计划`
6. `未处理订单`

`CB Data Sources` 只保留原始数据库和 schema，不承担首页入口角色。

## 剩余接入计划

推荐按这个顺序继续接：

1. 在 `do-sgp` 落 `holdings_snapshot`
2. 在 `do-sgp` 落 `ranking_snapshot`
3. 继续保留 `next_trade_plan.json/csv/html` 作为本地产物
4. 最后再做 Notion 同步

也就是说，Notion 同步不是 planner 的前提；结构化快照才是 planner 的前提。
