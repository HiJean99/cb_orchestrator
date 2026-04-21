# cb_orchestrator

一个很薄的下游编排器，只负责承接上游 `CB-Qlib-Infra` 的状态，驱动 `cb_online_runtime`，并在预测完成后生成下一交易日执行计划。

当前代码里的计划输入已经切到按 `signal_date` 对齐的结构化快照 bundle：`PLAN_INPUT_ROOT/<signal_date>/holdings_snapshot.json` 和 `ranking_snapshot.json`。面向正式流程、对象定义和 Notion 角色，见 [docs/next_trade_workflow.md](./docs/next_trade_workflow.md)。

这个仓库的边界很窄：

- 读取上游 `latest.json`
- 只在上游明确成功时继续下游
- 下游当前只做月度训练和日度预测
- 预测完成后生成下一交易日计划
- 当前只串 `cb_batch_15` 和 `cb_batch_27`
- 默认严格串行，不并发

它不是上游数据生产仓库，不负责 Qlib 数据生成，不负责 pack 导出，也不负责真实交易执行。

## 当前逻辑

一次 `cb-orchestrator-daily` 会做：

1. 调上游 `scripts/orchestrate_daily_update.py --mode daily`
2. 读取上游 `latest.json`
3. 用 trade calendar 解析：
   - `signal_date = target_trade_date`
   - `trade_date = signal_date` 的下一交易日
   - `first_trade_date_of_month = trade_date` 所在月首个交易日
4. 对 `cb_batch_15`、`cb_batch_27` 依次执行：
   - 若当月模型不存在，则先训练
   - 再做当日预测
5. 把本次编排结果写到：
   - `.../orchestrator/latest.json`
   - `.../orchestrator/runs/<run_id>.json`

如果上游状态不是明确成功，这里就直接停止，不继续下游 runtime。

## 当前实现状态

目前仓库里真正已经实现的是：

- `cb-orchestrator-daily` 的上游状态承接、训练、预测和 run summary 落盘
- `cb-orchestrator-plan-next-trade` 基于 `PLAN_INPUT_ROOT/<signal_date>/` 下的结构化快照 bundle 生成计划
- `cb-orchestrator-sync-notion` 把 holdings / rankings / plans 同步到既有 Notion 6 张库
- `next_trade_plan.json/csv/html` 这三份本地产物

目前还没有接入的是：

- iPhone 快捷指令上传 OCR 文本
- `do-sgp` 侧的快照写入和归档
- OCR 文本解析和人工修正回写

## 下一交易日计划

`cb-orchestrator-plan-next-trade` 会：

1. 读取最近一次成功的 orchestrator run，或者显式指定 `run_id` / `trade_date`
2. 用 `signal_date` 定位 `PLAN_INPUT_ROOT/<signal_date>/`
3. 读取：
   - `holdings_snapshot.json`
   - `ranking_snapshot.json`
4. 校验 run summary、持仓快照、排名快照的 `signal_date` 一致，且排名快照 `run_id` 与本次 run 一致
5. 按 `top6 + max_drop3` 生成下一交易日执行计划
6. 写本地产物：
   - `next_trade_plan.json`
   - `next_trade_plan.csv`
   - `daily_brief.html`

当前代码尚未接入：

- OCR 文本解析
- `do-sgp` 快照生产
- Lark / 飞书同步
- 本地 SQLite 审计账本
- accepted / submitted / filled 人工反馈流
- 三天仿真回放

## 目标流程

正式数据流不是“今天持仓 + 今天 top6”，而是同一 `signal_date` 上的结构化快照：

1. 交易日 `T` 完成后，通过 iPhone 快捷指令把当日持仓 OCR 文本发到 `do-sgp`
2. `do-sgp` 保存原始 OCR 文本并解析成 `holdings_snapshot(T)`
3. 当日数据更新和预测完成后，生成 `ranking_snapshot(T)`
4. planner 读取：
   - `holdings_snapshot(T)`
   - `ranking_snapshot(T)`
   - 策略参数 `target_n=6`、`max_drop=3`
5. 生成 `next_trade_plan(T -> T+1)`
6. 本地产物仍然先落在 `do-sgp` / orchestrator 侧
7. Notion 只做持仓确认、排名查看、计划复核和历史留痕

这里有两个边界必须固定：

- `signal_date` 是持仓快照和排名快照的共同日期
- `trade_date` 是 `signal_date` 的下一交易日

也就是说，`next-trade plan` 的正确输入是：

- 当天持仓快照
- 当天完整排名快照
- 策略参数

而不是只有：

- 当天持仓
- 当天 top6

原因是 `top6` 只能回答“应该补进谁”，不能回答“当前 6 只里哪 3 只最差，应该先换掉谁”。`max_drop3` 的裁剪必须依赖完整排名，至少也要知道当前持仓在当日排序里的位置。

`top6 + max_drop3` 的语义是：

- 目标持仓集合始终取最新排名的前 6 名
- 实际持仓基线来自 `holdings_snapshot(T)`
- 每个交易日最多卖出 3 个掉出 `top6` 的持仓
- 只在卖出腾出名额后补买缺失的 `top6` 标的
- `holdings_snapshot.positions` 为空时，走 bootstrap，直接生成前 6 名买入建议

## Plan Input Bundle

planner 固定从：

- `PLAN_INPUT_ROOT/<signal_date>/holdings_snapshot.json`
- `PLAN_INPUT_ROOT/<signal_date>/ranking_snapshot.json`

读取同一交易日的一对输入。

`holdings_snapshot.json` 最低契约：

```json
{
  "signal_date": "2026-04-20",
  "snapshot_key": "holding-2026-04-20",
  "parse_status": "parsed",
  "confirmed_at": null,
  "positions": [
    {"instrument": "A", "holding_qty": 100},
    {"instrument": "B", "holding_qty": 200}
  ]
}
```

规则：

- `instrument` 会自动转成大写
- 重复标的会自动合并数量
- `holding_qty = 0` 会被忽略
- `confirmed_at` 为空不会阻塞 planner；修正后可以重跑
- 文件缺失或 JSON 非法会直接报错，不会静默 bootstrap

`ranking_snapshot.json` 最低契约：

```json
{
  "signal_date": "2026-04-20",
  "run_id": "20260420_083000",
  "ranking_snapshot_key": "ranking-2026-04-20",
  "policy_name": "ensemble_daily",
  "generated_at": "2026-04-20T20:00:00+08:00",
  "ranked_universe_count": 3,
  "ranked_entries": [
    {
      "instrument": "A",
      "display_rank": 1,
      "final_score": 0.95,
      "source_strategies": ["cb_batch_15", "cb_batch_27"],
      "source_scores": {"cb_batch_15": 0.95, "cb_batch_27": 0.85}
    }
  ]
}
```

规则：

- `ranked_entries` 必须是完整排名，不接受只给 `top6`
- `ranked_universe_count` 必须和 `ranked_entries` 长度一致
- `ranking_snapshot.run_id` 必须和本次 resolved run 的 `run_id` 一致
- `source_strategies` / `source_scores` 会在 planner 里归一化成字符串输出

## 命令

执行一次下游编排：

```bash
cb-orchestrator-daily --env-file ~/.config/cb-orchestrator/cb-orchestrator.env
```

生成下一交易日计划：

```bash
cb-orchestrator-plan-next-trade --env-file ~/.config/cb-orchestrator/cb-orchestrator.env
```

把某次 run 的 holdings / rankings / plan 同步到 Notion：

```bash
cb-orchestrator-sync-notion --env-file ~/.config/cb-orchestrator/cb-orchestrator.env --run-id 20260420_083000
```

也可以显式指定某次运行：

```bash
cb-orchestrator-plan-next-trade --env-file ~/.config/cb-orchestrator/cb-orchestrator.env --run-id 20260420_083000
```

如果你只想读取已有上游状态，不真正启动子进程：

```bash
cb-orchestrator-daily \
  --env-file ~/.config/cb-orchestrator/cb-orchestrator.env \
  --dry-run \
  --skip-upstream
```

## 安装

```bash
cd cb_orchestrator
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip setuptools wheel
python -m pip install -e .
```

开发测试：

```bash
source .venv/bin/activate
python -m pip install -e .[dev]
pytest
```

## 环境变量

最小建议是复制一份：

```bash
cp deploy/cb-orchestrator.env.example ~/.config/cb-orchestrator/cb-orchestrator.env
```

关键变量：

- `UPSTREAM_REPO_ROOT`
- `UPSTREAM_PYTHON_BIN`
- `UPSTREAM_STATE_FILE`
- `UPSTREAM_ENV_FILE`
- `PROVIDER_URI`
- `TRADE_CALENDAR_PATH`
- `RUNTIME_REPO_ROOT`
- `PACKS_ROOT`
- `MODEL_ROOT`
- `BUILD_ROOT`
- `PREDICTION_ROOT`
- `LOG_ROOT`
- `RUNTIME_PYTHON_BIN`
- `RUNTIME_PYTHONPATH`
- `RUNTIME_TRAIN_BIN`
- `RUNTIME_PREDICT_BIN`
- `ORCH_STATE_ROOT`
- `ORCH_STRATEGIES`
- `MODEL_NUM_THREADS`
- `PLAN_INPUT_ROOT`
- `PLAN_OUTPUT_ROOT`
- `NEXT_TRADE_TOP_N`
- `NEXT_TRADE_MAX_DROP`

邮件相关变量会原样透传给 `predict_daily.sh`。邮件失败不会被当成 orchestrator 失败，只要预测文件产物落地成功即可。

`deploy/cb-orchestrator.env.example` 现在只保留为配置字段参考。调度方式和生产部署策略不在这个仓库里定义。
