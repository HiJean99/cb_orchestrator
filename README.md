# cb_orchestrator

一个很薄的下游编排器，只负责承接上游 `CB-Qlib-Infra` 的状态，驱动 `cb_online_runtime`，并在预测完成后基于本地 `holdings.json` 生成下一交易日执行计划。

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

## 下一交易日计划

`cb-orchestrator-plan-next-trade` 会：

1. 读取最近一次成功的 orchestrator run，或者显式指定 `run_id` / `trade_date`
2. 从每个策略的 `next_positions.csv` 回退到 `topN.csv`
3. 合并成一份统一排序
4. 读取本地 `holdings.json`
5. 按 `top6 + max_drop3` 生成下一交易日执行计划
6. 写本地产物：
   - `next_trade_plan.json`
   - `next_trade_plan.csv`
   - `daily_brief.html`

第一阶段不做：

- OCR 文本解析
- 消息推送
- Notion / Lark / 飞书同步
- 本地 SQLite 审计账本
- accepted / submitted / filled 人工反馈流
- 三天仿真回放

`top6 + max_drop3` 的语义是：

- 目标持仓集合始终取最新排名的前 6 名
- 实际持仓基线来自本地 `holdings.json`
- 每个交易日最多卖出 3 个掉出 `top6` 的持仓
- 只在卖出腾出名额后补买缺失的 `top6` 标的
- `holdings.json` 存在但 `positions` 为空时，走 bootstrap，直接生成前 6 名买入建议

## holdings.json 格式

```json
{
  "snapshot_at": "2026-04-20T15:00:00+08:00",
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
- 文件缺失或 JSON 非法会直接报错，不会静默 bootstrap

## 命令

执行一次下游编排：

```bash
cb-orchestrator-daily --env-file ~/.config/cb-orchestrator/cb-orchestrator.env
```

生成下一交易日计划：

```bash
cb-orchestrator-plan-next-trade --env-file ~/.config/cb-orchestrator/cb-orchestrator.env
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
- `CURRENT_POSITIONS_JSON_PATH`
- `PLAN_OUTPUT_ROOT`
- `NEXT_TRADE_TOP_N`
- `NEXT_TRADE_MAX_DROP`

邮件相关变量会原样透传给 `predict_daily.sh`。邮件失败不会被当成 orchestrator 失败，只要预测文件产物落地成功即可。

`deploy/cb-orchestrator.env.example` 现在只保留为配置字段参考。调度方式和生产部署策略不在这个仓库里定义。
