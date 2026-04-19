# cb_orchestrator

一个很薄的日常编排器，只负责把 `CB-Qlib-Infra` 和 `cb_online_runtime` 串起来。

固定边界：

- 先跑上游日更
- 再读上游 `latest.json`
- 只有上游明确成功时，才继续下游
- 下游当前只做两件事：月度训练、日度预测
- 当前只串 `cb_batch_15` 和 `cb_batch_27`
- 严格串行执行，不并发

它不负责因子挖掘，不负责 pack 导出，也不负责交易执行。

## 当前判定逻辑

只有同时满足下面 4 个条件，才会进入 runtime：

- `exit_class == success`
- `cb_status == success`
- `qlib_status == success`
- `target_trade_date` 非空

否则直接暂停下游，并把结果写入 orchestrator 自己的状态目录。

## 运行逻辑

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

可以用单独 env 文件，也可以直接走 shell 环境。

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
- `ORCH_STATE_ROOT`
- `ORCH_STRATEGIES`
- `MODEL_NUM_THREADS`

邮件相关变量会原样透传给 `predict_daily.sh`。邮件失败不会被当成 orchestrator 失败，只要预测文件产物落地成功即可。

## 手工运行

读现有上游状态，但不真正启动子进程：

```bash
cb-orchestrator-daily \
  --env-file ~/.config/cb-orchestrator/cb-orchestrator.env \
  --dry-run \
  --skip-upstream
```

正常执行：

```bash
cb-orchestrator-daily \
  --env-file ~/.config/cb-orchestrator/cb-orchestrator.env
```

## systemd

示例文件在：

- `deploy/systemd/cb-orchestrator.service`
- `deploy/systemd/cb-orchestrator.timer`

预期做法：

- 保留 `cb-qlib-repair.timer`
- 用 `cb-orchestrator.timer` 替换原先的 `cb-qlib-daily.timer`

## 设计取舍

- 不 import 上游仓库，也不 import runtime 仓库
- 全部通过外部命令调用，边界清楚
- 默认锁文件防重入
- 默认线程数保持低位，适合小 VPS
