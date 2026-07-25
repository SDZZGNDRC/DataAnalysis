# 加密货币量化交易策略研究计划（2026 年 6 月 OKX BTC-USDT-SWAP）

> 状态：Phase 0/1 完成, Phase 2 框架搭建并验证通过, Phase 3 起为后续阶段
> 启动：2026-07-25

## 进度记录

- **Phase 0（完成）**：`scripts/qc_june.py` 对 30 天做了文件级与 seqId 级质检；输出 `E:\tmp\qc\2026-06-DD.json` + `summary.csv`。结果：26/30 天文件级通过；其余 4 天缺陷为日内行情/采集小空洞（06-07、06-21、06-23、06-24）。质检发现 Raw Books JSON 实含 `localTs`，故随机延迟合成默认关闭改用真实 localTs。
- **Phase 1（完成）**：发现 seqId 会话横跨多个日历日（如 2026-06-15 全天仅 1 条链且链头 prevSeqId≠-1，无 snapshot），改为基于会话段切 npz：`scripts/build_june_segments.py`（复用 `reconstruct_segments`）→ `E:\tmp\segments.csv`（77 段，76 snapshot 头段，覆盖 29.5 天），`scripts/convert_segments_npz.py`（内存/mmap 自动切换）→ 49 个 npz，12.2GB。`scripts/make_manifest.py` 划分 train 38 段(19.8 天)/val 7 段(4.2 天)/test 4 段(3.3 天)。
  - 关键修正：`hftbacktest_okx.py`/`hftbacktest_okx_mmap.py` 的 `parse_filename` 原正则不识别 `-SWAP` 后缀，已修复为按 13 位时间戳尾部锚定解析 instId。
- **Phase 2（完成）**：`backtests/strategy_registry.py` + `backtests/worker_grid.py` + `backtests/grid_search.py`；`evaluation/aggregate.py` + `evaluation/select_and_report.py`。冒烟验证（seg64：mean_reversion 2 组）通过；多进程池在 train 子集（38 seg × 2 组 = 76 任务）批量回测成功（70 成功）。

## 待办（Phase 3-5）

1. **Phase 3**：对 mean_reversion / order_flow_imbalance / rejection 三个策略分别跑全网格：
   - `python backtests/grid_search.py --strategy <s> --manifest E:\tmp\npz\manifest.csv --split train --grid grids\<s>.json --contract contracts\btc_usdt_swap.json --out-dir E:\tmp\results\<s>_train --processes 8`
   - 验证段用 top-K 参数复跑（需先从 train aggregate 选参并写出 val grid json），测试段仅跑最终选定的 1 组参数。
   - 时间预算：mean_reversion 32 组 × 38 seg ≈ 1216 任务，长 seg 耗时较长，建议减小网格或夜间批量。
2. **Phase 4**：开发 `strategies/queue_imbalance_mm.py` 并在 `strategy_registry.py` 注册为 `queue_imbalance_mm`；smoke test seg64 后进训练网格。
3. **Phase 5**：`select_and_report.py` 对每策略跑一次（含 --val-agg/--test-agg），生成最终 `docs/research_report_2026-06.md`。
> 数据：`E:\datapool\2026-06-01 ~ 2026-06-30`（只读）
> 中间产物：`E:\tmp\`（qc / npz / results）

## 1. 目标与范围

在现有基建（7z→npz→hftbacktest→网格搜索）之上，对 **BTC-USDT-SWAP 永续合约** 2026-06 全月数据，系统化研究 **3 个现有策略 + 1 个新开发策略**，产出可复现的回测结果、参数稳健性分析与样本外评估结论。

### 数据前提（Phase 0 正式复核）

- `E:\datapool\2026-06-01..30` 全部 30 天存在；BTC-USDT-SWAP Books-400 每天 93~291 个 7z、Trades 每天数十个
- 抽查 2026-06-15：Books 146 个文件 0 缺口覆盖完整 24h，Trades 34 个文件 0 缺口
- 单日 BTC 全部类目 7z 压缩态约 133~436 MB

### 硬约束

- `E:\datapool` **只读**，所有中间产物写入 `E:\tmp`
- E:\ 剩余约 65.8GB → **按天流式处理**，原始 JSON 不落地（`hftbacktest_okx.py` 直接消费 7z）
- conda 环境 `CryptoSpider`（`D:\Software\miniconda3\envs\CryptoSpider\python.exe`，hftbacktest 2.4.2）

## 2. 时间划分（20/5/5）

| 阶段 | 日期 | 用途 |
|---|---|---|
| 训练段 | 06-01 ~ 06-20 | 参数网格搜索 |
| 验证段 | 06-21 ~ 06-25 | top-K 参数复跑、选定最终参数 |
| 测试段 | 06-26 ~ 06-30 | 样本外评估（**只跑一次**，防过拟合） |

## 3. 执行阶段

### Phase 0 — 数据质检（~1 天）

- 用 `check_seqId.py`、`check_timestamp_gaps.py`（均支持 7z）对 6 月 30 天的 BTC Books+Trades 做全量质检
- 产出：`E:\tmp\qc\2026-06-DD.json` + `valid_days.txt` 可用日清单
- 验收：质检报告覆盖 30 天，valid_days ≥ 25 天

### Phase 1 — 回测数据集构建（~2 天）

**关键发现（驱动 Phase 1 设计变更）**：

- 原始 Books 数据的 seqId 会话可跨越多个日历日：抽查 2026-06-15 整天仅 1 条 seqId 链且链头 prevSeqId≠-1，说明该日**无 snapshot**（会话在更早日期开始）。`hftbacktest_okx.py` 要求事件流首部必须有 snapshot 才能初始化订单簿，故"按日历日切 npz"会因缺 snapshot 而失败。
- 修正方案：**按 seqId 会话段（segment）切 npz**，与 repo 既有 `reconstruct_segments.py` + `process_segments_to_npz.py` 流程一致。每个 segment 从一个 snapshot（prevSeqId=-1 链头）开始，到下一次断链结束，自然满足 snapshot 要求，且无数据重复、不浪费磁盘。

**步骤**：

- 新增 `scripts\build_june_segments.py`：跨 30 天收集 `OKX-Books-BTC-USDT-SWAP-400-*.7z`，复用 `reconstruct_segments.process_file` + `merge_pairs` + `decode_bitmap`，输出 `E:\tmp\segments.csv`（每行一个 session，含 covered_files、start/end_ts、duration）
- 新增 `scripts\convert_segments_npz.py`：读取 segments.csv，按 duration≥1h 过滤可用段；每段配对时间范围内的 `OKX-Trades-BTC-USDT-SWAP-*.7z`，调用 `hftbacktest_okx.py` 生成 `E:\tmp\npz\seg_{index}_{start_ts}.npz`（用真实 localTs；--use-random-latency 仅在缺 localTs 时启用，本批数据已含 localTs 故可关闭随机延迟）
- 先试转最大 1~2 个段实测耗时/体积，再批量；`--skip-existing` 支持断点续跑
- 训练/验证/测试按 **segment 起始日期** 划分：06-01~06-20 / 06-21~06-25 / 06-26~06-30
- 合约参数校准：BTC-USDT-SWAP tick_size=0.1、lot_size=0.01（1 张=0.01 BTC）；费率校准为 OKX 实际（maker≈0.02% / taker≈0.05%，runner 默认 0.08%/0.10% 偏高，评估框架改为可配置）
- 验收：所有可用段 npz 生成并通过 `analyze_npz_gaps.py` 校验；记录每段事件数、首 snapshot ts、覆盖时长；可用段总时长覆盖训练段≥18 天、验证段≥4 天、测试段≥5 天

### Phase 2 — 评估框架改造（~2 天）

- 泛化网格搜索：新建 `backtests\grid_search.py`，将 `run.py`/`worker.py` 的硬编码改为 CLI 可配：
  - `--strategy {mean_reversion,order_flow_imbalance,rejection,queue_imbalance_mm}`、`--data-dir`、`--dates`、`--grid grid.json`、`--out`
  - 保留 shared_memory + mp.Pool 机制；worker 按策略名动态 import
- 新建评估模块 `evaluation\`：
  - `aggregate.py`：聚合 result JSON → 宽表（行=参数组合，列=日期，值=Return/Sharpe/Sortino/MDD）
  - `metrics.py`：补充日均收益、胜率、盈亏比、交易次数、参数稳健性（邻近参数性能方差）
  - 基准：`hft_ohlc.py` 重放 1m K 线算 buy&hold 基准
  - `report.py`：生成 markdown（训练期热力图、验证期 top-K、测试期对比）
- 验收：任意策略一条命令完成"训练网格→验证选参→测试评估→报告"

### Phase 3 — 三个现有策略系统化回测（~3 天）

| 策略 | 参数网格（初定） |
|---|---|
| A. 均值回归 `mean_reversion_demo` | step_ns{50ms,100ms} × ema_alpha{0.05,0.1,0.2,0.4} × threshold_ticks{1.0,1.5,2.0,3.0} |
| B. 订单流不平衡 `order_flow_imbalance` | volume_threshold{50,100,200,500} × tp{0.1%,0.2%,0.3%} × sl{0.05%,0.1%,0.2%} |
| C. Rejection `backtest_rejection_strategy` | 沿用现有 6 参数网格并扩展 |

- 流程：训练段网格搜索 → 验证段 top-5 复跑 → 按"验证期夏普最优 + 参数邻域稳健"定参
- 验收：每策略产出训练+验证两段完整结果表

### Phase 4 — 新策略开发（~2 天）

- 方向：**queue-imbalance 做市策略**（一档队列量不平衡调整报价 + 库存惩罚，`okx_mm_demo.py` 为起点）
- 交付：`strategies\queue_imbalance_mm.py` + runner + 纳入 grid_search；06-01 单日 smoke test 通过后进全量网格
- 验收：smoke test 通过 + 训练段网格搜索完成

### Phase 5 — 样本外测试与最终报告（~1 天）

- 4 个策略的最优参数在测试段（06-26~06-30）**各跑一次**
- 汇总对比：Return/Sharpe/Sortino/MDD/胜率/交易次数 vs buy&hold 基准
- 撰写 `docs\research_report_2026-06.md`
- 验收：报告含全部对比表与可复现命令

## 4. 交付物清单

| 类型 | 路径 |
|---|---|
| 本计划 | `docs\research_plan_2026-06.md` |
| 框架改造 | `backtests\grid_search.py`、worker 泛化、`evaluation\` 模块 |
| 新策略 | `strategies\queue_imbalance_mm.py` + runner |
| 数据产物（E:\tmp） | `qc\` 质检报告、`npz\` 回测数据集、`results\` 回测结果 |
| 最终报告 | `docs\research_report_2026-06.md` |

## 5. 风险与缓解

| 风险 | 缓解 |
|---|---|
| E:\ 空间不足（全月 npz 或超 45GB） | 按天流式转换；分段常驻；先试转单日实测体积 |
| 单日转换耗时不确定 | 06-15 试转测时后再排全月；`--num-processes` 拉满 |
| 费率/延迟模型与真实有偏 | 校准 OKX 实际费率；报告注明对数正态延迟假设 |
| 个别日期数据不完整 | Phase 0 剔除并记录；valid_days < 25 时降级为部分月份研究 |
| numba JIT 首编译慢 | smoke test 预热，worker 复用编译缓存 |

**总工期预估：约 7~11 个工作日**（不含全月 npz 转换的排队等待时间）。
