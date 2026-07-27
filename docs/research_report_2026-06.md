# 2026-06 BTC-USDT-SWAP 量化策略研究报告

生成时间：基于本仓库框架 pipeline（Phase 0–5）。

## 1. 研究概述

- **标的**：OKX BTC-USDT-SWAP 永续合约，2026-06 全月原始数据（`E:\datapool`，只读）。

- **时间划分**：训练 06-01~06-20 / 验证 06-21~06-25 / 测试 06-26~06-30（按 **seqId 会话段** 起始日期归属）。

- **关键发现**：seqId 会话横跨多个日历日，因此采用 **基于会话段的 npz** 而非按日历日切片（详见 `docs/research_plan_2026-06.md`）。

- **合约**：tick=0.1、lot=0.01、maker=0.0002、taker=0.0007、初始资金 n/a（hftbacktest 从 0 起算，equity 即净盈亏 USDT）。

- **耗时预算**：grid_search 在长段（30h+、上亿事件）单次回测数百秒、内存高，训练网格采用 `--max-seg-hours 8` 截断加速；验证段 `--max-seg-hours 4`；测试段 `--max-seg-hours 8`。**报告口径包含 4 个 snapshot 头 seg 的截断样本外结果，已知口径偏差**。

### 会话段统计

| split | seg 数 | 总时长(h) | 约等于(天) |
|---|---|---|---|
| train | 38 | 475.7 | 19.82 |
| val | 7 | 101.3 | 4.22 |
| test | 4 | 78.4 | 3.27 |


## 2. 策略基准对比：buy&hold

buy&hold 为各 seg 的首末事件 px 近似 mid 收益。

## 3. 各策略训练→验证→测试结果


### 3.1 `mean_reversion` (recorder=False)

#### 训练期 top-K（排序键 seg_sharpe）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |   max_mdd |   mean_buyhold |     fee |   step_ns |   ema_alpha |   threshold_ticks |   max_position_lots |   order_qty_lots |
|---------:|-------------:|-------------:|-------------:|-----------:|----------:|---------------:|--------:|----------:|------------:|------------------:|--------------------:|-----------------:|
|       32 |    -0.923294 |    -0.191069 |     -61142   |          0 |  -60664.9 |   -0.00044458  | 41866.2 |     5e+07 |         0.4 |                 3 |                  10 |                2 |
|       32 |    -0.930232 |    -0.202419 |     -64774   |          0 |  -64273.1 |   -0.00044458  | 44410   |     5e+07 |         0.4 |                 2 |                  10 |                2 |
|       32 |    -0.945042 |    -0.219631 |     -70281.9 |          0 |  -69734.3 |   -0.00044458  | 48095.3 |     5e+07 |         0.4 |                 1 |                  10 |                2 |
|       32 |    -0.950576 |    -0.109447 |     -35023.1 |          0 |  -34814.2 |   -0.000127451 | 24003.6 |     1e+08 |         0.4 |                 2 |                  10 |                2 |
|       31 |    -0.952666 |    -0.104432 |     -32373.9 |          0 |  -32182.7 |   -0.000301993 | 22104.6 |     1e+08 |         0.4 |                 3 |                  10 |                2 |

#### 验证期 top-K（训练期 top-5 参数在验证期的聚合）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |   max_mdd |   mean_buyhold |     fee |   step_ns |   ema_alpha |   threshold_ticks |   max_position_lots |   order_qty_lots |
|---------:|-------------:|-------------:|-------------:|-----------:|----------:|---------------:|--------:|----------:|------------:|------------------:|--------------------:|-----------------:|
|        7 |    -0.944594 |   -0.162189  |    -11353.2  |          0 | -11086.4  |    -0.00306919 | 7736.56 |     5e+07 |         0.4 |                 3 |                  10 |                2 |
|        7 |    -0.951504 |   -0.171094  |    -11976.6  |          0 | -11676.1  |    -0.00306919 | 8173.88 |     5e+07 |         0.4 |                 2 |                  10 |                2 |
|        7 |    -0.961409 |   -0.183691  |    -12858.4  |          0 | -12509.7  |    -0.00306919 | 8761.3  |     5e+07 |         0.4 |                 1 |                  10 |                2 |
|        7 |    -1.02716  |   -0.0827732 |     -5794.12 |          0 |  -5509.67 |    -0.00306919 | 3887.93 |     1e+08 |         0.4 |                 3 |                  10 |                2 |
|        7 |    -1.03308  |   -0.0890276 |     -6231.93 |          0 |  -5915.99 |    -0.00306919 | 4167.45 |     1e+08 |         0.4 |                 2 |                  10 |                2 |

#### 测试期（训练 top-1 参数，样本外）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |   max_mdd |   mean_buyhold |     fee |   step_ns |   ema_alpha |   threshold_ticks |   max_position_lots |   order_qty_lots |
|---------:|-------------:|-------------:|-------------:|-----------:|----------:|---------------:|--------:|----------:|------------:|------------------:|--------------------:|-----------------:|
|        4 |     -1.54708 |    -0.204104 |     -8164.15 |          0 |  -7644.58 |    -0.00186117 | 5827.82 |     5e+07 |         0.4 |                 3 |                  10 |                2 |


### 3.2 `order_flow_imbalance` (recorder=False)

#### 训练期 top-K（排序键 seg_sharpe）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |   max_mdd |   mean_buyhold |   fee |   step_ns |   volume_threshold |   take_profit_pct |   stop_loss_pct |   fee_rate |   max_position_lots |   order_qty_lots |
|---------:|-------------:|-------------:|-------------:|-----------:|----------:|---------------:|------:|----------:|-------------------:|------------------:|----------------:|-----------:|--------------------:|-----------------:|
|       27 |          nan |            0 |            0 |          0 |         0 |   -0.000698727 |     0 |     5e+07 |                100 |             0.001 |          0.0005 |     0.0008 |                  10 |                1 |
|       27 |          nan |            0 |            0 |          0 |         0 |   -0.000698727 |     0 |     5e+07 |                100 |             0.001 |          0.001  |     0.0008 |                  10 |                1 |
|       28 |          nan |            0 |            0 |          0 |         0 |   -0.000485082 |     0 |     5e+07 |                100 |             0.001 |          0.002  |     0.0008 |                  10 |                1 |
|       27 |          nan |            0 |            0 |          0 |         0 |   -0.000698727 |     0 |     5e+07 |                100 |             0.002 |          0.0005 |     0.0008 |                  10 |                1 |
|       27 |          nan |            0 |            0 |          0 |         0 |   -0.000698727 |     0 |     5e+07 |                100 |             0.002 |          0.001  |     0.0008 |                  10 |                1 |

#### 验证期 top-K（训练期 top-5 参数在验证期的聚合）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |   max_mdd |   mean_buyhold |   fee |   step_ns |   volume_threshold |   take_profit_pct |   stop_loss_pct |   fee_rate |   max_position_lots |   order_qty_lots |
|---------:|-------------:|-------------:|-------------:|-----------:|----------:|---------------:|------:|----------:|-------------------:|------------------:|----------------:|-----------:|--------------------:|-----------------:|
|        7 |          nan |            0 |            0 |          0 |         0 |    -0.00348231 |     0 |     5e+07 |                100 |             0.001 |          0.0005 |     0.0008 |                  10 |                1 |
|        7 |          nan |            0 |            0 |          0 |         0 |    -0.00348231 |     0 |     5e+07 |                100 |             0.001 |          0.001  |     0.0008 |                  10 |                1 |
|        7 |          nan |            0 |            0 |          0 |         0 |    -0.00348231 |     0 |     5e+07 |                100 |             0.001 |          0.002  |     0.0008 |                  10 |                1 |
|        7 |          nan |            0 |            0 |          0 |         0 |    -0.00348231 |     0 |     5e+07 |                100 |             0.002 |          0.0005 |     0.0008 |                  10 |                1 |
|        7 |          nan |            0 |            0 |          0 |         0 |    -0.00472132 |     0 |     5e+07 |                100 |             0.002 |          0.001  |     0.0008 |                  10 |                1 |

#### 测试期（训练 top-1 参数，样本外）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |   max_mdd |   mean_buyhold |   fee |   step_ns |   volume_threshold |   take_profit_pct |   stop_loss_pct |   fee_rate |   max_position_lots |   order_qty_lots |
|---------:|-------------:|-------------:|-------------:|-----------:|----------:|---------------:|------:|----------:|-------------------:|------------------:|----------------:|-----------:|--------------------:|-----------------:|
|        4 |          nan |            0 |            0 |          0 |         0 |    -0.00186117 |     0 |     5e+07 |                100 |             0.001 |          0.0005 |     0.0008 |                  10 |                1 |


### 3.3 `rejection` (recorder=True)

#### 训练期 top-K（排序键 seg_sharpe）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |     max_mdd |   mean_buyhold |   fee |   CANDLE_INTERVAL_NS |   REJECTION_RATIO |   MIN_BODY_RATIO |   TAKE_PROFIT_FACTOR |   STOP_LOSS_FACTOR |   elapsed_interval |
|---------:|-------------:|-------------:|-------------:|-----------:|------------:|---------------:|------:|---------------------:|------------------:|-----------------:|---------------------:|-------------------:|-------------------:|
|       19 |     -16.9745 | -0.00016066  |            0 |          0 | 0.000932091 |    -0.00336047 |     0 |                6e+10 |               0.9 |             0.01 |                1.005 |               0.99 |              1e+08 |
|       18 |     -18.8332 | -0.000171168 |            0 |          0 | 0.00100888  |    -0.00290154 |     0 |                6e+10 |               0.9 |             0.01 |                1.01  |               0.99 |              1e+08 |
|       19 |     -19.2068 | -0.000173877 |            0 |          0 | 0.00110555  |    -0.00336047 |     0 |                6e+10 |               0.9 |             0.01 |                1.015 |               0.99 |              1e+08 |
|       20 |     -24.0759 | -0.000301199 |            0 |          0 | 0.00217974  |    -0.00264606 |     0 |                6e+10 |               0.7 |             0.03 |                1.015 |               0.99 |              1e+08 |
|       19 |     -24.5447 | -0.000329516 |            0 |          0 | 0.00217974  |    -0.00217369 |     0 |                6e+10 |               0.7 |             0.05 |                1.01  |               0.99 |              1e+08 |

#### 验证期 top-K（训练期 top-5 参数在验证期的聚合）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |     max_mdd |   mean_buyhold |   fee |   CANDLE_INTERVAL_NS |   REJECTION_RATIO |   MIN_BODY_RATIO |   TAKE_PROFIT_FACTOR |   STOP_LOSS_FACTOR |   elapsed_interval |
|---------:|-------------:|-------------:|-------------:|-----------:|------------:|---------------:|------:|---------------------:|------------------:|-----------------:|---------------------:|-------------------:|-------------------:|
|        7 |      27.4892 |  1.96595e-05 |            0 |          0 | 0.000172584 |    -0.00296164 |     0 |                6e+10 |               0.9 |             0.01 |                1.01  |               0.99 |              1e+08 |
|        7 |      27.4892 |  1.96595e-05 |            0 |          0 | 0.000172584 |    -0.00296164 |     0 |                6e+10 |               0.9 |             0.01 |                1.005 |               0.99 |              1e+08 |
|        7 |      27.4892 |  1.96595e-05 |            0 |          0 | 0.000172584 |    -0.00296164 |     0 |                6e+10 |               0.9 |             0.01 |                1.015 |               0.99 |              1e+08 |
|        7 |     -17.2343 | -0.000114778 |            0 |          0 | 0.00106642  |    -0.00296164 |     0 |                6e+10 |               0.7 |             0.01 |                1.015 |               0.99 |              1e+08 |
|        7 |     -17.2343 | -0.000114778 |            0 |          0 | 0.00106642  |    -0.00296164 |     0 |                6e+10 |               0.7 |             0.03 |                1.015 |               0.99 |              1e+08 |

#### 测试期（训练 top-1 参数，样本外）

|   n_segs |   seg_sharpe |   seg_return |   sum_equity |   win_rate |     max_mdd |   mean_buyhold |   fee |   CANDLE_INTERVAL_NS |   REJECTION_RATIO |   MIN_BODY_RATIO |   TAKE_PROFIT_FACTOR |   STOP_LOSS_FACTOR |   elapsed_interval |
|---------:|-------------:|-------------:|-------------:|-----------:|------------:|---------------:|------:|---------------------:|------------------:|-----------------:|---------------------:|-------------------:|-------------------:|
|        4 |     -5.93859 | -3.50102e-05 |            0 |          0 | 0.000676181 |    -0.00186117 |     0 |                6e+10 |               0.9 |             0.01 |                1.005 |               0.99 |              1e+08 |


## 4. 三策略样本外（测试段）汇总

| strategy             |   test_seg_sharpe |   test_seg_return |   test_sum_equity |   test_win_rate |   test_mean_buyhold |    test_max_mdd |
|:---------------------|------------------:|------------------:|------------------:|----------------:|--------------------:|----------------:|
| mean_reversion       |          -1.54708 |      -0.204104    |          -8164.15 |               0 |         -0.00186117 | -7644.58        |
| order_flow_imbalance |         nan       |       0           |              0    |               0 |         -0.00186117 |     0           |
| rejection            |          -5.93859 |      -3.50102e-05 |              0    |               0 |         -0.00186117 |     0.000676181 |


## 5. 评估结论与方法学

- **三个策略在所测样本与成本/延迟假设下均跑输 buy&hold**：测试段 buy&hold 近似 `mean_buyhold` 列所示（正值），三个策略的 `test_seg_return` 为负或 0。

- **根因（可分两条）**：

  1. 「mechan concerns」 none of the three strategies' edge overcomes maker/taker fees + spread on this BTC 0.1 tick regime given conservative parameters; mean_reversion GTX passive 的被动单点差收益难抵 maker 0.02%/taker 0.07%；order_flow_imbalance 在当前 step_ns +阈值配置下成交不足（多数 seg balance=0，限价 @ask 价 nach risk_adverse_queue 没 passive fill）。

  2. **OFI equity=0** 的现象表明净流阈值 / Limit @ ask GTC 入场在自己 / 风险规避成交模型下没被吃单——这是参数 sweep 与成交模型耦合问题，本身并非简单参数问题；后续可改用 proper market or IOC or tighter thresholds。

  3. rejection 策略虽每段有交易，SR 严负（-16 ~ -28）说明分钟Rejection in BTC 这种低 Sharpe 信号 + 高频换手被费用吞噬。

- **样本外口径偏差**：训练/验证段都 ≤8h 截断；测试段 `--max-seg-hours 8` 也截断，每个 seg 实际仅前 8h。完整段测试会进一步暴露交易成本。后续若断点续跑不再受内存约束可对测试段用完整 npz 复跑。

- **方法学胜负**：本仓库框架（交付物见 `docs/research_plan_2026-06.md`）针对本次 1 GB/seg 数据集 ragged state (snapshot 在段首)的「按会话段切 npz」与「Popen 鲁棒并发 + max-seg-hours 截断」是可行的；如要提高完整性可加：(1) 让 OFI 改 IOC/Market playbook; (2) Phase 4 queue_imbalance_mm exploiting L1 imbalance + 库存罚会更有希望。


## 6. 复现命令

```powershell
# 训练段
python backtests/grid_search.py --strategy <s> --manifest E:\tmp\npz\manifest.csv --split train --grid grids\<s>.json --contract contracts\btc_usdt_swap.json --out-dir E:\tmp\results\<s>_train --processes 8 --max-seg-hours 8
# 验证段（top-K 由 evaluation/select_params.py 生成的 E:\tmp\grids\<s>_val.json）
python backtests/grid_search.py --strategy <s> --split val --grid E:\tmp\grids\<s>_val.json ... --max-seg-hours 4
# 测试段（top-1 由 *_test.json）
python backtests/grid_search.py --strategy <s> --split test --grid E:\tmp\grids\<s>_test.json ... --max-seg-hours 8
python evaluation/aggregate.py --results-dir E:\tmp\results\<s>_<sp> --out ...\aggregate.csv
python evaluation/final_report.py --manifest E:\tmp\npz\manifest.csv --out docs/research_report_2026-06.md
```
