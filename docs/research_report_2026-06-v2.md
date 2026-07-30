# BTC-USDT-SWAP 策略研究报告

本报告完全从本次 aggregate CSV 动态生成；不嵌入历史运行的固定数字。

## 数据与统计口径

- manifest：`E:\tmp\npz_v2\manifest.csv`
- 报告资金：100,000 USDT（若结果包含 `report_notional`，优先使用结果值）
- 主评分为跨 segment 收益 t 统计量；段内年化 SR 仅作诊断，不再求平均后称作组合 Sharpe。
- terminal equity 已计入按 bid/ask 平仓的点差和 taker fee。

### Manifest

| split   |   segments |   manifest_hours |
|:--------|-----------:|-----------------:|
| train   |         38 |         475.712  |
| val     |          7 |         101.309  |
| test    |          4 |          78.3859 |

## `mean_reversion`

### train

_结果缺失_

### val

_结果缺失_

### test

_结果缺失_


## `order_flow_imbalance`

### train

_结果缺失_

### val

_结果缺失_

### test

_结果缺失_


## `rejection`

### train

_结果缺失_

### val

_结果缺失_

### test

_结果缺失_


## `queue_imbalance_mm`

### train

_结果缺失_

### val

_结果缺失_

### test

_结果缺失_


## `rl_policy`

### train

_结果缺失_

### val

_结果缺失_

### test

|   n_segs |   return_t_stat |   total_return |   sum_equity |   win_rate |   max_drawdown |   fees |   liquidation_cost |   num_trades |   hours |   mean_buyhold | model_path                       |   step_ns |   max_position_lots |   order_qty_lots |
|---------:|----------------:|---------------:|-------------:|-----------:|---------------:|-------:|-------------------:|-------------:|--------:|---------------:|:---------------------------------|----------:|--------------------:|-----------------:|
|        4 |             nan |              0 |            0 |          0 |              0 |      0 |                  0 |            0 |  78.386 |    -0.00747692 | E:/tmp/rl/v2/ckpt/best_model.zip | 500000000 |                  10 |                1 |


## 样本外汇总

| strategy   |   n_segs |   return_t_stat |   total_return |   sum_equity |   win_rate |   max_drawdown |   fees |   liquidation_cost |   num_trades |
|:-----------|---------:|----------------:|---------------:|-------------:|-----------:|---------------:|-------:|-------------------:|-------------:|
| rl_policy  |        4 |             nan |              0 |            0 |          0 |              0 |      0 |                  0 |            0 |

## 自动结论

- `rl_policy`：持平 0.00 USDT，总收益 0.0000%，胜率 0.0%，跨段收益 t=nan，成交 0 笔。
- `rl_policy` 在全部测试段零成交；这是退化的空仓策略，不能将零收益解释为有效 alpha。
- t 统计量和胜率必须结合 segment 数量解释；少量测试段不能单独证明 alpha。

## 完整性警告

- 未检测到旧版零权益记录或测试集参数挑选。
