# 数据分析工具库 (DataAnalysis)

面向 **OKX 加密货币高频行情数据**（订单簿 Books / 成交 Trades）的端到端工具库：从原始 7z/JSON 采集数据出发，经清洗、parquet 化、指标计算，最终送入 [hftbacktest](https://github.com/nkaz001/hftbacktest) 框架做高频回测与参数网格搜索。

## 环境配置

```bash
# 创建 conda 环境（环境名：CryptoSpider）
conda env create -f environment.yml
conda activate CryptoSpider

# 拉取数据 Schema 子模块
git submodule update --init
```

> **注意**：`indicators/` 与 `dataset_factory.py` 依赖内部包 `pybacktest`（`Book`/`BookCore`/`SimTime`）；`gen_BLCSI.py`、`gen_NthBL.py` 依赖 C 加速的 `cbookcore`。这两个包不在 `environment.yml` 中，需单独安装。

## 数据处理流水线

```
[原始数据]  OKX-<类目>-<instId>-<start>-<end>.json(.7z)
    │
    ▼  清洗: crypto_data_processor.py   (修复 elapsedTime / 按 ts 排序 / 重命名)
    ▼  解压: unzip.py
[parquet 化] crypto_data_aggregator.py / trades_aggregator.py   (多进程聚合, agg_utils 规范化)
    │        aggregate_parquet.py + dataset_factory.py          (切分为 part-* 分区, chunk 首行 = snapshot)
    │        resort_parquet.py / rechunk_parquet.py / tail_books.py  (重排序 / 重分块 / 去尾部重叠)
    ▼
[指标层]    gen_BLCSI.py / gen_NthBL.py        (parquet → 指标 parquet, 多进程)
    │       indicators/ (AAP / ABP / TA / TV / BL / BLCSI) → (可选) InfluxDB
    │       trades_books_asop.py               (Trades ⋈ N 档盘口 as-of 合并)
    ▼
[回测数据]  hftbacktest_okx.py / hftbacktest_okx_mmap.py   (7z/json → npz 事件流)
    ▼
[回测层]    strategies/ + backtests/  (均值回归 / 订单流不平衡 / 做市 / Rejection)
    │       run.py + worker.py        (共享内存多进程参数网格搜索)
    ▼
[分析层]    hft_ohlc.py (K线) / hft_book_change.py (盘口更新频率) / 各类质检脚本
```

### 1. 原始数据清洗与 parquet 化

```powershell
# 按月归档 + 打包（参见「原数据处理.md」）
python ./recategory.py "E:\datapool\2025-03-.*" E:\tmp\2025-03
python ./grayscale_pack.py --workers 6 E:\tmp\2025-03\

# 解压 7z
python ./unzip.py E:\tmp\Books\OKX-Books-BTC-USDT-400\ --overwrite

# 清洗 JSON：修复 elapsedTime、按 ts 排序、重命名（可选回写 MySQL）
python crypto_data_processor.py --dir <json_dir> --batch-size 100
python crypto_data_processor.py --dir <json_dir> --db-user root --db-password *** --db-name crypto

# JSON 聚合为 parquet 数据集（按 DataName 前缀分类、多进程）
python crypto_data_aggregator.py --dir <json_dir> --output <out_dir>

# 处理时间戳 overlap、books 数据收尾
python ./resort_parquet.py --dir <parquet_dir>
python ./tail_books.py --dir <parquet_dir>
```

### 2. 指标生成

```powershell
# 生成 BLCSI（订单簿档位变化数）指标 parquet
python ./gen_BLCSI.py --dir <books_parquet_dir> --output <out_dir>

# 生成前 N 档盘口快照特征 (price/amount/count × N)
python ./gen_NthBL.py --dir <books_parquet_dir> --output <out_dir> --N 20

# Trades 与 N 档盘口特征按时间戳 as-of 合并
python trades_books_asop.py --trades-dir <trades_dir> --nth-bl-dir <bl20_dir> --output <out_dir>
```

### 3. 生成 hftbacktest 回测数据（npz）

```powershell
# 仅订单簿数据（固定延迟）
python hftbacktest_okx.py -o books.npz --feed-latency 200000000 --base-latency 100000000 <books_dir>

# 订单簿 + 成交数据，localTs = exchTs + 对数正态随机延迟
# 注意：`--latency-mu` 是对数正态分布的 mu，不是均值！
python hftbacktest_okx.py -o combined.npz --feed-latency 200000000 --base-latency 100000000 \
    --latency-mu -9.71 --latency-sigma 1.0 --use-random-latency <trades_dir> <books_dir>

# 大数据量推荐 mmap 版（中间结果落盘，内存峰值低）
python hftbacktest_okx_mmap.py <trades_dir> <books_dir> -o combined.npz --use-random-latency --tmp-dir D:\tmp
```

> localTs 合成算法的详细设计见 [深度数据与交易数据的合并方案.md](深度数据与交易数据的合并方案.md) 与 [doc/hftbacktest_okx.md](doc/hftbacktest_okx.md)。

### 4. 策略回测

```powershell
# EMA 均值回归（numba JIT）
python backtests/run_mean_reversion_demo.py --data OKX-HFT-BTC-USDT-2025-03-02.npz

# 订单流不平衡（止盈/止损 + 手续费）
python backtests/run_order_flow_imbalance.py --data OKX-HFT-BTC-USDT-2025-03-02.npz

# 做市 demo（imbalance 调整 fair price + 库存惩罚，需先改脚本内 npz 路径）
python okx_mm_demo.py

# Rejection 策略参数网格搜索（多进程 + 共享内存，路径与网格需手工编辑）
python run.py   # 结果写入 backtest_results/
```

RL v2 必须从精确切段数据重新训练，旧 checkpoint 会被 schema 校验拒绝：

```powershell
# 重新生成 exact-segment-v2 NPZ（不要再传未实现的 --max-depth-levels）
python scripts/convert_segments_npz.py --csv E:\tmp\segments.csv `
  --pool-root E:\datapool --out-dir E:\tmp\npz_v2 --processes 10 --min-duration 1

# 生成只接受 exact-segment-v2 metadata 的 manifest
python scripts/make_manifest.py --segments E:\tmp\segments.csv `
  --npz-dir E:\tmp\npz_v2 --out E:\tmp\npz_v2\manifest.csv

# 训练 rl-policy-v2
python rl/train.py --manifest E:\tmp\npz_v2\manifest.csv `
  --train-split train --val-split val --total-timesteps 5000000 `
  --eval-freq-timesteps 500000 --eval-max-seg-hours 1 `
  --out-dir E:\tmp\rl\v2\ckpt --tensorboard-log E:\tmp\rl\v2\tb

# 固定模型只运行一次测试参数组合
python backtests/grid_search.py --strategy rl_policy `
  --manifest E:\tmp\npz_v2\manifest.csv --split test --grid grids\rl_policy.json `
  --contract contracts\btc_usdt_swap.json --out-dir E:\tmp\results_v2\rl_policy_test `
  --processes 1 --max-seg-hours 0

python evaluation/aggregate.py --results-dir E:\tmp\results_v2\rl_policy_test `
  --out E:\tmp\results_v2\rl_policy_test\aggregate.csv
python evaluation/final_report.py --manifest E:\tmp\npz_v2\manifest.csv `
  --results-root E:\tmp\results_v2 --out docs\research_report_2026-06-v2.md
```

### 5. 衍生分析

```powershell
# 从 npz 重放生成 OHLCV K 线
python hft_ohlc.py "OKX-HFT-BTC-USDT-2025-03-*.npz" -o "OKX-HFT-BTC-USDT-2025-03.csv" -i 1m

# 统计订单簿档位更新频率
python hft_book_change.py combined.npz -o changes.csv -i 1s
```

## 项目结构

| 路径 | 说明 |
|---|---|
| `crypto_data_aggregator.py` / `trades_aggregator.py` | JSON → parquet 多进程聚合器（CLI） |
| `crypto_data_processor.py` / `crypto_data_processor/` | 原始 JSON 清洗（Python 版与 C++ 重实现） |
| `agg_utils/` | 数据类目规范化映射（`Books` / `pattern1-3`，被聚合器调用） |
| `DataFile/` | 文件名（DataName）解析与校验 |
| `DataSchema/` | **git 子模块**：各数据类目的 JSON Schema / parquet schema |
| `dataset_factory.py` | 连续 Books 流按 chunk 切分为 `part-*` 分区（保证 chunk 首行为 snapshot） |
| `indicators/` | 盘口指标库：AAP（均价）、ABP（量加权价）、TA（总量）、TV（总额）、BL（档位）、BLCSI（档位变化数）、`common/`（平滑/差值/对数收益等算子） |
| `gen_BLCSI.py` / `gen_NthBL.py` | parquet → 指标 parquet（cbookcore，多进程） |
| `hftbacktest_okx.py` / `hftbacktest_okx_mmap.py` | OKX 数据 → hftbacktest npz 事件流转换器（内存版 / memmap 版） |
| `hft_ohlc.py` / `hft_book_change.py` | npz 重放衍生分析：K 线聚合 / 盘口更新频率统计 |
| `strategies/` | 回测策略：均值回归、订单流不平衡 |
| `backtests/` | 策略 CLI runner |
| `backtest_rejection_strategy.py` / `worker.py` / `run.py` | Rejection 策略及多进程参数网格搜索 |
| `ray/` | Ray 分布式参数网格搜索示例；`cos_node_cache_actor.py`（COS 节点缓存 Actor） |
| `models/` | 实验性 PyTorch 模型 |
| `hftbacktest/` | hftbacktest 框架源码（vendored，供参考） |
| `test/` | 合成盘口测试数据（CONST / GAUSSIAN / SINE 等波形，由 `random_books_generator.py` 生成） |
| `doc/` | 设计文档 |

## 数据质量检查脚本

| 脚本 | 功能 |
|---|---|
| `check_seqId.py` | 检查 `seqId`/`prevSeqId` 连续性，识别断链与连续段（支持 7z 原始数据与 parquet） |
| `check_timestamp_gaps.py` | 检查时间戳空洞 |
| `find_snapshot_files.py` | 查找包含 snapshot 的数据文件（多进程） |
| `reconstruct_segments.py` | 基于 seqId 链重建连续段（位图记录覆盖文件） |
| `merge_segments.py` | 按最大时间间隔合并连续段 |
| `concat_books_7z.py` | 沿 seqId 链从多个 7z 拼接 Books 数据 |
| `find_trades_by_timestamp.py` | 按时间范围检索 Trades 文件并可合并 |
| `analyze_trades.py` / `analyze_npz_gaps.py` / `analyze_segments.py` | 各类统计与空洞分析 |
| `overlapped_analysis.py` | 时间戳重叠分析 |
| `validate_json.py` / `verify_checksum.py` | JSON 校验 / 校验和验证 |

## InfluxDB 数据模式

`indicators/` 中的指标可导入 InfluxDB，各 measurement 结构一致：

| Measurement | Tag key | Field key |
|---|---|---|
| BLCSI | exchange, instId, instType, level, side | val |
| ABP | exchange, instId, instType, level, side | val |
| AAP | exchange, instId, instType, level, side | val |
| TA | exchange, instId, instType, level, side | val |
| TV | exchange, instId, instType, level, side | val |

> ⚠️ 部分脚本的 `__main__` 中硬编码了 InfluxDB token / COS bucket 等敏感信息，部署时请改为环境变量配置。

## 旧流程（Legacy）

早期流程（仍可用，但已被 `crypto_data_aggregator.py` 取代）：

> 压缩的原始数据 (7z) → `unzip.py` → 原始 JSON → `j2p.py` → parquet → `aggregate_parquet.py` → 聚合数据集（用 `test_aggregate_parquet.py` 校验）

- `j2p.py`：将原始 JSON 数据转换为 parquet 格式
- `aggregate_parquet.py`：将多个小 parquet 文件聚合成较少但更大的 parquet 文件
- `random_books_generator.py`：生成随机的订单簿数据集（正弦/方波/高斯等波形，见 `test/`）

## 相关文档

- [原数据处理.md](原数据处理.md)：按月归档、打包、聚合、质检的完整命令手册
- [深度数据与交易数据的合并方案.md](深度数据与交易数据的合并方案.md)：npz 事件流 localTs 合成算法
- [doc/hftbacktest_okx.md](doc/hftbacktest_okx.md)：npz 转换器设计文档
- [note.md](note.md)：已知问题（原始数据 ts 重叠、乱序等）
