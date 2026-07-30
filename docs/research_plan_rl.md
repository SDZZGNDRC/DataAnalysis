# 强化学习量化交易策略研究计划（Phase RL0–RL5）

> 状态：进行中（2026-07-28 启动）
> 衔接：基于 Phase 0–5（`docs/research_plan_2026-06.md`）已建成的 npz / grid_search / evaluation 框架
> 数据：使用 `E:\tmp\npz_v2\` 的 49 个 exact-segment-v2 会话段（manifest：train 38 / val 7 / test 4）
> 中间产物：`E:\tmp\rl\`（ckpt / tb / smoke / results）
>
> **v2 修订（2026-07-30）**：旧 NPZ、旧 PPO checkpoint 和旧回测结果已判定
> 不兼容。新数据必须带 `exact-segment-v2` metadata；新模型必须带
> `rl-policy-v2` schema，输出放在 `E:\tmp\rl\v2\`，禁止复用 `ckpt2`。

## 1. 设计概要

在 Phase 5 框架基础上，新增第 5 个策略：**RL 策略**——一个由 PPO 训练的小 MLP（多层全连接网络）作为决策器，在 hftbacktest 模拟器内做离散动作交易。

| 维度 | 设定 |
|---|---|
| 网络结构 | SB3 `MlpPolicy` 默认 [64,64]；CPU 训练 |
| 动作空间 | **5 离散**：`0=强买(+2 lots), 1=买(+1), 2=hold, 3=卖(-1), 4=强卖(-2)` |
| 奖励 | `Δequity - churn_penalty×实际成交量`；终局计入 bid/ask 平仓点差和 taker fee |
| 观测向量 | ~25 维标准化特征（见下） |
| 决策频率 | step_ns = 500ms（2Hz；既够稀疏提速训练，又能用 L1 短反应） |
| 回合 | 1 个训练 seg = 1 episode，可向前 4000–8000 步大窗口 |
| 训练 | sb3 PPO + SubprocVecEnv ×4；总 ~5M 时间步（1–3 小时 CPU） |
| 评估 | 每次 checkpoint 固定轮播全部 val segments；测试以跨段收益 t 统计量、总收益、费用和胜率报告 |

### 观测向量（每步归一化，~25 维）

- 长短期 mid 收益（1m / 5m / 15m 标准化，差分规避价格尺度）
- L1 imbalance = `(bb_qty - ba_qty) / (bb_qty + ba_qty)`
- L1–L5 bid/ask 逐档累计 qty（按 10×下单量归一）
- 当前 position / max_pos、未实现 PnL 累计（已实现 + 持仓按市价折算 - 费）
- 距真实上次成交的时间（1 分钟截断归一）
- 1m/5m 的波动率与 high-low；历史窗口真实覆盖 15 分钟

### 动作→订单映射（成本现实口径）

| action | 含义 | 订单 |
|---|---|---|
| 0 | 强买 (+2 lots) | 唯一订单 ID 的 2-lot GTX 限价单 @ best_bid |
| 1 | 买 (+1 lot) | 限价 @ best_bid 单笔 |
| 2 | hold | 撤销活动订单，不增加新的成交意图 |
| 3 | 卖 (-1 lot) | 限价 @ best_ask 单笔 |
| 4 | 强卖 (-2 lots) | 唯一订单 ID 的 2-lot GTX 限价单 @ best_ask |

人物画像：让代理在成本约束下学会"无信号就 hold"，避免 Phase 2–4 出现的 churn 主导成本吞噬。

## 2. 执行阶段

### Phase RL0 — 依赖与基线（~0.5 天）

- 安装 `torch` CPU 版：`pip install torch --index-url https://download.pytorch.org/whl/cpu -q`；验证 `import torch, stable_baselines3, gymnasium` 通过
- 写 minimal `rl/hbt_smoke.py`：在 seg64 跑 100 步纯 Python hbt 循环（无 @njit），每步 `hbt.elapse(500ms) → depth → submit buy/sell`，验证 `elapse / depth / state_values / submit_buy_order / clear_inactive_orders` 非 njit 可用、累计权益变化合理
- 产出：`E:\tmp\rl\smoke.log` 含样例 step / PnL
- **验收**：torch ok；hbt 纯 Python loop 无 numba 兼容报错并产生非零 equity 变化

### Phase RL1 — 训练环境 Gym Env（~1.5 天）

- 新增 `rl/gym_env.py` 定义 `BTCUSDSwapMapsEnv(gymnasium.Env)`：
  - `__init__(seg_npz_paths, contract, max_position_lots=10, order_qty_lots=1, step_ns=500_000_000, lookback_window=...)` 加载 npz 缓存与合约参数
  - `reset(seed)`：随机选 seg；重建 `BacktestAsset` + `HashMapMarketDepthBacktest`；初始化滑窗 obs（前若干 step 仅 elapse 看行情、hold）
  - `step(action)`：`hbt.elapse(step_ns)` → 清非活动单 → action→订单映射 → `clear_inactive_orders` → 计算新 equity → reward=Δequity；done = hbt.exhausted
  - 观测 `Box(-inf,+inf,~25)`、`Discrete(5)`、额外 `info={equity, position, fee, n_trades}`
- obs 标准化：用运行 centered 滑动窗口（mid 收益、imbalance 自然在 [-1,1]）
- **验收**：在 seg3 / seg9 跑 random 策略 5000 步 = cumulative reward 在合理负区间且无 NaN / Crash；与"全 hold" reward ≈ -fees × trade churn

### Phase RL2 — 训练配置（~1 天）

- 新增 `rl/train.py`：
  - CLI `--train-manifest` `--val-manifest` `--total-timesteps` `--out`
  - `SubprocVecEnv([make_env(seg_list) for _ in range(4)])`
  - SB3 `PPO(MlpPolicy, vec_env, n_steps=4096, batch_size=256, n_epochs=10, learning_rate=3e-4, ent_coef=0.01, gamma=0.99, gae_lambda=0.95, verbose=1, tensorboard_log=E:\tmp\rl\tb)`
  - `EvalCallback` 默认每 500k 聚合训练步轮播全部 val segments；每段使用独立的 1h 评估窗口，避免验证成本反超训练本身；频率与窗口可由 `--eval-freq-timesteps`、`--eval-max-seg-hours` 调整
  - checkpoint 持久化与 resume（通过传 `reset_num_timesteps=False`）
- **验收**：训练能在 5 分钟内前进 ≥ 1 update 周期；tensorboard log 生成；EvalCallback 写出第一个 ckpt

### Phase RL3 — 训练运行（~2–4 小时）

- 命令：`python rl/train.py --manifest E:\tmp\npz_v2\manifest.csv --train-split train --val-split val --total-timesteps 5000000 --eval-freq-timesteps 500000 --eval-max-seg-hours 1 --out-dir E:\tmp\rl\v2\ckpt`
- 中途监控：log 与 `EvalCallback` 的最佳 reward 与 train episode reward
- 触发停训条件（手动）：reward 多次负值或不动、过度 churn（num_trades 远超其它基线 5× 倍数）→ 调 `ent_coef`、reward shaping、增大 `step_ns`
- **验收**：训练完成或达 5M 步；`best_reward.zip` 存在；val episode reward 显著优于 random / held 基线（非零 alpha 的弱证据）；若学不到 alpha，至少 churn 不失控

### Phase RL4 — 策略集成与样本外测试（~1 天）

- 新增 `strategies/rl_policy_strategy.py`：非 @njit 函数 `rl_policy_strategy(hbt, recorder, model_path, step_ns=500_000_000, ...)`：
  - 加载 best_reward.zip（torch 模型）
  - 逐 step 推理由 obs → action 映射到与 env 相同的订单逻辑；同时 `recorder.record(hbt)` 让 LinearAssetRecord 出 SR/Sortino/MDD
- 在 `backtests/strategy_registry.py` 注册 `rl_policy`（uses_recorder=True、params_as_object=False，仅 `model_path` 一参）
- 新增空 grid `grids/rl_policy.json`：`{"model_path": ["E:/tmp/rl/ckpt/best_reward.zip"]}`
- 跑 `test_split` 只 1 次：
  ```powershell
  python backtests/grid_search.py --strategy rl_policy --manifest E:\tmp\npz_v2\manifest.csv ^
      --split test --grid grids\rl_policy.json --contract contracts\btc_usdt_swap.json ^
      --out-dir E:\tmp\results_v2\rl_policy_test --processes 1 --skip-existing --max-seg-hours 0
  ```
- aggregate test 结果
- **验收**：test 4 seg 全部产出；与之前 4 策略口径直接可比

### Phase RL5 — 报告（~0.5 天）

- 修改 `evaluation/final_report.py` 加入第 5 策略对比项
- 摘录训练曲线截图指针 + tensorboard reward 标志
- 在结论第一节体现 RL 策略对照 cost-realistic baseline 的劣 / 优，给最终 ADR
- **验收**：报告完成且完整呈现 5 策略样本外汇总

## 3. 交付物

| 类型 | 路径 |
|---|---|
| Gym Env | `rl/gym_env.py` |
| 训练脚本 | `rl/train.py` |
| 策略 | `strategies/rl_policy_strategy.py`（非 njit） |
| 策略注册 | 修改 `backtests/strategy_registry.py` |
| Grid | `grids/rl_policy.json` |
| 报告更新 | `evaluation/final_report.py` + `docs/research_report_2026-06.md` |
| RL 输出（E:\tmp） | `E:\tmp\rl\smoke.log`、`ckpt\best_reward.zip`、`tb\*`、`results\rl_policy_test\` |

## 4. 风险与缓解

| 风险 | 缓解 |
|---|---|
| torch 装包失败 / 网络问题 | 用 CPU 索引 `whl/cpu`；保留 CPU 即可，4GB GPU 即便装也不稳 |
| hbt 纯 Python 慢 → 训练数日 | step_ns=500ms 决策稀疏；SubprocVecEnv ×4 并行；n_steps=4096 一轮实验在 4–12 分钟即可迭代 |
| agent 学到 churn 模式（Phase 2–4 的 cost 主导） | reward 直接是净值差（cost 已扣）；增加 entropy 衰减；reward shaping 成 `Δequity - λ*|Δpos|*fee` 如必要 |
| 训练不收敛（常见 PPO 不稳定） | MlpPolicy 小网格；多随机种子并行 + 早停；CKP-based early-stop on val reward |
| 5M 步可能不够 | 加快 smoke：先 500k 跑通；若 val reward 上升才追加大轮；4 seg test standalone 为快 |
| E:\ 空间跟踪 ckpt | ckpt ~5MB；log / tb 控制在 <1GB |

**总工期估算**：约 5–7 天（含 3–4 小时训练）。
