# RL v3 第一阶段研究记录

日期：2026-07-30

## 结论

第一阶段完成了退化诊断、成本感知 alpha probe 和 `rl-policy-v3`
基础设施改造。基础设施 smoke 已通过，但当前信号和 2,500 步 smoke
模型均没有产生正 alpha，不能进入最终测试。

现有 6 月 `test` 已在 v2 审查中被查看，后续只能作为开发诊断数据。
v3 的最终结论必须来自新的、未查看过的连续 holdout。

## v2 退化诊断

在旧 validation segment 65 的一小时窗口中：

- 决策数：5,399
- `neutral` 动作：5,399（100%）
- 提交订单：0
- 成交：0

因此 v2 的零成交不是排队模型过严造成，而是 PPO 确定性策略已经完全
退化为 neutral。

## 成本感知 alpha probe

探测器只在 `train` 拟合固定 ridge 系数和 10%/90% 信号阈值，再在
`val` 评估。没有读取 `test`。本轮为资源受限初筛：

- fit segments：3、13、35、44、49、56
- eval segments：64、65、68
- 每段最多使用首 1 小时，15 分钟 warmup
- maker 往返费用：4bp
- 结果是重叠 forward-return 信号诊断，不是可执行 PnL 或 Sharpe

| horizon | IC | gross edge | net edge | 费后为正的验证段 |
|---:|---:|---:|---:|---:|
| 10s | 0.1293 | 0.6996bp | -3.3005bp | 0/3 |
| 30s | 0.0615 | 0.6894bp | -3.3106bp | 0/3 |
| 120s | -0.0928 | -1.6226bp | -5.6226bp | 0/3 |
| 300s | -0.1128 | -2.1512bp | -6.1512bp | 1/3 |
| 900s | -0.3332 | -10.6036bp | -14.6036bp | 1/3 |

短周期存在少量方向信息，但不足以覆盖费用；慢周期的 train-to-val
方向稳定性更差。当前结果不支持直接开展大规模 PPO 参数搜索。

使用 128 个固定 `tanh` 随机特征、ridge=0.01 的非线性 probe 后：

| horizon | IC | gross edge | net edge | 费后为正的验证段 |
|---:|---:|---:|---:|---:|
| 10s | 0.1145 | 0.5954bp | -3.4046bp | 0/3 |
| 30s | 0.0129 | 0.2700bp | -3.7300bp | 0/3 |
| 120s | -0.1262 | -1.3839bp | -5.3839bp | 0/3 |

非线性模型没有改善结果，因此当前主要瓶颈不是线性模型容量，而是
现有特征的可交易 edge 与费用之间差距过大。

加入 1/10/30 秒收益和逐步/10 秒/60 秒盘口 OFI 后，线性 probe 有所改善：

| horizon | IC | gross edge | net edge | 费后为正的验证段 |
|---:|---:|---:|---:|---:|
| 10s | 0.1561 | 0.8403bp | -3.1597bp | 0/3 |
| 30s | 0.1000 | 0.9479bp | -3.0521bp | 0/3 |
| 120s | -0.0818 | -0.8664bp | -4.8664bp | 0/3 |

把信号尾部从每侧 10% 收紧到每侧 1% 后，30 秒毛 edge 提高到
1.8345bp，但费后仍为 -2.1655bp，三个验证段全部为负。继续收紧会让
训练样本降到几十个，停止继续挖掘阈值。

## `rl-policy-v3` 改造

- 动作改成目标仓位：`+100%/+50%/0/-50%/-100%`。
- 默认决策周期从 500ms 改为 2s。
- 子订单每次最多 1 lot，同时最多保留一笔活动订单。
- 同向订单至少存活 5s；15s 过期，偏离 best 至少 1 tick 后才允许重挂。
- 信号反转或目标仓位已达到时可以立即撤掉方向错误的订单。
- observation 从 25 维扩展到 43 维，新增 pending order、订单年龄、
  target position、spread、microprice，以及 1/10/60 秒主动成交不平衡
  和对数成交强度、盘口 OFI、1/10/30 秒收益。
- 模型 schema 固定为 `rl-policy-v3-ob43`，旧维度 checkpoint 会被拒绝。
- 训练和独立回测继续共用同一个 `RLPolicyCore`。
- validation 加入只影响 checkpoint 资格、不进入训练 reward 的成交闸门；
  零成交 episode 会被淘汰。
- backtest 输出新增动作分布、订单提交/撤销原因、订单寿命和成交等待时间。
- 非有限 JSON 数值写为标准 JSON `null`，不再输出非法 `NaN`。

## 集成 smoke

真实 segment 上的环境 smoke：

- observation shape：43，全部为有限值
- 40 个决策产生 7 次成交
- 平均撤单前订单年龄：约 6.29s
- 最大同时活动订单：1

最终 schema 的 1,024 步 PPO smoke 完整执行了两次评估，并保存
`best_model.zip`、checkpoint 和 `ppo_final.zip`。模型加载结果：

- observation space：43
- schema：`rl-policy-v3-ob43`
- 第二次 validation reward：-0.4873116

独立 worker 对同一 segment 64 窗口得到净权益 -0.4873116 USDT，与
validation reward 精确一致；成交 4 次、最终仓位为 0、fee 为
0.4993116 USDT。动作分布为 20% 半多、50% neutral、30% 最大空。

这证明最终 43 维 schema 下训练、评估、推理、费用和订单状态机一致。
短 smoke 模型仍然亏损，不是策略候选。

## 下一闸门

1. 扩大 alpha probe 的开发覆盖，并增加跨 segment 的 walk-forward
   稳定性统计；阈值仍只能在 train 确定。
2. 若任何 horizon 不能在多数 walk-forward fold 上产生稳定费后 edge，
   停止方向型 PPO，转向持仓周期更长的特征/标签或重新定义做市问题。
3. 通过信号闸门后，运行多 seed、walk-forward 的分阶段 PPO 搜索。
4. 冻结代码、模型和配置后，申请新的至少 30 天连续 holdout，一次性报告
   净 Sharpe、收益、成交覆盖、利润集中度和 block-bootstrap 区间。
