"""策略注册表：为 grid_search 提供统一接入。

每个策略以 StrategySpec 注册：
  - func: @njit 策略主循环函数（签名形如 func(hbt, [recorder,] <params...>)）
  - param_keys: 参数名顺序，与网格 dict 一致
  - uses_recorder: 是否接收 recorder（recorder-based 策略输出 SR/Sharpe 等）
  - is_success: 判定退出码是否正常结束（None 表示仅看 recorder）
"""
from collections import namedtuple
from strategies.mean_reversion_demo import mean_reversion_strategy, is_success as mr_is_success
from strategies.order_flow_imbalance import order_flow_imbalance_strategy, is_success as ofi_is_success
from backtest_rejection_strategy import backtest_rejection_strategy

StrategySpec = namedtuple("StrategySpec", ["func", "param_keys", "uses_recorder", "is_success"])

# 均值回归（无 recorder）
MR = StrategySpec(
    func=mean_reversion_strategy,
    param_keys=["step_ns", "ema_alpha", "threshold_ticks", "max_position_lots", "order_qty_lots"],
    uses_recorder=False,
    is_success=mr_is_success,
)

# 订单流不平衡（无 recorder）
OFI = StrategySpec(
    func=order_flow_imbalance_strategy,
    param_keys=["step_ns", "volume_threshold", "take_profit_pct", "stop_loss_pct",
                "fee_rate", "max_position_lots", "order_qty_lots"],
    uses_recorder=False,
    is_success=ofi_is_success,
)

# Rejection（recorder-based）
REJ = StrategySpec(
    func=backtest_rejection_strategy,
    param_keys=["CANDLE_INTERVAL_NS", "REJECTION_RATIO", "MIN_BODY_RATIO",
                "TAKE_PROFIT_FACTOR", "STOP_LOSS_FACTOR", "elapsed_interval"],
    uses_recorder=True,
    is_success=None,  # 返回 True 视为成功
)

# 队列不平衡做市（Phase 4 待注册；先用占位 None 占位以避免 KeyError）
QIMM = None  # 在 strategies/queue_imbalance_mm.py 完成后在此注册

REGISTRY = {
    "mean_reversion": MR,
    "order_flow_imbalance": OFI,
    "rejection": REJ,
}


def get_strategy(name: str) -> StrategySpec:
    if name not in REGISTRY or REGISTRY[name] is None:
        raise KeyError(f"未知策略: {name}（可用: {list(REGISTRY)}）")
    return REGISTRY[name]


def register(name: str, spec: StrategySpec):
    REGISTRY[name] = spec