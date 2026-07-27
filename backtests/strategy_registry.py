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

StrategySpec = namedtuple(
    "StrategySpec",
    ["func", "param_keys", "uses_recorder", "is_success", "params_as_object"],
)
# params_as_object=True：策略形参为 (hbt, [recorder,] params_namedtuple)，
# 即把网格 dict 打包为单一 namedtuple 传入（如 rejection）；
# False：把网格 dict 展开为 keyword 参数传入（mean_reversion / ofi）。

# 均值回归（无 recorder）
MR = StrategySpec(
    func=mean_reversion_strategy,
    param_keys=["step_ns", "ema_alpha", "threshold_ticks", "max_position_lots", "order_qty_lots"],
    uses_recorder=False,
    is_success=mr_is_success,
    params_as_object=False,
)

# 订单流不平衡（无 recorder）
OFI = StrategySpec(
    func=order_flow_imbalance_strategy,
    param_keys=["step_ns", "volume_threshold", "take_profit_pct", "stop_loss_pct",
                "fee_rate", "max_position_lots", "order_qty_lots"],
    uses_recorder=False,
    is_success=ofi_is_success,
    params_as_object=False,
)

# Rejection（recorder-based，params 作为单一 namedtuple 传入）
from collections import namedtuple as _nt
_RejectionParams = _nt("RejectionParams", [
    "CANDLE_INTERVAL_NS", "REJECTION_RATIO", "MIN_BODY_RATIO",
    "TAKE_PROFIT_FACTOR", "STOP_LOSS_FACTOR", "elapsed_interval",
])
REJ = StrategySpec(
    func=backtest_rejection_strategy,
    param_keys=["CANDLE_INTERVAL_NS", "REJECTION_RATIO", "MIN_BODY_RATIO",
                "TAKE_PROFIT_FACTOR", "STOP_LOSS_FACTOR", "elapsed_interval"],
    uses_recorder=True,
    is_success=None,  # 返回 True 视为成功
    params_as_object=True,
)

# 队列不平衡做市（Phase 4 新策略，recorder-based）
from strategies.queue_imbalance_mm import queue_imbalance_mm_strategy, is_success as qimm_is_success
QIMM = StrategySpec(
    func=queue_imbalance_mm_strategy,
    param_keys=["step_ns", "gamma", "inv_penalty", "half_spread_ticks",
                "max_position_lots", "order_qty_lots", "skew_floor_ticks"],
    uses_recorder=True,
    is_success=qimm_is_success,
    params_as_object=False,
)

REGISTRY = {
    "mean_reversion": MR,
    "order_flow_imbalance": OFI,
    "rejection": REJ,
    "queue_imbalance_mm": QIMM,
}


def get_strategy(name: str) -> StrategySpec:
    if name not in REGISTRY:
        raise KeyError(f"未知策略: {name}（可用: {list(REGISTRY)}）")
    if REGISTRY[name] is None:
        raise KeyError(f"策略 {name} 尚未注册")
    return REGISTRY[name]


def register(name: str, spec: StrategySpec):
    REGISTRY[name] = spec