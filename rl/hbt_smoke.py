r"""Phase RL0 smoke: 验证 hftbacktest 的 hbt 接口在纯 Python（非 @njit）Env 循环中可用。

跑 seg64 npz，纯 Python 循环 500 步：
  每步 hbt.elapse(500ms) -> clear_inactive_orders -> depth/state_values -> 简单策略
    （imbalance>0 示范挂被动买单；<0 挂被动卖单；hold 不挂）
  累计 equity 变化与 num_trades；最后保存到 E:\tmp\rl\smoke.log。

不依赖 numba @njit——目标只是验证接口可用，性能不要求。
"""
import sys
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, BUY, SELL, GTX, LIMIT


def main(npz_path=r"E:\tmp\npz_v2\seg_64_1782206125809.npz", n_steps=500, step_ns=500_000_000, out=r"E:\tmp\rl\v2\smoke.log"):
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    data = np.load(npz_path)["data"]
    asset = (
        BacktestAsset()
        .data([data])
        .linear_asset(1.0)
        .constant_order_latency(10_000_000, 10_000_000)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(0.0002, 0.0007)
        .tick_size(0.1)
        .lot_size(0.01)
        .last_trades_capacity(1_000_000)
    )
    hbt = HashMapMarketDepthBacktest([asset])
    lines = []
    buy_oid = 1; sell_oid = 2

    prev_eq = 0.0
    n_buy = 0; n_sell = 0; n_holds = 0
    for i in range(n_steps):
        rc = hbt.elapse(step_ns)
        if rc != 0:
            lines.append(f"step {i}: elapse rc={rc}, data exhausted. stopping.")
            break
        hbt.clear_inactive_orders(0)
        depth = hbt.depth(0)
        state = hbt.state_values(0)
        bb, ba = depth.best_bid, depth.best_ask
        bbq, baq = depth.best_bid_qty, depth.best_ask_qty
        if not np.isfinite(bb) or not np.isfinite(ba) or bb <= 0 or ba <= 0:
            n_holds += 1
            continue
        imb = (bbq - baq) / (bbq + baq + 1e-9)
        pos = state.position
        qty = 0.01  # 1 lot (0.01 BTC)
        # simple demo: imbalance>0.3 and pos<0.05 -> passive buy at best_bid; <-0.3 and pos>-0.05 -> passive sell
        placed = False
        if imb > 0.3 and pos < 0.05:
            hbt.submit_buy_order(0, buy_oid, bb, qty, GTX, LIMIT, False)
            n_buy += 1; placed = True
        elif imb < -0.3 and pos > -0.05:
            hbt.submit_sell_order(0, sell_oid, ba, qty, GTX, LIMIT, False)
            n_sell += 1; placed = True
        else:
            n_holds += 1
        # recompute state
        state2 = hbt.state_values(0)
        mid = 0.5 * (bb + ba)
        eq = float(state2.balance + state2.position * mid - state2.fee)
        d = eq - prev_eq
        if (i + 1) % 50 == 0 or i == 0:
            lines.append(f"step {i+1:4d}: imb={imb:+.3f} pos={pos:+.4f} eq={eq:+.4f} Δ={d:+.4f} buy/sell/hold={n_buy}/{n_sell}/{n_holds}")
        prev_eq = eq

    # final
    depth = hbt.depth(0); state = hbt.state_values(0)
    mid = 0.5 * (depth.best_bid + depth.best_ask)
    final_eq = float(state.balance + state.position * mid - state.fee)
    lines.append("=== SMOKE END ===")
    lines.append(f"final: balance={state.balance:+.4f} pos={state.position:+.4f} fee={state.fee:+.4f} mid={mid:.2f} equity={final_eq:+.4f}")
    lines.append(f"counts: buy={n_buy} sell={n_sell} hold={n_holds}")
    try:
        hbt.close()
    except Exception:
        pass

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines[-8:]))
    print(f"\nlog: {out}")


if __name__ == "__main__":
    main()
