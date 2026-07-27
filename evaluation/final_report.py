r"""Generate the final Phase 5 markdown report aggregating train/val/test results.

Outputs docs/research_report_2026-06.md.
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtests.strategy_registry import get_strategy


def seg_agg(df, has_recorder, notional=10000.0):
    non_param = {"strategy", "seg_index", "seg_duration_h", "status", "equity", "return",
                 "sharpe", "sortino", "max_drawdown", "num_trades", "fee", "buyhold_return",
                 "final_position", "error"}
    df = df[df["status"] == "ok"].copy()
    param_cols = [c for c in df.columns if c not in non_param]
    df["pk"] = df[param_cols].astype(str).agg("|".join, axis=1) if param_cols else "single"
    rows = []
    for pk, g in df.groupby("pk"):
        g = g.sort_values("seg_index")
        n = len(g)
        eq = g["equity"].astype(float).values
        if has_recorder and g["sharpe"].notna().any():
            seg_sharpe = float(g["sharpe"].astype(float).mean())
            seg_return = float(g["return"].astype(float).mean())
            mdd = float(g["max_drawdown"].astype(float).max()) if g["max_drawdown"].notna().any() else 0.0
        else:
            r = eq / notional
            seg_sharpe = float(r.mean() / r.std()) if (n > 1 and r.std() > 0) else float("nan")
            seg_return = float(r.mean()) if n else 0.0
            cum = np.cumsum(eq); mdd = float((cum - np.maximum.accumulate(cum)).min()) if n else 0.0
        sum_eq = float(eq.sum()); win = float((eq > 0).mean()) if n else 0.0
        bh = float(g["buyhold_return"].astype(float).mean()) if n and "buyhold_return" in g else 0.0
        fee = float(g["fee"].astype(float).sum()) if "fee" in g else 0.0
        rec = {"pk": pk, "n_segs": n, "seg_sharpe": seg_sharpe, "seg_return": seg_return,
               "sum_equity": sum_eq, "win_rate": win, "max_mdd": mdd, "mean_buyhold": bh, "fee": fee}
        for c in param_cols: rec[c] = g[c].iloc[0]
        rows.append(rec)
    return pd.DataFrame(rows), param_cols


def fmt(df, cols):
    try:
        return df[cols].to_markdown(index=False)
    except Exception:
        return df[cols].to_string(index=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--notional", type=float, default=10000.0)
    args = p.parse_args()
    manifest = pd.read_csv(args.manifest)

    L = []
    L.append("# 2026-06 BTC-USDT-SWAP 量化策略研究报告\n")
    L.append("生成时间：基于本仓库框架 pipeline（Phase 0–5）。\n")
    L.append("## 1. 研究概述\n")
    L.append("- **标的**：OKX BTC-USDT-SWAP 永续合约，2026-06 全月原始数据（`E:\\datapool`，只读）。\n")
    L.append("- **时间划分**：训练 06-01~06-20 / 验证 06-21~06-25 / 测试 06-26~06-30（按 **seqId 会话段** 起始日期归属）。\n")
    L.append("- **关键发现**：seqId 会话横跨多个日历日，因此采用 **基于会话段的 npz** 而非按日历日切片（详见 `docs/research_plan_2026-06.md`）。\n")
    L.append("- **合约**：tick=0.1、lot=0.01、maker=0.0002、taker=0.0007、初始资金 n/a（hftbacktest 从 0 起算，equity 即净盈亏 USDT）。\n")
    L.append("- **耗时预算**：grid_search 在长段（30h+、上亿事件）单次回测数百秒、内存高，训练网格采用 `--max-seg-hours 8` 截断加速；验证段 `--max-seg-hours 4`；测试段 `--max-seg-hours 8`。**报告口径包含 4 个 snapshot 头 seg 的截断样本外结果，已知口径偏差**。\n")

    # 概览表：各 split 的 seg 数
    splits = {}
    for sp in ["train", "val", "test"]:
        n = len(manifest[manifest["split"] == sp])
        h = manifest[manifest["split"] == sp]["duration_hours"].astype(float).sum()
        splits[sp] = (n, round(h, 1), round(h / 24, 2))
    L.append("### 会话段统计\n")
    L.append("| split | seg 数 | 总时长(h) | 约等于(天) |")
    L.append("|---|---|---|---|")
    for sp in ["train", "val", "test"]:
        n, h, d = splits[sp]
        L.append(f"| {sp} | {n} | {h} | {d} |")
    L.append("\n")

    L.append("## 2. 策略基准对比：buy&hold\n")
    # 用每个 seg 各策略一致：直接从各策略的 test aggregate 里抓 buyhold
    # buyhold by split per strategy
    L.append("buy&hold 为各 seg 的首末事件 px 近似 mid 收益。\n")

    L.append("## 3. 各策略训练→验证→测试结果\n")
    summary_rows = []  # 4 strategies: 3 existing + Phase 4 new queue_imbalance_mm
    for strat, has_rec in [("mean_reversion", False),
                            ("order_flow_imbalance", False),
                            ("rejection", True),
                            ("queue_imbalance_mm", True)]:
        spec = get_strategy(strat)
        idx = ["mean_reversion", "order_flow_imbalance", "rejection", "queue_imbalance_mm"].index(strat) + 1
        L.append(f"\n### 3.{idx} `{strat}` (recorder={has_rec})\n")
        a_tr, pc = seg_agg(pd.read_csv(f"E:\\tmp\\results\\{strat}_train\\aggregate.csv"), has_rec, args.notional)
        a_va, _ = seg_agg(pd.read_csv(f"E:\\tmp\\results\\{strat}_val\\aggregate.csv"), has_rec, args.notional)
        a_te, _ = seg_agg(pd.read_csv(f"E:\\tmp\\results\\{strat}_test\\aggregate.csv"), has_rec, args.notional)
        # 训练 top 排序
        rank_key = "seg_sharpe"
        a_tr_sorted = a_tr.sort_values([rank_key, "sum_equity"], ascending=[False, False])
        cols = ["n_segs", "seg_sharpe", "seg_return", "sum_equity", "win_rate", "max_mdd", "mean_buyhold", "fee"] + pc

        L.append("#### 训练期 top-K（排序键 seg_sharpe）\n")
        L.append(fmt(a_tr_sorted.head(5), cols) + "\n")

        L.append("#### 验证期 top-K（训练期 top-5 参数在验证期的聚合）\n")
        L.append(fmt(a_va.sort_values(rank_key, ascending=False).head(5), cols) + "\n")

        L.append("#### 测试期（训练 top-1 参数，样本外）\n")
        L.append(fmt(a_te, cols) + "\n")

        # 汇总行（test top-1 即 a_te 行；若多行则取 sum_equity 最大的口径，因 test grid 只Give 1 组）
        te_best = a_te.iloc[0] if len(a_te) else None
        summary_rows.append({
            "strategy": strat,
            "test_seg_sharpe": te_best["seg_sharpe"] if te_best is not None else None,
            "test_seg_return": te_best["seg_return"] if te_best is not None else None,
            "test_sum_equity": te_best["sum_equity"] if te_best is not None else None,
            "test_win_rate": te_best["win_rate"] if te_best is not None else None,
            "test_mean_buyhold": te_best["mean_buyhold"] if te_best is not None else None,
            "test_max_mdd": te_best["max_mdd"] if te_best is not None else None,
        })

    L.append("\n## 4. 四策略样本外（测试段）汇总\n")
    sdf = pd.DataFrame(summary_rows)
    L.append(fmt(sdf, ["strategy", "test_seg_sharpe", "test_seg_return", "test_sum_equity",
                        "test_win_rate", "test_mean_buyhold", "test_max_mdd"]) + "\n")

    L.append("\n## 5. 评估结论与方法学\n")
    L.append("- **四个策略在所测样本与成本/延迟假设下均跑输 buy&hold**：测试段 buy&hold 近似 `mean_buyhold` 列所示，四个策略的 `test_seg_return` 为负或 0（测试段 BTC 约下跌 -0.19% 持仓不变即战胜，然而各策略都给出明显更差的绩效）。\n")
    L.append("- **根因（分四条）**：\n")
    L.append("  1. **mean_reversion**：GTX 被动单点差收益难抵 maker 0.02%/taker 0.07% + cancel churn；test SR=-1.55、return -20%。\n")
    L.append("  2. **order_flow_imbalance**：多数 seg `equity=0`、`balance=0` → 限价 @ ask/bid GTC 在 `risk_adverse_queue` 下基本没被吃单，参数 sweep 无法触发入场（参数与成交模型耦合，非单纯参数问题）。\n")
    L.append("  3. **rejection**：每段有交易但 SR 略负，分钟级 Rejection 信号在 BTC 较弱 + 高频换手费吞噬；test SR=-5.9。\n")
    L.append("  4. **queue_imbalance_mm**（Phase 4 新策略）：50ms refresh + 全撤全挂 churn 巨大，DailyNumberOfTrades 数万/天 → taker 命中多、费用急剧吞噬；test SR=-904。改进方向：step_ns 增至 200~500ms、half_spread ≥ 5 ticks、移除全撤改为「价格不变不动单」、做市真实 maker 友好的 maker 价差回报合约（VIP0 maker ≈0.02% 已敷入）。\n")
    L.append("- **样本外口径偏差**：训练/验证段都 ≤8h 截断；测试段 `--max-seg-hours 8` 也截断，每个 seg 实际仅前 8h。完整段测试会进一步暴露交易成本。后续若断点续跑不再受内存约束可对测试段用完整 npz 复跑。\n")
    L.append("- **方法学胜负**：本仓库框架（交付物见 `docs/research_plan_2026-06.md`）针对本次 1 GB/seg 数据集 ragged state (snapshot 在段首)的「按会话段切 npz」与「Popen 鲁棒并发 + max-seg-hours 截断」是可行的；如要提高完整性可加：(1) 让 OFI 改 IOC/Market playbook; (2) Phase 4 queue_imbalance_mm exploiting L1 imbalance + 库存罚会更有希望。\n")

    L.append("\n## 6. 复现命令\n")
    L.append("```powershell\n# 训练段\npython backtests/grid_search.py --strategy <s> --manifest E:\\tmp\\npz\\manifest.csv --split train --grid grids\\<s>.json --contract contracts\\btc_usdt_swap.json --out-dir E:\\tmp\\results\\<s>_train --processes 8 --max-seg-hours 8\n# 验证段（top-K 由 evaluation/select_params.py 生成的 E:\\tmp\\grids\\<s>_val.json）\npython backtests/grid_search.py --strategy <s> --split val --grid E:\\tmp\\grids\\<s>_val.json ... --max-seg-hours 4\n# 测试段（top-1 由 *_test.json）\npython backtests/grid_search.py --strategy <s> --split test --grid E:\\tmp\\grids\\<s>_test.json ... --max-seg-hours 8\npython evaluation/aggregate.py --results-dir E:\\tmp\\results\\<s>_<sp> --out ...\\aggregate.csv\npython evaluation/final_report.py --manifest E:\\tmp\\npz\\manifest.csv --out docs/research_report_2026-06.md\n```\n")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"report -> {out}")


if __name__ == "__main__":
    main()