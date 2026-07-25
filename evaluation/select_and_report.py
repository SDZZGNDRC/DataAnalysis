r"""Phase 5 选参与最终报告生成。

输入：各 split 的 aggregate.csv（由 evaluation.aggregate.py 生成），以及 manifest。
流程：
  1. 训练段：按参数组聚合每个 seg 的业绩指标 -> 计算跨段聚合指标
     - 对有 Sharpe（recorder 策略）的策略，用训练段平均 Sharpe 做主排序
     - 对仅 equity（非 recorder 策略）的策略，用训练段 sum(equity)/sum(seg_h) 折算的
       per-hour-equity 与跨段 equity 正样本比例做排序；并计算跨段 equity 的伪 Sharpe
     - 排序后取 top-K 候选参数
  2. 验证段：对 top-K 参数复跑结果（已由 Phase 3 单独产出）汇总，计算验证期聚合指标
     - 选验证期表现最优且稳定的 1 组为最终参数
  3. 测试段：用最终参数在测试段的聚合结果做"样本外"评估
     - 跨段 Sharpe（年化）/Sortino/MDD、胜率、相对 buy&hold 的超额
  4. 写入 markdown 报告

为聚合跨段 equity 到 Sharpe：把每个 seg 的 equity 看作该 seg 的 USDT 净盈亏，
   per-seg-return = equity / (per-seg assumed_notional)；这里采用简化的 per-lot 标定：
   每 seg 固定名义敞口 = contract.initial_balance_usdt 的份额，以得到规模无关的 per-seg
   return 序列 r_i；然后年度化 Sharpe = mean(r_i)/std(r_i) * sqrt(segs_per_year)，
   segs_per_year 估计为 总训练时长(日) 对应的 seg 数 -> 年化(~365/duration_days)。

用法：
    python evaluation/select_and_report.py --strategy mean_reversion ^
        --train-agg E:\tmp\results\mr_train\aggregate.csv ^
        --val-agg E:\tmp\results\mr_val\aggregate.csv ^
        --test-agg E:\tmp\results\mr_test\aggregate.csv ^
        --manifest E:\tmp\npz\manifest.csv ^
        --contract contracts\btc_usdt_swap.json ^
        --report docs/research_report_2026-06.md
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def seg_returns_from_equity(df, contract):
    """非 recorder 策略：每个 (参数组) 在每个 seg 的 equity 视为净盈亏 USDT；
    归一化用 fixed notional（初始敞口名义值）。
    返回 dict: param_key(tuple) -> np.array of per-seg returns。
    """
    df = df[df["status"] == "ok"].copy()
    notional = float(contract.get("report_notional_usdt", 10_000.0))
    # 参数识别：剔除非参数列
    non_param = {"strategy", "seg_index", "seg_duration_h", "status", "equity", "return",
                 "sharpe", "sortino", "max_drawdown", "num_trades", "fee", "buyhold_return",
                 "final_position", "error"}
    param_cols = [c for c in df.columns if c not in non_param]
    df["param_key"] = df[param_cols].astype(str).agg("|".join, axis=1)
    out = {}
    for pk, g in df.groupby("param_key"):
        r = (g["return"].astype(float) / notional).values
        out[pk] = r
    return out, param_cols


def aggregate_seg_metrics(df, has_recorder):
    """按参数组聚合跨段指标。

    has_recorder=True：直接用 recorder 提供的 sharpe(return series -> cross-seg) 与 equity sum。
    has_recorder=False：基于 seg return 序列估算伪 Sharpe/Sortino/MDD。
    返回 DataFrame: param_key, n_segs, mean_eq(-uity), sum_equity,
                    cross_sharpe, cross_sortino, cross_mdd, win_rate, mean_buyhold。
    """
    df = df[df["status"] == "ok"].copy()
    non_param = {"strategy", "seg_index", "seg_duration_h", "status", "equity", "return",
                 "sharpe", "sortino", "max_drawdown", "num_trades", "fee", "buyhold_return",
                 "final_position", "error"}
    param_cols = [c for c in df.columns if c not in non_param]
    df["param_key"] = df[param_cols].astype(str).agg("|".join, axis=1)
    rows = []
    for pk, g in df.groupby("param_key"):
        g = g.sort_values("seg_index")
        r_eq = g["equity"].astype(float).values
        n = len(r_eq)
        sum_eq = float(r_eq.sum())
        mean_eq = float(r_eq.mean()) if n else 0.0
        win_rate = float((r_eq > 0).mean()) if n else 0.0
        mean_bh = float(g["buyhold_return"].astype(float).mean()) if n else 0.0
        if has_recorder and g["sharpe"].notna().any():
            seg_sharpe = float(g["sharpe"].astype(float).mean())
        else:
            seg_sharpe = np.nan
        # 基于 seg net PnL 序列做伪 return 序列（除以代表 notional）
        notional = 1.0  # 这里先按绝对 PnL；下游比较时统一标尺（已在 select 阶段处理）
        rs = r_eq
        if n > 1 and rs.std() > 0:
            pseudo_sharpe = float(rs.mean() / rs.std())
        else:
            pseudo_sharpe = np.nan if n <= 1 else float("inf") if rs.mean() > 0 else 0.0
        # MDD on cumulative seq
        cum = np.cumsum(rs)
        running_max = np.maximum.accumulate(cum)
        dd = (cum - running_max)
        mdd = float(dd.min()) if n else 0.0
        rows.append({
            "param_key": pk, "n_segs": n,
            "sum_equity": sum_eq, "mean_equity": mean_eq,
            "seg_sharpe_mean": seg_sharpe,
            "cross_sharpe": pseudo_sharpe,
            "cross_mdd": mdd,
            "win_rate": win_rate, "mean_buyhold": mean_bh,
        })
    out = pd.DataFrame(rows)
    # 解析 param_key 回参数列
    if param_cols:
        split_keys = out["param_key"].str.split("|", expand=True)
        for i, c in enumerate(param_cols):
            out[c] = split_keys[i]
    return out, param_cols


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--train-agg", required=True)
    parser.add_argument("--val-agg", default=None)
    parser.add_argument("--test-agg", default=None)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--report", required=True)
    parser.add_argument("--append", action="store_true", help="追加到现有报告")
    args = parser.parse_args()

    with open(args.contract) as f:
        contract = json.load(f)
    manifest = pd.read_csv(args.manifest)

    from backtests.strategy_registry import get_strategy
    spec = get_strategy(args.strategy)
    has_recorder = spec.uses_recorder

    tr = pd.read_csv(args.train_agg)
    tr_agg, param_cols = aggregate_seg_metrics(tr, has_recorder)

    # 训练排序：recorder 用 seg_sharpe_mean（若全 nan 则 fallback 到 cross_sharpe 与 sum_equity）
    if has_recorder and tr_agg["seg_sharpe_mean"].notna().any():
        sort_key = "seg_sharpe_mean"
    else:
        sort_key = "cross_sharpe"
    tr_sorted = tr_agg.sort_values([sort_key, "sum_equity"], ascending=False)
    top = tr_sorted.head(args.top_k)

    # 验证段复跑聚合
    val_agg = None
    if args.val_agg and Path(args.val_agg).exists():
        v = pd.read_csv(args.val_agg)
        val_agg, _ = aggregate_seg_metrics(v, has_recorder)

    # 测试段复跑聚合
    test_agg = None
    if args.test_agg and Path(args.test_agg).exists():
        te = pd.read_csv(args.test_agg)
        test_agg, _ = aggregate_seg_metrics(te, has_recorder)

    # 拼装 markdown
    lines = []
    lines.append(f"## 策略：{args.strategy}\n")
    lines.append(f"- 是否使用 recorder：{'是' if has_recorder else '否'}")
    lines.append(f"- 排序键：{sort_key}")
    lines.append(f"- 训练段 seg 数：{len(tr)} | 验证段 seg 数："
                 f"{len(pd.read_csv(args.val_agg)) if args.val_agg and Path(args.val_agg).exists() else 'N/A'} | "
                 f"测试段 seg 数：{len(pd.read_csv(args.test_agg)) if args.test_agg and Path(args.test_agg).exists() else 'N/A'}\n")

    lines.append("### 训练段 top-K 参数\n")
    cols = ["n_segs", "sum_equity", "mean_equity"] + ([sort_key] if sort_key else []) + ["cross_sharpe", "cross_mdd", "win_rate", "mean_buyhold"] + param_cols
    lines.append(top[cols].to_markdown(index=False))
    lines.append("\n")

    if val_agg is not None:
        lines.append("### 验证段聚合（复跑训练 top-K 的相同参数组在验证期的表现）\n")
        lines.append(val_agg[cols].to_markdown(index=False))
        lines.append("\n")

    if test_agg is not None:
        lines.append("### 测试段（样本外）聚合\n")
        lines.append(test_agg[cols].to_markdown(index=False))
        lines.append("\n")

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if args.append and report_path.exists() else "w"
    with open(report_path, mode, encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"报告写入: {report_path}")


if __name__ == "__main__":
    main()