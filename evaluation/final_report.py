"""Generate a data-driven train/validation/test research report."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

STRATEGIES = (
    "mean_reversion",
    "order_flow_imbalance",
    "rejection",
    "queue_imbalance_mm",
    "rl_policy",
)

NON_PARAM_COLUMNS = {
    "result_schema_version", "strategy", "seg_index", "seg_duration_h",
    "status", "equity", "return",
    "sharpe", "sortino", "max_drawdown", "num_trades", "daily_num_trades",
    "fee", "liquidation_cost", "buyhold_return", "final_position",
    "trading_volume", "trading_value", "backtest_duration_h",
    "report_notional", "error",
}


def _return_t_stat(returns: np.ndarray) -> float:
    if len(returns) < 2:
        return float("nan")
    std = float(np.std(returns, ddof=1))
    if std == 0:
        return float("nan")
    return float(np.mean(returns) / std * np.sqrt(len(returns)))


def aggregate_parameter_tuples(df: pd.DataFrame, fallback_notional: float):
    df = df[df["status"] == "ok"].copy()
    if df.empty:
        return pd.DataFrame(), []
    param_cols = [c for c in df.columns if c not in NON_PARAM_COLUMNS]
    df["parameter_tuple"] = (
        df[param_cols].astype(str).agg("|".join, axis=1) if param_cols else "single"
    )
    rows = []
    for key, group in df.groupby("parameter_tuple", dropna=False):
        group = group.sort_values("seg_index")
        returns = group["return"].astype(float).to_numpy()
        equity = group["equity"].astype(float).to_numpy()
        notional = (
            float(group["report_notional"].dropna().iloc[0])
            if "report_notional" in group and group["report_notional"].notna().any()
            else fallback_notional
        )
        cumulative = np.cumsum(returns)
        cross_drawdown = (
            float(np.max(np.maximum.accumulate(cumulative) - cumulative))
            if len(cumulative)
            else 0.0
        )
        within_drawdown = (
            float(group["max_drawdown"].astype(float).max())
            if group["max_drawdown"].notna().any()
            else 0.0
        )
        record = {
            "parameter_tuple": key,
            "n_segs": len(group),
            "return_t_stat": _return_t_stat(returns),
            "mean_intrasegment_sr": (
                float(group["sharpe"].astype(float).mean())
                if group["sharpe"].notna().any()
                else float("nan")
            ),
            "total_return": float(equity.sum() / notional),
            "sum_equity": float(equity.sum()),
            "win_rate": float((equity > 0).mean()),
            "max_drawdown": max(cross_drawdown, within_drawdown),
            "fees": float(group["fee"].fillna(0).astype(float).sum()),
            "liquidation_cost": float(
                group.get("liquidation_cost", pd.Series(0, index=group.index))
                .fillna(0).astype(float).sum()
            ),
            "num_trades": float(group["num_trades"].fillna(0).astype(float).sum()),
            "hours": float(
                group.get("backtest_duration_h", group["seg_duration_h"])
                .fillna(0).astype(float).sum()
            ),
            "mean_buyhold": float(group["buyhold_return"].astype(float).mean()),
        }
        for column in param_cols:
            record[column] = group[column].iloc[0]
        rows.append(record)
    result = pd.DataFrame(rows)
    result = result.sort_values(
        ["return_t_stat", "total_return"],
        ascending=[False, False],
        na_position="last",
    )
    return result, param_cols


def _table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return "_无有效结果_"
    selected = [column for column in columns if column in frame]
    try:
        return frame[selected].to_markdown(index=False)
    except Exception:
        return frame[selected].to_string(index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--results-root", default=r"E:\tmp\results")
    parser.add_argument("--out", required=True)
    parser.add_argument("--notional", type=float, default=100_000.0)
    args = parser.parse_args()

    manifest = pd.read_csv(args.manifest)
    results_root = Path(args.results_root)
    lines = [
        "# BTC-USDT-SWAP 策略研究报告",
        "",
        "本报告完全从本次 aggregate CSV 动态生成；不嵌入历史运行的固定数字。",
        "",
        "## 数据与统计口径",
        "",
        f"- manifest：`{args.manifest}`",
        f"- 报告资金：{args.notional:,.0f} USDT（若结果包含 `report_notional`，优先使用结果值）",
        "- 主评分为跨 segment 收益 t 统计量；段内年化 SR 仅作诊断，不再求平均后称作组合 Sharpe。",
        "- terminal equity 已计入按 bid/ask 平仓的点差和 taker fee。",
        "",
        "### Manifest",
        "",
    ]
    split_rows = []
    for split in ("train", "val", "test"):
        subset = manifest[manifest["split"] == split]
        split_rows.append({
            "split": split,
            "segments": len(subset),
            "manifest_hours": float(subset["duration_hours"].astype(float).sum()),
        })
    lines.append(_table(pd.DataFrame(split_rows), ["split", "segments", "manifest_hours"]))

    test_summary = []
    warnings = []
    for strategy in STRATEGIES:
        lines.extend(["", f"## `{strategy}`", ""])
        split_aggregates = {}
        split_param_cols = {}
        for split in ("train", "val", "test"):
            path = results_root / f"{strategy}_{split}" / "aggregate.csv"
            if not path.exists():
                lines.extend([f"### {split}", "", "_结果缺失_", ""])
                continue
            raw = pd.read_csv(path)
            required_v2 = {
                "result_schema_version",
                "report_notional", "liquidation_cost", "backtest_duration_h",
                "trading_volume", "trading_value",
            }
            missing_v2 = sorted(required_v2 - set(raw.columns))
            if missing_v2:
                warnings.append(
                    f"{strategy}/{split}: 缺少 v2 字段 {missing_v2}，必须重跑。"
                )
                lines.extend([
                    f"### {split}",
                    "",
                    f"_旧版结果无效：缺少 {', '.join(missing_v2)}_",
                    "",
                ])
                continue
            if not raw["result_schema_version"].eq("backtest-result-v2").all():
                warnings.append(
                    f"{strategy}/{split}: result schema 不是 backtest-result-v2，必须重跑。"
                )
                continue
            legacy_mask = (
                raw["return"].fillna(0).ne(0)
                & raw["equity"].fillna(0).eq(0)
            )
            if legacy_mask.any():
                warnings.append(
                    f"{strategy}/{split}: {int(legacy_mask.sum())} 行为旧版 "
                    "`return != 0 && equity == 0`，必须重跑。"
                )
            aggregate, param_cols = aggregate_parameter_tuples(raw, args.notional)
            split_aggregates[split] = aggregate
            split_param_cols[split] = param_cols
            columns = [
                "n_segs", "return_t_stat", "total_return", "sum_equity",
                "win_rate", "max_drawdown", "fees", "liquidation_cost",
                "num_trades", "hours", "mean_buyhold",
            ] + param_cols
            lines.extend([
                f"### {split}",
                "",
                _table(aggregate.head(5), columns),
                "",
            ])

        test = split_aggregates.get("test")
        if test is not None and len(test) == 1:
            row = test.iloc[0]
            test_summary.append({
                "strategy": strategy,
                "n_segs": row["n_segs"],
                "return_t_stat": row["return_t_stat"],
                "total_return": row["total_return"],
                "sum_equity": row["sum_equity"],
                "win_rate": row["win_rate"],
                "max_drawdown": row["max_drawdown"],
                "fees": row["fees"],
                "liquidation_cost": row["liquidation_cost"],
                "num_trades": row["num_trades"],
            })
        elif test is not None and len(test) > 1:
            warnings.append(
                f"{strategy}/test 包含 {len(test)} 个参数组合；"
                "报告拒绝在测试集上挑选最优组合。"
            )

    summary = pd.DataFrame(test_summary)
    lines.extend(["", "## 样本外汇总", ""])
    lines.append(_table(summary, [
        "strategy", "n_segs", "return_t_stat", "total_return", "sum_equity",
        "win_rate", "max_drawdown", "fees", "liquidation_cost", "num_trades",
    ]))

    lines.extend(["", "## 自动结论", ""])
    if summary.empty:
        lines.append("- 没有满足“测试集仅一个预先选定参数组合”的完整结果。")
    else:
        for _, row in summary.iterrows():
            if row["sum_equity"] > 0:
                direction = "盈利"
            elif row["sum_equity"] < 0:
                direction = "亏损"
            else:
                direction = "持平"
            lines.append(
                f"- `{row['strategy']}`：{direction} {abs(row['sum_equity']):.2f} USDT，"
                f"总收益 {row['total_return']:.4%}，胜率 {row['win_rate']:.1%}，"
                f"跨段收益 t={row['return_t_stat']:.3f}，"
                f"成交 {row['num_trades']:.0f} 笔。"
            )
            if row["num_trades"] == 0:
                lines.append(
                    f"- `{row['strategy']}` 在全部测试段零成交；这是退化的空仓策略，"
                    "不能将零收益解释为有效 alpha。"
                )
        lines.append(
            "- t 统计量和胜率必须结合 segment 数量解释；少量测试段不能单独证明 alpha。"
        )

    lines.extend(["", "## 完整性警告", ""])
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- 未检测到旧版零权益记录或测试集参数挑选。")

    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"report -> {output}")


if __name__ == "__main__":
    main()
