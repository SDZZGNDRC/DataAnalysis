import argparse
import sys
import os
import glob
import numpy as np
import polars as pl
from numba import njit, objmode
from numba.typed import List
from numba.types import Tuple, int64, float64
from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest
from hftbacktest.binding import HashMapMarketDepth

def parse_interval(interval_str):
    """
    解析时间间隔字符串，返回纳秒数。
    """
    interval_str = interval_str.strip().lower()
    if not interval_str:
        raise ValueError("Interval string cannot be empty")
        
    unit = interval_str[-1]
    if unit not in ['s', 'm', 'h', 'd']:
        if interval_str.isdigit():
             return int(interval_str) * 1_000_000_000
        raise ValueError(f"Unknown time unit in '{interval_str}'. Supported: s, m, h, d")
    
    try:
        value = int(interval_str[:-1])
    except ValueError:
        raise ValueError(f"Invalid number in interval '{interval_str}'")

    if unit == 's':
        return value * 1_000_000_000
    elif unit == 'm':
        return value * 60 * 1_000_000_000
    elif unit == 'h':
        return value * 3600 * 1_000_000_000
    elif unit == 'd':
        return value * 24 * 3600 * 1_000_000_000
    
    return 0

# ==========================================
# 核心修复：新增从 Depth 对象提取 Top N 的函数
# ==========================================

@njit
def get_top_bids(depth, n):
    """
    从 HashMapMarketDepth 获取前 N 档买单
    """
    result = List()
    if n <= 0:
        return result

    count = 0
    # 从最佳买价开始向下遍历
    # 假设最大搜索范围为 100,000 个 tick，防止死循环（根据实际波动率可调整）
    # 通常盘口是很密集的
    start_tick = depth.best_bid_tick
    end_tick = max(0, start_tick - 100000) 
    
    # 只有当 best_bid_tick 有效时（非极大值/极小值）才循环，但在 hftbacktest 中通常直接循环即可
    # 如果盘口为空，loop 可能不会执行或 best_bid_tick 很怪，加个 check 更好，但这里保持简单
    
    for t in range(start_tick, end_tick, -1):
        qty = depth.bid_qty_at_tick(t)
        if qty > 0:
            price = t * depth.tick_size
            result.append((price, qty))
            count += 1
            if count == n:
                break
    return result

@njit
def get_top_asks(depth, n):
    """
    从 HashMapMarketDepth 获取前 N 档卖单
    """
    result = List()
    if n <= 0:
        return result

    count = 0
    # 从最佳卖价开始向上遍历
    start_tick = depth.best_ask_tick
    # 向上搜索范围
    end_tick = start_tick + 100000
    
    for t in range(start_tick, end_tick, 1):
        qty = depth.ask_qty_at_tick(t)
        if qty > 0:
            price = t * depth.tick_size
            result.append((price, qty))
            count += 1
            if count == n:
                break
    return result

@njit
def count_different(prev_depth, curr_depth):
    """统计当前深度与前一个时刻深度的不同"""
    common_length = min(len(prev_depth), len(curr_depth))
    bias = max(len(prev_depth), len(curr_depth)) - common_length
    count = bias
    for i in range(common_length):
        # 比较价格(0)和数量(1)
        if prev_depth[i][0] != curr_depth[i][0] \
            or prev_depth[i][1] != curr_depth[i][1]:
            count += 1
    return count

def generate_snapshot_logic(hbt, out, output_interval_ns, check_interval_ns, top_n):
    # 初始化状态
    current_boundary = 0
    
    hbt.elapse(0) # 初始化 Backtest 状态
    
    # 第一次运行，先初始化边界
    current_boundary = (hbt.current_timestamp // output_interval_ns) * output_interval_ns
    next_boundary = current_boundary + output_interval_ns
    
    changes_count = 0

    # 存储上一次的快照 (初始化为空)
    # 类型推导需要
    prev_bids = List()
    prev_bids.append((0.0, 0.0))
    prev_bids.clear()

    prev_asks = List()
    prev_asks.append((0.0, 0.0))
    prev_asks.clear()
    
    # 为了避免第一次比较时全部视为变化，我们需要一个标志位
    first_run = True

    # 循环推进时间
    while hbt.elapse(check_interval_ns) == 0:
        now = hbt.current_timestamp
        # print(f"Current Timestamp: {now}")
        # print(f" Next Boundary: {next_boundary}", end='\n')
        # 1. 检查是否跨越了输出统计周期
        while now >= next_boundary:
            out.append((current_boundary, changes_count))
            # NOTICE: 这里输出的日志可以看到问题所在
            # print(f"Recorded: {current_boundary}, Changes: {changes_count}")
            # if changes_count == 0:
            #     exit(-1)
            current_boundary = next_boundary
            next_boundary += output_interval_ns
            changes_count = 0 
        
        # 2. 获取当前深度对象
        depth = hbt.depth(0)
        
        # 3. 提取当前的 Top N (使用新函数)
        # 注意：这里不再访问 .bids/.asks，而是使用 API 遍历
        curr_bids = get_top_bids(depth, top_n)
        curr_asks = get_top_asks(depth, top_n)
        
        if not first_run:
            changed_count_step = 0
            changed_count_step += count_different(prev_asks, curr_asks)
            changed_count_step += count_different(prev_bids, curr_bids)
            # 如果有变化，计数+1（这里逻辑是：这50ms内发生了至少一次盘口变动算作一次更新？
            # 或者你想统计变动的档位数？你的原逻辑是累加 count_different，即变动的档位总数）
            changes_count += changed_count_step
        else:
            first_run = False
        
        # 复制当前到前一个（Numba 中 List 是引用，需要复制内容或重新赋值）
        # get_top_bids 返回的是新 List，所以直接赋值引用即可
        prev_bids = curr_bids
        prev_asks = curr_asks

    return True

def run_analysis(data_list, args, out_data, interval_ns):
    check_interval_ns = int(args.sample_ms * 1_000_000) 

    asset = (
        BacktestAsset()
            .data(data_list)
            .constant_order_latency(1_000_000, 1_000_000) 
            .no_partial_fill_exchange()
            .tick_size(args.tick_size)
            .lot_size(args.lot_size)
            .last_trades_capacity(100) 
    )

    hbt = HashMapMarketDepthBacktest([asset])
    
    generate_snapshot_logic(hbt, out_data, interval_ns, check_interval_ns, args.top_n)
    
    hbt.close()

def main():
    parser = argparse.ArgumentParser(description="Estimate Orderbook Updates via Snapshot Diff (Safe Mode).")
    
    parser.add_argument("input_files", nargs='+', help="Input .npz files")
    parser.add_argument("--output", "-o", default="hft_book_change.csv", help="Output file path")
    parser.add_argument("--interval", "-i", default="1s", help="Output aggregation interval (e.g., 1s).")
    parser.add_argument("--sample-ms", type=float, default=50.0, help="Sampling interval in ms (e.g., 50).")
    parser.add_argument("--top-n", type=int, default=5, help="Top N levels to compare.")
    
    # hftbacktest asset settings
    parser.add_argument("--tick-size", type=float, default=0.1)
    parser.add_argument("--lot-size", type=float, default=0.001)
    parser.add_argument("--format", choices=['csv', 'parquet'], default='csv')

    args = parser.parse_args()

    input_files = []
    for pattern in args.input_files:
        matched = glob.glob(pattern, recursive=True)
        if not matched:
            if os.path.exists(pattern):
                matched = [pattern]
            else:
                print(f"Warning: No files found matching '{pattern}'")
        input_files.extend(matched)

    input_files = sorted(list(set(input_files)))

    if not input_files:
        print("Error: No valid input files found.")
        sys.exit(1)

    try:
        interval_ns = parse_interval(args.interval)
    except ValueError as e:
        print(f"Error parsing interval: {e}")
        sys.exit(1)

    # Numba List output
    tup_ty = Tuple((int64, int64))
    result_data = List.empty_list(tup_ty) # allocated 参数在新版 numba 有时可选，去掉更安全

    print(f"Running Snapshot Diff Analysis...")
    print(f"Output Interval: {args.interval}")
    print(f"Sampling Rate: {args.sample_ms} ms")
    print(f"Constraint: Top-{args.top_n} depth changes")

    run_analysis(input_files, args, result_data, interval_ns)

    if len(result_data) > 0:
        df = pl.DataFrame(result_data, schema=['Timestamp', 'Update_Count'], orient="row")
        df = df.with_columns(pl.from_epoch('Timestamp', time_unit='ns').alias('Datetime'))
        df = df.select(['Datetime', 'Timestamp', 'Update_Count'])
        
        print("\nResult Head:")
        print(df.head())
        
        if args.output:
            if args.format == 'csv':
                df.write_csv(args.output)
            else:
                df.write_parquet(args.output)
            print(f"Saved to {args.output}")
    else:
        print("No data processed.")

if __name__ == "__main__":
    main()