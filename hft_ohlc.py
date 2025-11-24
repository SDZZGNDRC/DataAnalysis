import argparse
import sys
import os
import glob
import numpy as np
import polars as pl
from numba import njit
from numba.typed import List
from numba.types import Tuple, float64, int64
from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest

def parse_interval(interval_str):
    """
    解析时间间隔字符串，返回纳秒数。
    支持 s (秒), m (分), h (小时), d (天)。
    """
    interval_str = interval_str.strip().lower()
    if not interval_str:
        raise ValueError("Interval string cannot be empty")
        
    unit = interval_str[-1]
    if unit not in ['s', 'm', 'h', 'd']:
        # 如果是纯数字，默认为秒
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

@njit
def generate_ohlc_logic(hbt, out, interval_ns):
    while hbt.elapse(interval_ns) == 0:
        # 获取上一个时间步长内发生的所有市场成交
        trades = hbt.last_trades(0)
        
        if len(trades) > 0:
            # 初始化当前 K 线的统计变量
            open_px = trades[0].px
            high_px = trades[0].px
            low_px = trades[0].px
            close_px = trades[-1].px # 收盘价是最后一笔成交的价格
            volume = 0.0
            
            # 遍历该时段内的每一笔成交，计算 High, Low, Volume
            for trade in trades:
                if trade.px > high_px:
                    high_px = trade.px
                if trade.px < low_px:
                    low_px = trade.px
                volume += trade.qty
            
            # 记录数据: (时间戳, Open, High, Low, Close, Volume)
            out.append((hbt.current_timestamp, open_px, high_px, low_px, close_px, volume))
        
        hbt.clear_last_trades(0)
        
    return True

def main():
    parser = argparse.ArgumentParser(description="Generate OHLC data from HFT backtest npz data.")
    
    parser.add_argument("input_files", nargs='+', help="Path(s) or pattern(s) to the input .npz file(s) containing 'data' array")
    parser.add_argument("--output", "-o", help="Path to save the output file (CSV or Parquet). Defaults to stdout (head only) if not specified.")
    parser.add_argument("--interval", "-i", default="1m", help="OHLC interval (e.g., 1s, 1m, 1h). Default: 1m")
    parser.add_argument("--tick-size", type=float, default=0.1, help="Tick size. Default: 0.1")
    parser.add_argument("--lot-size", type=float, default=0.001, help="Lot size. Default: 0.001")
    parser.add_argument("--latency", type=int, default=10_000_000, help="Order latency in ns. Default: 10,000,000 (10ms)")
    parser.add_argument("--format", choices=['csv', 'parquet'], default='csv', help="Output format if output file is specified. Default: csv")

    args = parser.parse_args()

    # 处理通配符并收集所有文件
    input_files = []
    for pattern in args.input_files:
        # glob.glob 支持通配符，如果 pattern 是具体文件路径也能工作
        # recursive=True 允许 ** 模式
        matched = glob.glob(pattern, recursive=True)
        if not matched:
            # 如果 glob 没找到，可能是具体文件但路径有问题，或者是无效模式
            # 这里尝试直接判断是否存在，以防 glob 行为差异
            if os.path.exists(pattern):
                matched = [pattern]
            else:
                print(f"Warning: No files found matching '{pattern}'")
        input_files.extend(matched)

    # 去重并按名称排序
    input_files = sorted(list(set(input_files)))

    if not input_files:
        print("Error: No valid input files found.")
        sys.exit(1)

    try:
        interval_ns = parse_interval(args.interval)
    except ValueError as e:
        print(f"Error parsing interval: {e}")
        sys.exit(1)

    print(f"Configuration: Interval={args.interval} ({interval_ns} ns), Tick={args.tick_size}, Lot={args.lot_size}")

    # 定义 Numba 列表用于存储结果 (Timestamp, Open, High, Low, Close, Volume)
    # 类型为: (int64, float64, float64, float64, float64, float64)
    tup_ty = Tuple((int64, float64, float64, float64, float64, float64))
    ohlc_data = List.empty_list(tup_ty, allocated=1_000_000)

    print("Starting OHLC generation...")

    for i, file_path in enumerate(input_files):
        try:
            print(f"Processing file {i+1}/{len(input_files)}: {file_path}")
            data = np.load(file_path)['data']
            
            asset = (
                BacktestAsset()
                    .data([data])
                    .linear_asset(1.0)
                    .constant_order_latency(args.latency, args.latency)
                    .risk_adverse_queue_model()
                    .no_partial_fill_exchange()
                    .trading_value_fee_model(0.0008, 0.0010)
                    .tick_size(args.tick_size)
                    .lot_size(args.lot_size)
                    .last_trades_capacity(1_000_000)
                    # 【非常重要】为了生成 OHLC，必须开启 last_trades_capacity
                    # 如果设为 0，hbt.last_trades(0) 将永远为空。
            )

            hbt = HashMapMarketDepthBacktest([asset])
            
            generate_ohlc_logic(hbt, ohlc_data, interval_ns)
            
            # 关闭回测并释放内存
            _ = hbt.close()
            del data
            del asset
            del hbt
            
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            sys.exit(1)

    if len(ohlc_data) > 0:
        # 使用 schema 创建 DataFrame
        schema = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
        df = pl.DataFrame(ohlc_data, schema=schema, orient="row")
        
        # 将纳秒时间戳转换为可读时间
        df = df.with_columns(
            pl.from_epoch('Timestamp', time_unit='ns').alias('Datetime')
        )
        
        # 调整列顺序
        df = df.select(['Datetime', 'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        
        print(f"\nOHLC Data Generated: {len(df)} rows")
        print(df.head(10))
        
        if args.output:
            output_path = args.output
            # 如果用户没有指定扩展名，根据 format 自动添加
            if not output_path.lower().endswith(f".{args.format}"):
                 # 简单的检查，如果路径不包含 .csv 或 .parquet，则追加
                 if '.' not in os.path.basename(output_path):
                     output_path += f".{args.format}"

            if args.format == 'csv':
                df.write_csv(output_path)
            elif args.format == 'parquet':
                df.write_parquet(output_path)
            
            print(f"Data saved to {output_path}")
    else:
        print("Warning: No OHLC data generated. Check if the interval is too small or data is empty.")

if __name__ == "__main__":
    main()