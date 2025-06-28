import argparse
import sys
import numpy as np
from typing import Dict, List, Tuple
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm
import multiprocessing
import os
from functools import partial
import ts2rg


def worker(trades_rg_idx: Tuple[int, int], trades_pfs: List[Path], books_pfs: List[Path], out_dir: Path):
    trades_df = pq.ParquetFile(trades_pfs[trades_rg_idx[0]]).read_row_group(trades_rg_idx[1]).to_pandas()
    # get the last row in the last row_group
    books_rg_idxs = ts2rg.asop_ts2rg(books_pfs, trades_df['ts'].iloc[0], trades_df['ts'].iloc[-1])
    if len(books_rg_idxs) == 0:
        print(f"Warning: No matching book row groups found for trades row group {trades_rg_idx}.")
        return
    books_dfs = []
    for books_rg_idx in books_rg_idxs:
        try:
            books_df = pq.ParquetFile(books_pfs[books_rg_idx[0]]).read_row_group(books_rg_idx[1]).to_pandas()
            # ensure ascending order of timestamp
            if not books_df['ts'].is_monotonic_increasing:
                print(f"Warning: books_df is not sorted by timestamp in ascending order for row group {books_rg_idx}; parquet file {books_pfs[books_rg_idx[0]].name}.")
                return
            books_dfs.append(books_df)
        except Exception as e:
            print(f"Warning: Failed to read row group {books_rg_idx} from {books_pfs[books_rg_idx[0]].name}: {e}")
            return
    if not books_dfs:
        print(f"Warning: No valid book data found for trades row group {trades_rg_idx}.")
        return
    # Concatenate all book dataframes
    books_df = pd.concat(books_dfs, ignore_index=True)
    ori_len = len(trades_df)

    # start to asop operation
    trades_df['timestamp'] = pd.to_datetime(trades_df['ts'], unit='ms')
    books_df['book_timestamp'] = pd.to_datetime(books_df['ts'], unit='ms')
    trades_df = trades_df.set_index('timestamp') # .sort_index()
    books_df = books_df.set_index('book_timestamp') # .sort_index()
    # 判断是否按照时间戳升序排列, 如果不是, 则报错
    if not trades_df.index.is_monotonic_increasing:
        print(f"Error: trades_df is not sorted by timestamp in ascending order. Please check the input data.")
        return
    merged_df = pd.merge_asof(
        trades_df,
        books_df,
        left_index=True,
        right_index=True,
        direction='backward',
        suffixes=('', '_book')
    )
    merged_df.dropna(inplace=True)
    if len(merged_df) != ori_len:
        print(f"Warning: Merged dataframe length {len(merged_df)} does not match original trades length {ori_len}.")
    
    # Save the merged dataframe to parquet
    output_path = out_dir / f"OKX-TradesBooks-{merged_df['ts'].iloc[0]}-{merged_df['ts'].iloc[-1]}.parquet"
    merged_table = pa.Table.from_pandas(merged_df)
    pq.write_table(merged_table, output_path,compression='ZSTD', compression_level=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Nth BookLevel features from parquet files.")
    parser.add_argument("--trades-dir", type=str, required=True, help="Directory containing the trades parquet files.")
    parser.add_argument("--nth-bl-dir", type=str, required=True, help="Directory containing the Nth-BL parquet files.")
    
    parser.add_argument("--output", type=str, required=True, help="Output directory for the generated parquet file.")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(),
                        help=f"Number of processes to use. Defaults to the number of CPU cores ({os.cpu_count()}).")
    parser.add_argument("--row-group-size", type=int, default=100_000, help="Row group size for the output parquet file.")
    
    args = parser.parse_args()
    trades_dir = Path(args.trades_dir)
    nth_bl_dir = Path(args.nth_bl_dir)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if directory is not empty
    if any(output_dir.iterdir()):
        print(f"Error: Output directory '{output_dir}' is not empty")
        sys.exit(1)
    
    trades_pfs = sorted(trades_dir.glob("*.parquet"))
    nth_bl_pfs = sorted(nth_bl_dir.glob("*.parquet"))
    
    if not trades_dir or not nth_bl_pfs:
        print(f"Error: trades_dir and nth_bl_pfs must not empty")
        sys.exit(1)
    else:
        print(f"info: Found {len(trades_pfs)} parquet files in {trades_dir}.")
        print(f"info: Found {len(nth_bl_pfs)} parquet files in {nth_bl_dir}.")
    
    # ensure not overlapped between parquet files
    tss = []
    for i, pf in enumerate(nth_bl_pfs):
        t = pf.stem.split('-')
        if tss and not (tss[-1] < t[-2]):
            print(f"Error: There overlap occurred between {i}th parquet file {pf} and the last one.")
            sys.exit(1)
        tss.extend([t[-2], t[-1]])
    tss = []
    for i, pf in enumerate(trades_pfs):
        t = pf.stem.split('-')
        if tss and not (tss[-1] < t[-2]):
            print(f"Error: There overlap occurred between {i}th parquet file {pf} and the last one.")
            sys.exit(1)
        tss.extend([t[-2], t[-1]])
    
    # ----------------------------------------------------------------
    # asop
    # ----------------------------------------------------------------
    # 遍历所有trades parquet 文件, 生成 trades_rg_idx
    print("Generating tasks...")
    all_trades_rg_idxs = []
    for i, pf_path in enumerate(tqdm(trades_pfs, desc="Scanning trades files")):
        try:
            pf = pq.ParquetFile(pf_path)
            for j in range(pf.num_row_groups):
                all_trades_rg_idxs.append((i, j))
        except Exception as e:
            print(f"Warning: Failed to read metadata from {pf_path.name}: {e}")

    # 使用多进程分配给worker,并使用tqdm展示处理进度
    print(f"Distributing {len(all_trades_rg_idxs)} tasks to {args.num_processes} processes...")
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        task = partial(worker, trades_pfs=trades_pfs, books_pfs=nth_bl_pfs, out_dir=output_dir)
        for _ in tqdm(pool.imap_unordered(task, all_trades_rg_idxs), total=len(all_trades_rg_idxs), desc="Processing row groups"):
            pass

    print("All tasks completed.")
