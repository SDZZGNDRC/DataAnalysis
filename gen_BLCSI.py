import argparse
from copy import deepcopy
from typing import Tuple
from pathlib import Path
import gc
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm

# from pybacktest.bookcore import BookCore
from cbookcore import BookCore


def diff(bookcore_1: BookCore, bookcore_2: BookCore) -> Tuple[int, int, int]:
    '''
    Calculate the number of different booklevel.
    '''
    res_asks = 0
    res_bids = 0
    # Asks
    L = min(bookcore_1.depth_asks, bookcore_2.depth_asks)
    asks_1, asks_2 = bookcore_1.asks[:L], bookcore_2.asks[:L]
    for i in range(L):
        if not asks_1[i].true_eq(asks_2[i]):
            res_asks += 1
    res_asks += max(bookcore_1.depth_asks, bookcore_2.depth_asks) - L
    
    # Bids
    L = min(bookcore_1.depth_bids, bookcore_2.depth_bids)
    bids_1, bids_2 = bookcore_1.bids[:L], bookcore_2.bids[:L]
    for i in range(L):
        if not bids_1[i].true_eq(bids_2[i]):
            res_bids += 1
    res_bids += max(bookcore_1.depth_bids, bookcore_2.depth_bids) - L
    
    return (res_asks, res_bids, res_asks + res_bids)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate BLCSI indicator from parquet files.")
    parser.add_argument("--dir", type=str, help="Directory containing the parquet files.")
    parser.add_argument("--output", type=str, help="Output file for the BLCSI indicator.")
    
    args = parser.parse_args()
    data_dir = Path(args.dir)
    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a valid directory.")
        exit(1)
    pfs = list(data_dir.glob("*.parquet"))
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        exit(1)
    else:
        print(f"Found {len(pfs)} parquet files in {data_dir}.")
    output_dir = Path(args.output)
    
    bc = None
    blcsi = {'ts': [], 'asks': [], 'bids': [], 'total': []}
    instId = None
    for pf in pfs:
        df = pq.ParquetFile(pf).read(columns=['action']).to_pandas()
        if df.empty:
            print(f"warning: {pf.name} is empty...")
            continue
        if df[df['action'] == 'snapshot'].empty:
            print(f"warning: {pf.name} does not contain snapshot, skipping...")
            continue
        df = pq.ParquetFile(pf).read().to_pandas()
        for row in tqdm(df.itertuples(), total=len(df), desc=f"Processing {pf.name}"):
            if bc == None:
                if row.action != 'snapshot':
                    continue
                instId = row.arg['instId']
                bc = BookCore(instId)
                bc.set_datapoint(row._asdict())
            else:
                ori_bc = deepcopy(bc)
                bc.set_datapoint(row._asdict())
                blcsi['ts'].append(row.ts)
                res_asks, res_bids, res_total = diff(ori_bc, bc)
                blcsi['asks'].append(res_asks)
                blcsi['bids'].append(res_bids)
                blcsi['total'].append(res_total)
        
    # dump to parquet
    if blcsi['ts']:
        blcsi_table = pa.Table.from_pydict(blcsi)
        start_ts = blcsi['ts'][0]
        end_ts = blcsi['ts'][-1]
        prefix = f"OKX-BLCSI-{instId}-{start_ts}-{end_ts}.parquet"
        output_file = output_dir / prefix
        pq.write_table(blcsi_table, output_file)
        print(f"BLCSI indicator saved to {output_file}")
    else:
        print("No data to save for BLCSI indicator.")