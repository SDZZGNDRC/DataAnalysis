import argparse
from pathlib import Path
import gc
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm

from pybacktest.bookcore import BookCore


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert snapshot into parquet files.")
    parser.add_argument("--dir", type=str, help="Directory containing the parquet files.")
    parser.add_argument("--interval", type=int, default=100_000, help="Interval for inserting snapshots (default: 100000).")
    args = parser.parse_args()

    data_dir = Path(args.dir)
    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a valid directory.")
        exit(1)
    interval = args.interval

    pfs = list(data_dir.glob("*.parquet"))
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        exit(1)
    else:
        print(f"Found {len(pfs)} parquet files in {data_dir}.")
    
    instId = None
    bc = None
    first_snapshot_index = -1
    iter_rows = 0
    for pf in pfs:
        df = pq.ParquetFile(pf).read(columns=['action']).to_pandas()
        if df.empty:
            print(f"warning: {pf.name} is empty...")
            continue
        # Check if need to insert snapshot.
        snapshot_index = df[df['action'] == 'snapshot'].index
        tmp = np.append(snapshot_index.values, df.index.values[-1])
        if (not snapshot_index.empty) and all(tmp[i] + interval >=tmp[i + 1] for i in range(len(tmp) - 1)):
            print(f"info: {pf.name} does not need to insert snapshot, skipping...")
            continue  # no need to insert snapshot
        
        df = pq.ParquetFile(pf).read().to_pandas()
        dps = df.to_dict(orient='records')
        del df  # free memory
        gc.collect()
        for dp in tqdm(dps, total=len(dps), desc=f"Processing {pf.name}"):
            if bc == None:
                if dp['action'] != 'snapshot':
                    continue
                instId = dp['arg']['instId']
                bc = BookCore(instId)
            
            try: 
                bc.set_datapoint(dp)
            except Exception as e:
                print(f"Error processing datapoint: {e}")
                print(f"Datapoint: {dp}")
                raise e
            iter_rows += 1

            if iter_rows >= interval: # should insert a snapshot
                asks_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.asks]
                bids_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.bids]
                # update the row with the new snapshot data
                dp['data']['asks'] = asks_bl
                dp['data']['bids'] = bids_bl
                dp['action'] = 'snapshot'
                iter_rows = 0
        table = pa.Table.from_pylist(dps)
        pq.write_table(table, pf, compression='ZSTD', compression_level=3)
    print('Finished all...')
