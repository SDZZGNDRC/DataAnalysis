import argparse
from pathlib import Path
import json
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm

# from pybacktest.bookcore import BookCore
from cbookcore import BookCore

import schema

def update_metadata(new_metadata, p: Path):
    with open(p, 'w') as f:
        json.dump(new_metadata, f, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tail ori parquet files.")
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
    
    # read .metadata as json file if exists
    metadata_file = data_dir / ".metadata"
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {
            "tailed": [],
            "version": "1.0",
        }
    instId = None
    bc = None
    first_snapshot_index = -1
    for pf in pfs:
        df = pq.ParquetFile(pf).read(columns=['action']).to_pandas()
        if df.empty:
            print(f"warning: {pf.name} is empty...")
            continue
        if pf.name in metadata['tailed']:
            print(f"Skipping {pf.name} as it has already been processed.")
            continue
        table = pq.read_table(pf)
        row_group = []
        schema = table.schema
        df = table.to_pandas()
        dps = df.to_dict(orient='records')
        del df  # free memory
        del table
        group_start = 0
        useless_group = 0
        iter_rows = 0
        for i, dp in enumerate(tqdm(dps, total=len(dps), desc=f"Processing {pf.name}", mininterval=1.0)):
            if bc == None:
                if dp['action'] != 'snapshot':
                    continue
                instId = dp['arg']['instId']
                bc = BookCore(instId)
                group_start = i
                useless_group = i
            
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
                row_group.append((group_start, i))
                group_start = i
        row_group.append((group_start, len(dps)))
        writer = pq.ParquetWriter(pf, schema=schema, compression='ZSTD', compression_level=2)
        # first write the useless group
        if useless_group > 0:
            writer.write_batch(pa.RecordBatch.from_pylist(dps[:useless_group], schema=schema), row_group_size=useless_group)
        for start, end in row_group:
            writer.write_batch(pa.RecordBatch.from_pylist(dps[start:end], schema=schema), row_group_size=end-start)
        writer.close()
        metadata['tailed'].append(pf.name)
        update_metadata(metadata, metadata_file)
    print('Finished all...')

