import argparse
from typing import List, Tuple
from pathlib import Path
import json
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm

# from pybacktest.bookcore import BookCore
from cbookcore import BookCore

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
    
    pfs = iter(pfs)
    # process first valid pf
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
        del df
        del table
        writer = pq.ParquetWriter(pf, schema=schema, compression='ZSTD', compression_level=2)
        group_start = 0
        for i, dp in enumerate(tqdm(dps, total=len(dps), desc=f"Processing {pf.name}", mininterval=1.0)):
            if bc == None:
                if dp['action'] != 'snapshot':
                    continue
                instId = dp['arg']['instId']
                bc = BookCore(instId)
                group_start = i
                if i > 0: # useless group
                    writer.write_batch(pa.RecordBatch.from_pylist(dps[:i], schema=schema), row_group_size=i)
            try:
                bc.set_datapoint(dp)
            except Exception as e:
                print(f"Error processing datapoint: {e}")
                print(f"Datapoint: {dp}")
                raise e
            if i - group_start >= interval: # should insert a snapshot
                asks_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.asks]
                bids_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.bids]
                # update the row with the new snapshot data
                dp['data']['asks'] = asks_bl
                dp['data']['bids'] = bids_bl
                dp['action'] = 'snapshot'
                iter_rows = 0
                row_group.append((group_start, i))
                group_start = i
        if group_start < len(dps):
            row_group.append((group_start, len(dps)))
        
        # write the row groups
        for start, end in row_group:
            writer.write_batch(pa.RecordBatch.from_pylist(dps[start:end], schema=schema), row_group_size=end-start)
        writer.close()
        metadata['tailed'].append(pf.name)
        update_metadata(metadata, metadata_file)
        break # only process the first valid file
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
        writer = pq.ParquetWriter(pf, schema=schema, compression='ZSTD', compression_level=2)
        for i, dp in enumerate(tqdm(dps, total=len(dps), desc=f"Processing {pf.name}", mininterval=1.0)):
            try: 
                bc.set_datapoint(dp)
            except Exception as e:
                print(f"Error processing datapoint: {e}")
                print(f"Datapoint: {dp}")
                raise e
            if i == 0: # the first row should be a snapshot
                if dp['action'] != 'snapshot':
                    asks_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.asks]
                    bids_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.bids]
                    # update the row with the new snapshot data
                    dp['data']['asks'] = asks_bl
                    dp['data']['bids'] = bids_bl
                    dp['action'] = 'snapshot'
                    group_start = i
            if i - group_start >= interval: # should insert a snapshot
                asks_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.asks]
                bids_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.bids]
                # update the row with the new snapshot data
                dp['data']['asks'] = asks_bl
                dp['data']['bids'] = bids_bl
                dp['action'] = 'snapshot'
                row_group.append((group_start, i))
                group_start = i
        if group_start < len(dps):
            row_group.append((group_start, len(dps)))
        
        # write the row groups
        for start, end in row_group:
            writer.write_batch(pa.RecordBatch.from_pylist(dps[start:end], schema=schema), row_group_size=end-start)
        writer.close()
        metadata['tailed'].append(pf.name)
        update_metadata(metadata, metadata_file)
    print('Finished all...')

