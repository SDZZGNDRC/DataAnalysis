import argparse
from copy import deepcopy
from typing import List, Tuple
from pathlib import Path
import json
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm
import shutil

from cbookcore import BookCore

from DataFile import data_name

def update_metadata(new_metadata, p: Path):
    with open(p, 'w') as f:
        json.dump(new_metadata, f, indent=4)

if __name__ == "__main__":
    # FIXME: 还需要检查`first_pf2`和`last_pf1`的`prevSeqId`和`seqId`是否相匹配
    parser = argparse.ArgumentParser(description="Concat two tailed parquet datasets")
    parser.add_argument("--dir1", type=str, required=True, help="First source directory (e.g., March).")
    parser.add_argument("--dir2", type=str, required=True, help="Second source directory (e.g., April).")
    parser.add_argument("--dest", type=str, required=True, help="Destination directory for concatenated files.")
    args = parser.parse_args()

    dir1 = Path(args.dir1)
    dir2 = Path(args.dir2)
    dest_dir = Path(args.dest)

    # Validate directories
    if not dir1.is_dir() or not dir2.is_dir():
        print(f"Error: {dir1} or {dir2} is not a valid directory.")
        exit(1)
    
    dest_dir.mkdir(parents=True, exist_ok=True)

    pfs1 = sorted(list(dir1.glob("*.parquet")))
    pfs2 = sorted(list(dir2.glob("*.parquet")))

    if not pfs1 or not pfs2:
        print("Error: Source directories must contain parquet files.")
        exit(1)
    
    print(f"Found {len(pfs1)} files in {dir1.name} and {len(pfs2)} files in {dir2.name}.")

    last_pf1 = pfs1[-1]
    first_pf2 = pfs2[0]

    if data_name.DataName(last_pf1.name).end_timestamp >= data_name.DataName(first_pf2.name).start_timestamp:
        print(f"Error: time overlapped -> {last_pf1.name}; {first_pf2.name}")
        exit(-1)

    # --- Step 1: Process the last row group of the last file in dir1 to get the final book state ---
    print(f"Reading final state from: {last_pf1.name}")
    pq_file1 = pq.ParquetFile(last_pf1)
    last_rg_idx = pq_file1.num_row_groups - 1
    
    if last_rg_idx < 0:
        print(f"Error: The last file in dir1 {last_pf1.name} has no row groups.")
        exit(1)

    last_rg_pylist = pq_file1.read_row_group(last_rg_idx).to_pylist()

    bc = None
    instId = None
    for dp in tqdm(last_rg_pylist, desc="Updating book from last group of dir1"):
        if bc is None:
            if dp['action'] == 'snapshot':
                instId = dp['arg']['instId']
                bc = BookCore(instId)
            else:
                # This should not happen if tail_books.py is correct
                continue
        bc.set_datapoint(dp)

    if bc is None:
        print(f"Error: Could not find a snapshot in the last row group of {last_pf1.name}.")
        exit(1)

    # --- Step 2: Modify the first file of dir2 with the new snapshot ---
    print(f"Modifying first file of dir2: {first_pf2.name}")
    
    table2 = pq.read_table(first_pf2)
    schema = table2.schema
    dps2 = table2.to_pylist()

    # Generate the new snapshot from the final state of BookCore
    asks_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.asks]
    bids_bl = [[str(bl.price), str(bl.amount), '0', str(bl.count)] for bl in bc.bids]
    
    # Create a new snapshot datapoint, inheriting metadata from the row to be replaced
    new_snapshot_dp = deepcopy(dps2[0])
    new_snapshot_dp['action'] = 'snapshot'
    new_snapshot_dp['data']['asks'] = asks_bl
    new_snapshot_dp['data']['bids'] = bids_bl
    
    # Replace the first row
    dps2[0] = new_snapshot_dp

    # Write the modified data to a new file in the destination directory
    dest_pf_path = dest_dir / first_pf2.name
    print(f"Writing modified file to {dest_pf_path}")
    
    # Preserve original row group structure
    pq_file2 = pq.ParquetFile(first_pf2)
    with pq.ParquetWriter(dest_pf_path, schema=schema, compression='ZSTD', compression_level=2) as writer:
        num_rows_written = 0
        for i in range(pq_file2.num_row_groups):
            rg_meta = pq_file2.metadata.row_group(i)
            rg_size = rg_meta.num_rows
            
            end_offset = num_rows_written + rg_size
            batch_data = dps2[num_rows_written:end_offset]
            
            record_batch = pa.RecordBatch.from_pylist(batch_data, schema=schema)
            writer.write_batch(record_batch)
            num_rows_written = end_offset

    # --- Step 3: Copy all files from dir1 and remaining files from dir2 ---
    print(f"Copying {len(pfs1)} files from {dir1.name} to {dest_dir.name}")
    for pf in tqdm(pfs1, desc=f"Copying from {dir1.name}"):
        shutil.copy(pf, dest_dir / pf.name)

    print(f"Copying {len(pfs2) - 1} remaining files from {dir2.name} to {dest_dir.name}")
    for pf in tqdm(pfs2[1:], desc=f"Copying from {dir2.name}"):
        shutil.copy(pf, dest_dir / pf.name)

    # --- Step 4: Merge .metadata files ---
    meta1_path = dir1 / ".metadata"
    meta2_path = dir2 / ".metadata"
    dest_meta_path = dest_dir / ".metadata"

    meta1 = {}
    meta2 = {}

    if meta1_path.exists():
        with open(meta1_path, 'r') as f:
            meta1 = json.load(f)
    
    if meta2_path.exists():
        with open(meta2_path, 'r') as f:
            meta2 = json.load(f)

    new_meta = {
        "tailed": meta1.get('tailed', []) + meta2.get('tailed', []),
        "version": meta2.get('version', '1.0') # Use version from the latest dataset
    }

    print(f"Writing merged .metadata to {dest_meta_path}")
    update_metadata(new_meta, dest_meta_path)

    print('Finished all.')
