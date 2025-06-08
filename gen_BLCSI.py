import argparse
from copy import deepcopy
from typing import Tuple, List
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from tqdm import tqdm
import multiprocessing
import os

# from pybacktest.bookcore import BookCore
from cbookcore import BookCore, diff

def blcsi(pf_path: Path, group_index: int, pfs: List[Path]) -> List[Tuple[int, int, int, int]]:
    pf = pq.ParquetFile(pf_path)
    row_group = pf.read_row_group(group_index)
    df = row_group.to_pandas()
    if df.empty:
        # It's a valid case that a row group is empty, so no warning here.
        return []
    # the first row must be snapshot
    if df.iloc[0]['action'] != 'snapshot':
        print(f"warning: {pf_path.name}'s {group_index}th row_group does not have snapshot at the first row, skipping...")
        return []
    instId = df.iloc[0]['arg']['instId']
    bc = BookCore(instId)
    blcsi = []
    for i, row in enumerate(df.itertuples()):
        if i == 0:
            # first row is snapshot
            bc.set_datapoint(row._asdict())
            continue
        ori_bc = deepcopy(bc)
        bc.set_datapoint(row._asdict())
        res_asks, res_bids, res_total = diff(ori_bc, bc)
        blcsi.append((row.ts, res_asks, res_bids, res_total))
    
    # read next row group to gen the last datapoint
    next_df = None
    if group_index + 1 < pf.num_row_groups:
        # Next row group is in the same file
        next_row_group = pf.read_row_group(group_index + 1)
        next_df = next_row_group.to_pandas()
    else:  # It's the last row group, try next file
        try:
            current_file_index = pfs.index(pf_path)
            if current_file_index + 1 < len(pfs):
                next_pf_path = pfs[current_file_index + 1]
                try:
                    next_pf = pq.ParquetFile(next_pf_path)
                    if next_pf.num_row_groups > 0:
                        next_row_group = next_pf.read_row_group(0)
                        next_df = next_row_group.to_pandas()
                except Exception as e:
                    print(f"Warning: Could not read metadata from next file {next_pf_path.name}: {e}")
        except ValueError:
            # This should not happen if tasks are created correctly.
            print(f"Warning: {pf_path} not found in the list of files.")

    if next_df is not None and not next_df.empty:
        # The first row of the next row group is used to calculate the diff.
        # It doesn't have to be a snapshot.
        ori_bc = deepcopy(bc)
        dp = next_df.iloc[0].to_dict()
        bc.set_datapoint(dp)
        res_asks, res_bids, res_total = diff(ori_bc, bc)
        blcsi.append((dp['ts'], res_asks, res_bids, res_total))
    return blcsi

# Wrapper function for multiprocessing that unpacks arguments
def worker(args):
    return blcsi(*args)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate BLCSI indicator from parquet files using multiprocessing.")
    parser.add_argument("--dir", type=str, required=True, help="Directory containing the parquet files.")
    parser.add_argument("--output", type=str, required=True, help="Output directory for the BLCSI indicator.")
    parser.add_argument("--num-processes", type=int, default=os.cpu_count(),
                        help=f"Number of processes to use. Defaults to the number of CPU cores ({os.cpu_count()}).")
    parser.add_argument("--row-group-size", type=int, default=100000, help="Row group size for the output parquet file.")
    
    args = parser.parse_args()
    data_dir = Path(args.dir)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a valid directory.")
        exit(1)
        
    pfs = sorted(list(data_dir.glob("*.parquet")))
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        exit(1)
    else:
        print(f"Found {len(pfs)} parquet files in {data_dir}.")

    # Create a list of tasks (file_path, row_group_index)
    tasks = []
    print("Scanning files to create tasks...")
    for pf_path in tqdm(pfs, desc="Scanning files"):
        try:
            pf = pq.ParquetFile(pf_path)
            for i in range(pf.num_row_groups):
                tasks.append((pf_path, i, pfs))
        except Exception as e:
            print(f"Could not read metadata from {pf_path.name}: {e}")
    
    if not tasks:
        print("No row groups to process. Exiting.")
        exit(0)

    # Try to get instId from the first file for the output filename
    instId = "UNKNOWN"
    if pfs:
        try:
            first_pf = pq.ParquetFile(pfs[0])
            if first_pf.num_row_groups > 0:
                first_rg_df = first_pf.read_row_group(0).to_pandas()
                if not first_rg_df.empty:
                    instId = first_rg_df.iloc[0]['arg']['instId']
        except Exception as e:
            print(f"Could not determine instId from {pfs[0].name}, will use '{instId}'. Error: {e}")

    all_results = []
    with multiprocessing.Pool(processes=args.num_processes) as pool:
        with tqdm(total=len(tasks), desc="Processing row groups") as pbar:
            for result in pool.imap_unordered(worker, tasks):
                if result:
                    all_results.extend(result)
                pbar.update(1)

    if not all_results:
        print("No data was generated. Exiting.")
        exit(0)
        
    # Sort by timestamp
    all_results.sort(key=lambda x: x[0])
    
    # Convert to pyarrow table
    ts, asks, bids, total = zip(*all_results)
    blcsi_dict = {'ts': ts, 'asks': asks, 'bids': bids, 'total': total}
    
    blcsi_table = pa.Table.from_pydict(blcsi_dict)
    
    # dump to parquet
    start_ts = blcsi_dict['ts'][0]
    end_ts = blcsi_dict['ts'][-1]
    prefix = f"OKX-BLCSI-{instId}-{start_ts}-{end_ts}.parquet"
    output_file = output_dir / prefix
    pq.write_table(
        blcsi_table,
        output_file,
        row_group_size=args.row_group_size,
        compression='ZSTD',
        compression_level=2
    )
    print(f"BLCSI indicator saved to {output_file}")