from calendar import c
import pathlib
from pathlib import Path
import sys
import pyarrow.parquet as pq
import pyarrow as pa
from DataFile.data_name import DataName
from typing import List
import argparse
import pandas as pd
import heapq




def iter_sorted_rows(parquet_files, sort_key="ts", batch_size=100_000):
    """
    以 batch 方式迭代多个 parquet 文件，按 sort_key 排序输出行。
    """
    import pyarrow.parquet as pq
    import pandas as pd
    def row_iter(pf):
        for batch in pq.ParquetFile(pf).iter_batches(batch_size=batch_size):
            df = pa.Table.from_batches([batch]).to_pandas()
            for row in df.itertuples(index=False, name=None):
                yield row
    # 获取列名
    columns = pq.ParquetFile(parquet_files[0]).schema_arrow.names
    key_idx = columns.index(sort_key)
    # 构造每个文件的迭代器
    iters = [row_iter(pf) for pf in parquet_files]
    # heapq.merge 按 key 排序合并
    merged = heapq.merge(*iters, key=lambda row: row[key_idx])
    return merged, columns




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resort Parquet files and remove overlaps.")
    parser.add_argument("--dir", type=str, help="Directory containing input Parquet files")
    parser.add_argument('--chunk', type=int, default=1_000_000, help="The number of row in each generated parquet file.")
    
    # start
    args = parser.parse_args()
    dest_dir = Path(args.dir)
    chunk_size = args.chunk
    if not dest_dir.is_dir():
        print(f"Directory {dest_dir} does not exist.")
        sys.exit(1)
    dest_dir = dest_dir.resolve()
    
    pfs = list(dest_dir.glob("*.parquet"))
    if not pfs:
        print(f"No Parquet files found in {dest_dir}.")
        sys.exit(1)
    
    dns_pfs = {DataName(pf.name): pf for pf in pfs}
    data_names = list(dns_pfs.keys())
    overlapped = DataName.group_overlapped(data_names)
    print(f"Found {len(overlapped)} groups of overlapping files.")
    for group in overlapped:
        if len(group) == 1:
            continue  # Skip single files, no overlap to resolve
        print(f"Processing group with {len(group)} files: {[dn.name for dn in group]}")
        group_files = [dns_pfs[dn] for dn in group]
        merged_rows, columns = iter_sorted_rows(group_files, sort_key="ts", batch_size=100_000)
        chunk = []
        total_rows = 0
        chunk_idx = 0
        for row in merged_rows:
            chunk.append(row)
            if len(chunk) >= chunk_size:
                df_chunk = pd.DataFrame(chunk, columns=columns)
                first_ts = df_chunk["ts"].iloc[0]
                last_ts = df_chunk["ts"].iloc[-1]
                new_filename = f"{group[0].prefix}-{first_ts}-{last_ts}.parquet"
                new_pf = dest_dir / new_filename
                chunk_table = pa.Table.from_pandas(df_chunk)
                pq.write_table(chunk_table, new_pf)
                total_rows += len(df_chunk)
                chunk = []
                chunk_idx += 1
        # 写最后一个 chunk
        if chunk:
            df_chunk = pd.DataFrame(chunk, columns=columns)
            first_ts = df_chunk["ts"].iloc[0]
            last_ts = df_chunk["ts"].iloc[-1]
            new_filename = f"{group[0].prefix}-{first_ts}-{last_ts}.parquet"
            new_pf = dest_dir / new_filename
            chunk_table = pa.Table.from_pandas(df_chunk)
            pq.write_table(chunk_table, new_pf)
            total_rows += len(df_chunk)
        # remove the original files
        for dn in group:
            pf = dns_pfs[dn]
            pf.unlink()




