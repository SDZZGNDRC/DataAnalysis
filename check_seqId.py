import pathlib
from pathlib import Path
import sys
import pyarrow.parquet as pq
from DataFile.data_name import DataName
from typing import List
import dask.dataframe as dd
import argparse
import json


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check sequence IDs in parquet files.")
    parser.add_argument("--dir", type=str, help="Directory containing the parquet files.")
    args = parser.parse_args()

    data_dir = Path(args.dir)
    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a valid directory.")
        sys.exit(1)

    pfs = list(data_dir.glob("*.parquet"))
    if not pfs:
        print(f"No parquet files found in {data_dir}.")
        sys.exit(1)
    
    ddf = dd.read_parquet(pfs, engine="pyarrow", columns=["action", "data"])
    # 先将data列从字符串解析为字典
    ddf["data_dict"] = ddf["data"].apply(lambda x: json.loads(x) if isinstance(x, str) else x, meta=("data_dict", "object"))
    ddf["prevSeqId"] = ddf["data_dict"].apply(lambda x: x.get("prevSeqId", None), meta=("prevSeqId", "object"))
    ddf["seqId"] = ddf["data_dict"].apply(lambda x: x.get("seqId", None), meta=("seqId", "object"))
    # for rows with action != "snapshot", check if the prevSeqId of the row is equal to the seqId of the previous row
    def check_seqId(df):
        errors = []
        for i in range(1, len(df)):
            if df.iloc[i]["action"] != "snapshot":
                if df.iloc[i]["prevSeqId"] != df.iloc[i - 1]["seqId"]:
                    errors.append(f"Error in row {i}: prevSeqId {df.iloc[i]['prevSeqId']} does not match seqId {df.iloc[i - 1]['seqId']}")
        return errors
    errors = ddf.map_partitions(check_seqId, meta=(None, "object")).compute()
    errors = [error for sublist in errors for error in sublist]  # Flatten the list of errors
    if errors:
        print("Errors found:")
        for error in errors:
            print(error)
    else:
        print("No errors found.")
    sys.exit(0 if not errors else 1)




