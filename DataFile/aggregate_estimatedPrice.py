from data_name import DataName

import json
from pathlib import Path
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from typing import List, Dict, Any
import sys

from schema.schema import get_schema

def aggregate_estimated_prices(paths: list[Path], output_path: Path) -> None:
    '''
    Aggregate estimated price data files. 
    
    Given a list of data files, this function will aggregate them into a single file.
    * The output json file's schema is defined in `schema/EstimatedPrice.json`, and is the same as the schema of the input data files.
    
    1. Check for timestamp ordering and uniqueness within each file
    2. Check for non-overlapping timestamp ranges between files
    3. Merge all data items and sort by timestamp
    4. Set the merged elapsedTime to the min/max of all input elapsedTimes
    5. Ensure all files have identical arg values
    6. Validate the output against the schema
    '''
    # 1. Validate input paths
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"File {path} does not exist")
    
    if len(paths) != len(set(paths)):
        raise ValueError("There are repeated paths")
    
    # 2. Validate data names
    data_names = []
    for path in paths:
        try:
            data_names.append(DataName(path.name, "json"))
        except ValueError:
            raise ValueError(f"File {path} is not a valid data name")
    
    if not data_names:
        raise ValueError("No paths provided")
    
    prefix = data_names[0].prefix
    for data_name in data_names:
        if data_name.prefix != prefix:
            raise ValueError(f"Data name {data_name.name} must have the prefix {prefix}")
        if data_name.category != "EstimatedPrice":
            raise ValueError(f"Data name {data_name.name} must have the category 'EstimatedPrice'")

    # 3. Ensure output directory exists
    output_dir = output_path.parent
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    # 4. Read and validate input files
    file_data_list = []
    
    for path in paths:
        try:
            with open(path, 'r') as f:
                file_data = json.load(f)
            
            validate(instance=file_data, schema=get_schema("EstimatedPrice"))
            file_data_list.append(file_data)
        except (json.JSONDecodeError, ValidationError) as e:
            raise ValueError(f"Invalid file {path}: {e}")

    # 5. Extract and validate data from all files
    all_root_data = []
    elapsed_times = []
    first_arg = None
    
    for i, root in enumerate(file_data_list):
        # Extract elapsedTime
        elapsed_times.append(root["elapsedTime"])
        
        # Check arg consistency within this file and across all files
        for item in root["data"]:
            
            if first_arg is None:
                first_arg = item["arg"]
            else:
                # Check if current arg matches first_arg
                if item["arg"].keys() != first_arg.keys():
                    raise ValueError(f"Arg keys mismatch in file {paths[i]}")
                
                for key in first_arg:
                    if item["arg"][key] != first_arg[key]:
                        raise ValueError(f"Arg value mismatch for '{key}' in file {paths[i]}")
        
        # Process data items and validate timestamps
        ts_values = []
        processed_data_items = []
        
        for item in root["data"]:
            for data_entry in item["data"]:
                
                ts = data_entry["ts"]
                ts_int = int(ts)
                ts_values.append(ts_int)
                
                processed_data_items.append({
                    "ts_int": ts_int,
                    "item": item
                })
        
        # Check for duplicate timestamps within this file
        if len(ts_values) != len(set(ts_values)):
            print(f"Error: Duplicate timestamps detected in file {paths[i]}")
            sys.exit(1)
        
        # Check if timestamps are sorted in ascending order
        if ts_values != sorted(ts_values):
            print(f"Error: Timestamps not in ascending order in file {paths[i]}")
            sys.exit(1)
        
        # Store processed data with file index for later range checking
        all_root_data.append({
            "file_index": i,
            "min_ts": min(ts_values) if ts_values else None,
            "max_ts": max(ts_values) if ts_values else None,
            "items": processed_data_items
        })
    
    # 6. Check for timestamp range overlaps between files
    # Sort roots by min_ts
    all_root_data.sort(key=lambda x: x["min_ts"] if x["min_ts"] is not None else float('-inf'))
    
    # Check for overlaps
    for i in range(1, len(all_root_data)):
        prev_max = all_root_data[i-1]["max_ts"]
        curr_min = all_root_data[i]["min_ts"]
        
        if prev_max is not None and curr_min is not None and prev_max >= curr_min:
            prev_file = paths[all_root_data[i-1]["file_index"]]
            curr_file = paths[all_root_data[i]["file_index"]]
            print(f"Error: Timestamp range overlap detected between files {prev_file} and {curr_file}")
            print(f"File {prev_file} has max_ts={prev_max}, File {curr_file} has min_ts={curr_min}")
            sys.exit(1)
    
    # 7. Merge all data items and sort by timestamp
    merged_data_items = []
    for root_data in all_root_data:
        merged_data_items.extend(root_data["items"])
    
    # Sort all items by timestamp
    merged_data_items.sort(key=lambda x: x["ts_int"])
    
    # 8. Build the final merged structure
    min_elapsed_time = min([et[0] for et in elapsed_times])
    max_elapsed_time = max([et[1] for et in elapsed_times])
    
    merged_root = {
        "elapsedTime": [min_elapsed_time, max_elapsed_time],
        "data": [item["item"] for item in merged_data_items]
    }
    
    # 9. Validate the merged data against the schema
    try:
        validate(instance=merged_root, schema=get_schema("EstimatedPrice"))
    except ValidationError as e:
        print(f"Error: Merged data does not conform to the schema: {e}")
        sys.exit(1)
    
    # 10. Write the merged data to the output file
    with open(output_path, 'w') as f:
        json.dump(merged_root, f)
    
    print(f"Successfully merged {len(paths)} files into {output_path}")
