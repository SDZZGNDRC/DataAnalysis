from data_name import DataName

import json
from pathlib import Path
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from schema.Books import books_schema

def aggregate_books(paths: list[Path], output_path: Path) -> None:
    '''
    Aggregate books data files. 
    
    Given a list of data files, this function will aggregate them into a single file.
    * The output json file's schema is defined in `schema/Books.py`, and is the same as the schema of the input data files.
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
        if data_name.category != "Books":
            raise ValueError(f"Data name {data_name.name} must have the category 'Books'")

    # 3. Ensure output directory exists
    output_dir = output_path.parent
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    # 4. Read and validate input files
    start_times = []
    end_times = []
    all_data = []
    
    for path in paths:
        try:
            with open(path, 'r') as f:
                file_data = json.load(f)
            
            validate(instance=file_data, schema=books_schema)
            
            start_times.append(file_data["elapsedTime"][0])
            end_times.append(file_data["elapsedTime"][1])
            
            all_data.extend(file_data["data"])
        except (json.JSONDecodeError, ValidationError, KeyError, IndexError) as e:
            raise ValueError(f"Invalid file {path}: {e}")

    # 5. Check consistency of 'action' and 'arg' across all data items
    if all_data:
        first_action = all_data[0]["action"]
        first_arg = all_data[0]["arg"]
        
        for item in all_data[1:]:
            if item["action"] != first_action:
                raise ValueError(f"Action mismatch: Expected '{first_action}', found '{item['action']}'")
            
            current_arg = item["arg"]
            if current_arg.keys() != first_arg.keys():
                raise ValueError(f"Arg keys mismatch: Expected {first_arg.keys()}, found {current_arg.keys()}")
            
            for key in first_arg:
                if current_arg[key] != first_arg[key]:
                    raise ValueError(f"Arg value mismatch for '{key}': Expected '{first_arg[key]}', found '{current_arg[key]}'")

    # 6. Merge and sort data entries by 'ts'
    merged_entries = []
    for item in all_data:
        merged_entries.extend(item["data"])

    # Sort by 'ts' (numeric comparison)
    try:
        merged_entries.sort(key=lambda x: int(x["ts"]))
    except KeyError:
        raise ValueError("Missing 'ts' field in data entry")
    
    # Check for duplicate 'ts'
    seen_ts = set()
    for entry in merged_entries:
        ts = entry["ts"]
        if ts in seen_ts:
            raise ValueError(f"Duplicate timestamp detected: {ts}")
        seen_ts.add(ts)

    # 7. Build aggregated data structure
    aggregated_data = {
        "elapsedTime": [min(start_times), max(end_times)],
        "data": [{
            "arg": all_data[0]["arg"] if all_data else {},
            "action": all_data[0]["action"] if all_data else "",
            "data": merged_entries
        }]
    }

    # 8. Validate and write output
    validate(instance=aggregated_data, schema=books_schema)
    
    with open(output_path, 'w') as f:
        json.dump(aggregated_data, f)