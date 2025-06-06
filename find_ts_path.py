import json
import os

def find_ts_path(data, current_path=""):
    """Recursively finds the path to the 'ts' key in a nested dictionary/list."""
    paths = []
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{current_path}.{key}" if current_path else key
            if key == "ts":
                paths.append(new_path)
            paths.extend(find_ts_path(value, new_path))
    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_path = f"{current_path}[{index}]"
            paths.extend(find_ts_path(item, new_path))
    return paths

schema_dir = "DataSchema/schema/jsonschema/"
schema_files = [f for f in os.listdir(schema_dir) if f.endswith(".json")]

ts_paths = {}
consistent = True

for file_name in schema_files:
    file_path = os.path.join(schema_dir, file_name)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
            paths = find_ts_path(schema_data)
            ts_paths[file_name] = paths
            if not paths:
                print(f"Warning: 'ts' not found in {file_name}")
    except Exception as e:
        print(f"Error reading or processing {file_name}: {e}")
        consistent = False # Consider inconsistent if a file cannot be processed

# Print all found paths
print("Found 'ts' paths in each file:")
for file_name, paths in ts_paths.items():
    print(f"{file_name}: \t\t{paths}")

# Check for consistency
consistent = True # Reset consistent flag for the check
if ts_paths:
    first_file = list(ts_paths.keys())[0]
    expected_paths = ts_paths[first_file]

    for file_name, paths in ts_paths.items():
        if paths != expected_paths:
            print(f"\nInconsistent paths found:")
            print(f"  {first_file}: {expected_paths}")
            print(f"  {file_name}: {paths}")
            consistent = False
            break # No need to check further if inconsistency found

    if consistent:
        print("\nAll 'ts' paths are consistent across the schemas.")
        if expected_paths:
            print(f"Consistent path(s): {expected_paths}")
        else:
            print("\nNo 'ts' field found in any schema.")
else:
    print("\nNo schema files found to process.")