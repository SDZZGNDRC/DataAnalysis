import argparse
import os
import glob
import csv
import multiprocessing
from pathlib import Path
import py7zr
from datetime import datetime
import tempfile
import orjson

def parse_args():
    parser = argparse.ArgumentParser(description='Analyze OKX Trades 7z files for ts min/max.')
    parser.add_argument('root_dir', help='Root directory to search for OKX-Trades-*.7z files')
    parser.add_argument('--output', '-o', default='trades_ts_summary.csv', help='Output CSV file path')
    return parser.parse_args()

def read_jf(jf: str):
    with open(jf, 'rb') as f:
        content = f.read()
    MAX_RETRY = 10000
    for _ in range(MAX_RETRY):
        try:
            try:
                root = orjson.loads(content)
                return root['data']
            except orjson.JSONDecodeError as e:
                if "unexpected character" in str(e) and "char" in str(e):
                    error_pos_str = str(e).split("char ")[-1].strip(")")
                    try:
                        error_pos = int(error_pos_str)
                        if chr(content[error_pos]) == ',':
                            fixed_content = content[:error_pos] + content[error_pos+1:]
                        else:
                            raise Exception(f'unknown error: {content[error_pos-10:error_pos+10]}')
                        content = fixed_content
                        continue
                    except (ValueError, orjson.JSONDecodeError) as fix_error:
                        print(f"无法修复文件 {jf} 中的JSON: {str(fix_error)}")
                        print(f'跳过文件 {jf}')
                        raise fix_error
                else:
                    raise Exception(f'read_jf: {e}')
        except Exception as e:
            print(f"read_jf: 处理文件 {jf} 时出错: {str(e)}")
            raise Exception(f'read_jf: {e}')
    raise Exception(f'修复重试次数超过最大值{MAX_RETRY}')

def find_7z_files(root_dir):
    pattern = os.path.join(root_dir, '**', 'OKX-Trades-*.7z')
    return glob.glob(pattern, recursive=True)

def process_7z_file(file_path):
    try:
        abs_path = os.path.abspath(file_path)
        filename = os.path.basename(file_path)
        with tempfile.TemporaryDirectory() as temp_dir:
            with py7zr.SevenZipFile(file_path, mode='r') as archive:
                archive.extractall(temp_dir)
            json_candidates = list(Path(temp_dir).rglob('*.json'))
            if not json_candidates:
                raise FileNotFoundError(f"No JSON file found in archive {file_path}")
            if len(json_candidates) > 1:
                raise RuntimeError(
                    f"发现多个JSON文件于压缩包 {file_path}: {', '.join(str(p.relative_to(temp_dir)) for p in json_candidates)}"
                )
            json_path = json_candidates[0]
            data_entries = read_jf(str(json_path))
            ts_values = []
            for item in data_entries:
                if isinstance(item, dict) and 'data' in item and item['data'] is not None:
                    for trade in item['data']:
                        if isinstance(trade, dict) and 'ts' in trade and trade['ts'] is not None:
                            try:
                                ts_values.append(int(trade['ts']))
                            except (TypeError, ValueError):
                                continue
            if not ts_values:
                raise ValueError(f"No ts values found in {file_path}")
            min_ts = min(ts_values)
            max_ts = max(ts_values)
            def ts_to_date(ts):
                return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
            min_date = ts_to_date(min_ts)
            max_date = ts_to_date(max_ts)
            return filename, min_ts, min_date, max_ts, max_date, abs_path
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main():
    args = parse_args()
    files = find_7z_files(args.root_dir)
    if not files:
        print("No OKX-Trades-*.7z files found.")
        return
    print(f"Found {len(files)} files to process.")
    with multiprocessing.Pool() as pool:
        results = pool.map(process_7z_file, files)
    # Filter out None results
    valid_results = [r for r in results if r is not None]
    # Write to CSV
    with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['filename', 'min_ts', 'min_ts_date', 'max_ts', 'max_ts_date', 'abs_path'])
        for row in valid_results:
            writer.writerow(row)
    print(f"Results written to {args.output}")

if __name__ == '__main__':
    main()
