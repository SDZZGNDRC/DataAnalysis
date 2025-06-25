import argparse
import sys
import json
import re
from pathlib import Path
import shutil
from tqdm import tqdm
from datetime import datetime, timezone
from multiprocessing import Pool, Manager

from DataFile.data_name import DataName

def process_file(args):
    file, dst_path, error_event = args
    if error_event.is_set():
        return  # 已有错误发生，直接返回
    try:
        if not file.is_file():
            raise RuntimeError(f"不是文件: {file}")
        dn = DataName(file.name)
        category = dn.category
        prefix = dn.prefix
        # 转换 start_timestamp 为 yyyy-mm-dd
        ts = dn.start_timestamp
        if ts > 1e15:
            ts = ts / 1000_000
        else:
            ts = ts / 1000
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        day_str = dt.strftime("%Y-%m-%d")
        target_dir = dst_path / category / prefix / day_str
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / file.name
        shutil.copy2(file, target_file)
    except Exception as e:
        error_event.set()
        raise RuntimeError(f"处理文件出错: {file}, 错误: {e}")

def main():
    parser = argparse.ArgumentParser(description="批量归类复制7z文件")
    parser.add_argument("src", type=str, help="源目录(支持正则或逗号分隔多个目录名)")
    parser.add_argument("dst", type=str, help="目标目录, 按`category/prefix/yyyy-mm-dd`归类")
    parser.add_argument("--workers", type=int, default=2, help="并发进程数，默认为2")
    args = parser.parse_args()

    dst_path = Path(args.dst)
    workers = args.workers

    # 解析src参数，支持绝对路径+正则或逗号分隔
    src_patterns = [pat.strip() for pat in args.src.split(",")]
    matched_dirs = []
    for pat in src_patterns:
        # 判断是否为绝对路径+正则
        p = Path(pat)
        if p.is_absolute():
            parent_dir = p.parent
            pattern = p.name
        else:
            # 相对路径或只有正则
            parent_dir = Path(".")
            pattern = pat
        if not parent_dir.exists() or not parent_dir.is_dir():
            continue
        all_dirs = [d for d in parent_dir.iterdir() if d.is_dir()]
        try:
            regex = re.compile(pattern)
        except Exception:
            regex = re.compile(re.escape(pattern))
        matched_dirs.extend([d for d in all_dirs if regex.fullmatch(d.name)])
    matched_dirs = list(set(matched_dirs))
    # 递归查找所有文件
    print("正在收集和校验文件列表...")
    all_disk_files = []
    for d in tqdm(matched_dirs, desc="扫描目录", unit="dir"):
        all_disk_files.extend(list(d.rglob("*.*")))
    
    # 过滤出真正的文件，排除目录
    all_disk_files = [f for f in all_disk_files if f.is_file()]

    # 找出所有.7z文件和它们的stem
    files_to_process = [f for f in all_disk_files if f.suffix.lower() == '.7z']
    seven_zip_stems = {f.stem for f in files_to_process}
    
    if not files_to_process:
        print("未找到任何.7z文件", file=sys.stderr)
        sys.exit(1)

    # 查找无效文件：既不是.7z，也不是与.7z配对的.json
    invalid_files = []
    for f in all_disk_files:
        is_7z = f.suffix.lower() == '.7z'
        is_paired_json = f.suffix.lower() == '.json' and f.stem in seven_zip_stems
        if not is_7z and not is_paired_json:
            invalid_files.append(f)

    if invalid_files:
        print(f"校验失败！发现 {len(invalid_files)} 个无效文件（非7z文件，也非配对的json文件）。", file=sys.stderr)
        print("原目录可能已损坏或包含非预期文件。无效文件列表如下：", file=sys.stderr)
        for f in invalid_files:
            print(f" - {f}", file=sys.stderr)
        sys.exit(1)

    # 如果校验通过，files_to_process 就是我们要处理的.7z文件列表
    files = files_to_process
    total_7z_files = len(files)
    print(f"校验通过，共找到 {total_7z_files} 个有效的.7z文件进行处理。")

    # 收集所有源目录的最后一层目录名
    src_last_dirs = [d.name for d in matched_dirs]

    manager = Manager()
    error_event = manager.Event()
    pool = Pool(processes=workers)
    tasks = [(file, dst_path, error_event) for file in files]
    try:
        for _ in tqdm(pool.imap_unordered(process_file, tasks), total=len(tasks), desc="处理进度", unit="file"):
            if error_event.is_set():
                break
    except Exception as e:
        error_event.set()
        pool.terminate()
        pool.join()
        print(f"处理出错: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        pool.close()
        pool.join()
    if error_event.is_set():
        print("处理过程中发生错误，已终止。", file=sys.stderr)
        sys.exit(1)

    # 全部成功后生成.metadata文件
    metadata = {
        "version": "1.0",
        "total_7z_files": total_7z_files,
        "src_last_dirs": src_last_dirs,
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "src_patterns": src_patterns,
    }
    metadata_path = dst_path / ".metadata"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    print("全部处理完成。")

if __name__ == "__main__":
    main()
