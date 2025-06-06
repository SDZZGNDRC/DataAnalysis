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
    if not matched_dirs:
        print(f"未找到匹配的目录: {src_patterns}", file=sys.stderr)
        sys.exit(1)

    # 收集所有源目录的最后一层目录名
    src_last_dirs = [d.name for d in matched_dirs]

    # 递归查找所有.7z文件
    files = []
    for d in matched_dirs:
        files.extend(list(d.rglob("*.7z")))
    if not files:
        print("未找到任何.7z文件", file=sys.stderr)
        sys.exit(1)

    # 统计7z文件总数
    total_7z_files = len(files)

    # 校验所有目录下的总文件数是否和7z文件总数一致
    total_files = 0
    for d in matched_dirs:
        total_files += sum(1 for _ in d.rglob("*.*") if _.is_file())
    if total_files != total_7z_files:
        print(f"目录下文件总数({total_files})与7z文件总数({total_7z_files})不一致，原目录可能已损坏，包含非7z文件。", file=sys.stderr)
        sys.exit(1)

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
