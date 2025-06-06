import argparse
import os
import tarfile
import shutil
import multiprocessing
from tqdm import tqdm


def find_prefix_dirs(root):
    """
    遍历root，返回所有prefix目录的绝对路径
    """
    prefix_dirs = []
    for category in os.listdir(root):
        category_path = os.path.join(root, category)
        if not os.path.isdir(category_path):
            continue
        for prefix in os.listdir(category_path):
            prefix_path = os.path.join(category_path, prefix)
            if os.path.isdir(prefix_path):
                prefix_dirs.append(prefix_path)
    return prefix_dirs


def pack_and_remove_prefix(prefix_path):
    """
    将prefix目录打包为tar文件，然后删除原目录
    """
    parent_dir = os.path.dirname(prefix_path)
    prefix_name = os.path.basename(prefix_path)
    tar_path = os.path.join(parent_dir, f"{prefix_name}.tar")
    # 打包
    with tarfile.open(tar_path, "w") as tar:
        tar.add(prefix_path, arcname=prefix_name)
    # 删除原目录
    shutil.rmtree(prefix_path)
    # print(f"打包并删除: {prefix_path} -> {tar_path}")


def main():
    parser = argparse.ArgumentParser(description="将灰度数据集的prefix目录打包为tar并删除原目录")
    parser.add_argument("root", help="灰度数据集根目录路径")
    parser.add_argument("--workers", type=int, default=None, help="并发进程数，默认CPU核心数")
    args = parser.parse_args()
    root = args.root
    if not os.path.isdir(root):
        print(f"指定的根目录不存在: {root}")
        return
    prefix_dirs = find_prefix_dirs(root)
    print(f"共找到{len(prefix_dirs)}个prefix目录")
    workers = args.workers or multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=workers) as pool:
        for _ in tqdm(pool.imap_unordered(pack_and_remove_prefix, prefix_dirs), total=len(prefix_dirs), desc="打包进度"):
            pass
    print("全部处理完成！")

if __name__ == "__main__":
    main()
