import os
import tarfile
import argparse

def count_files_in_tar(tar_path):
    count_7z = 0
    count_other = 0
    try:
        with tarfile.open(tar_path, 'r') as tar:
            for member in tar.getmembers():
                if member.isfile():
                    if member.name.lower().endswith('.7z'):
                        count_7z += 1
                    else:
                        count_other += 1
    except Exception as e:
        print(f"无法读取 {tar_path}: {e}")
    return count_7z, count_other

def main(root_dir):
    total_7z = 0
    total_other = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith('.tar'):
                tar_path = os.path.join(dirpath, filename)
                c7z, cother = count_files_in_tar(tar_path)
                total_7z += c7z
                total_other += cother
    print(f"7z 文件数量: {total_7z}")
    print(f"非 7z 文件数量: {total_other}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="统计指定目录下 tar 文件中 7z 和非 7z 文件数量")
    parser.add_argument("root_dir", help="根目录路径")
    args = parser.parse_args()
    main(args.root_dir)