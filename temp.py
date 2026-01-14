#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
依次解压给定路径列表中的 7z 文件，并把解压出的所有 json 文件解析成 Python 对象返回。
"""

from __future__ import annotations
import os
import json
import shutil
import tempfile
from pathlib import Path
from typing import List, Dict, Any

try:
    import py7zr
except ImportError:
    raise SystemExit("请先 pip install py7zr")


def extract_and_parse_json(seven_z_list: List[str | Path]) -> List[Dict[str, Any]]:
    """
    参数
    ----
    seven_z_list: 7z 文件路径列表

    返回
    ----
    List[Dict[str, Any]]: 所有 JSON 文件解析后的对象列表
    """
    all_json_objs: List[Dict[str, Any]] = []

    for z_file in map(Path, seven_z_list):
        if not z_file.is_file():
            print(f"[WARN] 文件不存在，跳过: {z_file}")
            continue

        # 用临时目录存放解压内容，with 块结束后自动删除
        with tempfile.TemporaryDirectory(prefix=z_file.stem + "_") as tmpdir:
            print(f"[INFO] 正在解压 {z_file.name} -> {tmpdir}")
            try:
                with py7zr.SevenZipFile(z_file, mode="r") as z:
                    z.extractall(path=tmpdir)
            except Exception as e:
                print(f"[ERROR] 解压失败 {z_file}: {e}")
                continue

            # 递归找出所有 json 文件
            json_files = list(Path(tmpdir).rglob("*.json"))
            if not json_files:
                print(f"[WARN] 压缩包 {z_file.name} 中未找到 json 文件")
                continue

            for jf in json_files:
                try:
                    with jf.open(encoding="utf-8") as fj:
                        data = json.load(fj)
                        # 如果你想保留来源信息，可以 data.update({"_src": str(jf)})
                        all_json_objs.append(data)
                except Exception as e:
                    print(f"[ERROR] 解析 JSON 失败 {jf}: {e}")
                    continue

    return all_json_objs


# 把你给出的列表直接粘过来即可
seven_z_files = ['E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-17\\OKX-Trades-BTC-USDT-1742254774519-1742256039693.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742256040113-1742257700450.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742257700911-1742259474415.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742259474787-1742260670845.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742260671270-1742261484929.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742261485533-1742262584143.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742262584574-1742263968137.7z', 'E:\\tmp\\OKX-Trades-BTC-USDT\\OKX-Trades-BTC-USDT-2025-03\\OKX-Trades-BTC-USDT\\2025-03-18\\OKX-Trades-BTC-USDT-1742263968546-1742265375355.7z']

result = extract_and_parse_json(seven_z_files)
print(f"\n[SUMMARY] 共解析出 {len(result)} 个 JSON 对象")

start_ts = '1742064425708'
end_ts = '1742264303801'

for obj in result:
    obj_start_ts = min(map(lambda x: x['data'][0]['ts'], obj['data']))
    obj_end_ts = max(map(lambda x: x['data'][0]['ts'], obj['data']))
    
    if obj_end_ts < start_ts or obj_start_ts > end_ts:
        print("发现不在时间范围内的对象")
        exit(-1)
print("所有对象均在时间范围内")