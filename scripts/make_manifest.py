"""生成 npz 清单：将 segments.csv 与已生成的 npz 配对，按段起始日期划分训练/验证/测试。

输出 E:\\tmp\\npz\\manifest.csv，列：seg_index,start_ts,end_ts,duration_hours,start_utc,
npz_path,split。
"""
import csv
from datetime import datetime, timezone
from pathlib import Path

SEG_CSV = Path(r"E:\tmp\segments.csv")
NPZ_DIR = Path(r"E:\tmp\npz")
OUT = NPZ_DIR / "manifest.csv"

rows = list(csv.DictReader(open(SEG_CSV, encoding="utf-8")))
have = {f.stem: f for f in NPZ_DIR.glob("*.npz")}

def split_of(start_ts_ms):
    d = datetime.fromtimestamp(start_ts_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d")
    if d <= "2026-06-20": return "train"
    if d <= "2026-06-25": return "val"
    return "test"

out_rows=[]
for r in rows:
    if r["has_snapshot"]!="1": continue
    if float(r["duration_hours"])<1.0: continue
    idx=r["seg_index"]; st=r["start_ts"]
    stem=f"seg_{idx}_{st}"
    if stem not in have: continue
    out_rows.append({
        "seg_index": idx,
        "start_ts": st,
        "end_ts": r["end_ts"],
        "duration_hours": f"{float(r['duration_hours']):.4f}",
        "start_utc": r["start_datetime"],
        "npz_path": str(have[stem]),
        "split": split_of(int(st)),
    })

with open(OUT,"w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
    w.writeheader(); w.writerows(out_rows)

from collections import Counter
c=Counter(x["split"] for x in out_rows)
h={s:sum(float(x["duration_hours"]) for x in out_rows if x["split"]==s) for s in c}
print(f"manifest: {OUT} | {len(out_rows)} 段")
for s in ["train","val","test"]:
    print(f"  {s}: {c[s]} 段, {h[s]:.1f}h ({h[s]/24:.2f}天)")