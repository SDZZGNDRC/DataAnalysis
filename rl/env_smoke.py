r"""Quick env sanity test: random policy ~200 steps."""
import json
import sys
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rl.gym_env import BTCUSDSwapMapsEnv, load_manifest

contract = json.load(open(PROJECT_ROOT / "contracts" / "btc_usdt_swap.json"))
segs = load_manifest(r"E:\tmp\npz\manifest.csv", split="train")
print("train segs:", len(segs))

env = BTCUSDSwapMapsEnv(segs[:4], contract, seed=0, max_seg_hours=2)
rng = np.random.default_rng(0)
obs, info = env.reset(seed=1)
print("reset obs shape", obs.shape, "info", info)
total_r = 0.0; n = 0
done = False
while not done and n < 600:
    a = int(rng.integers(0, 5))
    obs, r, terminated, truncated, info = env.step(a)
    total_r += r; n += 1
    done = terminated or truncated
    if n % 100 == 0:
        print(f"step {n:4d} a={a} r={r:+.6f} cumR={total_r:+.4f} eq={info['equity']:+.3f} pos={info['position']:+.4f} fee={info['fee']:+.4f}")
print("DONE", "steps", n, "cumReward", round(total_r, 4), "final_eq", round(info['equity'], 4))
env.close()